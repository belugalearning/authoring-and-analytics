var crypto = require('crypto')
  , zlib = require('zlib')
  , fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')

var batchFileNameRE = /^batch-([0-9a-f]{8})-([0-9a-f]{32})$/i
  , uuidRE = /^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$/i
  , interSep = String.fromCharCode(parseInt('91', 16)) // unicode "private use one" UTF-8: 0xC291 - represented as 0x91 in javascript string
  , intraSep = String.fromCharCode(parseInt('92', 16)) // unicode "private use two" UTF-8: 0xC292 - represented as 0x92 in javascript string 
  , couchServerURI
  , genLoggingDbName
  , genLoggingDbURI
  , genDesignDoc = 'logging-design-doc'
  , userDesignDoc = 'user-related-views'
  , paMetaDesignDoc = 'pa-meta-design-doc'
  , pendingLogBatchesDir = __dirname + '/pending-batches'
  , errorLogBatchesDir = __dirname + '/error-batches'
  , errorsWriteStream = fs.createWriteStream(__dirname + '/errors.log', { flags: 'a' })
  , userDbDirectoryDoc
  , userDbDirectoryDocURI

module.exports = function(config) {
  couchServerURI = config.couchServerURI.replace(/\/$/, '')
  genLoggingDbName = config.appWebService.loggingService.genLoggingDbName
  genLoggingDbURI = util.format('%s/%s', couchServerURI, genLoggingDbName)
  userDbDirectoryDocURI = util.format('%s/user-db-directory', genLoggingDbURI)

  //updateDesignDocs(genLoggingDbURI, [paMetaDesignDoc])
  init()
  return uploadBatchRequestHandler
}

var initFailing = false
function init() {
  if (!initFailing) console.log('log batch processor initialising...')
  request.get(userDbDirectoryDocURI, function(e,r,b) {
    if (!r || r.statusCode !== 200) {
      if (!initFailing) console.error('log batch processor: error during init retrieving doc with id user-db-directory - keep retrying...\n\t%s\n\t%d', e || b, r && r.statusCode || null)
      initFailing = true
      setTimeout(init, 3000)
      return
    }
    userDbDirectoryDoc = JSON.parse(b)
    initFailing = false
    console.log('log batch processor: got user db directory')
    runDaemon()
  })
}

// LOG BATCHES ARRIVE HERE: http request handler handler
function uploadBatchRequestHandler(req, res) {
  var batchFilePath = req.files.batchData.path
  var md5 = crypto.createHash('md5')

  fs.createReadStream(batchFilePath)
    .on('data', function(d) { md5.update(d) })
    .on('end', function() {
      var hash = md5.digest('hex')
      res.send(hash, 200)
    })

  fs.readFile(batchFilePath, function(e, buffer) {
    var lead0s = new Array(9).join('0')
      , dWords = []

    for (var i=0; i<5; i++) {
      dWords[i] = (lead0s + buffer.readUInt32BE(4*i).toString(16)).match(/([0-9a-f]{8})$/i)[1]
    }

    var batchDate = dWords[0]
      , batchUUID = dWords.slice(1).join('')

    zlib.unzip(buffer.slice(20), function(e, inflatedBuffer) {
      fs.writeFile( util.format('%s/batch-%s-%s', pendingLogBatchesDir, batchDate, batchUUID), inflatedBuffer, function(e) {
        if (e) console.log('failed to write log file. Will need to do something better than this very soon!!!') // TODO: Handle
      })
    })
  })
}

// LOG BATCHES PROCESSING MANAGED FROM HERE
function runDaemon() {
  processPendingBatches(function fn(numProcessed, eConnRefused) {
    //console.log('processed %d batches at %s', numProcessed, new Date().toString())
    if (eConnRefused === true) {
      init()
    } else if (numProcessed > 0) {
      processPendingBatches(fn)
    } else {
      setTimeout((function() { processPendingBatches(fn) }), 5000)
    }
  })
}

// called from runDaemon
function processPendingBatches(callback) {
  fs.readdir(pendingLogBatchesDir, function(e, dirFiles) {
    var batches = _.filter(dirFiles, function(f) { return batchFileNameRE.test(f) })
      , numOutstanding = batches.length
      , eConnRefused = false

    if (!numOutstanding) {
      callback(batches.length)
      return
    }

    var onProcessed = function onProcessed() {
      if (!--numOutstanding) callback(batches.length, eConnRefused)
    }

    _.each(batches, function(batch) {
      processBatch(batch, function(errorObj) {
        var batchPath = util.format('%s/%s', pendingLogBatchesDir, batch)

        if (!errorObj) {
          // rm batch file (it's saved as attachment on database)
          fs.unlink(batchPath, onProcessed)
        } else {
          errorsWriteStream.write(util.format('[%s]\tBatch File: %s\tErrorObj: "%s"\n\n', new Date().toString(), batch, JSON.stringify(errorObj,null,2)))

          if (errorObj.response && errorObj.response.e && /ECONNREFUSED/.test(errorObj.response.e)) {
            eConnRefused = true
            onProcessed()
          } else {
            // move batch to error dir
            var newPath = util.format('%s/%s', errorLogBatchesDir, batch)
            fs.rename(batchPath, newPath, onProcessed)
          }
        }
      })
    })
  })
}

// called from processPendingBatches()
function processBatch(batch, pbCallback) {
  var path = util.format('%s/%s', pendingLogBatchesDir, batch)

  // batchDate and batchUUID are in filename
    , match = batch.match(batchFileNameRE)
    , batchDate = parseInt(match[1], 16)
    , batchUUID = match[2]

  // batchProcessDate is now
    , batchProcessDate = Math.round(new Date().getTime() / 1000)

  // deviceId required on batchMetaDoc - will (should!) already be on every doc in batch
    , deviceId

  // read batch file...
  fs.readFile(path, 'utf8', function(e, str) {
    if (e) {
      pbCallback({ message: 'error reading batch file', e: e, path: path })
      return
    }

    // split the batch file into records AND set deviceId var above
    var records = _.map( str.split(interSep) , function(r) {
      var parts = r.split(intraSep)
        , uuid = parts[0]
        , jsonString = parts[1]
        , doc
        , err

      if (2 != parts.length) err = 'INVALID_SPLIT_LENGTH'

      if (!err && !uuidRE.test(uuid)) err = 'INVALID_UUID'

      if (!err) {
        try {
          doc = JSON.parse(jsonString)
          doc.batchDate = batchDate
          doc.batchUUID = batchUUID
          doc.batchProcessDate = batchProcessDate

          if (typeof deviceId != 'string' && typeof doc.device == 'string') deviceId = doc.device

          if (doc.type == 'ProblemAttempt' && Object.prototype.toString.call(doc.events) == '[object Array]') {
            for (var i=0, event;  event=doc.events[i]; i++) {
              if (event.eventType == 'PROBLEM_ATTEMPT_START') {
                var pdef = event.additionalData && event.additionalData.pdef
                if (pdef) {
                  doc._attachments = {
                    "static-generated-pdef.plist": {
                      "content-type": "application\/xml"
                      , data: new Buffer(pdef).toString('base64')
                    }
                  }
                }
                break
              }
            }
          }
        } catch (e) {
          err = 'INVALID_JSON'
        }
      }
      return { error:err, uuid:uuid, doc:doc, record:r }
    })

    // divide records into valid / invalid, pluck valid record doc ids
    var valid = _.filter(records, function(r) { return !r.error })
      , uuids = _.pluck(valid, 'uuid')
      , invalid = _.difference(records, valid)

    
    // group records by database
    var recordsByDbDestination = { general:[] }
    valid.forEach(function(r) {
      var ur = r.doc.user
      if (typeof ur == 'string') {
        var urDbRecords = recordsByDbDestination[ur]
        if (!urDbRecords) urDbRecords = recordsByDbDestination[ur] = []
        urDbRecords.push(r)
      } else {
        recordsByDbDestination.general.push(r)
      }
    })
    var userIds = Object.keys(recordsByDbDestination).slice(1) // first key is for 'general' non-user docs

    // onError() ensures pbCallback only called once
    var onError = function(o) {
      if (!arguments.callee.sentError) {
        arguments.callee.sentError = true
        pbCallback(o)
      }
    }

    // assignRecordActions() sets action (insert/update/ignore) and error (if applicable) on on each record in recs param
    var assignRecordActions = function(dbURI, recs, callback) {
      if (!recs.length) {
        callback([])
        return
      }

      var uuids = _.pluck(recs, 'uuid')
      var req = { uri: encodeURI(util.format('%s/_all_docs?include_docs=true&keys=%s', dbURI, JSON.stringify(uuids))) }
      var docsForInsertOrUpdate = []

      request(req, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc !== 200) {
          onError({
            message: 'Error retrieving database records'
            , request: req
            , response: { e: e, sc: r && r.statusCode, b: b }
          })
          return
        }

        var rows = JSON.parse(b).rows

        for (var i=0, record, row;  (record = recs[i]) && (row = rows[i]);  i++) {
          if (row.error) {
            if (row.error == 'not_found') { // TODO: will this error text will be retained across couch versions? Is there a better way of checking for not found?
              record.action = 'INSERT'
              docsForInsertOrUpdate.push(record.doc)
            } else {
              record.action = 'IGNORE_ON_UNHANDLED_ROW_RETRIVAL_ERROR'
              record.rowRetrievalError = row.error
            }
          } else {
            if (batchDate > row.doc.batchDate) {
              record.action = 'UPDATE'
              record.doc._rev = row.doc._rev
              docsForInsertOrUpdate.push(record.doc)
            } else {
              record.action = 'IGNORE_SUPERSEDED_VERSION'
            }
          }
        }
        callback(docsForInsertOrUpdate)
      })
    }

    // ensureUrDbExists() called per user in batch - will create user's log database and add it to the directory if it does not yet exist
    var ensureUrDbExists = function(urId, callback) {
      if (getUrDbURI(urId)) {
        callback()
      } else {
        var req = { uri:util.format('%s/ur-logging-%s', couchServerURI, urId.toLowerCase()), method:'PUT' }
        request(req, function(e,r,b) {
          var sc = r && r.statusCode
          // race-condition possibility - db could have been created between ensureUrDbExists() being called and this callback => allow 412
          if (sc == 412) {
            // make sure ur in directory
            if (!getUrDbURI(urId)) {
              userDbDirectoryDoc.dbs[urId] = req.uri
              writeUpdatedUserDbDirectory()
            }
            callback()
          } else if (sc == 201) {
            userDbDirectoryDoc.dbs[urId] = req.uri
            writeUpdatedUserDbDirectory()
            updateDesignDocs(req.uri, [genDesignDoc, userDesignDoc, paMetaDesignDoc], callback)
          } else {
            onError({
              message: 'Error creating user database'
              , request: req
              , response: { e: e, sc: r && r.statusCode, b: b }
            })
          }
        })
      }
    }

    // writeGenDbDocs() writes documents to the general logging db - a batch meta doc, and docs in recordsByDbDestination.general (i.e. device docs that don't relate to users)
    var writeGenDbDocs = function(docs, callback) {
      var req = {
        method:'POST'
        , uri: genLoggingDbURI + '/_bulk_docs'
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify({ docs: docs })
      }

      request(req, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc != 201) {
          onError({
            message: 'Error recording log batch gen docs'
            , request: req
            , response: { e: e, sc: r && r.statusCode, b: b }
          })
        }
        else callback()
      })
    }

    // writeUserDocs() called per user in the batch
    var writeUserDocs = function(urId, docs, callback) {
      if (!docs.length) {
        callback()
        return
      }

      var req = {
        method:'POST'
        , uri: getUrDbURI(urId) + '/_bulk_docs'
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify({ docs:docs })
      }

      request(req, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc !== 201) {
          onError({
            message: 'Error recording user log docs'
            , request: req
            , response: { e: e, sc: r && r.statusCode, b: b }
          })
        } else {
          callback()
        }
      })
    }

    var batchMetaDoc = {
      _id: batchUUID
      , type: 'LogBatch'
      , batchDate: batchDate
      , batchProcessDate: batchProcessDate
      , device: deviceId
      , records: records
      , _attachments: {
        "raw-log": {
          "content_type": "text\/plain; charset=utf-8"
          , data: new Buffer(str).toString('base64')
        }
      }
    }

    var processGenDocs = function() {
      assignRecordActions(genLoggingDbURI, recordsByDbDestination.general, function(docsForInsertOrUpdate) {
        writeGenDbDocs([ batchMetaDoc ].concat(docsForInsertOrUpdate), pbCallback)
      })
    }

    var numUserDbsOutstanding = userIds.length
    if (!numUserDbsOutstanding) {
      processGenDocs()
    } else {
      userIds.forEach(function(urId) {
        ensureUrDbExists(urId, function() {
          assignRecordActions(getUrDbURI(urId), recordsByDbDestination[urId], function(docsForInsertOrUpdate) {
            writeUserDocs(urId, docsForInsertOrUpdate, function() {
              if (!--numUserDbsOutstanding) processGenDocs()
            })
          })
        })
      })
    }
  })
}

// called from processBatch() when new user encountered and log db created
function writeUpdatedUserDbDirectory() {
  var fn = arguments.callee
    , cache = fn.cache

  if (!cache) cache = fn.cache = { isUpdating:false, pendingUpdates:false }

  if (cache.isUpdating) {
    cache.pendingUpdates = true
    return
  }

  cache.isUpdating = true
  cache.pendingUpdates = false

  request({
    method:'PUT'
    , uri: userDbDirectoryDocURI
    , body: JSON.stringify(userDbDirectoryDoc)
  }, function(e,r,b) {
    cache.isUpdating = false

    var sc = r && r.statusCode || 0
    if (sc !== 201) {
      // TODO: handle error! for the moment just trying again in hope of something better.
      fn()
    } else {
      userDbDirectoryDoc._rev = JSON.parse(b).rev
      if (cache.pendingUpdates) fn()
    }
  })
}

function getUrDbURI(urId) {
  return userDbDirectoryDoc.dbs[urId]
}

// design docs
function updateDesignDocs(dbURI, docsToUpdate, callback) {
  if (Object.prototype.toString.call(docsToUpdate) !== '[object Array]') {
    callback = docsToUpdate
    docsToUpdate = [genDesignDoc, userDesignDoc]
  }
  
  var toUpdate = 0
  ~docsToUpdate.indexOf(genDesignDoc) && toUpdate++
  ~docsToUpdate.indexOf(userDesignDoc) && toUpdate++
  ~docsToUpdate.indexOf(paMetaDesignDoc) && toUpdate++
  if (!toUpdate) {
    callback('no docs to update', 400)
    return
  }

  var ddBodies = {}
  var sentError = false

  var updateDesignDoc = function(docName, body, callback) {
    var uri = util.format('%s/_design/%s', dbURI, docName)

    request.get(uri, function(e,r,b) {
      var error

      if (!r) {
        //TODO handle
        var error = 'error connecting to database ' + uri
        callback && callback(error, 500)
        return
      }
      
      if (!~[200,404].indexOf(r.statusCode)) {
        var error = util.format('error retrieving design doc: "%s". Database Error: "%s"  Status Code:%d', uri, e, r.statusCode)
        callback && callback(error, r.statusCode || 500)
        return
      }

      if (r.statusCode == 200) body._rev = JSON.parse(b)._rev

      request({
        method:'PUT'
        , uri: uri
        , body: JSON.stringify(body)
      }, function(e,r,b) {
        callback && callback(e, r.statusCode)
      })
    })
  }

  ddBodies[genDesignDoc] = {
    views: {
      'app-errors': {
        map: (function (doc) {
          if (Object.prototype.toString.call(doc.events) == '[object Array]') {
            for (var i=0, len=doc.events.length; i<len; i++) {
              var event = doc.events[i]
                , date = new Date(event.date * 1000)
                , dateString = date.toJSON().replace(/^(.{10})T(.{8}).*/, '$1 $2')
                , errorType

              if (event.eventType == 'APP_ERROR') {
                if (event.additionalData && event.additionalData.type) errorType = event.additionalData.type
                emit(dateString, errorType)
              }
            }
          }
        }).toString()
      }
      , 'by-batchdate-type': {
        map: (function (doc) {
          if (doc.batchDate && doc.type) {
            var date = new Date(doc.batchDate * 1000)
              , dateString = date.toJSON().replace(/^(.{10})T(.{8}).*/, '$1 $2')
            emit([dateString, doc.type], null)
          }
        }).toString()
      }
      , 'by-type': {
        map:(function (doc) { emit(doc.type) }).toString()
        , reduce: (function(keys, values, rereduce) {
          if (!rereduce) return values.length
          else return sum(values)
        }).toString()
      }
      , 'events-by-date': {
        map: (function (doc) {
          if (Object.prototype.toString.call(doc.events) == '[object Array]') {
            for (var i=0, len=doc.events.length; i<len; i++) {
              var event = doc.events[i]
                , date = new Date(event.date * 1000)
                , dateString = date.toJSON().replace(/^(.{10})T(.{8}).*/, '$1 $2')
              emit([dateString, event.eventType])
            }
          }
        }).toString()
      }
      , 'events-by-type': {
        map: (function (doc) {
          var event, date, dateString

          if (Object.prototype.toString.call(doc.events) == '[object Array]') {
            for (var i=0, len=doc.events.length; i<len; i++) {
              event = doc.events[i]
              date = new Date(event.date * 1000)
              dateString = date.toJSON().replace(/^(.{10})T(.{8}).*/, '$1 $2')
              if (event && event.eventType) emit(event.eventType, dateString)
            }
          }
        }).toString()
        , reduce: (function(keys, values, rereduce) {
          if (!rereduce) return values.length
          else return sum(values)
        }).toString()
      }
      , 'logbatch-records-by-action': {
        map: (function (doc) {
          if (doc.type == 'LogBatch') {
            if (Object.prototype.toString.call(doc.records) != '[object Array]') {
              emit([ "#ERROR: value for key 'records' is not an array", doc._id ], doc)
              return
            }
            doc.records.forEach(function(r) {
              var key0
              if (r.error) {
                key0 = '#ERROR: ' + r.error 
              } else if (!r.action) {
                key0 = "#ERROR: LogBatch record does not contain key: 'action'"
              } else {
                key0 = r.action
              }
              emit([ key0, doc._id ], r)
            })
          }
        }).toString()
        , reduce: (function(keys, values, rereduce) {
          if (!rereduce) return values.length
          else return sum(values)
        }).toString()
      }
    }
  }

  ddBodies[userDesignDoc] = {
    views: {
      'log-batches-by-process-date': {
        map:(function(doc) {
          if (doc.batchUUID && doc.batchProcessDate) emit(doc.batchProcessDate, doc.batchUUID)
        }).toString()
        , reduce:(function(keys, values, rereduce) {
          return values[0]
        }).toString()
      }    
      , 'log-batches-by-device-process-date': {
        map:(function(doc) {
          if (doc.batchUUID && doc.batchProcessDate) emit([doc.device, doc.batchProcessDate], doc.batchUUID)
        }).toString()
        , reduce:(function(keys, values, rereduce) {
          return values[0]
        }).toString()
      }    
      , 'episodes-by-batch-process-date': {
        map: (function(doc) {
          if (doc.type == 'Episode') emit(doc.batchProcessDate)
        }).toString()
      }
      , 'problemattempt-events-by-user-date': {
        map: (function(doc) {
          var type = doc.type
          if (doc.type != 'ProblemAttempt' && doc.type != 'ProblemAttemptGOPoll' && (doc.type != 'TouchLog' || doc.context != 'ProblemAttempt')) return

          var formatDate = function(secs) {
            if (typeof secs != 'number' || isNaN(secs)) return null 
              var date = new Date(secs*1000)
            return date.toJSON()
          }    

          var ur = doc.user
          , pa = type == 'ProblemAttempt' ? doc._id : doc.problemAttempt
          , paStart = formatDate( type == 'ProblemAttempt' ? doc.events[0].date : doc.problemAttemptStartDate ) 

          if (type == 'ProblemAttempt') {
            Object.prototype.toString.call(doc.events) == '[object Array]' && doc.events.forEach(function(event) {
              emit([ur, paStart, pa, formatDate(event.date), type], event.eventType)
            })   
          } else if (type == 'ProblemAttemptGOPoll') {
            Object.prototype.toString.call(doc.deltas) == '[object Array]' && doc.deltas.forEach(function(event) {
              emit([ur, paStart, pa, formatDate(event.date), type], event.delta)
            })   
          } else if (type == 'TouchLog') {
            Object.prototype.toString.call(doc.touches) == '[object Array]' && doc.touches.forEach(function(touch) {
              Object.prototype.toString.call(touch.events) == '[object Array]' && touch.events.forEach(function(event) {
                emit([ur, paStart, pa, formatDate(event.date), type], { index:touch.index, phase:event.phase, x:event.x, y:event.y })
              })   
            })   
          }    
        }).toString()
      }
      , 'feature-key-encounters-by-batch-process-date': {
        map: (function(doc) {
          if (doc.type == 'UserSession' && Object.prototype.toString.call(doc.events) == '[object Array]') {
            doc.events.forEach(function(event) {
              if (event.eventType == 'USER_ENCOUNTER_FEATURE_KEY' && event.additionalData) {
                emit([doc.batchProcessDate, event.additionalData.key, event.additionalData.date])
              }
            })
          }
        }).toString()
      }
    }
  }

  ddBodies[paMetaDesignDoc] = {
    views: {
      'problemattempt-summary-by-user-date': {
        map:(function(doc) {
          var startEv
            , pdef
            , isMeta
            , isNP
            , descMatch
            , toolKeyMatch

          if (doc.type == 'ProblemAttempt' &&
              Object.prototype.toString.call(doc.events) == '[object Array]' &&
              (startEv = doc.events[0]) &&
              startEv.additionalData &&
              (pdef = startEv.additionalData.pdef.replace(/>\s+</g, '><')))
          {
            isMeta = pdef.indexOf('<key>META_QUESTION</key>') != -1
            isNP = pdef.indexOf('<key>NUMBER_PICKER</key>') != -1

            descMatch = pdef.match(new RegExp('<key>' + (isNP ? 'NUMBER_PICKER_DESCRIPTION' : (isMeta ? 'META_QUESTION_TITLE' : 'PROBLEM_DESCRIPTION')) + '</key><string>(.*?)</string>'))
            toolKeyMatch = pdef.match(/<key>TOOL_KEY<\/key><string>(.*?)<\/string>/)
            
            emit(
              [doc.user, startEv.date]
              , { user: doc.user
                , problemAttempt: doc._id
                , problem: doc.problemId
                , problemRev: doc.problemRev
                , startDate: new Date(startEv.date * 1000).toJSON()
                , isMetaQuestion: isMeta
                , isNumberPicker: isNP
                , description: descMatch && descMatch[1] || ''
                , tool: toolKeyMatch && toolKeyMatch[1] || ''
              })
          }
        }).toString()
      }
    }
  }

  docsToUpdate.forEach(function(ddName) {
    updateDesignDoc(ddName, ddBodies[ddName], function(e, statusCode) {
      if (!sentError) {
        if (e) sentError = true
        if (e || !--toUpdate) callback && callback(e, statusCode)
      }
    })
  })
}
