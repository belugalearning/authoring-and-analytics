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
  , mainLoggingDbName
  , mainLoggingDbURI
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
  mainLoggingDbName = config.appWebService.loggingService.databaseName
  mainLoggingDbURI = util.format('%s/%s', couchServerURI, mainLoggingDbName)

  //updateDesignDocs(mainLoggingDbURI, [paMetaDesignDoc])

  userDbDirectoryDocURI = util.format('%s/user-db-directory', mainLoggingDbURI)

  request.get(userDbDirectoryDocURI, function(e,r,b) {
    if (!r || r.statusCode !== 200) {
      console.error('error retrieving doc with id user-db-directory - cannot process log batches as a result.\n\t%s\n\t%d', e || b, r && r.statusCode || null)
      return
    }

    userDbDirectoryDoc = JSON.parse(b)

    // initiate process batches loop
    processPendingBatches(function(numProcessed) {
      var f = arguments.callee

      //console.log('processed %d batches at %s', numProcessed, new Date().toString())
      if (numProcessed > 0) {
        processPendingBatches(f)
      } else {
        setTimeout((function() { processPendingBatches(f) }), 5000)
      }
    })
  })

  return uploadBatchRequestHandler
}

// http request handler handler
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

// daemon
function processPendingBatches(callback) {
  fs.readdir(pendingLogBatchesDir, function(e, dirFiles) {
    var batches = _.filter(dirFiles, function(f) { return batchFileNameRE.test(f) })
      , numOutstanding = batches.length

    if (!numOutstanding) {
      callback(batches.length)
      return
    }

    _.each(batches, function(batch) {
      processBatch(batch, function(e) {
        var batchPath = util.format('%s/%s', pendingLogBatchesDir, batch)

        if (e) {
          // write error to log, move batch to error dir
          errorsWriteStream.write(util.format('[%s]\tBatch File: %s\tError: "%s"\n\n', new Date().toString(), batch, e))

          var newPath = util.format('%s/%s', errorLogBatchesDir, batch)
          fs.rename(batchPath, newPath, function(e) {
            if (!--numOutstanding) callback(batches.length)
          })
        } else {
          // rm batch file (it's saved as attachment on database)
          fs.unlink(batchPath, function(e) {
            if (!--numOutstanding) callback(batches.length)
          })
        }
      })
    })
  })
}

function processBatch(batch, pbCallback) {
  var path = util.format('%s/%s', pendingLogBatchesDir, batch)
    , match = batch.match(batchFileNameRE)
    , batchDate = parseInt(match[1], 16)
    , batchUUID = match[2]
    , batchProcessDate = Math.round(new Date().getTime() / 1000)

  fs.readFile(path, 'utf8', function(e, str) {
    if (e) {
      pbCallback('error reading batch file')
      return
    }

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

    var valid = _.filter(records, function(r) { return !r.error })
      , invalid = _.difference(records, valid)
      , uuids = _.pluck(valid, 'uuid')
      , numValid = uuids.length
      , deviceId
      , recordsByUrDb = {}
      , mainLoggingDbRecords = []
      , sentError = false

    var sendError = function(e) {
      if (!sentError) {
        pbCallback(e)
        sentError = true
      }
    }

    var assignRecordActions = function(dbURI, recs, callback) {
      var uuids = _.pluck(recs, 'uuid')
      var matchingDocsURI = encodeURI(util.format('%s/_all_docs?include_docs=true&keys=%s', dbURI, JSON.stringify(uuids)))
      var docsForInsertOrUpdate = []

      request(matchingDocsURI, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc !== 200) {
          // TODO: Needs Handling!
          sendError(util.format('Error retrieving database records.\n\tDatabase URI: %s\n\tRecords: %s\n\tError: "%s"\n\tSort Code: %d', dbURI, JSON.stringify(recs), e || b, sc || 0))
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
            deviceId = deviceId || record.doc.device

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

    var ensureUrDbExists = function(urId, callback) {
      if (getUrDbURI(urId)) {
        callback()
      } else {
        var urDbURI = util.format('%s/ur-logging-%s', couchServerURI, urId.toLowerCase())

        request({ uri:urDbURI, method: 'PUT' }, function(e,r,b) {
          var sc = r && r.statusCode
          // race-condition possibility - db could have been created between ensureUrDbExists() being called and this callback => allow 412
          if (sc == 412) {
            // make sure ur in directory
            if (!getUrDbURI(urId)) {
              userDbDirectoryDoc.dbs[urId] = urDbURI
              writeUpdatedUserDbDirectory()
            }
            callback()
          } else if (sc == 201) {
            userDbDirectoryDoc.dbs[urId] = urDbURI
            writeUpdatedUserDbDirectory()
            updateDesignDocs(urDbURI, [genDesignDoc, userDesignDoc, paMetaDesignDoc], callback)
          } else {
            sendError(util.format('error creating user database\n\tURI: "%s"\n\tUser: "%s".\n\tError: %s\n\tSort Code: %d', urDbURI, urId, e || b, sc || 0))
          }
        })
      }
    }

    var writeUserDocs = function(urId, docs, callback) {
      if (!docs.length) {
        callback()
        return
      }

      request({
        method:'POST'
        , uri: getUrDbURI(urId) + '/_bulk_docs'
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify({ docs:docs })
      }, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc !== 201) {
          sendError(util.format('error recording user log docs\n\tUser: %s\n\tDatabase: %s\n\tError: %s\n\tSort Code: %d', urId, getUrDbURI(urId), e || b, sc || 0))
        } else {
          callback()
        }
      })
    }

    var recordBatch = function() {
      // summary/aggregate doc with information about whole batch
      var batchDoc = {
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

      var saveDocs = function() {
        // bulk insert/update: send each document in the batch, together with the batchDoc above
        request({
          method:'POST'
          , uri: mainLoggingDbURI + '/_bulk_docs'
          , headers: { 'content-type':'application/json', accepts:'application/json' }
          , body: JSON.stringify({
              docs: [ batchDoc ].concat( _.pluck(mainLoggingDbRecords, 'doc') )
          })
        }, function(e,r,b) {
          var sc = r && r.statusCode
            , error = sc !== 201 && util.format('error recording log batch\n\tError: %s\n\tStatus Code: %d', e || b, sc || 0)
          pbCallback(error)
        })
      }

      if (!mainLoggingDbRecords.length) {
        saveDocs()
      } else {
        assignRecordActions(mainLoggingDbURI, mainLoggingDbRecords, saveDocs)
      }
    }

    if (0 == numValid) {
      recordBatch()
      return
    }

    // separate out valid records into dbs that they're going to be recorded on 
    // and set deviceId
    valid.forEach(function(r) {
      deviceId = deviceId || r.doc.device

      var ur = r.doc.user
      if (typeof ur == 'string') {
        var urDbRecords = recordsByUrDb[ur]
        if (!urDbRecords) urDbRecords = recordsByUrDb[ur] = []
        urDbRecords.push(r)
      } else {
        mainLoggingDbRecords.push(r)
      }
    })

    var userIds = Object.keys(recordsByUrDb)
      , usersRemaining = userIds.length

    if (usersRemaining) {
      userIds.forEach(function(urId) {
        // callback from any of the following functions implies success
        ensureUrDbExists(urId, function() {
          assignRecordActions(getUrDbURI(urId), recordsByUrDb[urId], function(docsForInsertOrUpdate) {
            writeUserDocs(urId, docsForInsertOrUpdate, function() {
              if (!--usersRemaining) recordBatch()
            })
          })
        })
      })
    } else {
      recordBatch()
    }
  })
}

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

// design doc
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
