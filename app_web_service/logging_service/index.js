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
  , dbName
  , dbURI
  , designDoc = 'logging-design-doc'
  , pendingLogBatchesDir = __dirname + '/pending-batches'
  , errorLogBatchesDir = __dirname + '/error-batches'
  , errorsWriteStream = fs.createWriteStream(__dirname + '/errors.log', { flags: 'a' })

// previously log batches were uploaded without batch uuid
// edit pending log filenames from form: "batch-<DATE>" to form: "batch-<DATE>-<UUID>"
// TODO: Delete when no longer required
if (false) {
  (function() {
    fs.readdir(pendingLogBatchesDir, function(e, dirFiles) {
      var batchesToRename = _.filter(dirFiles, function(file) { return /^batch-[0-9a-f]{8}$/i.test(file) })
      var len = batchesToRename.length
      var uri = encodeURI(util.format('%s_uuids?count=%d', couchServerURI, len))

      if (0 == len) return

      request(uri, function(e,r,b) {
        if (!r || 200 != r.statusCode) {
          console.log('error generating uuids. error="%s", statusCode=%d', e, r && r.statusCode)
          return
        }

        var uuids = JSON.parse(b).uuids

        for (var i=0; i<len; i++) {
          var oldPath = util.format('%s/%s', pendingLogBatchesDir, batchesToRename[i])
          var newPath = util.format('%s-%s', oldPath, uuids[i])
          fs.rename(oldPath, newPath, function(e) { /*console.log('rename batch complete. error:',e)*/ })
        }
      })
    })
  })()
}

module.exports = function(config) {
  couchServerURI = config.couchServerURI.replace(/([^/])$/, '$1/')
  dbName = config.appWebService.loggingService.databaseName
  dbURI = couchServerURI + dbName + '/'

  //updateDesignDoc()

  processPendingBatches(function(numProcessed) {
    var f = arguments.callee

    //console.log('processed %d batches at %s', numProcessed, new Date().toString())
    if (numProcessed > 0) {
      processPendingBatches(f)
    } else {
      setTimeout((function() { processPendingBatches(f) }), 5000)
    }
  })

  return {
    uploadBatchRequestHandler: uploadBatchRequestHandler
  }
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
    var batchUUID = dWords.slice(1).join('')

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
          errorsWriteStream.write(util.format('[%s]\tBatch File: %s\tError: "%s"\n', new Date().toString(), batch, e))

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

function processBatch(batch, callback) {
  var path = util.format('%s/%s', pendingLogBatchesDir, batch)
    , match = batch.match(batchFileNameRE)
    , batchDate = parseInt(match[1], 16)
    , batchUUID = match[2]

  fs.readFile(path, 'utf8', function(e, str) {
    if (e) {
      callback('error reading batch file')
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
      , getMatchingDocsURI = numValid && encodeURI(util.format('%s_all_docs?keys=%s&include_docs=true', dbURI, JSON.stringify(uuids)))
      , docsForInsertOrUpdate = []

    var recordBatch = function() {
      // summary/aggregate doc with information about whole batch
      var batchDoc = {
        _id: batchUUID
        , type: 'LogBatch'
        , batchDate: batchDate
        , batchProcessDate: Math.round(new Date().getTime() / 1000)
        , records: records
      }

      // bulk insert/update: send each document in the batch, together with the batchDoc above
      request({
        method:'POST'
        , uri: dbURI + '_bulk_docs'
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify({ docs: [ batchDoc ].concat( docsForInsertOrUpdate ) })
      }, function(e,r,b) {
        if (!r) {
          callback('no database response to record log batch bulk request')
          return
        }
        if (r.statusCode != 201) {
          callback(util.format('error recording log batch\n\terror: %s\n\tstatusCode: %d\n\tresponse body: %s', e, r.statusCode, b))
          return
        }

        var batchDocRev = JSON.parse(b)[0].rev

        if (!batchDocRev) {
          callback(util.format('error retrieving batchDoc revision from bulk insert/update response\n\tbatch doc data: %s', JSON.stringify(JSON.parse(b)[0])))
          return
        }

        // attach the batch file to the batch doc
        request({
          method:'PUT'
          , uri: encodeURI(util.format('%s%s/raw-log?rev=%s', dbURI, batchUUID, batchDocRev))
          , headers: { 'content-type':'text/plain; charset=utf-8', accepts:'application/json' }
          , body: str
        }, function(e,r,b) {
          if (!r) {
            callback('no database response to request attaching batch file to batch doc')
            return
          }
          else if (r.statusCode != 201) {
            callback(util.format('error attaching batch file to batch doc\n\terror: %s\n\tstatusCode: %d\n\tresponse body: %s', e, r.statusCode, b))
            return
          }
          callback()
        })
      })
    }

    if (0 == numValid) {
      recordBatch()
      return
    }

    request(getMatchingDocsURI, function(e,r,b) {
      if (!r) {
        // can't connect to db, just return
        callback('no response from database')
        return
      }
      if (200 != r.statusCode) {
        // TODO: handle
        callback(util.format('Request Error\n\turi: "%s"\n\terror: "%s"\n\tstatusCode: %d\n\t', getMatchingDocsURI, e, r.statusCode))
        return
      }

      var rows = JSON.parse(b).rows

      for (var i=0; i<numValid; i++) {
        var record = valid[i]
        var row = rows[i]

        if (row.error) {
          if (row.error == 'not_found') {
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
      recordBatch()
    })
  })
}

// design doc
function updateDesignDoc(callback) {
  var uri = dbURI + '/_design/' + designDoc

  console.log('LoggingService\t\t\tupdating design doc:\t%s', uri)


  var body = {
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
        map: (function (doc) { if (doc.batchDate && doc.type) emit([doc.batchDate, doc.type], null) }).toString()
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
    }
  }

  request.get(uri, function(e,r,b) {
    var error

    if (!r) {
      //TODO handle
      error = 'error connecting to database'
      console.log('LoggingService#updateDesignDoc() - ' +  error)
      callback && callback(error, 500)
      return
    }

    if (404 != r.statusCode && 200 != r.statusCode) {
      var error = util.format('error retrieving design doc. Database Error: "%s"  Status Code:%d', e, r.statusCode)
      console.log('LoggingService#updateDesignDoc() - ' + error)
      callback && callback(error, r.statusCode || 500)
      return
    }

    if (r.statusCode == 200) body._rev = JSON.parse(b)._rev

    request({
      method:'PUT'
      , uri: uri
      , body: JSON.stringify(body)
    }, function(e,r,b) {
      console.log('LoggingService\t\t\tupdated design doc:\tstatusCode:%d error="%s"', r.statusCode, e)
      callback && callback(e, r.statusCode)
    })
  })
}
