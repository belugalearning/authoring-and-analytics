var util = require('util')
  , request = require('request')
  , sqlite3 = require('sqlite3').verbose()
  , crypto = require('crypto')
  , exec = require('child_process').exec
  , fs = require('fs')
  , Stream = require('stream')
  , zlib = require('zlib')

var model
  , config

function guid() {
  return crypto.randomBytes(16)
    .toString('hex')
    .replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})/i, '$1-$2-$3-$4-$5')
    .toUpperCase()
}

module.exports = function(model_, config_) {
  model = model_
  config = config_
  return exports
}

exports.syncUsers = function(req, res) {
  // send success straight away. Will get data sent again pretty regularly
  var users = JSON.parse(req.body.users)

  model.syncUsers(users, function(e, statusCode, updates) {
    res.send(e || updates, statusCode || 500)
  })
}

exports.checkNickAvailable = function(req, res) {
  var nick = req.body.nick

  model.userMatchingNick(nick, function(e,r,b) {
    if (200 != r.statusCode) {
      res.send(e, r.statusCode || 500)
      return
    }

    res.send(JSON.parse(b).rows.length == 0, 200)
  })
}

exports.getUserMatchingNickAndPassword = function(req, res) {
  var nick = req.body.nick
    , password = req.body.password

  model.userMatchingNickAndPassword(nick, password, function(e,r,b) {
    if (200 != r.statusCode) {
      //console.log(nick, password, e, r.statusCode)
      res.send(e, r.statusCode || 500)
      return
    }

    var rows = JSON.parse(b).rows

    if (!rows.length) {
      res.send(404)
      return
    }

    var urData = rows[0].doc
      , ur = { id:urData._id, nick:urData.nick, password:urData.password, nodesCompleted:urData.nodesCompleted }

    res.send(ur, 200)
  })
}

exports.getState = function(req,res) {
  // params / query components
  var urId = req.params.userId
    , device = req.query['device']
    , since = req.query['last_batch_process_date'] // the last time the device synched this user, this was the latest processed batch

  // successful response components
  var lastBatchDate
    , processedDeviceBatchIds
    , b64ZippedStateDb

  var sendSuccessResponse = function() {
    res.send({
      lastProcessedBatchDate: lastBatchDate
      , processedDeviceBatchIds: processedDeviceBatchIds
      , gzippedStateDatabase: b64ZippedStateDb
    })
  }

  var sentError = false
  var sendError = function(e) {
    if (!sentError) {
      sentError = true
      console.log('\nERROR in getState()\n\tat %s\n\t{ userId:"%s", device:"%s", last_process_batch_date:"%s" }\n\t%s\n', (new Date).toString(), urId, device, since, e)
      res.send(e || 'error retrieving user state', 500)
      return
    }
  }

  var checkResponseError = function(e, r, b, expectedSortCode, uri) {
    var sc = r && r.statusCode
    if (!r || r.statusCode != expectedSortCode) {
console.log('RESPONSE ERROR ARGUMENTS SPLIT ----\n\t\t%s', [].slice.call(arguments).join('\n\t\t'))
      var errorMessage = util.format('Couch Response error.\n\t\tError: %s\n\t\tSort Code: %d\n\tExpected Sort Code: %d\n\t\tBody: %s\n\t\tURI: %s', e, sc || 0, expectedSortCode, b.replace(/\s*$/,''), uri)
      sendError(errorMessage)
      return true
    }
  }

  var checkNullError = function(v, keyDesc) {
    if (!v) {
      sendError(util.format('Null Error - \t\tKey Description="%s"', keyDesc))
      return true
    }
  }

  if (!isNaN(since)) {
    since = Number(since)
  } else {
    sendError('last_batch_process_date is not a number. value = %s', since)
    return
  }

  // TODO: check that user and device are paired - if not send 403
  
  // look up user's db in directory
  var uri = util.format('%s/%s/user-db-directory', config.couchServerURI, config.appWebService.loggingService.databaseName)
  request(uri, function(e,r,b) {
    if (checkResponseError(e, r, b, 200, uri)) return

    var urDbURI = JSON.parse(b).dbs[urId]
console.log('\n**temp urDbURI=',urDbURI)
    if (checkNullError(urDbURI, 'user logging db uri')) return

    var pathToViews = util.format('%s/_design/user-related-views/_view', urDbURI)
console.log('**temp pathToViews=',pathToViews)

    // get latest batch procesed. Any batches processed from now will be ignored for remainder of this iteration of getState()
    var uri = util.format('%s/log-batches-by-process-date?group_level=1&descending=true&limit=1', pathToViews)
console.log('**temp most recent processed batch uri=',uri)
    request(uri, function(e,r,b) {
      if (checkResponseError(e, r, b, 200, uri)) return

      var rows = JSON.parse(b).rows
      if (checkNullError(rows.length, '0 user log batches')) return

      // included in response
      lastBatchDate = rows[0].key

      // get ids of batches processed by from device between 'since' and lastBatchDate
      var sKey = encodeURIComponent(JSON.stringify([device, lastBatchDate]))
        , eKey = encodeURIComponent(JSON.stringify([device, since + 0.001]))
        , query = util.format('%s/log-batches-by-device-process-date?group_level=2&descending=true&startkey=%s&endkey=%s', pathToViews, sKey, eKey)

console.log('**temp ids batches processed in range query=',query)
      request(query, function(e,r,b) {
        if (checkResponseError(e, r, b, 200, query)) return

        // included in response
        processedDeviceBatchIds = JSON.parse(b).rows.map(function(r) { return r.value })

        // get epsisodes processed before or on lastBatchDate
        // generate state from episodes
        var uri = util.format('%s/episodes-by-batch-process-date?include_docs=true&end_key=%d', pathToViews, lastBatchDate)
console.log('**temp episodes-by-batch-process-date uri=',uri)
        request(uri, function(e,r,b,uri) {
          if (checkResponseError(e,r,b,200,uri)) return

          var episodes = JSON.parse(b).rows.map(function(r) { return r.doc })
            , nodes = {}

          episodes.forEach(function(ep) {
            var n = nodes[ep.nodeId]

            if (!n) {
              n = nodes[ep.nodeId] = {
                id: ep.nodeId
                , time_played: 0
                , last_played: 0
                , last_score: 0
                , total_accumulated_score: 0
                , high_score: 0
                , first_completed: 0
                , last_completed: 0
                , artifact_1_last_achieved: 0
                , artifact_2_last_achieved: 0
                , artifact_3_last_achieved: 0
                , artifact_4_last_achieved: 0
                , artifact_5_last_achieved: 0
              }
            }
            
            if (Object.prototype.toString.call(ep.events) != '[object Array]') return // TODO: error, corrupted doc - this is bad, we should know about it!

            var lastEvent = ep.events[ep.events.length - 1]
              , lastEventDate = lastEvent && lastEvent.date

            if (!lastEvent) return // TODO: record doc corruption
            if (isNaN(lastEventDate)) return // TODO: record doc corruption

            // TODO: stop assuming rest of doc is well-formatted, and instead check every field before continuing

            n.time_played += ep.timeInPlay
            n.last_played = lastEventDate
            n.last_score = ep.score

            if (ep.score) {
              n.total_accumulated_score += ep.score
              n.high_score = Math.max(n.high_score, ep.score)

              if (!n.first_completed) n.first_completed = lastEventDate
              n.last_completed = lastEventDate

              if (ep.score > config.appScoring.artifact1) {
                n.artifact_1_last_achieved = lastEventDate

                if (ep.score > config.appScoring.artifact2) {
                  n.artifact_2_last_achieved = lastEventDate

                  if (ep.score > config.appScoring.artifact3) {
                    n.artifact_3_last_achieved = lastEventDate

                    if (ep.score > config.appScoring.artifact4) {
                      n.artifact_4_last_achieved = lastEventDate

                      if (ep.score > config.appScoring.artifact5) {
                        n.artifact_5_last_achieved = lastEventDate
                      }
                    }
                  }
                }
              }
            }
          })

          // is there a way to zip ':memory:' database instead of writing to disk?
          var dbPath = util.format('/tmp/ur-state-%s.db', guid())

          var zipDb = function() {
            // zip up db
            var inp = fs.createReadStream(dbPath) // read db
              , gzip = zlib.createGzip() // zip it
              , ws = new Stream() // write zip into buffer (out). on end base64 encode the zip and send response
              , out = new Buffer(1024)
              , numBytes = 0

            ws.writable = true
            ws.write = function(buf) {
              if (buf.length + numBytes > out.length) {
                var newOut = new Buffer(2 * out.length)
                out.copy(newOut)
                out = newOut
              }
              buf.copy(out, numBytes)
              numBytes += buf.length
            }
            ws.end = function(buf) {
              if (buf) ws.write(buf)
              b64ZippedStateDb = out.toString('base64', 0, numBytes)
              sendSuccessResponse()
              fs.unlink(dbPath) // file no longer required
            }

            inp.pipe(gzip).pipe(ws)
          }

          var db = new sqlite3.Database(dbPath)
          var cols =
            [ 'id TEXT PRIMARY KEY ASC'
            , 'time_played REAL'
            , 'last_played REAL'
            , 'last_score INTEGER'
            , 'total_accumulated_score INTEGER'
            , 'high_score INTEGER'
            , 'first_completed REAL'
            , 'last_completed REAL'
            , 'artifact_1_last_achieved REAL'
            , 'artifact_1_curve TEXT'
            , 'artifact_2_last_achieved REAL'
            , 'artifact_2_curve TEXT'
            , 'artifact_3_last_achieved REAL'
            , 'artifact_3_curve TEXT'
            , 'artifact_4_last_achieved REAL'
            , 'artifact_4_curve TEXT'
            , 'artifact_5_last_achieved REAL'
            , 'artifact_5_curve TEXT']
          var colNames = cols.map(function(col) { return col.match(/^\S+/)[0] })

          db.serialize(function() {
            db.run(util.format('CREATE TABLE Nodes (%s)', cols.join(',')))

            var ins = db.prepare(util.format('INSERT INTO Nodes (%s) VALUES (?%s)', colNames.join(','), Array(colNames.length).join(',?')))

            Object.keys(nodes).forEach(function(nId) {
              ins.run.apply(ins, colNames.map(function(col) { return nodes[nId][col] }))
            })

            ins.run(zipDb)
          })
        })
      })
    })
  })
}
