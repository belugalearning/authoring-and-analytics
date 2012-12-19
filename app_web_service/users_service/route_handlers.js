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
    , b64GZippedStateDb

  var sendSuccessResponse = function() {
    res.send({
      lastProcessedBatchDate: lastBatchDate
      , processedDeviceBatchIds: processedDeviceBatchIds
      , gzippedStateDatabase: b64GZippedStateDb
    })
  }

  var sentError = false
  var sendError = function(e, suppressConsoleLog) {
    if (!sentError) {
      sentError = true
      if (!suppressConsoleLog) console.log('\nERROR in getState()\n\tat %s\n\t{ userId:"%s", device:"%s", last_batch_process_date:"%s" }\n\t%s\n', (new Date).toString(), urId, device, since, e)
      res.send(e || 'error retrieving user state', 500)
      return
    }
  }

  var checkResponseError = function(e, r, b, expectedSortCode, uri) {
    var sc = r && r.statusCode
    if (!r || r.statusCode != expectedSortCode) {
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
  
  // look up user's logging db in directory
  var lookupUrDbURI = util.format('%s/%s/user-db-directory', config.couchServerURI, config.appWebService.loggingService.databaseName)
  request(lookupUrDbURI, function(e,r,b) {
    if (checkResponseError(e, r, b, 200, lookupUrDbURI)) return

    var urDbURI = JSON.parse(b).dbs[urId]
    if (!urDbURI) {
      // (assuming no errors) nothing logged against the user yet, so user's logging db yet to be created
      sendError('User logging db not found. Assume no log batches for user processed yet', true)
      return
    }

    var pathToViews = util.format('%s/_design/user-related-views/_view', urDbURI)

    // get latest batch procesed. Any batches processed from now will be ignored for remainder of this iteration of getState()
    var lastBatchProcessedURI = util.format('%s/log-batches-by-process-date?group_level=1&descending=true&limit=1', pathToViews)
    request(lastBatchProcessedURI, function(e,r,b) {
      if (checkResponseError(e, r, b, 200, lastBatchProcessedURI)) return

      var rows = JSON.parse(b).rows
      if (checkNullError(rows.length, '0 user log batches')) return

      // included in response
      lastBatchDate = rows[0].key

      // get ids of batches processed by from device between 'since' and lastBatchDate
      var sKeyDeviceAndJustAfterSince = encodeURIComponent(JSON.stringify([device, since + 0.001]))
        , eKeyDeviceAndLastBatchDate = encodeURIComponent(JSON.stringify([device, lastBatchDate]))
        , deviceBatchIdsQueryURI = util.format('%s/log-batches-by-device-process-date?group_level=2&startkey=%s&endkey=%s', pathToViews, sKeyDeviceAndJustAfterSince, eKeyDeviceAndLastBatchDate)

      request(deviceBatchIdsQueryURI , function(e,r,b) {
        if (checkResponseError(e, r, b, 200, deviceBatchIdsQueryURI)) return

        // included in response
        processedDeviceBatchIds = JSON.parse(b).rows.map(function(r) { return r.value })

        // is there a way to zip ':memory:' database instead of writing to disk?
        var dbPath = util.format('/tmp/ur-state-%s.db', guid())

        var dbTablesData = {
          nodes: null
          , featureKeys: null
        }

        var getNodesTableData = function(successfn) {
          // get epsisodes processed before or on lastBatchDate
          var episodesURI = util.format('%s/episodes-by-batch-process-date?include_docs=true&end_key=%d', pathToViews, lastBatchDate)
          request(episodesURI, function(e,r,b) {
            if (checkResponseError(e,r,b,200,episodesURI)) return

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

                if (ep.score >= config.appScoring.artifact1) {
                  n.artifact_1_last_achieved = lastEventDate

                  if (ep.score >= config.appScoring.artifact2) {
                    n.artifact_2_last_achieved = lastEventDate

                    if (ep.score >= config.appScoring.artifact3) {
                      n.artifact_3_last_achieved = lastEventDate

                      if (ep.score >= config.appScoring.artifact4) {
                        n.artifact_4_last_achieved = lastEventDate

                        if (ep.score >= config.appScoring.artifact5) {
                          n.artifact_5_last_achieved = lastEventDate
                        }
                      }
                    }
                  }
                }
              }
            })

            dbTablesData.nodes = nodes
            successfn()
          })
        }

        var getFeatureKeysTableData = function(successfn) {
          var featureKeyEncountersURI = util.format('%s/feature-key-encounters-by-batch-process-date?endkey=%5B%d%2C%7B%7D%5D', pathToViews, lastBatchDate)
          request(featureKeyEncountersURI, function(e,r,b) {
            if (checkResponseError(e,r,b,200,featureKeyEncountersURI)) return

            var featureKeys = {}

            // view key: [ 0: batch process date, 1: feature key, 2: encounter date ]

            JSON.parse(b).rows.forEach(function(row) {
              var key = row.key[1]
                , encounterDate = row.key[2]
                , fk = featureKeys[key]

              if (!fk) fk = featureKeys[key] = { key:key, encounters:[] }
              fk.encounters.push(encounterDate)
            })

            Object.keys(featureKeys).forEach(function(key) {
              var fk = featureKeys[key]
              fk.encounters = JSON.stringify(fk.encounters)
            })

            dbTablesData.featureKeys = featureKeys
            successfn()
          })
        }

        var createDb = function createDb(successfn) {
          var db = new sqlite3.Database(dbPath)

          var nodeCols =
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

          var fkCols =
            [ 'key TEXT PRIMARY KEY ASC'
            , 'encounters TEXT' ]

          var nodeColNames = nodeCols.map(function(col) { return col.match(/^\S+/)[0] })
          var fkColNames = fkCols.map(function(col) { return col.match(/^\S+/)[0] })

          db.serialize(function() {
            db.run(util.format('CREATE TABLE Nodes (%s)', nodeCols.join(',')))
            db.run(util.format('CREATE TABLE FeatureKeys (%s)', fkCols.join(',')))

            var nodeIns = db.prepare(util.format('INSERT INTO Nodes       (%s) VALUES (?%s)', nodeColNames.join(','), Array(nodeColNames.length).join(',?')))
            var fkIns =   db.prepare(util.format('INSERT INTO FeatureKeys (%s) VALUES (?%s)', fkColNames.join(','),   Array(fkColNames.length).join(',?')))

            Object.keys(dbTablesData.nodes).forEach(function(nId) {
              var n = dbTablesData.nodes[nId]
              nodeIns.run.apply(nodeIns, nodeColNames.map(function(col) { return n[col] }))
            })

            Object.keys(dbTablesData.featureKeys).forEach(function(key) {
              var fk = dbTablesData.featureKeys[key]
              fkIns.run.apply(fkIns, fkColNames.map(function(col) { return fk[col] }))
            })

            db.close(successfn)
          })
        }

        var zipDb = function(successfn) {
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
            b64GZippedStateDb = out.toString('base64', 0, numBytes)
            successfn()
          }

          inp.pipe(gzip).pipe(ws)
        }

        var onGotDbTableData = function onGotDbTableData() {
          for (var key in dbTablesData) {
            if (!dbTablesData[key]) return
          }

          createDb(function() {
            zipDb(function() {
              sendSuccessResponse()
              fs.unlink(dbPath) // file no longer required
            })
          })
        }

        getNodesTableData(onGotDbTableData)
        getFeatureKeysTableData(onGotDbTableData)
      })
    })
  })
}

function guid() {
  return crypto.randomBytes(16)
    .toString('hex')
    .replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})/i, '$1-$2-$3-$4-$5')
    .toUpperCase()
}
