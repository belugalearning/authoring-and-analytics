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
  , verboseLogging = false

module.exports = function(model_, config_) {
  model = model_
  config = config_
  return exports
}

exports.syncUsers = function syncUsers(req, res) {
  if (verboseLogging) {
    console.log('\nAppWebServices -> UsersService -> routeHandlers -> syncUsers')
  }

  var users
  if (req.body && req.body.users) {
    users = JSON.parse(req.body.users)
  }

  if (!Array.isArray(users)) {
    console.log('%s\tsyncUsers Bad Args: req.body.users array expected.\treq.body: %j, users: %j', niceConciseDate(), req.body, users)
    res.send(500)
    return
  }

  // pre 1.2 version of app has bug that makes it crash on processing data sent in success response so send error if possibility app version is pre 1.2 (assignmentFlags object not present on user)
  var sendErrorResponseToOldAppVersion = !users.length || typeof users[0].assignmentFlags != 'object'

  model.syncUsers(users, function(e, statusCode, updates) {
    if (verboseLogging) {
      console.log('----- End Sync Users: %s', JSON.stringify({ e:e, sc:statusCode, updates:updates }, null, 2))
      console.log('DEBUG STACK AT END SYNC USERS: %s', new Error().stack)
    }
    if (!e && sendErrorResponseToOldAppVersion) {
      e = 'Bug in pre-v1.2 version of app causes crash processing data in success response -> send error'
      statusCode = 500
    }
    res.send(e || updates, statusCode || 500)
  })
}

exports.checkNickAvailable = function checkNickAvailable(req, res) {
  var nick = req.body.nick

  model.userMatchingNick(nick, function(e,r,b) {
    if (200 != r.statusCode) {
      res.send(e, r.statusCode || 500)
      return
    }

    res.send(JSON.parse(b).rows.length == 0, 200)
  })
}

exports.changeNick = function changeNick(req, res) {
  var sentResponse = false
    , databaseURI = util.format('%s/%s', config.couchServerURI.replace(/^(.+[^/])\/*$/, '$1'), config.appWebService.usersService.databaseName)
    , id = req.body.id
    , password = req.body.password
    , newNick = req.body.newNick

  var sendError = function sendError(e, sc, suppressConsoleLog) {
    if (!sentResponse) {
      sentResponse = true
      if (!suppressConsoleLog) {
        console.log('\n%s\tERROR in changeNick()\n\tid:%s password:%s newNick:%s\n\terror: %s\n', niceConciseDate(), id, password, newNick, e)
      }
      res.send(e || 'error changing user nick', sc || 500)
      return
    }
  }

  var checkResponseError = function(e, r, b, validStatusCodes, uri) {
    var sc = r && r.statusCode

    if (typeof validStatusCodes == 'number') validStatusCodes = [validStatusCodes]

    if (!r || !~validStatusCodes.indexOf(sc)) {
      sendError(util.format('Couch Response error.\n\t\tDecoded Request URI: %s\n\t\tValid Status Codes: %s\n\t\tResponse Status Code: %s\n\t\tResponse Error: %s\n\t\tResponse Body: %s'
        , decodeURIComponent(uri)
        , JSON.stringify(validStatusCodes)
        , sc || 'NO RESPONSE'
        , e || 'NULL'
        , (b || '').replace(/\s*$/,'')))
      return true
    }
  }

  if (Object.prototype.toString.call(id) != '[object String]') {
    sendError('string required for id', 422, true)
    return
  }
  if (Object.prototype.toString.call(password) != '[object String]') {
    sendError('string required for password', 422, true)
    return
  }
  if (Object.prototype.toString.call(newNick) != '[object String]' || !newNick.length) {
    sendError('string required for newNick', 422, true)
    return
  }

  var userURI = util.format('%s/%s', databaseURI, id)
  request(userURI, function(e,r,b) {
    if (checkResponseError(e, r, b, [200,404], userURI)) return

    if (r.statusCode == 404) {
      res.send('user not found', 404)
      return
    }

    var ur = JSON.parse(b)

    if (ur.password != password) {
      if (!sentResponse) res.send(401)
      return
    }

    model.userMatchingNick(newNick, function(e,r,b) {
      if (checkResponseError(e, r, b, 200, '[user-matching-nick]')) return

      if (JSON.parse(b).rows.length) {
        if (!sentResponse) res.send(409)
        return
      }

      ur.nick = newNick

      request({
        method:'PUT'
        , uri: userURI
        , headers: { 'Content-Type':'application/json' }
        , body: JSON.stringify(ur)
      }, function(e,r,b) {
        if (checkResponseError(e, r, b, 201, userURI)) return
        if (!sentResponse) res.send(201)
      })
    })
  })
}

// restricted to users with valid nicks - i.e. nickClash == 1
exports.getUserMatchingNickAndPassword = function getUserMatchingNickAndPassword(req, res) {
  var nick = req.body.nick
    , password = req.body.password

  model.userMatchingNickAndPassword(nick, password, function(e,r,b) {
    if (r && r.statusCode != 200) return res.send(e, r.statusCode || 500)

    var rows = JSON.parse(b).rows

    if (!rows.length) return res.send(404)

    var users = rows.map(function(r) { return r.doc })

    var user = null
    if (users.length == 1) {
      user = users[0]
    } else {
      var usersWithoutNickClash = users.filter(function(ur) { return ur.nickClash == 1 }) // max length 1
      if (usersWithoutNickClash.length) {
        user = usersWithoutNickClash[0]
      } else {
        return res.send(409)
      }
    }

    return res.send({ id:user._id, nick:user.nick, password:user.password, assignmentFlags:user.assignmentFlags, nickClash:user.nickClash }, 200)
  })
}

exports.getState = function getState(req,res) {
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

  var checkResponseError = function(e, r, b, validStatusCodes, uri) {
    var sc = r && r.statusCode

    if (typeof validStatusCodes == 'number') validStatusCodes = [validStatusCodes]

    if (!r || !~validStatusCodes.indexOf(sc)) {
      sendError(util.format('Couch Response error.\n\t\tDecoded Request URI: %s\n\t\tValid Status Codes: %s\n\t\tResponse Status Code: %s\n\t\tResponse Error: %s\n\t\tResponse Body: %s'
        , decodeURIComponent(uri)
        , JSON.stringify(validStatusCodes)
        , sc || 'NO RESPONSE'
        , e || 'NULL'
        , (b || '').replace(/\s*$/,'')))
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
  var lookupUrDbURI = util.format('%s/%s/user-db-directory', config.couchServerURI, config.appWebService.loggingService.genLoggingDbName)
  request(lookupUrDbURI, function(e,r,b) {
    if (checkResponseError(e, r, b, 200, lookupUrDbURI)) return

    var urDbURI = JSON.parse(b).dbs[urId]
    if (!urDbURI) {
      // (assuming no errors) nothing logged against the user yet, so user's logging db yet to be created
      sendError('User logging db not found. Assume no log batches for user processed yet', true)
      return
    }

    // local testing with ssh tunnel
    if (config.localise) {
      urDbURI = urDbURI.replace(/^http:\/\/.*:\d*/, 'http://127.0.0.1:5984')
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

      if (since >= lastBatchDate) {
        if (!sentError) res.send(204)
        return
      }

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

function niceConciseDate() {
  return new Date()
  .toJSON()
  .replace(/T/, '-')
  .replace(/\.[0-9]{3}Z$/, '')
}
