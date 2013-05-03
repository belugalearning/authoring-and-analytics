var util = require('util')
  , request = require('request')
  , _ = require('underscore')

var couchServerURI
  , dbName
  , designDoc
  , databaseURI
  , sapDbURI

module.exports = function(config) {
  couchServerURI = config.couchServerURI.replace(/^(.+[^/])\/*$/, '$1')
  dbName = config.appWebService.usersService.databaseName
  designDoc = config.appWebService.usersService.databaseDesignDoc
  databaseURI = util.format('%s/%s', couchServerURI, dbName)
  sapDbURI = util.format('%s/%s', couchServerURI, config.sap.databaseName)
  console.log(util.format('AppWebService -> usersService\tdesignDoc="%s"\tdatabaseURI="%s"', designDoc, databaseURI))

  return {
    syncUsers: syncUsers
    , userMatchingNick: userMatchingNick
    , userMatchingNickAndPassword: userMatchingNickAndPassword
  }
}

// this function sets nickClash and add users to db that were previously missing - no longer handles game state
function syncUsers(clientDeviceUsers, callback) {
  var urIds = _.pluck(clientDeviceUsers, 'id')
    , newUsersProcessed = false
    , existingUsersProcessed = false
    , clientUpdates = []
    , serverUpdates = []
    , hasSentResponse = false

  var niceConciseDate = function niceConciseDate() {
    return new Date()
    .toJSON()
    .replace(/T/, '-')
    .replace(/\.[0-9]{3}Z$/, '')
  }

  var sendError = function sendError(e, sc) {
    console.log('%s\tError in UsersService#syncUsers:\n\t%s', niceConciseDate(), e)
    if (!hasSentResponse) {
      callback(e, sc || 500)
      hasSentResponse = true
    }
  }

  var checkResponseError = function checkResponseError(e, r, b, expectedStatusCode, uri) {
    var sc = r && r.statusCode
    if (!r || r.statusCode != expectedStatusCode) {
      sendError(util.format('Couch Response error.\n\tDecoded Request URI: %s\n\tExpected Status Code: %d\n\tResponse Status Code: %s\n\tResponse Error: %s\n\tResponse Body: %s'
        , decodeURIComponent(uri)
        , expectedStatusCode
        , sc || 'NO RESPONSE'
        , e || 'NULL'
        , (b || '').replace(/\s*$/,'')), sc)
      return true
    }
  }

  var processNewUsers = function(newUsers, fn) {
    // do their nicks clash? set nickClash=1 if no, nickClash=2 if yes
    // as such all new users will need updating on client
    // and being new users, they'll need adding to the server db too

    if (!newUsers.length) {
      newUsersProcessed = true
      return fn()
    }

    var nickClashesURI = util.format('%s/_design/%s/_view/users-by-nick?keys=%s', databaseURI, designDoc, encodeURIComponent(JSON.stringify(_.pluck(newUsers, 'nick'))))
    request(nickClashesURI, function(e,r,b) {
      if (checkResponseError(e, r, b, 200, nickClashesURI)) return

      var takenNicks = JSON.parse(b).rows.map(function(r) {  return r.key })

      newUsers.forEach(function(ur) {
        if (~takenNicks.indexOf(ur.nick)) {
          ur.nickClash = 2
          clientUpdates.push(ur)
        } else {
          ur.nickClash = 1
        }
      })

      serverUpdates = serverUpdates.concat(_.map(newUsers, function(ur) {
        return {
          type: 'USER'
          , _id: ur.id
          , nick: ur.nick
          , password: ur.password
          , nickClash: ur.nickClash
          , assignmentFlags: {}
        }
      }))

      newUsersProcessed = true
      callback()
    })
  }

  var processExistingUsers = function(existingUrs, fn) {
    // ur.{assignmentFlags}.{pupil}.{pin}.{pin_type / LAST_COMPLETED} = {date}

    var updatedAppUsers = []
    var updatedPupils = {} // pupilId: appUserId

    existingUrs.forEach(function(ur, urIx) {
      var serverAssignments = ur.server.assignmentFlags || {} // ur.server.assignmentFlags should always exist but just in case...
      var clientAssignments = ur.client.assignmentFlags
      var completions = {}

      if (clientAssignments) {
        // first remove properties from client that no longer exist on server
        // (this step should be unnecessary but feeling too cautious to take it out!)
        Object.keys(clientAssignments).forEach(function(pupilId) {
          if (!serverAssignments[pupilId]) return delete clientAssignments[pupilId]
          Object.keys(clientAssignments[pupilId]).forEach(function(pinId) {
            if (!serverAssignments[pupilId][pinId]) return delete clientAssignments[pupilId][pinId]
            Object.keys(clientAssignments[pupilId][pinId]).forEach(function(flagType) {
              if (!serverAssignments[pupilId][pinId][flagType]) delete clientAssignments[pupilId][pinId][flagType]
            })
          })
        })

        // get client pin completions since last sync
        Object.keys(clientAssignments).forEach(function(pupilId) {
          Object.keys(clientAssignments[pupilId]).forEach(function(pinId) {
            if (typeof clientAssignments[pupilId][pinId]['LAST_COMPLETED'] == 'number') {
              completions[pinId] = clientAssignments[pupilId][pinId]['LAST_COMPLETED']
            }
          })
        })
      }

      // update serverAssignments by removing pins completed later than date assigned to pupil
      Object.keys(completions).forEach(function(pinId) {
        var completedAt = completions[pinId]
        Object.keys(serverAssignments).forEach(function(pupilId) {
          var pinFlags = serverAssignments[pupilId][pinId]
          if (!pinFlags) return
          Object.keys(pinFlags).forEach(function(flagType) {
            if (flagType != 'LAST_COMPLETED' && completedAt > pinFlags[flagType]) {
              delete pinFlags[flagType]
              if (!~updatedAppUsers.indexOf(ur.server)) updatedAppUsers.push(ur.server)
              updatedPupils[pupilId] = ur.server.assignmentFlags[pupilId]
            }
          })
          if (!Object.keys(pinFlags).length) delete serverAssignments[pupilId][pinId]
          if (!Object.keys(serverAssignments[pupilId]).length) delete serverAssignments[pupilId]
        })
      })


      // update client assignments to match server
      ur.client.assignmentFlags = serverAssignments

      // and have assignment flags sent back down to client
      clientUpdates.push(ur.client)
    })


    var updatedPupilIds = Object.keys(updatedPupils)
    if (updatedPupilIds.length) {
      var reqOpts = {
        uri: sapDbURI + '/_all_docs?include_docs=true',
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ keys: updatedPupilIds })
      }

      request(reqOpts, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc != 200) return console.log('syncUsers: error retreiving pupils: uri=%s (sc=%s, e=%s, b=%s)', reqOpts.uri, sc, e, b.replace(/\s*$/, ''))
        
        var pupils = JSON.parse(b).rows
          .map(function(r) { return r.doc })
          .filter(function(d) { return !!d }) // TODO: log error if missing

        pupils.forEach(function(pupil) {
          var ur = existingUrs.filter(function(ur) { return ur.server._id == pupil.appUser })
          if (ur) {
            var appUr = ur[0].server
            pupil.assignmentFlags = appUr.assignmentFlags[pupil._id] || {}
          }
        })

        var updatePupilsReq = {
          uri: sapDbURI + '/_bulk_docs',
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ docs: pupils })
        }

         request(updatePupilsReq, function(e,r,b) {
           var sc = r && r.statusCode
           if (sc != 201) {
             return console.log('error updating pupils: %s', JSON.stringify(updatePupilsReq,null,2).replace(/\n/g, '\n\t'))
           }
         })
      })
    }

    serverUpdates = serverUpdates.concat(updatedAppUsers)
    existingUsersProcessed = true
    fn()
  }

  var onProcessUserGroup = function onProcessUserGroup() {
    if (!hasSentResponse && newUsersProcessed && existingUsersProcessed) {
      callback(null, 200, clientUpdates)
      hasSentResponse = true

      if (serverUpdates.length) {
        var uri = util.format('%s/_bulk_docs', databaseURI)
        request({
          uri: uri
          , method: 'POST'
          , headers: { 'content-type':'application/json', accepts:'application/json' }
          , body: JSON.stringify({ docs: serverUpdates })
        }, function(e,r,b) {
          console.log('\nonProcessUserGroup:', r.statusCode, JSON.stringify(JSON.parse(b, null, 2)))
          checkResponseError(e, r, b, 201, uri)
        })
      }
    }
  }

  var getExistingUsersURI = util.format('%s/_all_docs?include_docs=true&keys=%s', databaseURI, encodeURI(JSON.stringify(urIds)))

  request(getExistingUsersURI, function(e,r,b) {
    if (checkResponseError(e, r, b, 200, getExistingUsersURI)) return

    var existingUsers = []
    var newUsers = []

    JSON.parse(b).rows.forEach(function(row, i) {
      var clientUr = clientDeviceUsers[i]

      if (row.doc) {
        existingUsers.push({ server: row.doc, client: clientUr })
      } else {
        newUsers.push(clientUr)
      }
    })

    processExistingUsers(existingUsers, onProcessUserGroup)
    processNewUsers(newUsers, onProcessUserGroup)
  })
}

function userMatchingNick(nick, callback) {
  queryView('users-by-nick', 'key', nick, 'include_docs', true, callback)
}

function userMatchingNickAndPassword(nick, password, callback) {
  queryView('users-by-nick-password', 'key', [nick,password], 'include_docs', true, callback)
}

function getDoc(id, callback) {
  request({
    uri: util.format('%s/%s', databaseURI, id)
    , headers: { 'content-type':'application/json', accepts:'application/json' }
  }, callback)
}

function queryView(view) {
  var uri = util.format('%s/_design/%s/_view/%s', databaseURI, designDoc, view)
  var callbackIx = arguments.length - 1
  var callback = arguments[callbackIx]

  if ('function' != typeof callback) return

  for (var i=1; i<callbackIx; i+=2) {
    uri += i == 1 ? '?' : '&'

    var field = arguments[i]
    if ('string' != typeof field) {
      callback(util.format('argument at index %d is not a string.', i))
      return
    }
    uri += field + '='

    var value = arguments[i+1]
    uri += (function rec(val) {
      var valType = Object.prototype.toString.call(val).match(/^\[object ([a-z]+)\]$/i)[1]

      switch (valType) {
        case 'Null':
          case 'Undefined':
          return 'null'
        case 'Number':
          case 'Boolean':
          return val.toString()
        case 'String':
          return '%22' + encodeURIComponent(val) + '%22'
        case 'Array':
          return '%5B' + _.map(val, function(el) { return rec(el) }).join(',') + '%5D'
        case 'Object':
          // Empty object comes after everything else, so is used for endkey. e.g. startkey=['A']&endkey=['A',{}] will return all keys with 'A' as first element
          return Object.keys(val).length === 0 ? '%7B%7D' : '#'
        default:
          return '#'
      }
    })(value)

    if (~uri.indexOf('#')) {
      callback(util.format('argument at index %d is invalid.', i+1))
      return
    }
  }
  request.get(uri, callback)
}
