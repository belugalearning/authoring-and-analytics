var util = require('util')
  , request = require('request')
  , _ = require('underscore')

var couchServerURI
  , dbName
  , designDoc
  , databaseURI

module.exports = function(config) {
  couchServerURI = config.couchServerURI.replace(/^(.+[^/])\/*$/, '$1')
  dbName = config.appWebService.usersService.databaseName
  designDoc = config.appWebService.usersService.databaseDesignDoc
  databaseURI = util.format('%s/%s', couchServerURI, dbName)
  console.log(util.format('AppWebService -> usersService\tdesignDoc="%s"\tdatabaseURI="%s"', designDoc, databaseURI))

  /*
  request({
    method: 'PUT'
    , uri: databaseURI
    , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
  }, function(e,r,b) {
    if (!r) {
      console.log('Error - could not connect to Couch Server testing for existence of database:"%s"', databaseURI)
      return
    }
    if (!r || 201 != r.statusCode && 412 != r.statusCode) {
      console.log('Error other than "already exists" when attempting to create database:"%s". Error:"%s" StatusCode:%d', databaseURI, e, r && r.statusCode)
      return
    }

    updateDesignDoc(function(e,statusCode) {
      if (e || 201 != statusCode) {
        console.log('error updating %s design doc. error="%s", statusCode=%d', databaseURI, e, r.statusCode)
      }
    })
  })
  //*/

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

  var processNewUsers = function(newUserIds, callback) {
    // do their nicks clash? set nickClash=1 if no, nickClash=2 if yes
    // as such all new users will need updating on client
    // and being new users, they'll need adding to the server db too

    if (!newUserIds.length) {
      newUsersProcessed = true
      callback()
      return
    }

    var newUsers = _.map(
      newUserIds
      , function(id) { return _.find(clientDeviceUsers, function(ur) { return ur.id == id }) })

    var nickClashesURI = util.format('%s/_design/%s/_view/users-by-nick?keys=%s', databaseURI, designDoc, encodeURI(JSON.stringify(_.pluck(newUsers, 'nick'))))
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
        }
      }))

      newUsersProcessed = true
      callback()
    })
  }

  // this func currently doesn't do much. Not synching required for existing users - see comment at top of sycnUsers()
  var processExistingUsers = function(serverVersions, callback) {
    /*
    var clientVersions = _.map(
      serverVersions
      , function(serverUr) { _.find(clientDeviceUrs, function(clientUr) { return clientUr.id == serverUr._id }) })
    */
    existingUsersProcessed = true
    callback()
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
          checkResponseError(e, r, b, 201, uri)
        })
      }
    }
  }

  var getExistingUsersURI = util.format('%s/_all_docs?include_docs=true&keys=%s', databaseURI, encodeURI(JSON.stringify(urIds)))

  request(getExistingUsersURI, function(e,r,b) {
    if (checkResponseError(e, r, b, 200, getExistingUsersURI)) return

    // existing users
    var serverVersionExistingUrs =_.filter(
      _.pluck(JSON.parse(b).rows, 'doc')
      , function(d) { return d != null })

    processExistingUsers(serverVersionExistingUrs, onProcessUserGroup)

    // new users
    var newUserIds = _.difference(urIds , _.pluck(serverVersionExistingUrs, '_id')) // users that need adding to the server
    processNewUsers(newUserIds, onProcessUserGroup)
  })
}

function userMatchingNick(nick, callback) {
  queryView('users-by-nick', 'key', nick, 'include_docs', true, callback)
}

function userMatchingNickAndPassword(nick, password, callback) {
  queryView('users-by-nick-password', 'key', [nick,password], 'include_docs', true, callback)
}

function updateDesignDoc(callback) {
  console.log('AppWebService -> usersService\tupdating design doc:\t%s/_design/%s', databaseURI, designDoc)

  var dd = {
    _id: '_design/' + designDoc
    , views: {
      'users-by-nick' : {
        map: (function(doc) { if ('USER' == doc.type) emit(doc.nick, null) }).toString()
      }
      , 'users-by-nick-password' : {
        map: (function(doc) { if ('USER' == doc.type) emit([doc.nick, doc.password], null) }).toString()
      }
    }
  }

  getDoc(dd._id, function(e,r,b) {
    if (!r || !~[200,404].indexOf(r.statusCode)) {
      callback('error retrieving design doc from database', sc)
      return
    }

    if (r.statusCode == 200) dd._rev = JSON.parse(b)._rev

      request({
        uri: util.format('%s/%s', databaseURI, dd._id)
        , method: 'PUT'
        , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
        , body: JSON.stringify(dd)
      }, function(e,r,b) {
        callback(e, r && r.statusCode || 500)
      })
  })
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
