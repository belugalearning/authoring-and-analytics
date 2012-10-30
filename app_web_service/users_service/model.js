var util = require('util')
  , request = require('request')
  , _ = require('underscore')

var couchServerURI
  , dbName
  , designDoc
  , databaseURI
  , viewsPath

module.exports = function(config) {
  couchServerURI = config.couchServerURI.replace(/^(.+[^/])\/*$/, '$1/')
  dbName = config.appWebService.usersService.databaseName
  designDoc = config.appWebService.usersService.databaseDesignDoc
  databaseURI = couchServerURI + dbName + '/'
  viewsPath = databaseURI + '_design/' + designDoc + '/_view/'
  console.log(util.format('AppWebService -> usersService\tdesignDoc="%s"\tdatabaseURI="%s"', designDoc, databaseURI))

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

  return {
    syncUsers: syncUsers
    , userMatchingNick: userMatchingNick
    , userMatchingNickAndPassword: userMatchingNickAndPassword
  }
}

function syncUsers(clientDeviceUsers, callback) {
  var urIds = _.pluck(clientDeviceUsers, 'id')

  request(encodeURI(databaseURI + '_all_docs?include_docs=true&keys=' + JSON.stringify(urIds)), function(e,r,b) {
    if (!r || 200 != r.statusCode) {
      callback(e, r && r.statusCode || 500)
      return
    }

    var existingUsers = _.filter(_.pluck(JSON.parse(b).rows, 'doc'), function(d) { return d != null }) // users that already exist on the server
      , newUserIds = _.difference(urIds, _.pluck(existingUsers, '_id')) // users that need adding to the server
      , newUsers = _.map(newUserIds, function(id) { return _.find(clientDeviceUsers, function(ur) { return ur.id == id }) })

    // mapped array with client/server update status for each user that already exists on the server and the concatenated completed nodes of client/server
    var existingUsersData = _.map(existingUsers, function(serverUr) {
      var clientUr = _.find(clientDeviceUsers, function(u) { return u.id == serverUr._id })

      if (!clientUr.nodesCompleted) clientUr.nodesCompleted = []
      if (!serverUr.nodesCompleted) serverUr.nodesCompleted = []

      // remove duplicates from nodesCompleted arrays
      clientUr.nodesCompleted = _.uniq(clientUr.nodesCompleted)
      serverUr.nodesCompleted = _.uniq(serverUr.nodesCompleted)

      var nodesCompleted = _.uniq(serverUr.nodesCompleted.concat(clientUr.nodesCompleted))
        , numNodesCompleted = nodesCompleted.length
      
      return {
        clientUr: clientUr
        , serverUr: serverUr
        , updateOnClient: clientUr.nodesCompleted.length < numNodesCompleted
        , updateOnServer: serverUr.nodesCompleted.length < numNodesCompleted
        , nodesCompleted: nodesCompleted
      }
    })

    _.each(existingUsersData, function(urData) {
      urData.clientUr.nodesCompleted = urData.nodesCompleted
      urData.serverUr.nodesCompleted = urData.nodesCompleted
    })

    var clientUpdates = _.pluck( _.filter(existingUsersData, function(urData) { return urData.updateOnClient }), 'clientUr' )
    var serverUpdates = _.pluck( _.filter(existingUsersData, function(urData) { return urData.updateOnServer }), 'serverUr' )

    // call the callback function with the client updates
    callback(null, 200, clientUpdates)

    // update couch db if required
    var bulkUpdateDocs = _.map(newUsers, function(ur) {
      return {
        _id: ur.id
        , nick: ur.nick
        , password: ur.password
        , nodesCompleted: ur.nodesCompleted
        , type: 'USER'
      }
    }).concat(serverUpdates)

    if (bulkUpdateDocs.length) {
      request({
        uri: databaseURI + '_bulk_docs'
        , method: 'POST'
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify({ docs: bulkUpdateDocs })
      }, function(e,r,b) {
        // TODO: handle errors
      })
    }
  })
}

function userMatchingNick(nick, callback) {
  queryView('users-by-nick', 'key', nick, 'include_docs', true, callback)
}

function userMatchingNickAndPassword(nick, password, callback) {
  queryView('users-by-nick-password', 'key', [nick,password], 'include_docs', true, callback)
}

function updateDesignDoc(callback) {
  console.log('AppWebService -> usersService\tupdating design doc:\t%s_design/%s', databaseURI, designDoc)

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
        uri: databaseURI + dd._id
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
    uri: databaseURI + id
    , headers: { 'content-type':'application/json', accepts:'application/json' }
  }, callback)
}

function queryView(view) {
  var uri = viewsPath + view
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
