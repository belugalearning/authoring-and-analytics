var express = require('express')
  , async = require('async')
  , util = require('util')
  , request = require('request')
  , loggingServiceInit = require('./logging_service')
  , usersServiceInit = require('./users_service')

var server = express.createServer()

// server config
server.configure(function() {
  server.use(express.bodyParser())
  server.use(express.cookieParser())
  server.use(express.methodOverride())
  server.use(server.router)
})

server.configure('development', function(){
  server.use(express.errorHandler({ dumpExceptions: true, showStack: true }))
})

server.configure('production', function(){
  server.use(express.errorHandler())
})

module.exports = function(config) {
  // logging service
  server.post('/app-logging/upload', loggingServiceInit(config))

  // users service
  var usersService = usersServiceInit(config)
  server.post('/app-users/sync-users', usersService.syncUsers)
  server.post('/app-users/check-nick-available', usersService.checkNickAvailable)
  server.post('/app-users/get-user-matching-nick-password', usersService.getUserMatchingNickAndPassword)
  server.post('/app-users/change-user-nick', usersService.changeNick)
  server.get('/app-users/:userId/state', usersService.getState)

  server.put('/app-users/:userId/tokens/:token', function(req, res) {
    var sapDbURI = config.couchServerURI + '/sap-dev-1'
      , appUserId = req.params.userId
      , appUserURI = util.format('%s/%s/%s', config.couchServerURI, config.appWebService.usersService.databaseName, appUserId)
      , appUser
      , token = req.params.token
      , pupil
      , successText

    async.waterfall([
      getAppUser,
      getPupil,
      getSuccessText,
      updatePupil,
      function() { res.send(successText, 201) }
    ])

    function getAppUser(callback) {
      request(appUserURI, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc != 200) return res.send('error retrieving app user data', sc || 500)
        appUser = JSON.parse(b)
        callback()
      })
    }

    function getPupil(callback) {
      var uri = util.format('%s/_design/general-views/_view/pupils-by-token?key=%22%s%22&include_docs=true', sapDbURI, encodeURIComponent(token))
      request(uri, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc != 200) return res.send('error retreiving schools data', sc || 500)

        var row = JSON.parse(b).rows[0]
        if (!row) return res.send('Invalid token', 400)

        pupil = row.doc
        callback()
      })
    }

    function getSuccessText(callback) {
      var keys = JSON.stringify([ pupil.school, pupil.classroom ])
      var uri = util.format('%s/_design/general-views/_view/name?keys=%s', sapDbURI, encodeURIComponent(keys))
      request(uri, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc != 200) return res.send('error retrieving schools data', sc || 500)

        var rows = JSON.parse(b).rows
        successText = util.format('Your account has been linked to "%s" in class "%s" at %s', pupil.firstName + ' ' + pupil.lastName, rows[1].value, rows[0].value)

        callback()
      })
    }

    function updatePupil(callback) {
      pupil.token = null
      pupil.appUser = appUserId
      pupil.appUsername = appUser.nick

      var req = {
        uri: util.format('%s/%s', sapDbURI, pupil._id),
        method: 'PUT',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(pupil)
      }

      request(req, function(e,r,b) {
        var sc = r && r.statusCode
        if (sc != 201) return res.send('error updating schools data', sc || 500)
        callback()
      })
    }
  })

  // listen
  var port = parseInt(config.appWebService.port, 10) || 3000
  server.listen(port)
  console.log("AppWebService Express server listening on port %d in %s mode", server.address().port, server.settings.env)

  return server
}
