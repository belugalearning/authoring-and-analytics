var express = require('express')
  , _ = require('underscore')
  , format = require('util').format
  , request = require('request')

module.exports = WebPortal

function WebPortal(config) {
  var self = this
    , port = parseInt(config.webPortal && config.webPortal.port, 10)
    , server = self.server = express.createServer()

  if (!(port > 0)) {
    console.log("Could not start WebPortal Express Server as valid port not set in config.json")
    return
  }

  server.configure(function() {
    server.set('views', __dirname + '/views')
    server.set('view engine', 'jade')
    server.set('view options', { layout: false })

    server.use(express.favicon())
    server.use(express.bodyParser())
    server.use(express.cookieParser())
    //server.use(server.sessionService.httpRequestHandler)
    server.use(express.methodOverride())
    server.use(server.router)
    server.use(express.static(__dirname + '/public'))
  })

  self.config = config
  server.listen(port)

  console.log("WebPortal Express server listening on port %d in %s mode", server.address().port, server.settings.env)

  server.get('/', function(req,res) {
    self.usersByNick(function(e,r,b) {
      var users
        , sc = r && r.statusCode || 500

      if (sc === 200) {
        users = _.map(JSON.parse(b).rows, function(r) { return { id:r.id, name:r.key } })
        res.render('replay', { title:'Problem Attempt Replay', users:users })
      } else {
        res.send(format('error retrieving users. Error="%s" SortCode=%d', e, sc), sc)
      }
    })
  })
  server.get('/pa-events-for-user/:urId', function(req,res) {
    self.paEventsForUr(req.params.urId, function(e,r,b) {
      var sc = r && r.statusCode || 500
        , body
      if (sc === 200) body = JSON.parse(b).rows
      else body = format('error retrieving problem attempt events for user="%s". Error="%s" SortCode=%d', ur, e, sc)
      res.send(body, sc)
    })
  })
}

WebPortal.prototype.paEventsForUr = function(ur, callback) {
  var qs = {
    startkey: [ ur, {} ]
    , endkey: [ ur ]
    , descending: true
  }
  var url = format( '%s/%s/_design/user-related-views/_view/problemattempt-events-by-user-date%s', this.config.couchServerURI, this.config.appWebService.loggingService.databaseName, formatQueryString(qs) )
  request.get(url, callback)
}

WebPortal.prototype.usersByNick = function(callback) {
  var url = format('%s/%s/_design/%s/_view/users-by-nick', this.config.couchServerURI, this.config.appWebService.usersService.databaseName, this.config.appWebService.usersService.databaseDesignDoc)
  request.get(url, callback)
}

function formatQueryString(o) {
  return '?' +
    _.map(o, function(val, key) {
      return key + '=' + encodeURIComponent( JSON.stringify(val) )
    }).join('&')
}

