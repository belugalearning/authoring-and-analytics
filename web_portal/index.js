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

  //problemattempt-events-by-user-date

  var qs = {
    startkey: [ "25D71875-F035-4A47-87A3-82D9B7EEF55E", {} ]
    , endkey: [ "25D71875-F035-4A47-87A3-82D9B7EEF55E" ]
    , descending: true
  }

  url = format( '%s/%s/_design/%s/_view/%s%s', config.couchServerURI, config.appWebService.loggingService.databaseName, 'user-related-views', 'problemattempt-events-by-user-date', formatQueryString(qs) )

  server.get('/', function(req,res) {
    request.get(url, function(e,r,b) {
      if (!r || r.statusCode != 200) {
        res.send(e, 500)
        return
      }
      res.render('replay', { title:'Problem Attempt Replay', replayData:JSON.parse(b).rows })
    })
  })
}

function formatQueryString(o) {
  return '?' +
    _.map(o, function(val, key) {
      return key + '=' + encodeURIComponent( JSON.stringify(val) )
    }).join('&')
}
