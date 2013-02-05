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
    self.paEventsForUr(req.params.urId, res)
  })
  server.get('/problem-attempt/:paId/static-generated-pdef', function(req,res) {
    var url = format('%s/%s/%s/static-generated-pdef.plist', self.config.couchServerURI, self.config.appWebService.loggingService.databaseName, req.params.paId)
    request({ url:url, 'Content-Type':'application/xml' }, function(e,r,b) {
      var sc = r && r.statusCode
      if (sc == 200) {
        req.query.download && req.query.download.toLowerCase() === 'true' && res.header('Content-Disposition', format('attachment; filename=static-generated-pdef-%s.plist', req.params.paId))
        res.header('Content-Type', 'application/xml')
        res.send(b)
      } else {
        res.send(format('could not retrieve plist.\nerror="%s"\nstatusCode=%d',e,sc), sc || 500)
      }
    })
  })
}

WebPortal.prototype.paEventsForUr = function(ur, res) {
  var urDbDirURI = format( '%s/%s/user-db-directory', this.config.couchServerURI, this.config.appWebService.loggingService.databaseName)
  request(urDbDirURI, function(e,r,b) {
    var sc = r && r.statusCode
    if (sc != 200) {
      res.send(sc)
      return
    }

    var urDbURI = JSON.parse(b).dbs[ur]
    if (!urDbURI) {
      res.send('user\'s logging database not found in directory', 404)
      return
    }

    var paEventsURI = format('%s/_design/user-related-views/_view/problemattempt-events-by-user-date?descending=true', urDbURI)
    request.get(paEventsURI).pipe(res)
  })
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

function niceConciseDate() {
    return new Date()
        .toJSON()
            .replace(/T/, '-')
                .replace(/\.[0-9]{3}Z$/, '')
}
