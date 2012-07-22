var WebSocketServer = require('ws').Server
  , kcmEmitter = require('./model/kcm-emitter')
  , format = require('util').format


exports.create = function(server, config) {
  var wss = new WebSocketServer({
    server:server
    , verifyClient: function(ws, fn) {
      server.sessionService.get(ws.req, function(e, session) {
        fn(session != null)
      })
    }
  })

  wss.on('connection', function(ws) {

    ws.on('message', function(data) {
      console.log('message:', data)
    })
  })

  wss.broadcast = function(message) {
    wss.clients.forEach(function(ws) {
    })
  }

  var kcmChangesStream = kcmEmitter.create(format('%s/%s', config.couchServerURI.match(/^(.+[^\/])\/?$/)[1], config.authoring.kcmDatabaseName))

  kcmChangesStream.on('change', function(change) {
    wss.clients.forEach(function(ws) {
      server.sessionService.get(ws.upgradeReq, function(e, session) {
        if (!session) ws.terminate()
        else ws.send(JSON.stringify(change))
      })
    })
  })

  return wss
}
