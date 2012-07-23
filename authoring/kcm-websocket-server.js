var WebSocketServer = require('ws').Server
  , util = require('util')

module.exports = KCMWebSocketServer
util.inherits(KCMWebSocketServer, WebSocketServer)

function KCMWebSocketServer(kcm, sessionService, options, callback) {
  var self = this

  WebSocketServer.call(self, options, callback)

  this.sessionService = sessionService
  this.kcmChanges = []

  kcm.on('change', function(change) {
    self.kcmChanges.push(change)

    self.kcmStreamSubscribers(function(subscribers) {
      subscribers.forEach(function(ws) {
        ws.send(JSON.stringify(change))
      })
    })
  })

  this.on('connection', function(ws) {
    ws.on('message', function(message) {
      self.verifiedClientSession(ws, function(session) {
        var i = 0
          , change

        if (session) {
          try {
            message = JSON.parse(message)
          } catch (e) {
            return
          }

          if (message.event == 'subscribe-kcm-changes' && message.update_seq === parseInt(message.update_seq, 10)) {
            while(null != (change = self.kcmChanges[i++])) {
              if (change.seq > message.update_seq) ws.send(JSON.stringify(change))
            }
            ws.update_seq = message.update_seq
          }
        }
      })
    })
  })
}

KCMWebSocketServer.prototype.verifiedClientSession = function(ws, callback)  {
  this.sessionService.get(ws.upgradeReq, function(e, session) {
    if (!session) ws.terminate()
    callback(session)
  })
}

KCMWebSocketServer.prototype.kcmStreamSubscribers = function(callback) {
  var self = this
    , subscribers = []
    , remaining = this.clients.length

  if (!remaining) {
    callback(subscribers)
  } else {
    this.clients.forEach(function(ws) {
      self.verifiedClientSession(ws, function(session) {
        if (session && typeof ws.update_seq == 'number') subscribers.push(ws)
        if (!--remaining) callback(subscribers)
      })
    })
  }
}
