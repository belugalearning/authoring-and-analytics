var request = require('request')
  , events = require('events')
  , querystring = require('querystring')
  , util = require('util')

module.exports = CouchDBStream
util.inherits(CouchDBStream, events.EventEmitter)

function CouchDBStream(uri, since) {
  var self = this

  events.EventEmitter.call(this)

  this.since = since || 0
  this.buffer = ''
  this.writable = true

  ;(function connect() {
    var qs = querystring.stringify({
      include_docs: "true"
      , heartbeat: 5000
      , feed: 'continuous'
      , since: self.since
    })

    var r = request({
      uri: util.format('%s/_changes?%s', uri, qs)
      , headers: {'content-type':'application/json', connection:'keep-alive'}
    })

    r.pipe(self)

    r.on('end', function () {
      connect()
      self.emit('close')
    })
  })()
}

CouchDBStream.prototype.write = function(chunk) {
  var line
    , change
    , nlIx

  this.buffer += chunk.toString()

  while (~(nlIx = this.buffer.indexOf('\n'))) {
    line = this.buffer.slice(0, nlIx)

    if (line.length > 1) {
      change = JSON.parse(line)
      if (change.last_seq) {
        this.since = change.last_seq
      } else if (change.seq) {
        this.since = change.seq
        this.emit('change', change)
      }
    }

    this.buffer = this.buffer.slice(nlIx+1)
  }
}

CouchDBStream.prototype.end = function(){}
