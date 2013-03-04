// very heavily indebted to: https://github.com/mikeal/dbemitter

var request = require('request')
  , events = require('events')
  , querystring = require('querystring')
  , util = require('util')

module.exports = CouchDBChanges
util.inherits(CouchDBChanges, events.EventEmitter)

function CouchDBChanges(dbURI, queryOptions) {
  var self = this

  self.query = {
    include_docs: true
    , heartbeat: 5000
    , feed: "continuous"
    , since: 0
  }

  queryOptions && Object.keys(queryOptions).forEach(function(key) {
    self.query[key] = queryOptions[key]
  })

  events.EventEmitter.call(self)
  self.buffer = ''
  self.writable = true

  ;(function connect() {
    self.input = request({
      uri: util.format('%s/_changes?%s', dbURI, querystring.stringify(self.query))
      , headers: {'content-type':'application/json', connection:'keep-alive'}
    })

    self.input.pipe(self)

    self.input.on('error', function(e) {
      self.emit('error', e)
      setTimeout(connect, 2000)
    })

    self.input.on('close', function() {
      self.emit('error', 'close')
      setTimeout(connect, 2000)
    })
  })()
}

CouchDBChanges.prototype.write = function(chunk) {
  var line
    , change
    , nlIx

  this.buffer += chunk.toString()

  while (~(nlIx = this.buffer.indexOf('\n'))) {
    line = this.buffer.slice(0, nlIx)

    if (line.length > 1) {
      change = JSON.parse(line)
      if (change.last_seq) {
        this.query.since = change.last_seq
      } else if (change.seq) {
        this.query.since = change.seq
        this.emit('change', change)
      }
    }

    this.buffer = this.buffer.slice(nlIx+1)
  }
}

CouchDBChanges.prototype.end = function() { console.log('end') }
