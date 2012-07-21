var request = require('request')
  , events = require('events')
  , querystring = require('querystring')
  , format = require('util').format

exports.createEmitter = function(uri, since) {
  if (uri[uri.length - 1] == '/') uri = uri.slice(0, ur.length-1)

  var changesStream = new events.EventEmitter()
  changesStream.since = since || 0
  changesStream.buffer = ''
  changesStream.writable = true

  changesStream.write = function (chunk) {
    var line
      , change
      , nlIx

    changesStream.buffer += chunk.toString()

    while (~(nlIx = changesStream.buffer.indexOf('\n'))) {
      line = changesStream.buffer.slice(0, nlIx)

      if (line.length > 1) {
        change = JSON.parse(line)
        if (change.last_seq) {
          changesStream.since = change.last_seq
        } else if (change.seq) {
          changesStream.since = change.seq
          changesStream.emit('change', change)
        }
      }

      changesStream.buffer = changesStream.buffer.slice(nlIx+1)
    }
  }

  changesStream.end = function(){}

  ;(function connect() {
    var qs = querystring.stringify({
      include_docs: "true"
      , heartbeat: 5000
      , feed: 'continuous'
      , since: changesStream.since
    })

    var r = request({
      uri: format('%s/_changes?%s', uri, qs)
      , headers: {'content-type':'application/json', connection:'keep-alive'}
    })

    r.pipe(changesStream)

    r.on('end', function () {
      connect()
      changesStream.emit('close')
    })
  })()

  return changesStream
}
