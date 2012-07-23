var request = require('request')
  , util = require('util')
  , events = require('events')
  , _ = require('underscore')
  , CouchDBStream = require('./couchdb-stream')

var format = util.format

module.exports = KCM
util.inherits(KCM, events.EventEmitter)

function KCM(config) {
  var self = this
  events.EventEmitter.call(self)

  self.couchServerURI = config.couchServerURI.match(/^(.+[^\/])\/?$/)[1]
  self.databaseName = config.authoring.kcmDatabaseName.match(/^(.+[^\/])\/?$/)[1]
  self.dbURI = format('%s/%s', self.couchServerURI, self.databaseName)

  self.nodes = {}
  self.pipelines = {}
  self.relations = {}
  self.tools = {}

  request.get(self.dbURI + '/_all_docs?include_docs=true', function(e,r,b) {
    if (!r || r.statusCode != 200) {
      // TODO: handle
      return
    }

    JSON.parse(b).rows.forEach(function(row) {
      var o = self.docStore(row.doc)
      if (o) o[row.id] = row.doc
    })

    request.get(self.dbURI, function(e,r,b) {
      var couchDBStream

      if (!r || r.statusCode != 200) {
        //TODO: handle
        return
      }

      self.update_seq = JSON.parse(b).update_seq

      couchDBStream = new CouchDBStream(self.dbURI, self.update_seq)
      couchDBStream.on('change', function(change) {
        self.seq = change.seq
        
        var store = self.docStore(change.doc)
        if (store) {
          if (change.delete === true) delete store[change.doc._id]
          else store[change.doc._id] = change.doc
          self.emit('change', change)
        }
      })
    })
  })
}

KCM.prototype.docStore = function(doc) {
  return ({
    'concept node': this.nodes
    , 'pipeline': this.pipelines
    , 'problem': this.problems
    , 'relation': this.relations
    , 'tool': this.tools
  })[doc.type]
}
