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

  self.initialised = false
  self.couchServerURI = config.couchServerURI.match(/^(.+[^\/])\/?$/)[1]
  self.databaseName = config.authoring.kcmDatabaseName.match(/^(.+[^\/])\/?$/)[1]
  self.dbURI = format('%s/%s', self.couchServerURI, self.databaseName)

  self.docStores = {
    nodes: {}
    , pipelines: {}
    , problems: {}
    , relations: {}
    , tools: {}
  }

  request.get(self.dbURI + '/_all_docs?include_docs=true', function(e,r,b) {
    if (!r || r.statusCode != 200) {
      // TODO: handle
      return
    }

    JSON.parse(b).rows.forEach(function(row) {
      var o = self.storeForDoc(row.doc)
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
        var store

        self.update_seq = change.seq
        
        if (change.deleted === true) {
          for (var key in self.docStores) {
            store = self.docStores[key]
            if (store[change.id]) {
              store[change.id]._rev = change.doc._rev
              store[change.id]._deleted = true
              change.doc = store[change.id]
              delete store[change.id]
              break
            }
          }
        } else {
          store = self.storeForDoc(change.doc)
          if (store) store[change.id] = change.doc
        }

        if (store) self.emit('change', change)
      })

      self.initialised = true
      self.emit('initialised')
    })
  })
}

KCM.prototype.storeForDoc = function(doc) {
  return ({
    'concept node': this.docStores.nodes
    , 'pipeline': this.docStores.pipelines
    , 'problem': this.docStores.problems
    , 'relation': this.docStores.relations
    , 'tool': this.docStores.tools
  })[doc.type]
}

KCM.prototype.getDoc = function(id, type) {
  var store
  if (type) {
    store = ({
      'concept node': this.docStores.nodes
      , 'pipeline': this.docStores.pipelines
      , 'problem': this.docStores.problems
      , 'relation': this.docStores.relations
      , 'tool': this.docStores.tools
    })[type]
  } else {
    for (var i=0; i<docStores.length; i++) {
      store = docStores[i]
      if (store[id]) break
    }
  }
  if (store) return store[id]
}

KCM.prototype.getDocClone = function(id, type) {
  var doc = this.getDoc(id, type)
  if (doc) return JSON.parse(JSON.stringify(doc))
}
