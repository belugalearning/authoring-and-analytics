var request = require('request')
  , util = require('util')
  , events = require('events')
  , _ = require('underscore')
  , CouchDbStream = require('./lib/couchdb-changes')

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
    , users: {}
  }

  var populate = function populate(callback) {
    if (!self.retryingPopulate) console.log('kcm populate...')
    request.get(self.dbURI + '/_all_docs?include_docs=true', function(e,r,b) {
      if (!r || r.statusCode != 200) {
        if (!self.retryingPopulate) console.log('KCM populate error - failed to retrieve KCM all docs -- cycle retries\n\te: %s\n\tsc: %s\n\tb: %s', e, r && r.statusCode, b)
        self.retryingPopulate = true
        setTimeout(function() { populate(callback) }, 2000)
        return
      }

      self.retryingPopulate = false

      JSON.parse(b).rows.forEach(function(row) {
        var o = self.storeForDoc(row.doc)
        if (o) o[row.id] = row.doc
      })

      callback()
    })
  }

  var getUpdateSeq = function getUpdateSeq(callback) {
    if (!self.retryingGetUpdateSeq) console.log('kcm get update_seq ...')
    request.get(self.dbURI, function(e,r,b) {
      var sc = r && r.statusCode
      if (sc != 200) {
        if (!self.retryingGetUpdateSeq) console.log('KCM get update_seq error - getting update_seq -- cycle retries\n\te: %s\n\tsc: %s\n\tb: %s', e, sc, b)
        self.retryingGetUpdateSeq = true
        setTimeout(function() { getUpdateSeq(callback) }, 2000)
        return
      }

      self.update_seq = JSON.parse(b).update_seq
      callback()
    })
  }

  var initChangesFeed = function initChangesFeed() {
    console.log('kcm setup changes feed ...')
    var changes = new CouchDbStream(self.dbURI, { since:self.update_seq })
      .on('change', function(change) {
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
      .on('error', function(e) { })
  }

  populate(function() {
    getUpdateSeq(function() {
      // set up db _changes stream listen / emit
      initChangesFeed()
      self.initialised = true
      self.emit('initialised')
    })
  })
}

KCM.prototype.storeForDocType = function(type) {
  return ({
    'concept node': this.docStores.nodes
    , 'pipeline': this.docStores.pipelines
    , 'problem': this.docStores.problems
    , 'relation': this.docStores.relations
    , 'tool': this.docStores.tools
    , 'User': this.docStores.users
  })[type]
}

KCM.prototype.storeForDoc = function(doc) {
  return this.storeForDocType(doc.type)
}

KCM.prototype.getDoc = function(id, type) {
  var store

  if (type) {
    store = this.storeForDocType(type)
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

KCM.prototype.cloneDocByTypeName = function(type, name) {
  var store = this.storeForDocType(type)
  if (store) {
    for (var id in store) {
      if (store[id].name == name) {
        return this.getDocClone(id, type)
      }
    }
  }
}

KCM.prototype.cloneRelationNamed = function(name) {
  return this.cloneDocByTypeName('relation', name)
}

KCM.prototype.chainedBinaryRelationMembers = function(cbr) {
  var self = this
    , br
    , tailIx, headIx
    , chains, extensions, aggregates, members

  for (var i=0; i<cbr.chain.length; i++) {
    br = self.docStores.relations[cbr.chain[i].relation]
    tailIx = cbr.chain[i].direction == 1 ? 0 : 1
    headIx = 1 - tailIx

    if (i == 0) {
      chains = br.members.map(function(pair) { return [ pair[tailIx], pair[headIx] ] })
    } else {
      extensions = []
      chains.forEach(function(chain) {
        br.members.forEach(function(pair) {
          if (chain[i] == pair[tailIx] && !~chain.indexOf(pair[headIx])) {
            extensions.push(chain.concat(pair[headIx]))
          }
        })
      })
      chains = extensions
    }
    if (!chains.length) return []
  }

  aggregates = {}
  chains.forEach(function(chain) {
    var ends = [ chain[0], chain[cbr.chain.length] ]
      , key = ends.join()

    if (!aggregates[key]) aggregates[key] = ends.concat(1)
    else aggregates[key][2]++
  })

  members = []
  for (var key in aggregates) members.push(aggregates[key])
  return members
}
