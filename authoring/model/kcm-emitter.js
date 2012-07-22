var createEmitter = require('./couchdb-emitter').createEmitter

exports.create = function(kcmURI, since) {
  var dbEmitter = createEmitter(kcmURI, since)
  // 'concept node' 'pipeline' 'problem'??? 'relation'
  return dbEmitter
}
