// CONSTANT
//var serverURI = 'http://localhost:5984/'//'http://109.109.247.18:5984/';
var serverURI = 'http://sofaraslant.com:5984/'
var databaseURI = serverURI + 'blm-users/';

// module dependencies
var request = require('request');

module.exports = {
    getDoc: getDoc
    , queryView: queryView
    , insertDoc: insertDoc
    , insertDocs: insertDocs
    , deleteAllDocs: deleteAllDocs
    , deleteDoc: deleteDoc
    , createViews: createViews
    , updateView: function() {
        var id = "_design/user-views";
        var code = {
            "_id": id
            , "views": {
                "all-users": {
                    "map": "function(doc) { if (doc.type == 'user')  emit(doc.nickName, doc) }"
                }
                ,"problem-attempts": {
                    "map": "function(doc) { if (doc.type == 'problem-attempt')  emit(doc.endDateTime, doc) }"
                }
            }
        };
        getDoc(id, function(e,r,b){
            code._rev = JSON.parse(b)._rev;
            request({
                method:'PUT'
                , uri: databaseURI + id
                , headers: { 'content-type':'application/json', accepts:'application/json' }
                , body: JSON.stringify(code)
            }, function(e, r, b) {
                if (typeof callback != 'function') { return; }
                callback(e, r.statusCode, !e && r.statusCode === 201 && JSON.parse(b).rev || null);
            });
        });

    }
    , getZubiForDocId: function(id, callback) {
        request({
            method:'GET'
            , uri: databaseURI + id + '/zubi_screenshot.png'
        }, function(e,r,b) {
            if (typeof callback == 'function') callback(e,r,b);
        });
    }
    , content: require('./content')(serverURI)
    , users: require('./users')(serverURI)
};

function getDoc(id, callback) {
    request({
        uri: databaseURI + id
        , headers: { 'content-type':'application/json', accepts:'application/json' }
    }, callback);
}

function queryView(designDoc, view, callback) {
    getDoc('_design/' + designDoc + '/_view/' + view, callback);
}

function insertDoc(doc, callback) {
    request({
        method: 'POST'
        , uri: databaseURI
        , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
        , body: JSON.stringify(doc)
    }, callback);
}

function insertDocs(docs, callback) {
    var errors = []
      , successes = [] 
      , docCount = docs.length
      , done = 0
    ;

    docs.forEach(function(doc, index) {
        insertDoc(doc, function(err, res, body) {
            if (err || res.statusCode !== 201) {
                errors.push({ doc: doc, error: err, statusCode: res.statusCode });
            } else {
                successes.push({ doc: doc, body: body });
            }
            if (++done == docCount && typeof callback == 'function') callback(errors.length && errors || null, successes);
        });
    });
}

function deleteDoc(id, rev, callback) {
    request({
        method: "DELETE"
        , uri: databaseURI + id + '?rev=' + rev
    }, callback);
}

function deleteAllDocs(callback) {
    request.get(databaseURI + '_all_docs', function(err, res, body) {
        var bodyObj = JSON.parse(body)
          , rows = bodyObj.rows
          , todo = bodyObj.total_rows
          , done = 0;

        if (!todo && typeof callback == 'function') { callback(0, 0); }

        while (rows.length) { 
            var row = rows.pop();
            request({
                method: "DELETE"
                , uri: databaseURI + row.id + '?rev=' + row.value.rev
            }, function(e, r, b) {
                if (!e && r.statusCode == 200) { ++done; }
                if (!--todo && typeof callback == 'function') { callback(bodyObj.total_rows - done, done);  }
            });
        }
    });
}

function createViews() {
    insertDoc({
        "_id":"_design/user-views",
        "views": {
            "all-users": {
                "map": "function(doc) { if (doc.Type == 'user')  emit(doc.nickName, doc) }"
            }
        }
    }
    , function(err, res, body) {
        console.log('createViews() - insertDoc() callback: ', err, body);
    });
}

