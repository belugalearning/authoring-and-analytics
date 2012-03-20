var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
;

var contentDatabase = 'temp-blm-users/'
  , designDoc = 'users-views'
  , couchServerURI
  , databaseURI
;

module.exports = function(serverURI) {
    couchServerURI = serverURI;
    databaseURI = serverURI + contentDatabase;
    console.log("users module: databaseURI =", databaseURI);
    
    return {
        queryView: queryView
        , updateViews: updateViews
    };
};

function queryView(view, callback) {
    getDoc('_design/' + designDoc + '/_view/' + view, callback);
};

// Views
function updateViews(callback) {
    var contentViews = {
        _id: '_design/' + designDoc
        , views: {
            'users-by-name': {
                map: (function(doc) { if (doc.type == 'user') emit(doc.nickName, null); }).toString()
            }
            , 'devices': {
                map: (function(doc) { if (doc.type == 'device') emit(doc.firstLaunchDateTime, null); }).toString()
            }
            /*, 'users-by-device': {
                map: (function(doc) { if (doc.type == 'device') emit(doc.name, null); }).toString()
            }*/
        }
    };

    getDoc(contentViews._id, validatedResponseCallback([200,404], function(e,r,b) {
        var writeViews = r.statusCode === 404 ? insertDoc : updateDoc;
        if (r.statusCode === 200) contentViews._rev = JSON.parse(b)._rev;
        
        writeViews(contentViews, validatedResponseCallback(201, callback));
    }));
}

function getDoc(id, callback) {
    request({
        uri: databaseURI + id
        , headers: { 'content-type':'application/json', accepts:'application/json' }
    }, callback);
};

function insertDoc(doc, callback) {
    request({
        method: 'POST'
        , uri: databaseURI
        , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
        , body: JSON.stringify(doc)
    }, callback);
}

function updateDoc(doc, callback) {
    request({
        method:'PUT'
        , uri: databaseURI + doc._id 
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify(doc)
    }, callback);
};

function validatedResponseCallback(validStatusCodes, callback) {
    if (typeof validStatusCodes == 'number') validStatusCodes = [ validStatusCodes ];
    return function(e, r, b) {
        if (!e && r && validStatusCodes.indexOf(r.statusCode) > -1) {
            if (typeof callback == 'function') callback(e,r,b);
            return;
        }
        console.log('\nHTTP RESPONSE ERROR');
        console.log('\tError:', e);
        console.log('\tValid Status Codes:', validStatusCodes);
        console.log('\tActual Status Code:', (r && r.statusCode || ''));
        console.log('\tBody:', b);
        process.exit(1);
    }
}
