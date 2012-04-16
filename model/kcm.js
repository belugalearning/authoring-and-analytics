var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
;

var kcmDatabaseName = 'temp-map-d/'
  , designDoc = 'kcm-views'
  , couchServerURI
  , databaseURI
;

var relationTypes = ['binary'];

module.exports = function(serverURI) {
    couchServerURI = serverURI;
    databaseURI = serverURI + kcmDatabaseName;
    console.log("knowledge concept map model: databaseURI =", databaseURI);
    
    return {
        createDB: createDB
        , updateViews: updateViews
        , queryView: queryView
        , insertConceptNode: insertConceptNode
        , insertRelation: insertRelation
        , addOrderedPairToBinaryRelation: addOrderedPairToBinaryRelation
    };
};

function createDB(callback) {
    request({
        method: 'PUT'
        , uri: databaseURI
        , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
    }, function(e,r,b) {
        if (r.statusCode == 412) {
            console.log(util.format('could not create database. Database with name "%s" already exists', databaseURI));
            updateViews(callback);
        } else if (r.statusCode != 201) {
            if (!e) e = 'error creating database';
            if (typeof callback == 'function') callback(e,r,b);
            else console.log(util.format('%s with uri "%s". StatusCode=%d', e, databaseURI, r.statusCode));
            return;
        } else {
            console.log('created database at uri:', databaseURI);

            updateViews(function(e,r,b) {
                insertRelation({
                    relationType: 'binary'
                    , name: 'Prerequisite'
                    , relationDescription: 'first element is a prerequisite of second element'
                }, callback);
            });
        }
    })
}

// Views
function updateViews(callback) {
    console.log('updating views');

    var kcmViews = {
        _id: '_design/' + designDoc
        , views: {
            'concept-nodes': {
                map: (function(doc) { if (doc.type == 'concept node') emit(doc._id, null); }).toString()
            }
            , 'relations-by-name': {
                map: (function(doc) { if (doc.type == 'relation') emit(doc.name, null); }).toString()
            }
            , 'relations-by-relation-type-name': {
                map: (function(doc) { if (doc.type == 'relation') emit([doc.relation-type, doc.name], null); }).toString()
            }
        }
    };

    getDoc(kcmViews._id, validatedResponseCallback([200,404], function(e,r,b) {
        var writeViews = r.statusCode === 404 ? insertDoc : updateDoc;
        if (r.statusCode === 200) kcmViews._rev = JSON.parse(b)._rev;
        
        writeViews(kcmViews, validatedResponseCallback(201, callback));
    }));
}

function queryView(view, callback) {
    getDoc('_design/' + designDoc + '/_view/' + view, callback);
};

function insertConceptNode(o, callback) {
    var errors = '';
    if (typeof o.nodeDescription != 'string' || !o.nodeDescription.length) errors += 'String value required for "nodeDescription". "';
    if (isNaN(o.x)) errors += 'Float value required for "x". ';
    if (isNaN(o.y)) errors += 'Float value required for "y". ';
    if (isNaN(o.width)) errors += 'Float value required for "width". ';
    if (isNaN(o.height)) errors += 'Float value required for "height". ';

    if (errors.length) {
        if (typeof callback == 'function') callback('Error. Could not create concept node. Bad argument. ' + errors, 500);
        return;
    }

    insertDoc({
        type:'concept node'
        , nodeDescription: o.nodeDescription
        , notes: o.notes || ''
        , x: o.x
        , y: o.y
        , width: o.width
        , height: o.height
    }, function(e,r,b) {
        if (r.statusCode != 201) {
            if (!e) e = 'Error inserting concept node';
            if (typeof callback == 'function') callback(e,r,b);
            return;
        }

        getDoc(JSON.parse(b).id, function(e,r,b) {
            if (r.statusCode != 200)  {
                if (!e) e = 'Error retrieving newly created concept node';
            } else {
                r.statusCode = 201;
            }
            if (typeof callback == 'function') callback(e,r,b);
            return;
        });
    });
}

function insertRelation(o, callback) {
    console.log('inserting relation');

    if (!o.relationType || relationTypes.indexOf(o.relationType) == -1) {
        if (typeof callback == 'function') callback(util.format('could not insert relation - relation type "%s" not recognised', o.relationType), 500);
        return;
    }

    if (typeof o.name != 'string') {
        if (typeof callback == 'function') callback('could not insert relation - name required', 500);
        return;
    }

    queryView(util.format('relations-by-name?key="%s"', o.name), function(e,r,b) {
        if (r.statusCode == 200) {
            r.statusCode = 409;
            if (typeof callback == 'function') callback(util.format('could not insert relation - a relation with name "%s" already exists', r, b));
            return;
        }
        insertDoc({
            type:'relation'
            , relationType: o.relationType
            , name: o.name
            , relationDescription: o.relationDescription || ""
            , members: []
        }, function(e,r,b) {
            if (r.statusCode != 201) {
                if (!e) e = 'Error inserting relation';
                if (typeof callback == 'function') callback(e,r,b);
                return;
            }

            getDoc(JSON.parse(b).id, function(e,r,b) {
                if (r.statusCode != 200)  {
                    if (!e) e = 'Error retrieving newly created relation';
                    if (typeof callback == 'function') callback(e,r,b);
                    return;
                }

                r.statusCode = 201;
                if (typeof callback == 'function') callback(e,r,b);
                return;
            });
        });
    });
}

function addOrderedPairToBinaryRelation(relationId, el1Id, el2Id, callback) {
    getDoc(relationId, function(e,r,b) {
        if (r.statusCode != 200)  {
            if (typeof callback == 'function') callback('Error retrieving relation',r,b);
            return;
        }
        var relation = JSON.parse(b);

        if (!relation.relationType || relationTypes.indexOf(relation.relationType) == -1) {
            if (typeof callback == 'function') callback(util.format('could not insert relation - relation type "%s" not recognised', relation.relationType));
            return
        }
    
        if (relation.type != 'relation' || relation.relationType != 'binary') {
            r.statusCode = 500;
            if (typeof callback == 'function') callback(util.format('document with id "%s" does not represent a binary relation', relationId), r, b);
            return;
        }

        var duplicate = _.find(relation.members, function(pair) {
            if (pair[0] == el1Id && pair[1] == el2Id) {
                return true;
            }
        });
        if (duplicate) {
            r.statusCode = 500;
            if (typeof callback == 'function') callback(util.format('ordered pair is already a member of the binary relation: [el1Id=%s, el1Id=%s] at index:%d', el1Id, el2Id, relation.members.indexOf(duplicate)), r, b);
            return;
        }

        relation.members.push([el1Id, el2Id]);
        updateDoc(relation, function(e,r,b) {
            if (r.statusCode != 201) {
                e = 'unable to update relation';
            }
            if (typeof callback == 'function') callback(e, r, b);
        });
    });
}

// TODO: The following functions should be shared across model modules
// Private functions
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
    //console.log('updateDoc:', JSON.stringify(doc,null,2));
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

