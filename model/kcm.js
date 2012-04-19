var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
  , libxmljs = require('libxmljs')
;

var kcmDatabaseName = 'tmp-blm-kcm3/'
  , designDoc = 'kcm-views'
  , couchServerURI
  , databaseURI
;

var relationTypes = ['binary'];

module.exports = function(serverURI) {
    couchServerURI = serverURI;
    databaseURI = serverURI + kcmDatabaseName;
    console.log("knowledge concept map model: databaseURI =", databaseURI);
    
    //importGraffleMapIntoNewDB();

    return {
        importGraffleMapIntoNewDB: importGraffleMapIntoNewDB
        , createDB: createDB
        , updateViews: updateViews
        , queryView: queryView
        , insertConceptNode: insertConceptNode
        , insertRelation: insertRelation
        , addOrderedPairToBinaryRelation: addOrderedPairToBinaryRelation
    };
};

function importGraffleMapIntoNewDB() {
    var map = {}
      , graffle = libxmljs.parseXmlString(fs.readFileSync(process.cwd()+'/resources/kcm.graffle', 'utf8'))
      , svg = libxmljs.parseXmlString(fs.readFileSync(process.cwd()+'/resources/kcm.svg', 'utf8'))
      , graphicsElm = graffle.get('//key[text()="GraphicsList"]/following-sibling::array')
      , conceptElms = graphicsElm.find('./dict[child::key[text()="Class"]/following-sibling::string[text()="ShapedGraphic"]][child::key[text()="Shape"]/following-sibling::string[text()="Rectangle"]]')
      , linkElms = graphicsElm.find('./dict[child::key[text()="Class"]/following-sibling::string[text()="LineGraphic"]]')
    ;

    function getElmId(el) {
        return parseInt(el.get('./key[text()="ID"]/following-sibling::*').text());
    } 

    map.nodes = _.filter(
        _.map(conceptElms, function(el) {
            var bounds = el.get('./key[text()="Bounds"]/following-sibling::string').text().match(/\d+\.\d+?/g)
              , nodeId = getElmId(el)
              , svgElSelector = util.format('//g[@id="id%d_Graphic"]', nodeId)
              , svgEl = svg.get(svgElSelector)
              , desc = ''
              , tspans, tspan
              , y, lastY, i=0
            ;

            if (!svgEl) {
                console.log('NO CORRESPONDING NODE IN SVG IMPORT. NODE NOT IMPORTED\t graffle node id:%d\t svgElSelector: %s', nodeId, svgElSelector);
            }

            if (svgEl) {
                tspans = svgEl.find('./text/tspan').sort(function(a,b) {
                    var ax = parseFloat(a.attr('x').value()) || 0
                      , ay = parseFloat(a.attr('y').value()) || 0
                      , bx = parseFloat(b.attr('x').value()) || 0
                      , by = parseFloat(b.attr('y').value()) || 0
                    ;
                    return ay == by ? ax - bx : ay - by;
                });

                while (tspan = tspans[i++]) {
                    y = tspan.attr('y').value();
                    if (y != lastY) {
                        if (lastY) desc += '\n';
                        lastY = y;
                    }
                    desc += tspan.text();
                }
            }

            return {
                id: nodeId // will be overwritten with db uuid
                , graffleId: nodeId // used for matching links to nodes after id property is overwritten with db uuid
                , nodeDescription: desc
                , x: bounds[0]
                , y: bounds[1]
                , width: bounds[2]
                , height: bounds[3]
                , valid: svgEl != undefined
            };
        })
        , function(n) { return n.valid; }
    );


    // translate graph so that top-left node is at (0,0)
    var firstX = _.sortBy(map.nodes, function(n) { return n.x })[0].x
      , firstY = _.sortBy(map.nodes, function(n) { return n.y })[0].y
    ;
    _.each(map.nodes, function(n) {
        n.x -= firstX;
        n.y -= firstY;
    });

    var prerequisitePairs = _.filter(
        _.map(linkElms
            , function(el) {
                var tailId = getElmId(el.get('./key[text()="Tail"]/following-sibling::dict'))
                  , headId = getElmId(el.get('./key[text()="Head"]/following-sibling::dict'))
                ;
                return [ tailId, headId ];
        })
        , function(pair) { 
            var tail = _.find(map.nodes, function(n) { return n.id == pair[0]; })
              , head = _.find(map.nodes, function(n) { return n.id == pair[1]; })
            ;

            if (tail == undefined || head == undefined) {
                console.log('INVALID LINK - NODE(S) NOT FOUND. LINK NOT IMPORTED.\t Tail Id="%s" is %s.\t Head Id="%s" is %s', pair[0], (tail ? 'valid' : 'invalid'), pair[1], (head ? 'valid' : 'invalid'));
            }

            return tail != undefined && head != undefined;
        }
    );

    console.log('\nvalid nodes: %d', map.nodes.length);
    console.log('valid prerequisite pairs: %d', prerequisitePairs.length);

    //insert into db
    createDB(function(e,r,b) {
        if (e || r.statusCode != 201) {
            console.log('Error creating database. error="%s", statusCode="%d"', e, r.statusCode);
            return;
        }

        if (map.nodes.length) {
            queryView(encodeURI('relations-by-name?key="Prerequisite"'), function(e,r,b) {
                var rows = r.statusCode == 200 && JSON.parse(b).rows;
                var prerequisiteRelationId = rows && rows.length && rows[0].id;
                if (!prerequisiteRelationId) {
                    console.log('error - could not retrieve prerequisite relation. \ne:"%s", \nstatusCode:%d, \nb:"%s"', e, r.statusCode, b);
                    return;
                }

                (function insertCNode(nodeIndex) {
                    console.log('inserting concept node at index:%d of array length:%d', nodeIndex, map.nodes.length);

                    var node = map.nodes[nodeIndex];
                    insertConceptNode({
                        nodeDescription: node.nodeDescription
                        , x: node.x
                        , y: node.y
                        , width: node.width
                        , height: node.height
                    }, function(e,r,b) {
                        if (r.statusCode != 201) {
                            console.log('error inserting concept node: (e:"%s", statusCode:%d, b:"%s")', e, r.statusCode, b);
                            return;
                        }
                        var dbId = JSON.parse(b)._id
                          , graffleId = node.id
                        ;
                        node.id = dbId;
                        _.each(prerequisitePairs, function(pair) {
                            if (pair[0] == graffleId) pair[0] = dbId;
                            else if (pair[1] == graffleId) pair[1] = dbId;
                        });

                        if (++nodeIndex < map.nodes.length) {
                            insertCNode(nodeIndex);
                        } else if (prerequisitePairs.length) {

                            (function insertPrerequisitePair(pairIndex) {
                                console.log('inserting prerequisite pair at index:%d of array length:%d', pairIndex, prerequisitePairs.length);

                                var pair = prerequisitePairs[pairIndex];
                                addOrderedPairToBinaryRelation(prerequisiteRelationId, pair[0], pair[1], function(e,r,b) {
                                    if (r.statusCode != 201) {
                                        console.log('error inserting prerequisite relation member: (e:"%s", statusCode:%d)', e, r.statusCode);
                                        var nodes = _.filter(map.nodes, function(n) { return n.id == pair[0] || n.id == pair[1]; });
                                        return;
                                    }
                                    if (++pairIndex < prerequisitePairs.length) {
                                        insertPrerequisitePair(pairIndex);
                                    } else {
                                        console.log('all prerequisite pairs inserted');
                                    }
                                });
                            })(0);
                        } else {
                            console.log('no prerequisite links on graph');
                            return;
                        }
                    });
                })(0);
            });
        } else {
            console.log('no concept nodes on graph.');
            return;
        }
    });
}

function createDB(callback) {
    request({
        method: 'PUT'
        , uri: databaseURI
        , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
    }, function(e,r,b) {
        if (r.statusCode == 412) {
            console.log('could not create database. Database with name "%s" already exists', databaseURI);
            updateViews(callback);
        } else if (r.statusCode != 201) {
            if (!e) e = 'error creating database';
            if (typeof callback == 'function') callback(e,r,b);
            else console.log('%s with uri "%s". StatusCode=%d', e, databaseURI, r.statusCode);
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
                map: (function(doc) { if (doc.type == 'relation') emit([doc.relationType, doc.name], null); }).toString()
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

// Private functions

// TODO: The following functions should be shared across model modules
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
