var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
  , libxmljs = require('libxmljs')
;

var kcmDatabaseName = 'tmp-blm-kcm23/'
  , designDoc = 'kcm-views'
  , couchServerURI
  , databaseURI
;

var relationTypes = ['binary'];

module.exports = function(serverURI) {
    couchServerURI = serverURI;
    databaseURI = serverURI + kcmDatabaseName;
    console.log("knowledge concept map model: databaseURI =", databaseURI);
    
    //importGraffleMapIntoNewDB(2, 0, false);
    
    return {
        importGraffleMapIntoNewDB: importGraffleMapIntoNewDB
        , createDB: createDB
        , pullReplicate: pullReplicate
        , updateViews: updateViews
        , queryView: queryView
        , insertConceptNode: insertConceptNode
        , insertRelation: insertRelation
        , addOrderedPairToBinaryRelation: addOrderedPairToBinaryRelation
        , getDoc: getDoc
        , addNewPipelineToConceptNode: addNewPipelineToConceptNode
        , deletePipeline: deletePipeline
        , updatePipelineSequence: updatePipelineSequence
        , updateDoc: updateDoc // TODO: Shoud not give direct access to this function if we're doing undo functionality.
    };
};

// the following can safely be deleted. Only retained here in case it saves me a couple of mins somewhere down the line
//function replaceUUIDWithGraffleId() {
//    var file = process.cwd() + '/resources/nc-graffle-import-notes.csv'
//      , notes = fs.readFileSync(file, 'UTF8')
//      , idMap = {}
//    ;
//
//    queryView(encodeURI('concept-nodes?include_docs=true'), function(e,r,b) {
//        var nodes = JSON.parse(b).rows
//          , docNodeIds = _.uniq(notes.match(/2e203[a-z0-9]+/ig))
//        ;
//        _.each(nodes, function(n) {
//            idMap[n.id] = n.doc.graffleId;
//        });
//
//        _.each(docNodeIds, function(id) {
//            notes = notes.replace(new RegExp(id,'ig'), 'GraffleId:'+idMap[id]);
//        });
//
//        fs.writeFileSync(file, notes, 'UTF8');
//    });
//}

function importGraffleMapIntoNewDB(conceptNodeLayer, toolsLayer, dummyRun) {
    // files from which to import
    var graffleDoc = '/resources/kcm-120420/kcm-120420-tools.graffle'
      , svgDoc = '/resources/kcm-120420/kcm-120420-tools.svg'
    ;

    // check args
    var errors = false
    if (parseInt(conceptNodeLayer) !== conceptNodeLayer) {
        console.log('BAD ARG: integer must be provided for parameter: conceptNodeLayer. The map was not imported.');
        errors = true;
    }
    if (parseInt(toolsLayer) !== toolsLayer) {
        console.log('BAD ARG: integer must be provided for parameter: toolsLayer. The map was not imported.');
        errors = true;
    }
    if (errors) return;

    var map = {}
      , graffle = libxmljs.parseXmlString(fs.readFileSync(process.cwd() + graffleDoc, 'utf8'))
      , graffleGraphicsElm = graffle.get('//key[text()="GraphicsList"]/following-sibling::array')
      , graffleConceptElms = graffleGraphicsElm.find('./dict' + 
                                                     '[child::key[text()="Layer"]/following-sibling::integer[text()="'+conceptNodeLayer+'"]]' +
                                                     '[child::key[text()="Class"]/following-sibling::string[text()="ShapedGraphic"]]' +
                                                     '[child::key[text()="Shape"]/following-sibling::string[text()="Rectangle"]]')
      , graffleLinkElms = graffleGraphicsElm.find('./dict' + 
                                                  '[child::key[text()="Layer"]/following-sibling::integer[text()="'+conceptNodeLayer+'"]]' +
                                                  '[child::key[text()="Class"]/following-sibling::string[text()="LineGraphic"]]')
      , svg = libxmljs.parseXmlString(fs.readFileSync(process.cwd() + svgDoc, 'utf8'))
      , svgConceptElms = svg.find('./g/g[child::title[text()="Concepts"]]/g[@id]')
      , svgToolElms = svg.find('./g/g[child::title[text()="Tool Coverage"]]/g[@id]')
    ;
    
    // function takes text element, orders tspans inside it according to positon, joins text inside tspans into single string
    // N.B. line-breaks on map are automated and are thus ignored below
    function svgTextString(textEl) {
        var orderedTSpans = textEl.find('./tspan').sort(function(a,b) {
            var ax = parseFloat(a.attr('x').value()) || 0
              , ay = parseFloat(a.attr('y').value()) || 0
              , bx = parseFloat(b.attr('x').value()) || 0
              , by = parseFloat(b.attr('y').value()) || 0
            ;
            return ay == by ? ax - bx : ay - by;
        })
          , s = ''
          , i = 0
          , tspan, lastY, y
        ;

        while (tspan = orderedTSpans[i++]) {
            y = tspan.attr('y').value();
            if (y != lastY) {
                if (lastY) s += '\n'; // temporarily insert newline
                lastY = y;
            }
            s += tspan.text();
        }

        // replace newlines without an adjacent space with a space. Replace newlines with an adjacent space with nothing.
        return s.replace(/(\S)\n(\S)/g, '$1 $2').replace(/\n/g, '');
    }

    function getGraffleElmId(el) {
        return parseInt(el.get('./key[text()="ID"]/following-sibling::*').text());
    } 

    // finds rectangles on the tools layer that overlap with the concept node
    function conceptToolsText(svgConceptElm) {
        var cRect = svgConceptElm.get('./rect')
          , cl = parseFloat(cRect.attr('x').value())
          , ct = parseFloat(cRect.attr('y').value())
          , cr = parseFloat(cRect.attr('width').value()) + cl
          , cb = parseFloat(cRect.attr('height').value()) + ct
        ;

        var toolElms = _.filter(svgToolElms, function(elm) {
            var tRect = elm.get('./rect')
              , tl = parseFloat(tRect.attr('x').value())
              , tt = parseFloat(tRect.attr('y').value())
              , tr = parseFloat(tRect.attr('width').value()) + tl
              , tb = parseFloat(tRect.attr('height').value()) + tt
            ;
            return tl <= cr && tr >= cl && tt <= cb && tb >= ct;
        });

        return _.map(toolElms, function(elm) {
            return 'Suggested Tool: ' + svgTextString(elm.get('./text'));
        }).join('\n\n');
    }

    map.nodes = _.filter(
        _.map(graffleConceptElms, function(el) {
            var bounds = el.get('./key[text()="Bounds"]/following-sibling::string').text().match(/\d+\.\d+?/g)
              , coordMultiplier = 1.6 // used to generate additional spacing between nodes enabling consistent automated sizing of node description rectangles without overlap
              , nodeId = getGraffleElmId(el)
              , svgEl = _.find(svgConceptElms, function(elm) { return elm.attr('id').value() == 'id'+nodeId+'_Graphic'; })
              , desc = svgEl && svgTextString(svgEl.get('./text'))
              , syallabusTags
              , toolsText
            ;

            if (!svgEl) {
                console.log('NO CORRESPONDING NODE IN SVG IMPORT. NODE NOT IMPORTED\t graffle node id:%d\t svgElSelector: %s', nodeId, svgElSelector);
            } else {
                toolsText = conceptToolsText(svgEl);
                ccssTagRegExp = new RegExp(/\([0-9a-z]+\.[a-z]+\.[0-9a-z]+\)/ig);
                syllabusTags = _.map(desc.match(ccssTagRegExp), function(ccssCode) { return 'CCSS:' + ccssCode.match(/[^()]+/)[0]; });
                desc = desc.replace(ccssTagRegExp, '');
            }

            return {
                id: nodeId // will be overwritten with db uuid
                , graffleId: nodeId
                , nodeDescription: desc
                , x: parseInt(coordMultiplier * (parseFloat(bounds[0]) + 0.5 * parseFloat(bounds[2]))) // x coord + half width = horizontal centre
                , y: parseInt(coordMultiplier * (parseFloat(bounds[1]) + 0.5 * parseFloat(bounds[3]))) // y coord + half height = vertical centre
                , valid: svgEl != undefined
                , tags: syllabusTags
                , notes: toolsText

            };
        })
        , function(n) { return n.valid; }
    );

    // position leftmost node at x=0, topmost node at y=0. Not important.
    var minX = _.min(map.nodes, function(n) { return n.x; }).x;
    var minY = _.min(map.nodes, function(n) { return n.y; }).y;
    _.each(map.nodes, function(n) {
        n.x -= minX;
        n.y -= minY;
    });

    var prerequisitePairs = _.filter(
        _.map(graffleLinkElms
            , function(el) {
                var tailId = getGraffleElmId(el.get('./key[text()="Tail"]/following-sibling::dict'))
                  , headId = getGraffleElmId(el.get('./key[text()="Head"]/following-sibling::dict'))
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

    if (dummyRun === true) return;

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
                    delete node.id;
                    delete node.valid;
                    insertConceptNode(node, function(e,r,b) {
                        if (r.statusCode != 201) {
                            console.log('error inserting concept node: (e:"%s", statusCode:%d, b:"%s")', e, r.statusCode, b);
                            return;
                        }

                        var dbId = JSON.parse(b)._id;

                        _.each(prerequisitePairs, function(pair) {
                            if (pair[0] == node.graffleId) pair[0] = dbId;
                            else if (pair[1] == node.graffleId) pair[1] = dbId;
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
        var insertDBCallbackArgs = [e,r,b];

        if (r.statusCode == 412) {
            console.log('could not create database. Database with name "%s" already exists', databaseURI);
            updateViews(callback);
        } else if (r.statusCode != 201) {
            if (!e) e = 'error creating database';
            if (typeof callback == 'function') callback(e,r,b);
            else console.log('%s with uri "%s". StatusCode=%d', e, databaseURI, r.statusCode);
            return;
        } else {
            console.log('created database at uri:\nupdate views...', databaseURI);
            updateViews(function(e,r,b) {
                console.log('insert "Prerequisite" binary relation...');
                insertRelation({
                    relationType: 'binary'
                    , name: 'Prerequisite'
                    , relationDescription: 'first element is a prerequisite of second element'
                }, function(e,r,b) {
                    if (typeof callback == 'function') {
                        if (r.statusCode == 201) callback.apply(null, insertDBCallbackArgs);
                        else callback('eror inserting prerequisite relation', r.statusCode);
                    }
                });
            });
        }
    })
}

function pullReplicate(source, filter, continuous) {
    var target = kcmDatabaseName.replace(/\/$/,'')
    console.log('target:',target);

    request({
        method:'POST'
        , uri: couchServerURI + '_replicate'
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify({ source:source, target:target, filter:filter, continuous:continuous===true })
    }, function() {
        console.log(arguments, '\n\n\n');
    });
}

// Views
function updateViews(callback) {
    console.log('updating views on database:', databaseURI);

    var kcmViews = {
        _id: '_design/' + designDoc
        , views: {
            'by-type': {
                map: (function(doc) { emit(doc.type, null); }).toString()
            }
            , 'concept-nodes': {
                map: (function(doc) { if (doc.type == 'concept node') emit(doc._id, null); }).toString()
            }
            , 'concept-nodes-by-graffle-id': {
                map: (function(doc) { if (doc.type == 'concept node') emit(doc.graffleId, null); }).toString()
            }
            , 'pipelines-by-name': {
                map: (function(doc) { if (doc.type == 'pipeline') emit(doc.name, null); }).toString()
            }
            , 'relations-by-name': {
                map: (function(doc) { if (doc.type == 'relation') emit(doc.name, null); }).toString()
            }
            , 'relations-by-relation-type-name': {
                map: (function(doc) { if (doc.type == 'relation') emit([doc.relationType, doc.name], null); }).toString()
            }
            , 'tools-by-name': {
                map: (function(doc) { if (doc.type == 'tool') emit(doc.name, null); }).toString()
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

    if (errors.length) {
        if (typeof callback == 'function') callback('Error. Could not create concept node. Bad argument. ' + errors, 500);
        return;
    }

    o.type = 'concept node';
    o.pipelines = [];

    insertDoc(o, function(e,r,b) {
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
            if (typeof callback == 'function') callback(util.format('ordered pair ["%s", "%s"] is already a member of binary relation id="%s"', el1Id, el2Id, relationId), r, b);
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

function addNewPipelineToConceptNode(pipelineName, conceptNodeId, callback) {
    if (typeof pipelineName != 'string' || !pipelineName.length) {
        if (typeof callback == 'function') callback('BAD ARGS - pipelineName requires string', 500);
        return;
    }
    if (typeof conceptNodeId != 'string' || !conceptNodeId.length) {
        if (typeof callback == 'function') callback('BAD ARGS - conceptNodeId requires string', 500);
        return;
    }

    getDoc(conceptNodeId, function(e,r,b) {
        if (r.statusCode != 200) {
            callback(util.format('could not retrieve concept node. (Database Error:"%s"). The pipeline was not created.',e), r.statusCode);
            return;
        }

        var conceptNode = JSON.parse(b);
        if (!conceptNode.pipelines) conceptNode.pipelines = [];

        insertDoc({ type:'pipeline', name:pipelineName, problems:[] }, function(e,r,b) {
            if (r.statusCode != 201) {
                callback(util.format('Failed to create pipeline. (Database Error:"%s"). The pipeline was not created.',e), r.statusCode);
                return;
            }

            var newPipelineId = JSON.parse(b).id; 
            conceptNode.pipelines.push(newPipelineId);

            updateDoc(conceptNode, function(e,r,b) {
                if (r.statusCode != 201) {
                    callback(util.format('Error adding new pipeline to concept node. (Database Error:"%s"). The pipeline was created with id="%s".', e, newPipelineId), r.statusCode, newPipelineId);
                    return;
                }

                getDoc(newPipelineId, function(e,r,b) { callback(null,201,b); }); //TODO: Check for r.statusCode == 200 here
            });
        });
    });
}

function deletePipeline(id, rev, conceptNodeId, callback) {
    if (typeof id != 'string' || !id.length) {
        if (typeof callback == 'function') callback('BAD ARGS - id requires string', 500);
        return;
    }
    if (typeof rev != 'string' || !rev.length) {
        if (typeof callback == 'function') callback('BAD ARGS - rev requires string', 500);
        return;
    }
    if (typeof conceptNodeId != 'string' || !conceptNodeId.length) {
        if (typeof callback == 'function') callback('BAD ARGS - conceptNodeId requires string', 500);
        return;
    }

    getDoc(id, function(e,r,b) {
        if (r.statusCode != 200) {
            callback(util.format('could not retrieve pipeline. (Database Error:"%s"). The pipeline was not deleted.',e), r.statusCode);
            return;
        }
        if ('pipeline' != JSON.parse(b).type) {
            callback(util.format('Error: Document with id="%s" is not a pipeline. The document was not deleted.',id), 500);
            return;
        }

        getDoc(conceptNodeId, function(e,r,b) {
            var conceptNode, pipelineIndex;

            if (r.statusCode != 200) {
                callback(util.format('Error: could not retrieve concept node. (Database Error:"%s"). The pipeline was not deleted.',e), r.statusCode);
                return;
            }

            conceptNode = JSON.parse(b);
            if ('concept node' != conceptNode.type) {
                callback(util.format('Error: Document with id="%s" is not a concept node. The pipeline was not deleted.',conceptNodeId), 500);
                return;
            }

            pipelineIndex = conceptNode.pipelines.indexOf(id);
            if (-1 == pipelineIndex) {
                callback(util.format('Error: Concept node with id="%s" is not associated with pipeline id="%s". The pipeline was not deleted.',conceptNodeId,id), 500);
                return;
            }

            conceptNode.pipelines.splice(pipelineIndex, 1);
            updateDoc(conceptNode, function(e,r,b) {
                if (r.statusCode != 201) {
                    callback('Error: Failed to update concept node. Database Error:"%s". The pipeline was not deleted.', r.statusCode);
                    return;
                }

                deleteDoc(id, rev, function(e,r,b) {
                    if (r.statusCode != 200) {
                        callback(util.format('Error: Failed to delete pipeline. Database Error:"%s". The pipeline was removed from the given node but was not deleted.',e), r.statusCode);
                        return;
                    }
                    callback(e,r.statusCode,b);
                });
            });
        });
    });
}

function updatePipelineSequence(pipelineId, problemSequence, callback) {
    var errors = '';
    if (typeof pipelineId != 'string' || !pipelineId.length) errors += 'BAD ARG: String value required for "pipelineId"';
    if (problemSequence.slice == undefined) errors += 'BAD ARG: Array required for "problemSequence"';
    if (errors.length) {
        callback('Error. Could not update pipeline sequence.\n' + errors, 500);
        return;
    }
                 
    getDoc(pipelineId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback('Could not retrieve pipeline from database. The problem sequence was not updated.', r.statusCode);
            return;
        }
        var pl = JSON.parse(b);

        if ('pipeline' != pl.type) {
            callback(util.format('invalid pipeline - id="%s" does not correspond to pipeline document.', pipelineId), 500);
            return;
        }

        pl.problems = problemSequence;
        updateDoc(pl, function(e,r,b) {
            if (201 != r.statusCode) callback(util.format('The pipeline problem sequence failed to update. Database Error: "%s"', e), r.statusCode);
            else callback(null,201);
        });
    });
}

// TODO: The following functions should be shared across model modules
function getDoc(id, callback) {
    request({
        uri: databaseURI + id
        , headers: { 'content-type':'application/json', accepts:'application/json' }
    }, callback);
};

function insertDoc(doc, callback) {
    request({
        method: 'post'
        , uri: databaseURI
        , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
        , body: JSON.stringify(doc)
    }, (typeof callback == 'function' && callback || function(){}));
}

function updateDoc(doc, callback) {
    //console.log('updateDoc:', JSON.stringify(doc,null,2));
    request({
        method:'PUT'
        , uri: databaseURI + doc._id 
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify(doc)
    }, (typeof callback == 'function' && callback || function(){}));
}

function deleteDoc(id, rev, callback) {
    request({
        method:'DELETE'
        , uri: databaseURI + id + '?rev=' + rev
    }, callback);
 
};

//
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
