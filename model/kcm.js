var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
  , libxmljs = require('libxmljs')
;

var kcmDatabaseName = 'tmp-blm-kcm3'
  , designDoc = 'kcm-views'
  , couchServerURI
  , databaseURI
;

var relationTypes = ['binary'];

module.exports = function(serverURI) {
    couchServerURI = serverURI.replace(/([^/])$/, '$1');
    databaseURI = serverURI + kcmDatabaseName.replace(/([^/])$/, '$1') + '/';
    console.log("knowledge concept map model: databaseURI =", databaseURI);
    
    //importGraffleMapIntoNewDB(2, 0, false);
    
    return {
        databaseName: kcmDatabaseName
        , generateUUID: generateUUID
        , importGraffleMapIntoNewDB: importGraffleMapIntoNewDB
        , createDB: createDB
        , pullReplicate: pullReplicate
        , updateViews: updateViews
        , queryView: queryView
        , insertConceptNode: insertConceptNode
        , updateConceptNodePosition: updateConceptNodePosition
        , insertConceptNodeTag: insertConceptNodeTag
        , deleteConceptNodeTag: deleteConceptNodeTag
        , editConceptNodeTag: editConceptNodeTag
        , insertBinaryRelation: insertBinaryRelation
        , addOrderedPairToBinaryRelation: addOrderedPairToBinaryRelation
        , getDoc: getDoc
        , addNewPipelineToConceptNode: addNewPipelineToConceptNode
        , deletePipeline: deletePipeline
        , removeProblemFromPipeline: removeProblemFromPipeline
        , updatePipelineSequence: updatePipelineSequence
        , pipelineProblemDetails: pipelineProblemDetails
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

function generateUUID(callback) {
    console.log(couchServerURI);
    request({
        uri: couchServerURI + '_uuids'
        , headers: { 'content-type':'application/json', accepts:'application/json' }
    }, function(e,r,b) {
        callback(JSON.parse(b).uuids[0]);
    });
}

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
    createDB(function(e,statusCode,b) {
        if (e || statusCode != 201) {
            console.log('Error creating database. error="%s", statusCode="%s"', e, statusCode);
            return;
        }

        if (map.nodes.length) {
            queryView(encodeURI('relations-by-name?key="Prerequisite"'), function(e,r,b) {
                var rows = r.statusCode == 200 && JSON.parse(b).rows
                  , prereqId = rows && rows.length && rows[0].id
                  , prereqRev = rows && rows.length && rows[0].rev
                ;
                if (!prereqId) {
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
                                addOrderedPairToBinaryRelation(prereqId, prereqRev, pair[0], pair[1], function(e,statusCode,rev) {
                                    if (201 != statusCode) {
                                        console.log('error inserting prerequisite relation member: (e:"%s", statusCode:%d)', e, statusCode);
                                        var nodes = _.filter(map.nodes, function(n) { return n.id == pair[0] || n.id == pair[1]; });
                                        return;
                                    }
                                    prereqRev = rev;
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
            if ('function' == typeof callback) callback(e,r,b);
            else console.log('%s with uri "%s". StatusCode=%d', e, databaseURI, r.statusCode);
            return;
        } else {
            console.log('created database at uri:\nupdate views...', databaseURI);
            updateViews(function(e,r,b) {
                console.log('insert "Prerequisite" binary relation...');
                insertBinaryRelation('Prerequisite', 'is a prerequisite of', function(e,statusCode,b) {
                    if ('function' == typeof callback) {
                        if (statusCode == 201) callback.apply(null, insertDBCallbackArgs);
                        else callback('eror inserting prerequisite relation', statusCode);
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
            , 'concept-node-pipelines': {
                map: (function(doc) {
                    if (doc.type == 'concept node') {
                        var len = doc.pipelines.length;
                        for (var i=0; i<len; i++) emit (doc._id, doc.pipelines[i]);
                    }
                }).toString()
            }
            , 'pipelines-by-name': {
                map: (function(doc) { if (doc.type == 'pipeline') emit(doc.name, null); }).toString()
            }
            , 'pipeline-problems': {
                map: (function(doc) {
                    if (doc.type == 'pipeline') {
                        var len = doc.problems.length;
                        for (var i=0; i<len; i++) emit (doc._id, doc.problems[i]);
                    }
                }).toString()
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


            // TODO: The following is copied from content. Sort out
            , 'any-by-type-name': {
                map: (function(doc) { if (doc.type && doc.name) emit([doc.type, doc.name], null); }).toString()
            }
            , 'syllabi-by-name': {
                map: (function(doc) { if (doc.type == 'syllabus') emit(doc.name, null); }).toString()
            }
            , 'topics-modules-elements': {
                map: (function(doc) {
                    switch(doc.type) {
                        case 'syllabus':
                            emit(doc._id, doc.topics);
                            break;
                        case 'topic':
                            emit(doc._id, doc.modules);
                            break;
                        case 'module':
                            emit(doc._id, doc.elements);
                            break;
                        default:
                            break;
                    }
                }).toString()
            }
            , 'names-types-by-id': {
                map: (function(doc) { emit(doc._id, [doc.name, doc.type]); }).toString()
            }
            , 'topics': {
                map: (function(doc) { if (doc.type == 'topic') emit(doc._id, doc.name); }).toString()
            }
            , 'topics-by-name': {
                map: (function(doc) { if (doc.type == 'topic') emit(doc.name, null); }).toString()
            }
            , 'modules': {
                map: (function(doc) { if (doc.type == 'module') emit(doc._id, doc.name); }).toString()
            }
            , 'modules-by-topicid-name': {
                map: (function(doc) { if (doc.type == 'module') emit([doc.topicId, doc.name], null); }).toString()
            }
            , 'elements': {
                map: (function(doc) { if (doc.type == 'element') emit(doc._id, doc.name); }).toString()
            }
            , 'elements-by-moduleid-name': {
                map: (function(doc) { if (doc.type == 'element') emit([doc.moduleId, doc.name], null); }).toString()
            }
            , 'assessment-criteria-by-element': {
                map: (function(doc) { if (doc.type == 'assessment criterion') emit([doc.elementId, doc.name], null); }).toString()
            }
            , 'assessment-criteria': {
                map: (function(doc) { if (doc.type == 'assessment criterion') emit(doc._id, doc.name); }).toString()
            }
            , 'assessment-criteria-problems': {
                map: (function(doc) {
                    if (doc.type == 'problem') {
                        for (var i = 0; i < doc.assessmentCriteria.length; i++) {
                            var c = doc.assessmentCriteria[i];
                            emit([c.id, doc.problemDescription], c);
                        }
                    }
                }).toString()
            }
            , 'problem-descriptions': {
                map: (function(doc) { if (doc.type == 'problem') emit(doc._id, doc.problemDescription); }).toString()
            }
            , 'problem-descriptions-and-notes': {
                map: (function(doc) { if (doc.type == 'problem') emit(doc._id, [doc.problemDescription, doc.problemNotes]); }).toString()
            }
            , 'problems-by-description': {
                map: (function(doc) { if (doc.type == 'problem') emit(doc.problemDescription, null); }).toString()
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
        if ('function' == typeof callback) callback('Error. Could not create concept node. Bad argument. ' + errors, 500);
        return;
    }

    o.type = 'concept node';
    o.pipelines = [];

    insertDoc(o, function(e,r,b) {
        if (r.statusCode != 201) {
            if (!e) e = 'Error inserting concept node';
            if ('function' == typeof callback) callback(e,r,b);
            return;
        }

        getDoc(JSON.parse(b).id, function(e,r,b) {
            if (r.statusCode != 200)  {
                if (!e) e = 'Error retrieving newly created concept node';
            } else {
                r.statusCode = 201;
            }
            if ('function' == typeof callback) callback(e,r,b);
            return;
        });
    });
}

function updateConceptNodePosition(id, rev, x, y, callback) {
    getDoc(id, function(e,r,b) {
        if (isNaN(x) || isNaN(y)) {
            callback(util.format('BAD ARGS: numberic values required for x and y. Supplied: "%s" and "%s". The concept node position was not updated.', x, y), 500);
            return;
        }

        if (r.statusCode != 200) {
            callback(util.format('Could not retrieve concept node. Database Error: "%s". The concept node position was not updated.', e), r.statusCode);
            return;
        }

        var node = JSON.parse(b);

        if (node._rev != rev) {
            callback(util.format('concept node revisions do not correspond. supplied:"%s", database:"%s". The concept node position was not updated.', rev, node._rev), 500);
            return;
        }

        node.x = x;
        node.y = y;

        updateDoc(node, function(e,r,b) {
            if (r.statusCode != 201) e = util.format('Error updating concept node position. Database Error: "%s"', e);
            callback(e, r.statusCode, JSON.parse(b).rev);
        });
    });
}

function insertConceptNodeTag(conceptNodeId, conceptNodeRev, tag, callback) {
    if ('string' != typeof tag) {
        callback('tag supplied is not a string', 500);
        return;
    }

    getDoc(conceptNodeId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('Could not retrieve concept node. Database Error:"%s"', e), r.statusCode);
            return;
        }

        var cn = JSON.parse(b);

        if (cn._rev != conceptNodeRev) {
            callback(util.format('supplied revision:"%s" does not match latest document revision:"%s"', conceptNodeRev, cn._rev), 500);
            return;
        }

        if ('concept node' != cn.type) {
            callback(util.format('id:"%s" does not correspond to a concept node', conceptNodeId), r.statusCode);
            return;
        }

        if (!cn.tags) cn.tags = [];
        cn.tags.push(tag);

        updateDoc(cn, function(e,r,b) {
            if (201 != r.statusCode) {
                callback('failed to update concept node', r.statusCode);
                return;
            }
            callback(null, 201, JSON.parse(b).rev);
        });
    });
}

function deleteConceptNodeTag(conceptNodeId, conceptNodeRev, tagIndex, tagText, callback) {
    if ('string' != typeof tagText) {
        callback('tag supplied is not a string', 500);
        return;
    }
    if (tagIndex != parseInt(tagIndex)) {
        callback('tagIndex is not an integer', 500);
        return;
    }

    getDoc(conceptNodeId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('Could not retrieve concept node. Database Error:"%s"', e), r.statusCode);
            return;
        }

        var cn = JSON.parse(b);

        if (cn._rev != conceptNodeRev) {
            callback(util.format('supplied revision:"%s" does not match latest document revision:"%s"', conceptNodeRev, cn._rev), 500);
            return;
        }

        if ('concept node' != cn.type) {
            callback(util.format('id:"%s" does not correspond to a concept node', conceptNodeId), r.statusCode);
            return;
        }

        if (!cn.tags || tagText != cn.tags[tagIndex]) {
            callback(util.format('supplied text:"%s" does not match tag at supplied index:"%s".', tagText, tagIndex), 500);
            return;
        }

        cn.tags.splice(tagIndex, 1);

        updateDoc(cn, function(e,r,b) {
            if (201 != r.statusCode) {
                callback('failed to update concept node', r.statusCode);
                return;
            }
            callback(null, 201, JSON.parse(b).rev);
        });
    });
}

function editConceptNodeTag(conceptNodeId, conceptNodeRev, tagIndex, currentText, newText, callback) {
    if ('string' != typeof currentText) {
        callback('current text supplied for tag is not a string', 500);
        return;
    }
    if ('string' != typeof newText) {
        callback('new text supplied for tag is not a string', 500);
        return;
    }
    if (tagIndex != parseInt(tagIndex)) {
        callback('tagIndex is not an integer', 500);
        return;
    }

    getDoc(conceptNodeId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('Could not retrieve concept node. Database Error:"%s"', e), r.statusCode);
            return;
        }

        var cn = JSON.parse(b);

        if (cn._rev != conceptNodeRev) {
            callback(util.format('supplied revision:"%s" does not match latest document revision:"%s"', conceptNodeRev, cn._rev), 500);
            return;
        }

        if ('concept node' != cn.type) {
            callback(util.format('id:"%s" does not correspond to a concept node', conceptNodeId), r.statusCode);
            return;
        }

        if (!cn.tags || currentText != cn.tags[tagIndex]) {
            callback(util.format('supplied text:"%s" does not match tag at supplied index:"%s".', currentText, tagIndex), 500);
            return;
        }

        cn.tags.splice(tagIndex, 1, newText);

        updateDoc(cn, function(e,r,b) {
            if (201 != r.statusCode) {
                callback('failed to update concept node', r.statusCode);
                return;
            }
            callback(null, 201, JSON.parse(b).rev);
        });
    });
}

function insertBinaryRelation(name, description, callback) {
    if ('string' != typeof name) {
        if ('function' == typeof callback) callback('could not insert relation - name required', 500);
        return;
    }
    if ('string' != typeof description) {
        if ('function' == typeof callback) callback('could not insert relation - description required', 500);
        return;
    }

    queryView(util.format('relations-by-name?key="%s"', o.name), function(e,r,b) {
        if (200 == r.statusCode) {
            if ('function' == typeof callback) callback(util.format('could not insert relation - a relation with name "%s" already exists', 409));
            return;
        }
        insertDoc({
            type:'relation'
            , relationType: 'binary'
            , name: name
            , relationDescription: description
            , members: []
        }, function(e,r,b) {
            if (201 != r.statusCode) {
                if ('function' == typeof callback) callback(util.format('Error inserting binary relation. Database Error: "%s"', e), r.statusCode);
                return;
            }
            callback(null,201,b);
        });
    });
}

function addOrderedPairToBinaryRelation(relationId, relationRev, el1Id, el2Id, callback) {
    getDoc(relationId, function(e,r,b) {
        if (r.statusCode != 200)  {
            if ('function' == typeof callback) callback(util.format('Error retrieving relation. Database reported error:"%s". The pair was not added the relation', e),r.statusCode);
            return;
        }

        var relation = JSON.parse(b);

        if (relation._rev != relationRev) {
            callback(util.format('supplied revision:"%s" does not match latest document revision:"%s"', relationRev, relation.rev), 500);
            return;
        }

        if (relation.type != 'relation' || relation.relationType != 'binary') {
            r.statusCode = 500;
            if ('function' == typeof callback) callback(util.format('document with id "%s" does not represent a binary relation - pair not added to relation', relationId), r.statusCode);
            return;
        }

        var duplicate = _.find(relation.members, function(pair) {
            if (pair[0] == el1Id && pair[1] == el2Id) {
                return true;
            }
        });
        if (duplicate) {
            if ('function' == typeof callback) callback(util.format('ordered pair ["%s", "%s"] is already a member of binary relation id="%s"', el1Id, el2Id, relationId), 500);
            return;
        }

        relation.members.push([el1Id, el2Id]);

        updateDoc(relation, function(e,r,b) {
            if (201 != r.statusCode) {
                if ('function' == typeof callback) callback(util.format('Error updating relation. Database reported error:"%s". The pair was not added to the relation', e), r.statusCode);
                return;
            }
            if ('function' == typeof callback) callback(null, 201, JSON.parse(b).rev);
        });
    });
}

function addNewPipelineToConceptNode(pipelineName, conceptNodeId, conceptNodeRev, callback) {
    if (typeof pipelineName != 'string' || !pipelineName.length) {
        if ('function' == typeof callback) callback('BAD ARGS - pipelineName requires string', 500);
        return;
    }
    if (typeof conceptNodeId != 'string' || !conceptNodeId.length) {
        if ('function' == typeof callback) callback('BAD ARGS - conceptNodeId requires string', 500);
        return;
    }

    getDoc(conceptNodeId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('could not retrieve concept node. (Database Error:"%s"). The pipeline was not created.',e), r.statusCode);
            return;
        }

        var conceptNode = JSON.parse(b);

        if (conceptNode._rev != conceptNodeRev) {
            callback(util.format('concept node revisions do not correspond. supplied:"%s", database:"%s". The concept node position was not updated.', conceptNodeRev, conceptNode._rev), 500);
            return;
        }

        if (!conceptNode.pipelines) conceptNode.pipelines = [];

        insertDoc({ type:'pipeline', name:pipelineName, problems:[] }, function(e,r,b) {
            if (201 != r.statusCode) {
                callback(util.format('Failed to create pipeline. (Database Error:"%s"). The pipeline was not created.',e), r.statusCode);
                return;
            }

            var plData = JSON.parse(b);
            conceptNode.pipelines.push(plData.id);

            updateDoc(conceptNode, function(e,r,b) {
                if (201 != r.statusCode) {
                    callback(util.format('Error adding new pipeline to concept node. (Database Error:"%s"). The pipeline was created with id="%s".', e, plData.id), r.statusCode, plData);
                    return;
                }

                var nodeRevision = JSON.parse(b).rev;

                getDoc(plData.id, function(e,r,b) {
                    if (200 != r.statusCode) {
                        callback(util.format('Error retrieving new pipeline. Database Error:"%s". The pipeline was created with id="%s" and the concept node was updated to revision="%s".'
                                             , e, plData.id, nodeRevision), r.statusCode || 500);
                    } else {
                        callback(null, 201, JSON.parse(b), nodeRevision);
                    }
                });
            });
        });
    });
}

function deletePipeline(plId, plRev, cnId, cnRev, callback) {
    getDoc(plId, function(e,r,b) {
        if (r.statusCode != 200) {
            callback(util.format('could not retrieve pipeline. (Database Error:"%s"). The pipeline was not deleted.',e), r.statusCode);
            return;
        }
        var pl = JSON.parse(b);

        if (pl._rev != plRev) {
            callback(util.format('Error: Pipeline revisions do not correspond. Supplied:"%s", Database:"%s". The pipeline was not deleted', plRev, pl._rev), 500);
            return;
        }

        if ('pipeline' != pl.type) {
            callback(util.format('Error: Document with id="%s" is not a pipeline. The document was not deleted.',plId), 500);
            return;
        }

        getDoc(cnId, function(e,r,b) {
            var cn, plIx;

            if (r.statusCode != 200) {
                callback(util.format('Error: could not retrieve concept node. (Database Error:"%s"). The pipeline was not deleted.',e), r.statusCode);
                return;
            }

            cn = JSON.parse(b);

            if ('concept node' != cn.type) {
                callback(util.format('Error: Document with id="%s" is not a concept node. The pipeline was not deleted.',cnId), 500);
                return;
            }
            if (cnRev != cn._rev) {
                callback(util.format('Error: concept node revisions do not correspond. Supplied:"%s", Database:"%s", The pipeline was not deleted', cnRev, cn._rev), 500);
                return;
            }

            plIx = cn.pipelines.indexOf(plId);

            if (-1 == plIx) {
                callback(util.format('Error: Pipeline with id="%s" not found on concept node with id="%s". The pipeline was not deleted.', plId, cnId), 500);
                return;
            }

            cn.pipelines.splice(plIx, 1);

            updateDoc(cn, function(e,r,b) {
                if (r.statusCode != 201) {
                    callback('Error: Failed to remove pipeline from concept node. Database Error:"%s". The pipeline was not deleted.', r.statusCode);
                    return;
                }

                var cnUpdatedRev = JSON.parse(b).rev;

                deleteDoc(plId, plRev, function(e,r,b) {
                    if (r.statusCode != 200) {
                        callback(util.format('Error: Failed to delete pipeline. Database Error:"%s". The pipeline was removed from its concept node but not deleted.', e), r.statusCode);
                        return;
                    }
                    callback(null, 200, cnUpdatedRev);
                });
            });
        });
    });
}

function removeProblemFromPipeline(pipelineId, pipelineRev, problemId, callback) {
    var argErrors = [];
    if ('string' != typeof pipelineId) argErrors.push('pipelineId');
    if ('string' != typeof pipelineRev) argErrors.push('pipelineRev');
    if ('string' != typeof problemId) argErrors.push('problemId');
    if (argErrors.length) {
        callback('BAD ARGS: strings required for ' + argErrors.join(' and ') +'. The problem was not removed from the pipeline', 500);
        return;
    }

    getDoc(pipelineId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('could not retrieve pipeline. (Database Error:"%s"). The pipeline was not deleted.',e), r.statusCode);
            return;
        }
        var pl = JSON.parse(b);

        if (pl._rev != pipelineRev) {
            callback(util.format('Error: Pipeline revisions do not correspond. Supplied:"%s", Database:"%s". The pipeline was not deleted', pipelineRev, pl._rev), 500);
            return;
        }

        if ('pipeline' != pl.type) {
            callback(util.format('Error: Document with id="%s" is not a pipeline. The document was not deleted.',plId), 500);
            return;
        }

        var problemIx = pl.problems.indexOf(problemId);

        if (!~problemIx) {
            callback(util.format('Problem with id="%s" not found on pipeline with id="%s". The problem was not deleted from the pipeline.', problemId, pipelineId), 500);
            return;
        }

        pl.problems.splice(problemIx, 1);

        updateDoc(pl, function(e,r,b) {
            if (201 != r.statusCode) {
                callback(util.format('Error removing problem with id="%s" form pipeline with id="%s". Database reported error:"%s"', problemId, pipelineId, e), r.statusCode);
                return;
            }
            callback(null,201,JSON.parse(b).rev);
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

function pipelineProblemDetails(id, rev, callback) {
    var errors = [];
    if (typeof id != 'string' || !id.length) errors.push('BAD ARG: String value required for "id"');
    if (typeof rev != 'string' || !id.length) errors.push('BAD ARG: String value required for "rev"');
    if (errors.length) {
        callback('Error. Could not update pipeline sequence.\n' + errors.join('\n'), 500);
        return;
    }

    getDoc(id, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('Error could not retrieve document with id="%s". Database reported error:"%s"',id,e), r.statusCode);
            return;
        }
        
        var pl = JSON.parse(b);

        if (pl._rev != rev) {
            callback(util.format('pipeline revisions do not correspond. supplied:"%s", database:"%s". The concept node position was not updated.', rev, pl._rev), 500);
            return;
        }

        if ('pipeline' != pl.type) {
            callback(util.format('Document with id="%s" does not have type "pipeline"', id), 500);
            return;
        }

        request({
            uri: encodeURI(util.format('%s_all_docs?keys=%s&include_docs=true', databaseURI, JSON.stringify(pl.problems)))
            , headers: { 'content-type':'application/json', accepts:'application/json' }
        }, function(e,r,b) {
            if (200 != r.statusCode) {
                callback(util.format('Error retrieving pipeline problems. Database reported error:"%s"', e), r.statusCode);
                return;
            }

            var problems = _.map(JSON.parse(b).rows, function(p) {
                return {
                    id: p.id
                    , desc: typeof p.doc.internalDescription == 'string' ? p.doc.internalDescription : p.doc.problemDescription
                    , lastModified: typeof p.doc.dateModified == 'string' ? p.doc.dateModified.replace(/.{8}$/, '').replace(/[a-z]/gi, ' ') : ''
                };
            });

            callback(null, 200, problems);
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
    }, ('function' == typeof callback && callback || function(){}));
}

function updateDoc(doc, callback) {
    //console.log('updateDoc:', JSON.stringify(doc,null,2));
    request({
        method:'PUT'
        , uri: databaseURI + doc._id 
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify(doc)
    }, ('function' == typeof callback && callback || function(){}));
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
            if ('function' == typeof callback) callback(e,r,b);
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

