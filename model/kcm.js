var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
  , libxmljs = require('libxmljs')
  , exec = require('child_process').exec
  , sqlite3 = require('sqlite3').verbose()
;

var kcmDatabaseName
  , designDoc
  , couchServerURI
  , databaseURI
  , relationTypes = ['binary']
;

module.exports = function(config) {
    couchServerURI = config.couchServerURI.replace(/^(.+[^/])\/*$/, '$1/');
    kcmDatabaseName = config.kcmDatabaseName;
    designDoc = config.kcmDatabaseDesignDoc;
    databaseURI = couchServerURI + kcmDatabaseName + '/';
    console.log(util.format('kcm module:\tdesignDoc="%s"\tdatabaseURI="%s"', designDoc, databaseURI));
    
    //importGraffleMapIntoNewDB(2, 0, false);
    
    return {
        databaseName: kcmDatabaseName
        , generateUUID: generateUUID
        , importGraffleMapIntoNewDB: importGraffleMapIntoNewDB
        , createDB: createDB
        , pullReplicate: pullReplicate
        , updateViews: updateViews
        , queryView: queryView
        , insertProblem: insertProblem
        , insertConceptNode: insertConceptNode
        , deleteConceptNode: deleteConceptNode
        , updateConceptNodePosition: updateConceptNodePosition
        , updateConceptNodeDescription: updateConceptNodeDescription
        , insertConceptNodeTag: insertConceptNodeTag
        , deleteConceptNodeTag: deleteConceptNodeTag
        , editConceptNodeTag: editConceptNodeTag
        , insertBinaryRelation: insertBinaryRelation
        , getChainedBinaryRelationsWithMembers: getChainedBinaryRelationsWithMembers
        , addOrderedPairToBinaryRelation: addOrderedPairToBinaryRelation
        , removeOrderedPairFromBinaryRelation: removeOrderedPairFromBinaryRelation
        , getDoc: getDoc
        , addNewPipelineToConceptNode: addNewPipelineToConceptNode
        , deletePipeline: deletePipeline
        , reorderConceptNodePipelines: reorderConceptNodePipelines
        , appendProblemsToPipeline: appendProblemsToPipeline
        , removeProblemFromPipeline: removeProblemFromPipeline
        , updatePipelineSequence: updatePipelineSequence
        , pipelineProblemDetails: pipelineProblemDetails
        , reorderPipelineProblems: reorderPipelineProblems
        , getAppContent: getAppContent
        , updateDoc: updateDoc // TODO: Shoud not give direct access to this function if we're doing undo functionality.
    };
};

function replaceUUIDWithGraffleId() {
    var file = process.cwd() + '/resources/nc-graffle-import-notes.csv'
      , notes = fs.readFileSync(file, 'UTF8')
      , graffleIdNodeMap = {}
    ;

    queryView(encodeURI('concept-nodes?include_docs=true'), function(e,r,b) {
        var nodes = JSON.parse(b).rows
          , docNodeIds = _.uniq(notes.match(/2e203[a-z0-9]+/ig))
        ;

        _.each(nodes, function(n) {
            graffleIdNodeMap[n.doc.graffleId] = n.doc;
            idMap[n.doc.graffleId] = n.id;
        });

        _.each(docNodeIds, function(id) {
            notes = notes.replace(new RegExp(id,'ig'), 'GraffleId:'+idMap[id]);
        });

        fs.writeFileSync(file, notes, 'UTF8');
    });
}

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
                    insertConceptNode(node, function(e,statusCode,conceptNode) {
                        if (statusCode != 201) {
                            console.log('error inserting concept node: (e:"%s", statusCode:%d, b:"%s")', e, r.statusCode, b);
                            return;
                        }

                        _.each(prerequisitePairs, function(pair) {
                            if (pair[0] == node.graffleId) pair[0] = conceptNode._id;
                            else if (pair[1] == node.graffleId) pair[1] = conceptNode._id;
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
// ********
// Don't do anything more - get everything else from pull replication
        callback(e,r,b);
        return;
// ********
        
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

function pullReplicate(source, filter, continuous, cancel) {
    var target = kcmDatabaseName.replace(/\/$/,'');

    filter = filter || undefined;
    continuous = continuous === true;
    cancel = cancel === true;
    
    console.log('-----\nPULL REPLICATION\nsource: %s\ntarget: %s\nfilter: %s\ncontinuous: %s\ncancel: %s\n-----', source, target, filter, continuous, cancel);

    request({
        method:'POST'
        , uri: couchServerURI + '_replicate'
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify({ source:source, target:target, filter:filter, continuous:continuous===true, cancel:cancel===true })
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
            , 'concept-nodes-by-pipeline': {
                map: (function(doc) {
                    if (doc.type == 'concept node') {
                        var len = doc.pipelines.length;
                        for (var i=0; i<len; i++) emit (doc.pipelines[i], doc._id);
                    }
                }).toString()
            }
            , 'concept-node-pipelines': {
                map: (function(doc) {
                    if (doc.type == 'concept node') {
                        var len = doc.pipelines.length;
                        for (var i=0; i<len; i++) emit (doc._id, doc.pipelines[i]);
                    }
                }).toString()
            }

            // Reduce off / Group-Level >= 2:   key: <tag>          value: <node_id>
            // Group-Level 1:                   key: <tag>          value: <total_number_of_instances_of_tag>
            // Group-Level 0:                   key: null           value: <total_number_of_tags>
            , 'concept-node-tags': {
                map: (function(doc) {
                    if ('concept node' == doc.type) {
                        doc.tags.forEach(function(tag) { emit(tag, doc._id); });
                    }
                }).toString()
                , reduce: (function(keys, values, rereduce) {
                    if (!rereduce) return values.length;
                    else return sum(values);
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

            // N.B. only emits results for pipelines with >=1 problem
            // Reduce off:                  key: [<pipeline_name>, <pipeline_id>, <pipeline_rev>]       value: <pipeline_problems>
            // Group-Level >=2:             key: [<pipeline_name>, <pipeline_id>, <pipeline_rev>]       value: <num_problems_in_pipeline>
            // Group-Level 1:               key: [<pipeline_name>, <pipeline_rev>]                      value: <num_problems_in_pipelines_of_name>
            // group-level none:            key: null,                                                  value: <total_number_of_problems_in_all_pipelines>
            , 'pipelines-with-problems-by-name': {
                map: (function(doc) {
                    if (doc.type == 'pipeline' && doc.problems.length) {
                        emit([doc.name, doc._id, doc._rev], doc.problems);
                    }
                }).toString()
                , reduce: (function(keys, values, rereduce) {
                    if (!rereduce) {
                        var numProblems = 0
                          , i, len = values.length
                        ;
                        for (i=0; i<len; i++) {
                            numProblems += values[i].length;
                        }
                        return numProblems;
                    } else {
                        return sum(values);
                    }
                }).toString()
            }

            , 'relations-by-name': {
                map: (function(doc) { if (doc.type == 'relation') emit(doc.name, null); }).toString()
            }
            , 'relations-by-relation-type': {
                map: (function(doc) { if (doc.type == 'relation') emit(doc.relationType, null); }).toString()
            }
            , 'relations-by-relation-type-name': {
                map: (function(doc) { if (doc.type == 'relation') emit([doc.relationType, doc.name], null); }).toString()
            }
            , 'tools-by-name': {
                map: (function(doc) { if (doc.type == 'tool') emit(doc.name, null); }).toString()
            }
            , 'num-per-type': {
                map: (function(doc) {
                    emit (doc.type || doc._id, null);
                }).toString().replace(/\s+/g, ' ')
                , reduce: (function(keys, values, rereduce) {
                    if (!rereduce) {
                        return values.length;
                    } else {
                        return sum(values);
                    }
                }).toString().replace(/\s+/g, ' ')
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

    getDoc(kcmViews._id, function(e,r,b) {
        var sc = r.statusCode
          , writeViewsFunc = sc == 200 ? updateDoc : insertDoc
        ;
        if (200 != sc && 404 != sc) {
            callback('error retrieving design doc from database', sc);
            return;
        }

        if (200 == sc) kcmViews._rev = JSON.parse(b)._rev;

        writeViewsFunc(kcmViews, function(e,r,b) {
            callback(e, r.statusCode);
        });
    });
}

function queryView(view, callback) {
    getDoc('_design/' + designDoc + '/_view/' + view, callback);
};

function insertProblem(plist, callback) {
    getProblemInfoFromPList(plist, function(e, plistString, problemDescription, toolId, internalDescription) {
        if (e) {
            callback(e);
            return;
        }
        request({ method:'GET', uri:couchServerURI+'_uuids' }, function(e,r,b) {
            if (r.statusCode != 200) {
                callback(util.format('could not generate uuid -- Database callback: (error:%s, statusCode:%d, body:%s)', e, r.statusCode, b));
                return;
            }
            var now = (new Date()).toJSON();

            request({
                method: 'PUT'
                , uri: databaseURI + JSON.parse(b).uuids[0]
                , multipart: [
                    { 'Content-Type': 'application/json'
                        , 'body': JSON.stringify({
                            type: 'problem'
                            , problemDescription: problemDescription
                            , internalDescription: internalDescription
                            , problemNotes: ''
                            , toolId: toolId
                            , dateCreated: now
                            , dateModified: now
                            , assessmentCriteria: []
                            , _attachments: {
                                'pdef.plist': {
                                    follows: true
                                    , length: plistString.length
                                    , 'Content-Type': 'application/xml'
                                }
                            }
                        })
                }
                , { 'body': plistString }
                ]
            }, function(e,r,b) {
                if (r.statusCode != 201) {
                    if (typeof callback == 'function') callback(util.format('Error creating problem -- Database callback: (error:%s, statusCode:%d, body:%s)', e, r.statusCode, b), r.statusCode);
                    return;
                }
                callback(null, 201, JSON.parse(b));
            });
        });
    });
}

function getProblemInfoFromPList(plist, callback) {
    fs.readFile(plist, 'utf8', function(e, plistString) {
        if (e) {
            if (typeof callback == 'function') callback('could not load plist. error: ' + e);
            return;
        }

        var matchMETA_QUESTION = plistString.match(/<key>META_QUESTION<\/key>/i)
          , matchValMETA_QUESTION_TITLE = plistString.match(/META_QUESTION_TITLE<\/key>\s*<string>([^<]+)/i)
          , matchValTOOL_KEY = plistString.match(/TOOL_KEY<\/key>\s*<string>([^<]+)/i)
          , matchValPROBLEM_DESCRIPTION = plistString.match(/PROBLEM_DESCRIPTION<\/key>\s*<string>([^<]+)/i)
          , matchValINTERNAL_DESCRIPTION = plistString.match(/INTERNAL_DESCRIPTION<\/key>\s*<string>([^<]+)/i)
          , matchNUMBER_PICKER = plistString.match(/<key>NUMBER_PICKER<\/key>/i)
          , matchValNUMBER_PICKER_DESCRIPTION = plistString.match(/NUMBER_PICKER_DESCRIPTION<\/key>\s*<string>([^<]+)/i)
          , isMetaQuestion = matchMETA_QUESTION && matchMETA_QUESTION.length > 0
          , isNumberPicker = matchNUMBER_PICKER && matchNUMBER_PICKER.length > 0
          , toolName = (matchValTOOL_KEY && matchValTOOL_KEY.length > 1 && matchValTOOL_KEY[1]) || ((isMetaQuestion || isNumberPicker) && 'NONE') // Meta Questions optionally have TOOL_KEY. Other questions must have TOOL_KEY
          , internalDescription = matchValINTERNAL_DESCRIPTION && matchValINTERNAL_DESCRIPTION.length > 1 && matchValINTERNAL_DESCRIPTION[1] || ''
          , problemDescription
        ;

        if (isMetaQuestion) {
            problemDescription = matchValMETA_QUESTION_TITLE && matchValMETA_QUESTION_TITLE.length > 1 && matchValMETA_QUESTION_TITLE[1];
        } else if (isNumberPicker) {
            problemDescription = matchValNUMBER_PICKER_DESCRIPTION && matchValNUMBER_PICKER_DESCRIPTION.length > 1 && matchValNUMBER_PICKER_DESCRIPTION[1];
        } else {
            problemDescription = matchValPROBLEM_DESCRIPTION && matchValPROBLEM_DESCRIPTION.length > 1 && matchValPROBLEM_DESCRIPTION[1]
        }

        if (!toolName || !problemDescription) {
            if (typeof callback == 'function') callback(
                'invalid plist - missing values for keys: '
                    + (toolName ? '' : ' TOOL_KEY')
                    + (toolName || problemDescription ? '' : ' and')
                    + (problemDescription ? '' : (isMetaQuestion ? ' META_QUESTION_TITLE' : (toolName == 'NUMBER_PICKER' ? 'NUMBER_PICKER_DESCRIPTION' : ' PROBLEM_DESCRIPTION')))
            );
            return;
        }
        queryView(encodeURI('any-by-type-name?key=' + JSON.stringify(['tool',toolName])), function(e,r,b) {
            var rows = !e && r && r.statusCode == 200 && JSON.parse(b).rows
              , toolId = rows && rows.length && rows[0].id
            ;

            if (toolId || isMetaQuestion || isNumberPicker) callback(null, plistString, problemDescription, toolId, internalDescription);
            else callback(util.format('invalid plist. Could not retrieve tool with name %s. -- Database callback: (error:%s, statusCode:%d)', toolName, e, r.statusCode));
        });
    });
}

function insertConceptNode(o, callback) {
    var errors = '';
    if (typeof o.nodeDescription != 'string' || !o.nodeDescription.length) errors += 'String value required for "nodeDescription". "';
    if (isNaN(o.x)) errors += 'Float value required for "x". ';
    if (isNaN(o.y)) errors += 'Float value required for "y". ';

    if (errors.length) {
        callback('Error. Could not create concept node. Bad argument. ' + errors, 500);
        return;
    }

    o.type = 'concept node';
    o.pipelines = [];
    o.tags = [];
    o.taggedNotes = {};

    insertDoc(o, function(e,r,b) {
        if (201 != r.statusCode) {
            callback(util.format('Error inserting concept node. Database reported error:"%s"',e), r.statusCode);
            return;
        }

        getDoc(JSON.parse(b).id, function(e,r,b) {
            if (200 != r.statusCode)  {
                callback(util.format('Error retrieving newly created concept node. Database reported error: "%s"',e), r.statusCode);
            } else {
                callback(null, 201, JSON.parse(b));
            }
        });
    });
}

function deleteConceptNode(conceptNodeId, conceptNodeRev, callback) {
    getDoc(conceptNodeId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('Could not retrieve concept node. Database Error: "%s". The concept node was not deleted.', e), r.statusCode);
            return;
        }

        var node = JSON.parse(b);

        if (node._rev != conceptNodeRev) {
            callback(util.format('Concept node revisions do not correspond. supplied:"%s", database:"%s". The concept node was not deleted.', conceptNodeRev, node._rev), 500);
            return;
        }
        
        if (node.pipelines.length) {
            callback('Concept node contains pipelines. The concept node was not deleted.', 500);
            return;
        }

        // N.B. if/when relations other than binary relations come into being, this function will need updating
        // TODO: genericise
        // remove from binary relations
        
        queryView(encodeURI('relations-by-relation-type-name?startkey='+JSON.stringify(['binary'])+'&endkey='+JSON.stringify(['binary',{}])+'&include_docs=true'), function(e,r,b) {
            var updatedBinaryRelations = []
              , binaryRelations = 200 == r.statusCode && JSON.parse(b).rows
              , i
              , len = binaryRelations && binaryRelations.length
              , relation, extantRelationPairs
              , ix
            ;

            if (200 != r.statusCode) {
                callback(util.format('error retrieving binary relations. Datbase reported error: "%s". The concept node was not deleted.', e), r.statusCode);
                return;
            }

            for (i=0; i<len; i++) {
                relation = binaryRelations[i].doc;
                extantRelationPairs = _.filter(relation.members, function(pair) { return !~pair.indexOf(conceptNodeId); });
                if (extantRelationPairs.length < relation.members.length) {
                    relation.members = extantRelationPairs;
                    updatedBinaryRelations.push(relation);
                }
            } 

            if (updatedBinaryRelations.length) {
                request({
                    method:'POST'
                    , uri: databaseURI + '_bulk_docs'
                    , headers: { 'content-type':'application/json', accepts:'application/json' }
                    , body: JSON.stringify({ docs:updatedBinaryRelations })
                }, function(e,r,b) {
                    if (201 != r.statusCode) {
                        callback(util.format('error updating binary relations featuring concept node. Database reported error: "%s". The concept node was not deleted.', e), r.statusCode);
                        return;
                    }

                    _.each(JSON.parse(b), function(update) {
                        _.find(updatedBinaryRelations, function(rel) { return rel._id == update.id; })._rev = update.rev;
                    });
                    
                    delCN();
                });
            } else {
                delCN();
            }

            function delCN() {
                deleteDoc(conceptNodeId, conceptNodeRev, function(e,r,b) {
                    if (200 != r.statusCode) {
                        callback(util.format('error deleting concept node. Database reported error: "%s". The node was removed from all binary relations but not deleted', e), r.statusCode, updatedBinaryRelations);
                        return;
                    }
                    callback(null,200,updatedBinaryRelations);
                });
            }
        });
    });
}

function updateConceptNodePosition(id, rev, x, y, callback) {
    getDoc(id, function(e,r,b) {
        if (isNaN(x) || isNaN(y)) {
            callback(util.format('BAD ARGS: numeric values required for x and y. Supplied: "%s" and "%s". The concept node position was not updated.', x, y), 500);
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

function updateConceptNodeDescription(id, rev, desc, callback) {
    if ('string' != typeof(desc) || !desc.length) {
        callback('BAD ARGS: string required for concept node description', 500);
    }

    getDoc(id, function(e,r,b) {
        if (r.statusCode != 200) {
            callback(util.format('Could not retrieve concept node. Database Error: "%s". The concept node description was not updated.', e), r.statusCode);
            return;
        }

        var node = JSON.parse(b);

        if (node._rev != rev) {
            callback(util.format('concept node revisions do not correspond. supplied:"%s", database:"%s". The concept node description was not updated.', rev, node._rev), 500);
            return;
        }

        node.nodeDescription = desc;

        updateDoc(node, function(e,r,b) {
            if (201 != r.statusCode) e = util.format('Error updating concept node description. Database Error: "%s"', e);
            callback(e, r.statusCode, JSON.parse(b).rev);
        });
    });
}

function insertConceptNodeTag(conceptNodeId, conceptNodeRev, tagIndex, tagText, callback) {
    if ('string' != typeof tagText) {
        callback('tag supplied is not a string', 500);
        return;
    }

    getDoc(conceptNodeId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('Could not retrieve concept node. Database Error:"%s"', e), r.statusCode || 500);
            return;
        }

        var cn = JSON.parse(b);

        if (cn._rev != conceptNodeRev) {
            callback(util.format('supplied revision:"%s" does not match latest document revision:"%s"', conceptNodeRev, cn._rev), 409);
            return;
        }

        if ('concept node' != cn.type) {
            callback(util.format('id:"%s" does not correspond to a concept node', conceptNodeId), r.statusCode);
            return;
        }

        // TODO: This is ad hoc consideration of mastery tags in combo with prereqs. Genericise
        if ('mastery' != tagText) {
            insertTag();
        } else {
            queryView(encodeURI('relations-by-name?key="Prerequisite"&include_docs=true'), function (e,r,b) {
                if (200 != r.statusCode) {
                    callback(util.format('Could not retrieve "Prerequisite" binary relation. Database Error:"%s"', e), r.statusCode || 500);
                    return;
                }

                var cnPrereqPair = _.find(JSON.parse(b).rows[0].doc.members, function(p) {
                    return conceptNodeId == p[0] || conceptNodeId == p[1];
                });

                if (cnPrereqPair) {
                    callback('node cannot be tagged mastery because it features in a "Prerequisite" relationship', 500);
                    return;
                } else {
                    insertTag();
                }
            });
        }

        function insertTag() {
            if (!cn.tags) cn.tags = [];
            
            if (Math.floor(tagIndex) === tagIndex && tagIndex >= 0 && tagIndex < cn.tags.length) {
                cn.tags.splice(tagIndex, 0, tagText);
            } else {
                cn.tags.push(tagText);
            }

            updateDoc(cn, function(e,r,b) {
                if (201 != r.statusCode) {
                    callback('failed to update concept node', r.statusCode);
                    return;
                }
                callback(null, 201, JSON.parse(b).rev);
            });
        }
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
            callback(util.format('supplied revision:"%s" does not match latest document revision:"%s"', conceptNodeRev, cn._rev), 409);
            return;
        }

        if ('concept node' != cn.type) {
            callback(util.format('id:"%s" does not correspond to a concept node', conceptNodeId), r.statusCode);
            return;
        }

        if (!cn.tags || tagText != cn.tags[tagIndex]) {
            callback(util.format('supplied text:"%s" does not match tag at supplied index:"%s".', tagText, tagIndex), 409);
            return;
        }

        // TODO: This is ad hoc consideration of mastery tags in combo with Mastery relation. Genericise
        if ('mastery' != tagText) {
            deleteTag();
        } else {
            queryView(encodeURI('relations-by-name?key="Mastery"&include_docs=true'), function (e,r,b) {
                if (200 != r.statusCode) {
                    callback(util.format('Could not retrieve "Mastery" binary relation. Database Error:"%s"', e), r.statusCode || 500);
                    return;
                }

                var cnMasteryPair = _.find(JSON.parse(b).rows[0].doc.members, function(p) {
                    return conceptNodeId == p[0] || conceptNodeId == p[1];
                });

                if (cnMasteryPair) {
                    callback('cannot remove tag "mastery" from node because node features in a "Mastery" relationship', 500);
                    return;
                } else {
                    deleteTag();
                }
            });
        }

        function deleteTag() {
            cn.tags.splice(tagIndex, 1);

            updateDoc(cn, function(e,r,b) {
                if (201 != r.statusCode) {
                    callback('failed to update concept node', r.statusCode);
                    return;
                }
                callback(null, 201, JSON.parse(b).rev);
            });
        }
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
            callback(util.format('supplied revision:"%s" does not match latest document revision:"%s"', conceptNodeRev, cn._rev), 409);
            return;
        }

        if ('concept node' != cn.type) {
            callback(util.format('id:"%s" does not correspond to a concept node', conceptNodeId), r.statusCode);
            return;
        }

        if (!cn.tags || currentText != cn.tags[tagIndex]) {
            callback(util.format('supplied text:"%s" does not match tag at supplied index:"%s".', currentText, tagIndex), 409);
            return;
        }

        function updateTag() {
            cn.tags.splice(tagIndex, 1, newText);

            updateDoc(cn, function(e,r,b) {
                if (201 != r.statusCode) {
                    callback('failed to update concept node', r.statusCode);
                    return;
                }
                callback(null, 201, JSON.parse(b).rev);
            });
        }
    });
}

function getChainedBinaryRelationsWithMembers(relationIds, callback) {
    if ('function' == typeof relationIds) {
        callback = relationIds;
        relationIds = null;
    }

    if (relationIds) {
        request(encodeURI(databaseURI + '_all_docs?include_docs=true&keys=' + JSON.stringify(relationIds)), next);
    } else {
        queryView(encodeURI('relations-by-relation-type?key="chained-binary"&include_docs=true'), next);
    }

    function next(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('could not retrieve chained relations. Database reported error: "%s"', e), r.statusCode || 500);
            return;
        }

        var cRs = _.map(JSON.parse(b).rows, function(row) { return row.doc; });

        var bRIds = _.uniq(_.flatten(
              _.map(
                  cRs
                  , function(cR) { return _.map(cR.chain, function(link) { return link.relation; })
              })
        ));

        request(encodeURI(util.format('%s_all_docs?include_docs=true&keys=%s', databaseURI, JSON.stringify(bRIds))), function(e,r,b) {
            if (200 != r.statusCode) {
                callback(util.format('could not retrieve binary relations from chained relations. Database reported error: "%s"', e), r.statusCode || 500);
                return;
            }

            var bRs = _.map(JSON.parse(b).rows, function(row) { return row.doc; });

            cRs.forEach(function(cR) {
                var chainLength = cR.chain.length, i, cRMembers;
                for (i=0; i<chainLength; i++) {
                    var link = cR.chain[i]
                      , tIx = link.direction == -1 ? 1 : 0
                      , hIx = 1  - tIx
                      , bR = _.find(bRs, function(r) { return link.relation == r._id; })
                    ;

                    if (i == 0) {
                        cRMembers = bR.members;
                        if (tIx == 1) cRMembers = _.map(cRMembers, function(cRM) { return [cRM[1],cRM[0]]; });
                    } else {
                        cRMembers = _.map(
                            cRMembers
                            , function(cRM) {
                                return _.map(
                                    _.filter(bR.members, function(bRP) { return cRM[1] == bRP[tIx]; })
                                    , function(bRP) { return [ cRM[0], bRP[hIx] ]; }
                                );
                            }
                        );
                        cRMembers = _.filter(cRMembers, function(cRM) { return cRM.length; });
                        if (!cRMembers.length) break;
                        cRMembers = _.flatten(cRMembers, true);
                    }
                }

                if (cRMembers.length) {
                    cRMembers = _.chain(cRMembers)
                        .groupBy(function(m) { return m.join(','); })
                        .map(function(m) { return [ m[0][0], m[0][1], m.length ]; })
                        .value();
                }

                cR.members = cRMembers;
            });
            callback(null,200,cRs);
        });
    }
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

function addOrderedPairToBinaryRelation(relationId, relationRev, cn1Id, cn2Id, callback) {
    getDoc(relationId, function(e,r,b) {
        if (200 != r.statusCode)  {
            if ('function' == typeof callback) callback(util.format('Error retrieving relation. Database reported error:"%s". The pair was not added to the relation', e),r.statusCode);
            return;
        }

        var relation = JSON.parse(b);

        function addPair() {
            relation.members.push([cn1Id, cn2Id]);

            updateDoc(relation, function(e,r,b) {
                if (201 != r.statusCode) {
                    callback(util.format('Error updating relation. Database reported error:"%s". The pair was not added to the relation', e), r.statusCode);
                    return;
                }

                relation._rev = JSON.parse(b).rev;

                queryView(encodeURI('relations-by-relation-type?include_docs=true&key="chained-binary"'), function(e,r,b) {
                    if (200 != r.statusCode) { 
                        // don't throw error as it's not especially important
                        // TODO: this error however should be logged somewhere for developer attention
                        callback(null, 201, { relation:relation });
                        return;
                    }

                    var cRIds = _.chain(JSON.parse(b).rows)
                        .map(function(r) { return r.doc; })
                        .filter(function(r) {
                            return ~_.pluck(r.chain, 'relation').indexOf(relation._id);
                        })
                        .pluck('_id')
                        .value()
                    ;

                    if (!cRIds.length) {
                        callback(null, 201, { relation:relation });
                        return;
                    }

                    getChainedBinaryRelationsWithMembers(cRIds, function(e, statusCode, cRs) {
                        callback(null, 201, { relation:relation, chainedBinaryRelations:cRs });
                    });
                });
            });
        }

        if ('relation' != relation.type || 'binary' != relation.relationType) {
            callback(util.format('document with id "%s" does not represent a binary relation - pair not added to relation', relationId), r.statusCode);
            return;
        }

        if (relation._rev != relationRev) {
            callback(util.format('supplied revision:"%s" does not match latest document revision:"%s" on document id:"%s"', relationRev, relation._rev, relationId), 409);
            return;
        }

        var duplicate = _.find(relation.members, function(pair) {
            return (pair[0] == cn1Id && pair[1] == cn2Id);
        });

        if (duplicate) {
            callback(util.format('ordered pair ["%s", "%s"] is already a member of binary relation id="%s"', cn1Id, cn2Id, relationId), 500);
            return;
        }

        request(
            encodeURI(util.format('%s_all_docs?keys=%s&include_docs=true', databaseURI, JSON.stringify([cn1Id, cn2Id])))
            , function(e,r,b) {
                if (200 != r.statusCode) {
                    callback(util.format('error retrieving nodes. Database reported error="%s". The pair was not added to the relation', e), r.statusCode);
                    return;
                }

                var rows = JSON.parse(b).rows
                  , r1 = _.find(rows, function(r) { return cn1Id == r.id; })
                  , r2 = _.find(rows, function(r) { return cn2Id == r.id; })
                  , cn1 = r1 && r1.doc
                  , cn2 = r2 && r2.doc
                  , error = ''
                ;

                if (!cn1) error = util.format(' Concept node with id="%s" was not found.', cn1Id);
                if (!cn2) error += util.format(' Concept node with id="%s" was not found.', cn2Id);
                if (error.length) {
                    callback(util.format('Error retrieving concept nodes.%s', error), 404);
                    return;
                }

                if ('concept node' != cn1.type) error = util.format(' Document with id="%s" has type="%s". Required:"concept node".', cn1Id, cn1.type);
                if ('concept node' != cn2.type) error += util.format(' Document with id="%s" has type="%s". Required:"concept node".', cn2Id, cn2.type);
                if (error.length) {
                    callback(util.format('Error retrieving concept nodes.%s', error), 500);
                    return;
                }
                // TODO: This block is an ugly ad hoc hack, get generic asap
                switch (relation.name) {
                    case 'Prerequisite':
                        if (~cn1.tags.indexOf('mastery') || ~cn2.tags.indexOf('mastery')) {
                            callback('Error: one or both concept nodes are tagged "mastery". The prerequisite relationship is invalid for mastery nodes.', 500);
                            return;
                        }
                        queryView(encodeURI('relations-by-name?include_docs=true&key="Mastery"'), function(e,r,b) {
                            if (200 != r.statusCode) {
                                callback(util.format('Error: could not retrieve relation named "Mastery" to check for implicit creation of bi-directional derived link between mastery nodes. Database reported error="%s"', e), r.statusCode);
                                return;
                            }
                            var rows = JSON.parse(b).rows
                              , mastery = rows.length && rows[0].doc
                              , ds = cn1
                              , us = cn2
                            ;
                            if (!mastery) {
                                callback('Error: could not retrieve relation named "Mastery" to check for implicit creation of bi-directional derived link between mastery nodes. Zero rows returned', 500);
                                return;
                            }

                            var usMasteryPair = _.find(mastery.members, function(p) { return p[0] == us._id; })
                              , dsMasteryPair = _.find(mastery.members, function(p) { return p[0] == ds._id; })
                              , us_m_id = usMasteryPair && usMasteryPair[1]
                              , ds_m_id = dsMasteryPair && dsMasteryPair[1]
                            ;

                            // if either node doesn't have mastery node, then creation of prerequisite link won't implicitly create bi-directional link between mastery nodes
                            // likewise if both nodes have same mastery node
                            if (!us_m_id || !ds_m_id || us_m_id == ds_m_id) {
                                addPair();
                                return;
                            }

                            // are the two mastery nodes linked in opposite direction to that which new prereq will bring about?
                            var us_m_c_ids = _.map(_.filter(mastery.members, function(p) { return p[1] == us_m_id; }), function(p) { return p[0]; })
                              , ds_m_c_ids = _.map(_.filter(mastery.members, function(p) { return p[1] == ds_m_id; }), function(p) { return p[0]; })
                              , oppDirInterMasteryLinks = _.filter(relation.members, function(p) {
                                  return ~us_m_c_ids.indexOf(p[0]) && ~ds_m_c_ids.indexOf(p[1]);
                              })
                            ;

                            // if two mastery nodes linked in opposite direction, then there's a problem
                            if (oppDirInterMasteryLinks.length) {
                                callback(util.format('Error: prerequisite relationship would bring about bi-directional link between mastery nodes with ids "%s" and "%s"', ds_m_id, us_m_id), 500);
                                return;
                            }

                            // all good
                            addPair();
                            return;
                        });
                    break;
                    case 'Mastery':
                        if (~cn1.tags.indexOf('mastery') || !~cn2.tags.indexOf('mastery')) {
                            callback('Error: invalid "mastery" relationship. Link tail node MUST NOT be tagged "mastery" and link head node MUST be tagged "mastery"', 500);
                            return;
                        }
                        if (_.find(relation.members, function(p) { return p[0] == cn1Id; })) {
                            callback(util.format('Error: node id="%s" already contributes towards a mastery node.', cn1Id), 500);
                            return;
                        }
                        queryView(encodeURI('relations-by-name?include_docs=true&key="Prerequisite"'), function(e,r,b) {
                            if (200 != r.statusCode) {
                                callback(util.format('Error: could not retrieve relation named "Prerequisite" to check for bi-directional derived link between mastery nodes. Database reported error="%s"', e), r.statusCode);
                                return;
                            }
                            var rows = JSON.parse(b).rows
                              , prereq = rows.length && rows[0].doc
                              , c = cn1
                              , m = cn2
                            ;
                            if (!prereq) {
                                callback('Error: could not retrieve relation named "Prerequisite" to check for bi-directional derived link between mastery nodes. Zero rows returned', 500);
                                return;
                            }
                            // find the mastery nodes of c's prereqs/dependees
                            // (1) ids of nodes for which c is a prerequisite
                            var c_us_c_ids = _.uniq(_.map(
                                _.filter(prereq.members, function(p) { return p[0] == c._id; })
                                , function(p) { return p[1]; }
                            ));
                            // (2) ids of nodes that are prerequisites of c
                            var c_ds_c_ids = _.uniq(_.map(
                                _.filter(prereq.members, function(p) { return p[1] == c._id; })
                                , function(p) { return p[0]; }
                            ));
                            // (3) ids of mastery nodes of (1) excluding m._id -- i.e. upstream mastery nodes of c
                            var c_us_c_m_ids = _.uniq(_.map(
                                _.filter(relation.members, function(p) { return m._id != p[1] && ~c_us_c_ids.indexOf(p[0]) })
                                , function(p) { return p[1]; }
                            ));
                            // (4) ids of mastery nodes of (2) excluding m._id -- i.e. downstream mastery nodes of c
                            var c_ds_c_m_ids = _.uniq(_.map(
                                _.filter(relation.members, function(p) { return m._id != p[1] && ~c_ds_c_ids.indexOf(p[0]) })
                                , function(p) { return p[1]; }
                            ));
                            // find the mastery nodes that m is connected to
                            // (5) m's children ids
                            var m_c_ids = _.map(
                                _.filter(relation.members, function(p) { return p[1] == m._id; })
                                , function(p) { return p[0]; }
                            );
                            // (6) m's children's upstream node ids
                            var m_us_c_ids = _.uniq(_.map(
                                _.filter(prereq.members, function(p) { return ~m_c_ids.indexOf(p[0]); })
                                , function(p) { return p[1]; }
                            ));
                            // (7) m's children's downstream node ids
                            var m_ds_c_ids = _.uniq(_.map(
                                _.filter(prereq.members, function(p) { return ~m_c_ids.indexOf(p[1]); })
                                , function(p) { return p[0]; }
                            ));
                            // (8) mastery nodes of (6) -- i.e. upstream mastery nodes of m
                            var m_us_c_m_ids = _.uniq(_.map(
                                _.filter(relation.members, function(p) { return m._id != p[1] && ~m_us_c_ids.indexOf(p[0]) })
                                , function(p) { return p[1]; }
                            ));
                            // (9) mastery nodes of (7) -- i.e. downstream mastery nodes of m
                            var m_ds_c_m_ids = _.uniq(_.map(
                                _.filter(relation.members, function(p) { return m._id != p[1] && ~m_ds_c_ids.indexOf(p[0]) })
                                , function(p) { return p[1]; }
                            ));

                            // if there's any intersection between (3) and (9), or between (4) and (8), then there's circular dependency
                            var is = _.intersection(c_us_c_m_ids, m_ds_c_m_ids).concat(_.intersection(c_ds_c_m_ids, m_us_c_m_ids));
                            /*
                            console.log(
                                '\n\nc_us_c_ids: %s\nc_ds_c_ids: %s\nc_us_c_m_ids: %s\nc_ds_c_m_ids: %s\nm_c_ids: %s\nm_us_c_ids: %s\nm_ds_c_ids: %s\nm_us_c_m_ids: %s\nm_ds_c_m_ids: %s\nis: %s\n'
                                , c_us_c_ids, c_ds_c_ids, c_us_c_m_ids, c_ds_c_m_ids, m_c_ids, m_us_c_ids, m_ds_c_ids, m_us_c_m_ids, m_ds_c_m_ids, is);
                            //*/
                            if (is.length) {
                                callback(util.format('Error: adding this link would create bi-directional links between the selected mastery node and the following mastery node(s): %s.', JSON.stringify(is), 500));
                                return;
                            }

                            // all good
                            addPair();
                            return;
                        });
                    break;
                }
            }
        );
    });
}

function removeOrderedPairFromBinaryRelation(relationId, relationRev, pair, callback) {
    getDoc(relationId, function(e,r,b) {
        if (200 != r.statusCode)  {
            if ('function' == typeof callback) callback(util.format('Error retrieving relation. Database reported error:"%s". The pair was not removed from the relation', e),r.statusCode);
            return;
        }

        var relation = JSON.parse(b);

        if ('relation' != relation.type || 'binary' != relation.relationType) {
            callback(util.format('document with id "%s" does not represent a binary relation - pair not removed from relation', relationId), r.statusCode);
            return;
        }

        if (relation._rev != relationRev) {
            callback(util.format('supplied revision:"%s" does not match latest document revision:"%s" on document id:"%s"', relationRev, relation._rev, relationId), 409);
            return;
        }

        var match = _.find(relation.members, function(p) {
            return pair[0] == p[0] && pair[1] == p[1];
        });

        if (!match) {
            callback(util.format('ordered pair ["%s", "%s"] not a member of binary relation id="%s"', pair[0], pair[1], relationId), 500);
            return;
        }

        relation.members.splice(relation.members.indexOf(match), 1);

        updateDoc(relation, function(e,r,b) {
            if (201 != r.statusCode) {
                callback(util.format('Error updating relation. Database reported error:"%s". The pair was not removed from the relation', e), r.statusCode);
                return;
            }

            var rev = JSON.parse(b).rev;

            queryView(encodeURI('relations-by-relation-type?include_docs=true&key="chained-binary"'), function(e,r,b) {
                if (200 != r.statusCode) { 
                    // don't throw error as it's not especially important
                    // TODO: this error however should be logged somewhere for developer attention
                    callback(null, 201, { rev:rev });
                    return;
                }

                var cRIds = _.chain(JSON.parse(b).rows)
                    .map(function(r) { return r.doc; })
                    .filter(function(r) {
                        return ~_.pluck(r.chain, 'relation').indexOf(relation._id);
                    })
                    .pluck('_id')
                    .value()
                ;

                if (!cRIds.length) {
                    callback(null, 201, { rev:rev });
                    return;
                }

                getChainedBinaryRelationsWithMembers(cRIds, function(e, statusCode, cRs) {
                    callback(null, 201, { rev:rev, chainedBinaryRelations:cRs });
                });
            });
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
            callback(util.format('concept node revisions do not correspond. supplied:"%s", database:"%s". The pipeline was not created.', conceptNodeRev, conceptNode._rev), 500);
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

function reorderConceptNodePipelines(conceptNodeId, conceptNodeRev, pipelineId, oldIndex, newIndex, callback) {
    if (!/^\d+$/.test(oldIndex)) {
        callback('bad argument suppied for oldIndex:"%s" - positive integer required', oldIndex);
        return;
    }
    if (!/^\d+$/.test(newIndex)) {
        callback('bad argument suppied for newIndex:"%s" - postive integer required', newIndex);
        return;
    }

    getDoc(conceptNodeId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('could not retrieve document with id="%s" from database. Error reported: "%s"', conceptNodeId, e), r.statusCode);
            return;
        }
        var cn = JSON.parse(b);

        if (cn._rev != conceptNodeRev) {
            callback(util.format('Error: Concept node revisions do not correspond. Supplied:"%s", Database:"%s". The pipelines were not reordered', conceptNodeRev, cn._rev), 500);
            return;
        }
        if ('concept node' != cn.type) {
            callback(util.format('Error: Document with id="%s" is not a concept node. The pipelines were not reordered.',conceptNodeId), 500);
            return;
        }
        if (pipelineId != cn.pipelines[oldIndex]) {
            callback(util.format('Concept node id="%s" does not have pipeline id="%s" at index="%s". The pipelines were not reordered', conceptNodeId, pipelineId, oldIndex));
            return;
        }
        if (newIndex >= cn.pipelines.length) {
            callback(util.format('Invalid new index:"%s" supplied for pipeline id="%s" on concept node id="%s" which contains %s pipelines.', newIndex, pipelineId, cn._id, cn.pipelines.length),500);
            return;
        }

        cn.pipelines.splice(oldIndex,1);
        cn.pipelines.splice(newIndex,0,pipelineId);

        updateDoc(cn, function(e,r,b) {
            if (201 != r.statusCode) {
                callback(util.format('error reordering concept node pipelines. Database reported error:"%s".', e), r.statusCode);
                return;
            }
            callback(null,201,JSON.parse(b).rev);
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

function appendProblemsToPipeline(plId, problemIds, callback) {
    getDoc(plId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback('could not retrieve pipeline. Problems not added to pipeline.', r.statusCode);
            return;
        }

        var pl = JSON.parse(b);
        pl.problems = pl.problems.concat(problemIds);

        updateDoc(pl, function(e,r,b) {
            if (201 != r.statusCode) {
                callback(util.format('could not add problems to pipeline. DB error: "%s"', e), r.statusCode);
                return;
            }
            callback(null, 201, JSON.parse(b));
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
            callback(util.format('pipeline revisions do not correspond. supplied:"%s", database:"%s". The pipeline problems could not be retrieved.', rev, pl._rev), 500);
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
                    , desc: typeof p.doc.internalDescription == 'string' && p.doc.internalDescription.length ? p.doc.internalDescription : p.doc.problemDescription
                    , lastModified: typeof p.doc.dateModified == 'string' ? p.doc.dateModified.replace(/.{8}$/, '').replace(/[a-z]/gi, ' ') : ''
                };
            });

            callback(null, 200, problems);
        });
    });
}

function reorderPipelineProblems(pipelineId, pipelineRev, problemId, oldIndex, newIndex, callback) {
    if (!/^\d+$/.test(oldIndex)) {
        callback('bad argument suppied for oldIndex:"%s" - positive integer required', oldIndex);
        return;
    }
    if (!/^\d+$/.test(newIndex)) {
        callback('bad argument suppied for newIndex:"%s" - postive integer required', newIndex);
        return;
    }

    getDoc(pipelineId, function(e,r,b) {
        if (200 != r.statusCode) {
            callback(util.format('could not retrieve document with id="%s" from database. Error reported: "%s"', pipelineId, e), r.statusCode);
            return;
        }
        var pl = JSON.parse(b);

        if (pl._rev != pipelineRev) {
            callback(util.format('Error: Pipeline revisions do not correspond. Supplied:"%s", Database:"%s". The pipeline problems were not reordered', pipelineRev, pl._rev), 500);
            return;
        }
        if ('pipeline' != pl.type) {
            callback(util.format('Error: Document with id="%s" is not a pipeline. The pipeline problems were not reordered.',pipelineId), 500);
            return;
        }
        if (problemId != pl.problems[oldIndex]) {
            callback(util.format('Pipeline id="%s" does not have problem id="%s" at index="%s". The pipeline problems were not reordered', pipelineId, problemId, oldIndex));
            return;
        }
        if (newIndex >= pl.problems.length) {
            callback(util.format('Invalid new index:"%s" supplied for problem id="%s" on pipeline id="%s". The pipeline contains %s problems.', newIndex, problemId, pl._id, pl.problems.length),500);
            return;
        }

        pl.problems.splice(oldIndex,1);
        pl.problems.splice(newIndex,0,problemId);

        updateDoc(pl, function(e,r,b) {
            if (201 != r.statusCode) {
                callback(util.format('error reordering pipeline. Database reported error:"%s".',e), r.statusCode);
                return;
            }
            callback(null,201,JSON.parse(b).rev);
        });
    });
}

function getAppContent(callback) {
    // TODO: include database revision
    var validPipelineNames = ['25May']
      , guaranteeExportNodeTags = ['mastery']
      , nodeTagBitCols = ['mastery']
      , path
      , dbPath
      , pdefsPath
    ;

    //console.log('get app content...');

    request(couchServerURI + '_uuids', function(e,r,b) {
        if (200 != r.statusCode) {
            callback(e || 'error generating uuid', statusCode || 500);
            return;
        }

        path = '/tmp/' + JSON.parse(b).uuids[0];
        pdefsPath = path + '/pdefs';
        dbPath = path + '/content.db';

        fs.mkdirSync(path);
        fs.mkdirSync(pdefsPath);

        //console.log('content path:', path);

        getAppContentJSON(function(e, statusCode, content) {
            if (200 != statusCode) {
                callback(e || 'error retrieving app json content', statusCode || 500);
                return;
            }

            //console.log('\n\nJSON:\n%s\n', JSON.stringify(content,null,2));

            createSQLiteDB(content, function(e, statusCode) {
                if (201 != statusCode) {
                    callback(e || 'error creating content database', statusCode || 500);
                    return;
                }
                writePDefs(content.problems, function(e, statusCode, pdefs) {
                    if (201 != statusCode) {
                        callback(e || 'error writing problem pdefs', statusCode || 500);
                        return;
                    }

                    exec('zip content.zip -r pdefs content.db', { cwd:path }, function(e, stdout, stderr) {
                        if (e) {
                            callback(util.format('error zipping content:"%s"', e), 500);
                            return;
                        }
                        callback(null, 200, path + '/content.zip');
                    });
                });
            });
        });
    });

    function getAppContentJSON(jsonCallback) {
        getPipelines(function(e, statusCode, pipelines) {
            if (200 != statusCode) {
                jsonCallback(e || 'error retrieving pipelines', statusCode || 500);
                return;
            }
            getNodes(_.keys(pipelines), function(e, statusCode, nodes) {
                if (200 != statusCode) {
                    jsonCallback(e || 'error retrieving concept nodes', statusCode || 500);
                    return;
                }

                getBinaryRelations(_.keys(nodes), function(e, statusCode, binaryRelations) {
                    if (200 != statusCode) {
                        jsonCallback(e || 'error retrieving binary relations', statusCode || 500);
                        return;
                    }

                    getProblems(pipelines, function(e, statusCode, problems) {
                        if (200 != statusCode) {
                            jsonCallback(e || 'error retrieving problems', statusCode || 500);
                            return;
                        }
                        //console.log('app content JSON generated.');
                        jsonCallback(e, statusCode, { conceptNodes:_.values(nodes), pipelines:_.values(pipelines), binaryRelations:binaryRelations, problems:problems });
                    });
                });
            });
        });

        function getPipelines(cb) {
            var len = validPipelineNames.length
              , pipelines = {}
            ;

            if (!len) {
                cb('case when validPipelineNames.length == 0 not handled', 500);
                return;
            }

            (function getPipelinesMatchingNameAtIndex(i) {
                var name = validPipelineNames[i]
                  , qry = encodeURI('pipelines-with-problems-by-name?reduce=false&startkey=' + JSON.stringify([name]) + '&endkey=' + JSON.stringify([name,{}]))
                ;
                if (len) {
                    queryView(qry, function(e,r,b) {
                        if (200 != r.statusCode) {
                            cb(util.format('Error retrieving pipelines with name="%s". db reported error: "%s"', name, e), r.statusCode);
                            return;
                        }

                        var rows = JSON.parse(b).rows
                          , row, key
                          , len = rows.length, i
                        ;

                        for (i=0; i<len; i++) {
                            row = rows[i];
                            pipelines[row.id] = { id:row.id, rev:row.key[2], name:name, problems:row.value };
                        };

                        if (++i < len) {
                            getPipelinesMatchingNameAtIndex(i);
                        } else {
                            cb (null, 200, pipelines);
                        }
                    });
                }
            })(0);
        }

        function getNodes(pipelineIds, cb) {
            getNodesByPipeline(function(plNodeDocs) {
                getNodesByTag(function(tagNodeDocs) {
                    var uniqDocs = _.uniq(plNodeDocs.concat(tagNodeDocs), false, function(doc) { return doc._id; })
                      , nodes = {}
                    ;
                    uniqDocs.forEach(function(doc) {
                        var n = { id:doc._id, rev:doc._rev, pipelines:_.intersect(doc.pipelines, pipelineIds), x:doc.x, y:doc.y };
                        nodeTagBitCols.forEach(function(tag) { n[tag] = ~doc.tags.indexOf(tag) ? 1 : 0 });
                        nodes[n.id] = n;
                    });
                    cb(null,200,nodes);
                });
            });
            
            function getNodesByPipeline(successCB) {
                queryView(encodeURI('concept-nodes-by-pipeline?include_docs=true&keys=' + JSON.stringify(pipelineIds)), function(e,r,b) {
                    if (200 != r.statusCode) {
                        cb(util.format('Error retrieving concept nodes. db reported error: "%s"', e), r.statusCode || 500);
                        return;
                    }
                    successCB(_.map(JSON.parse(b).rows, function(row) { return row.doc; }));
                });
            }

            function getNodesByTag(successCB) {
                if (!guaranteeExportNodeTags.length) {
                    successCB([]);
                    return;
                }
                queryView(encodeURI(util.format('concept-node-tags?keys=%s&include_docs=true&reduce=false', JSON.stringify(guaranteeExportNodeTags))), function(e,r,b) {
                    if (200 != r.statusCode) {
                        cb(util.format('Error retrieving concept node tags. db reported error: "%s"', e), r.statusCode || 500);
                        return;
                    }
                    successCB(_.map(JSON.parse(b).rows, function(row) { return row.doc; }));
                });
            }
        }

        function getBinaryRelations(nodeIds, cb) {
            queryView(encodeURI('relations-by-relation-type?include_docs=true&key="binary"'), function(e,r,b) {
                if (200 != r.statusCode) {
                    cb(util.format('Error retrieving binary relations. db reported error: "%s"', e), r.statusCode || 500);
                    return;
                }
                var binaryRelations = _.map(JSON.parse(b).rows, function(row) {
                    return {
                        id: row.id
                        , rev: row.doc._rev
                        , name: row.doc.name
                        , members: _.filter(row.doc.members, function(m) { return ~nodeIds.indexOf(m[0]) && ~nodeIds.indexOf(m[1]); })
                    };
                });
                
                getChainedBinaryRelationsWithMembers(function(e, statusCode, chainedBinaryRelations) {
                    if (200 != r.statusCode) {
                        cb(util.format('Error retrieving chained binary relations. db reported error: "%s"', e), statusCode || 500);
                        return;
                    }

                    binaryRelations = _.map(chainedBinaryRelations, function(cbr) {
                        return {
                            id: cbr._id
                            , rev: cbr._rev
                            , name: cbr.name
                            , members: cbr.members
                        };
                    }).concat(binaryRelations);

                    cb(null, 200, binaryRelations);
                });
            });
        };

        function getProblems(pipelines, cb) {
            var problemIds = _.uniq(_.flatten(_.map(pipelines, function(pl) { return pl.problems; })));

            request(encodeURI(databaseURI + '_all_docs?keys=' + JSON.stringify(problemIds)), function(e,r,b) {
                if (200 != r.statusCode) {
                    cb(util.format('Error retrieving binary relations. db reported error: "%s"', e), r.statusCode || 500);
                    return;
                }

                var problems = _.map(JSON.parse(b).rows, function(row) { return { id:row.id, rev:row.value.rev }; });
                cb(null, 200, problems);
            });
        }
    }

    function writePDefs(problems, cb) {
        var pIds = _.map(problems, function(p) { return p.id; });
        var pdefs = [];

        (function getPDefsRec() {
            var i = pdefs.length
              , id = pIds[i]
            ;
            if (i == pIds.length) {
                //console.log('all pdefs retrieved');
                writePDefsRec(0);
            } else {
                //console.log('get pdef at index', i);
                request({
                    uri: databaseURI + id + '/pdef.plist'
                }, function(e,r,b) {
                    pdefs[i] = b;
                    getPDefsRec();
                });
            }
        })();

        function writePDefsRec(i) {
            if (i == pdefs.length) {
                //console.log('all pdefs written');
                cb(null, 201);
            } else {
                //console.log('write pdef at index:',i);
                fs.writeFileSync(pdefsPath + '/' + pIds[i] + '.plist', pdefs[i]);
                writePDefsRec(i+1);
            }
        }
    }

    function createSQLiteDB(content, cb) {
        var db = new sqlite3.Database(dbPath);
        db.serialize(function() {
            var tagBitColDecs = _.map(nodeTagBitCols, function(tag) { return util.format(', %s INTEGER', tag); }).join('');
            var cnTableCreateScript = util.format("CREATE TABLE ConceptNodes (id TEXT PRIMARY KEY ASC, rev TEXT, pipelines TEXT, x INTEGER, y INTEGER%s)", tagBitColDecs);
            db.run(cnTableCreateScript);

            var cnIns = db.prepare(util.format("INSERT INTO ConceptNodes VALUES (?,?,?,?,?%s)", new Array(nodeTagBitCols.length+1).join(',?')));
            content.conceptNodes.forEach(function(n) {
                var tags = _.map(nodeTagBitCols, function(tag) { return n[tag]; })
                  , cols = [n.id, n.rev, JSON.stringify(n.pipelines), n.x, n.y].concat(tags)
                ;
                cnIns.run.apply(cnIns, cols);
            });
            cnIns.finalize();

            db.run("CREATE TABLE Pipelines (id TEXT PRIMARY KEY ASC, rev TEXT, name TEXT, problems TEXT)");
            var plIns = db.prepare("INSERT INTO Pipelines VALUES (?,?,?,?)");
            content.pipelines.forEach(function(pl) {
                plIns.run(pl.id, pl.rev, pl.name, JSON.stringify(pl.problems));
            });
            plIns.finalize();

            db.run("CREATE TABLE Problems (id TEXT PRIMARY KEY ASC, rev TEXT)");
            var probsIns = db.prepare("INSERT INTO Problems VALUES (?,?)");
            content.problems.forEach(function(p) {
                probsIns.run(p.id, p.rev);
            });
            probsIns.finalize();

            db.run("CREATE TABLE BinaryRelations (id TEXT PRIMARY KEY ASC, rev TEXT, name TEXT, members TEXT)");
            var brIns = db.prepare("INSERT INTO BinaryRelations VALUES (?,?,?,?)");
            content.binaryRelations.forEach(function(br) {
                brIns.run(br.id, br.rev, br.name, JSON.stringify(br.members));
            });
            brIns.finalize();

            /*
            db.each("SELECT * FROM ConceptNodes", function(err, row) {
                console.log('ConceptNode row:', JSON.stringify(row,null,2));
            });
            db.each("SELECT * FROM Pipelines", function(err, row) {
                console.log('Pipeline row:', JSON.stringify(row,null,2));
            });
            db.each("SELECT * FROM Problems", function(err, row) {
                console.log('Problem row:', JSON.stringify(row,null,2));
            });
            db.each("SELECT * FROM BinaryRelations", function(err, row) {
                console.log('BinaryRelation row:', JSON.stringify(row,null,2));
            });
            //*/
            
            //console.log('database created');
            cb(null,201);
        });  
        db.close();
    }
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


