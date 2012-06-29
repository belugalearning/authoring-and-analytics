var fs = require('fs')
  , config = JSON.parse(fs.readFileSync(process.cwd() + '/config.json'))
  , _ = require('underscore')
  , util = require('util')
  , exec = require('child_process').exec
  , kcmModel
;

module.exports = function(kcm_model) {
    kcmModel = kcm_model;

    return {
        pullReplicate: function(req, res) {
            var sourceMatch = req.url.match(/(?:\?|\&)source\=([^&]+)/)
              , filterMatch = req.url.match(/(?:\?|\&)filter\=([^&]+)/)
              , continuousMatch = req.url.match(/(?:\?|\&)continuous\=([^&]+)/)
              , cancelMatch = req.url.match(/(?:\?|\&)cancel\=([^&]+)/)
              , source = sourceMatch && sourceMatch.length > 1 && sourceMatch[1].replace(/^%22/, '').replace(/%22$/, '')
              , filter = filterMatch && filterMatch.length > 1 && filterMatch[1].replace(/^%22/, '').replace(/%22$/, '')
              , continuous = continuousMatch && continuousMatch.length > 1 && continuousMatch[1].replace(/['"]/g, '') == 'true'
              , cancel = cancelMatch && cancelMatch.length > 1 && cancelMatch[1].replace(/['"]/g, '') == 'true'
            ;
            
            if (source.length > 1) {
                kcmModel.pullReplicate(source, filter, continuous, cancel);
                res.send('Replication request received OK. Check stdout for further details');
            } else {
                res.send(500);
            }
        }
        , cannedDatabase: function(req, res) {
            kcmModel.generateUUID(function(uuid) {
                var dbPath = config.databasePath
                  , dbName = kcmModel.databaseName
                  , zipFile = util.format('/tmp/canned-db-%s.zip', uuid)
                ;
                exec(util.format('zip %s %s.couch .%s_design', zipFile, dbName, dbName), { cwd:config.couchDatabasesDirectory }, function(e,stdout,stderr) {
                    if (e) {
                        res.send(e, 500);
                        return;
                    }
                    res.download(zipFile, 'canned-db.zip');
                });
            });
        }
        , getAppContent: function(req, res) {
            kcmModel.getAppContent(function(e, statusCode, contentZip) {
                if (200 != statusCode) {
                    res.send(e || 'error retrieving canned application content');
                    return;
                }
                res.download(contentZip, 'canned-content.zip');
            });
        }
        , updateViewSettings: function(req, res) {
            kcmModel.updateViewSettings(req.body.viewSettings, function(e, statusCode, rev) {
                res.send(e || rev, statusCode || 500);
            });
        }
        , updateExportSettings: function(req, res) {
            kcmModel.updateExportSettings(req.body.exportSettings, function(e, statusCode, rev) {
                res.send(e || rev, statusCode || 500);
            });
        }
        , getMap: function(req, res) {
            var map = { pipelines:{}, nodes:[], prerequisites:[] };
            kcmModel.queryView(encodeURI('pipelines-by-name?include_docs=true'), function(e,r,b) {
                _.each(JSON.parse(b).rows, function(row) { map.pipelines[row.id] = row.doc; });
                kcmModel.queryView(encodeURI('concept-nodes?include_docs=true'), function(e,r,b) {
                    map.nodes = _.map(JSON.parse(b).rows, function(row) { return row.doc; });

                    kcmModel.queryView(encodeURI('relations-by-relation-type-name?startkey='+JSON.stringify(['binary'])+'&endkey='+JSON.stringify(['binary',{}])+'&include_docs=true'), function(e,r,b) {
                        map.binaryRelations = _.map(JSON.parse(b).rows, function(row) { return row.doc; });

                        kcmModel.getChainedBinaryRelationsWithMembers(function(e, statusCode, chainedBinaryRelations) {
                            map.chainedBinaryRelations = chainedBinaryRelations;

                            kcmModel.getDocs(['kcm-export-settings','kcm-view-settings'], function(e,r,b) {
                                var rows = JSON.parse(b).rows;
                                map.exportSettings = rows[0].doc;
                                map.viewSettings = rows[1].doc;
                                res.render('kcm/map', { title:'Knowledge Concept Map', map:map });
                            });
                        });
                    });
                });
            });
        }
        , insertConceptNode: function(req, res) {
            kcmModel.insertConceptNode({ nodeDescription:req.body.nodeDescription, x:req.body.x, y:req.body.y }, function(e, statusCode, conceptNode) {
                res.send(e || conceptNode, statusCode || 500);
            });
        }
        , deleteConceptNode: function(req, res) {
            kcmModel.deleteConceptNode(req.params.conceptNodeId, req.body.conceptNodeRev, function(e, statusCode, updatedBinaryRelations) {
                if (200 != statusCode) {
                    if (updatedBinaryRelations && updatedBinaryRelations.length) {
                        e += ' You will need to refresh the page to bring back an up-to-date version of the map.';
                    }
                    res.send(e, statusCode || 500);
                    return;
                }

                res.send(updatedBinaryRelations);
            });
        }
        , updateConceptNodeDescription: function(req, res) {
            kcmModel.updateConceptNodeDescription(req.params.conceptNodeId, req.body.rev, req.body.nodeDescription, function(e, statusCode, conceptNodeRevision) {
                res.send(e || conceptNodeRevision, statusCode || 500);
            });
        }
        , insertConceptNodeTag: function(req, res) {
            kcmModel.insertConceptNodeTag(req.body.conceptNodeId, req.body.conceptNodeRev, null, req.body.tag, function(e, statusCode, conceptNodeRevision) {
                res.send(e || conceptNodeRevision, statusCode || 500);
            });
        }
        , deleteConceptNodeTag: function(req, res) {
            kcmModel.deleteConceptNodeTag(req.body.conceptNodeId, req.body.conceptNodeRev, req.body.tagIndex, req.body.tagText, function(e, statusCode, conceptNodeRevision) {
                res.send(e || conceptNodeRevision, statusCode || 500);
            });
        }
        , editConceptNodeTag: function(req, res) {
            kcmModel.editConceptNodeTag(req.body.conceptNodeId, req.body.conceptNodeRev, req.body.tagIndex, req.body.currentText, req.body.newText, function(e, statusCode, conceptNodeRevision) {
                res.send(e || conceptNodeRevision, statusCode || 500);
            });
        }
        , addNewPipelineToConceptNode: function(req, res) {
            var cnId = req.body.conceptNodeId
              , cnRev = req.body.conceptNodeRev
              , plName = req.body.pipelineName
            ;
            kcmModel.addNewPipelineToConceptNode(plName, cnId, cnRev, function(e, statusCode, newPipeline, conceptNodeRev) {
                res.send(e || { pipeline:newPipeline, conceptNodeRev:conceptNodeRev }, statusCode || 500);
            });
        }
        , deletePipeline: function(req, res) {
            kcmModel.deletePipeline(req.body.pipelineId, req.body.pipelineRev, req.body.conceptNodeId, req.body.conceptNodeRev, function(e, statusCode, conceptNodeRev) {
                res.send(e || conceptNodeRev, statusCode || 500);
            });
        }
        , reorderConceptNodePipelines: function(req, res) {
            kcmModel.reorderConceptNodePipelines(req.params.conceptNodeId, req.body.conceptNodeRev, req.body.pipelineId, req.body.oldIndex, req.body.newIndex, function(e,statusCode,cnRev) {
                res.send(e || cnRev, statusCode || 500);
            });
        }
        , removeProblemFromPipeline: function(req,res) {
            kcmModel.removeProblemFromPipeline(req.body.pipelineId, req.body.pipelineRev, req.body.problemId, function(e,statusCode,rev) {
                res.send(e || rev, statusCode || 500);
            });
        }
        , updatePipelineSequence: function(req, res) {
            kcmModel.updatePipelineSequence(req.body.pipelineId, req.body.problems, function(e,statusCode) {
                res.send(e, statusCode || 500);
            });
        }
        , pipelinePage: function(req, res) {
            kcmModel.queryView('pipelines-by-name', function(e,r,b) {
                var plId = req.params.pipelineId
                  , pipelines
                ;

                if (200 != r.statusCode) {
                    res.render(util.format('could not retrieve pipeline list: DB Error: "%s"',e) ,r.statusCode);
                    return;
                }

                pipelines = _.map(JSON.parse(b).rows, function(row) { return { id:row.id, name:row.key }; });

                pipelineSequenceViewData(plId, function(data) {
                    data.title = 'Pipelines';
                    data.pipelines = pipelines;
                    res.render('kcm/pipeline-page', data);
                });
            });
        }
        , pipelineSequenceTables: function(req, res) {
            var plId = req.params.pipelineId;
            pipelineSequenceViewData(req.params.pipelineId, function(data) {
                res.render('kcm/pipeline-sequence-tables', data);
            });
        }
        , pipelineProblemDetails: function(req, res) {
            kcmModel.pipelineProblemDetails(req.body.id, req.body.rev, function(e,statusCode,problemDetails) {
                res.send(e || problemDetails, statusCode);
            });
        }
        , uploadProblems: function(req, res) {
            var files = req.files.pdefs
              , numFiles
              , decompiledPDefs = []
              , problems = []
              , plId = req.body['pipeline-id']
              , plRev = req.body['pipeline-rev']
            ;

            if (typeof files == 'undefined') {
                res.send('no files found on form', 500);
                return;
            }

            if (typeof files.slice == 'undefined') files = [files]; // single pdefs not put in array
            numFiles = files.length;

            (function decompileNext(file) {
                var numDecompiled;

                decompileFormPList(file, function(e, decompiledPDef) {
                    if (e) {
                        res.send(e, 500);
                        return;
                    }

                    decompiledPDefs.push(decompiledPDef);
                    numDecompiled = decompiledPDefs.length;

                    if (numDecompiled < numFiles) {
                        decompileNext(files[numDecompiled]);
                    } else {
                        // all pdefs decompiled fine. use them to create problems
                        (function createProblem(pdef) {
                            var numProblems;

                            kcmModel.insertProblem(pdef, function(e, statusCode, newProblem) {
                                if (201 != statusCode) {
                                    res.send('error creating problem: ' + e, statusCode || 500);
                                    return;
                                }

                                problems.push(newProblem);
                                numProblems = problems.length;
                                if (numProblems < numFiles) {
                                    createProblem(decompiledPDefs[numProblems]);
                                } else {
                                    kcmModel.appendProblemsToPipeline(plId, _.map(problems, function(p){return p.id;}), function(e, statusCode, plUpdate) {
                                        if (201 != statusCode) {
                                            res.send(e, statusCode || 500);
                                            return;
                                        }

                                        kcmModel.pipelineProblemDetails(plId, plUpdate.rev, function(e, statusCode, problemDetails) {
                                            res.send(e || { problemDetails:problemDetails, pipelineId:plUpdate.id, pipelineRev:plUpdate.rev }, statusCode || 500);
                                        });
                                    });
                                }
                            });
                        })(decompiledPDefs[0]);
                    }
                });
            })(files[0]);
        }
        , reorderPipelineProblems: function(req, res) {
            kcmModel.reorderPipelineProblems(req.body.pipelineId, req.body.pipelineRev, req.body.problemId, req.body.oldIndex, req.body.newIndex, function(e,statusCode,plRev) {
                res.send(e || plRev, statusCode || 500);
            });
        }
        , updateConceptNodePosition: function(req,res) {
            kcmModel.updateConceptNodePosition(req.body.id, req.body.rev, req.body.x, req.body.y, function(e, statusCode, nodeRevision) {
                res.send(e || nodeRevision, statusCode || 500);
            });
        }
        , addPairToBinaryRelation: function(req,res) {
            var argErrors = []
              , relation = req.body.relation
              , pair = req.body.pair
            ;
            if (!relation) argErrors.push('"relation"');
            if (!pair) argErrors.push('"pair"');
            if (argErrors.length) {
                res.send('Bad Arguments - ' + argErrors.join(' and ') + ' required and missing', 400);
                return;
            }

            if (relation.type != 'new' && relation.type != 'existing') {
                res.send('Bad Arguments - either "new" or "existing" required for relation.type', 400);
                return;
            }

            if (relation.type == 'existing') {
                addPair();
            } else {
                if ('string' != typeof relation.name) argErrors.push('"relation.name"');
                if ('string' != typeof relation.description) argErrors.push('"relation.description"');
                if (argErrors.length) {
                    res.send('Bad Arguments - strings required for ' + argErrors.join(' and '), 400);
                    return;
                }

                kcmModel.insertBinaryRelation(relation.name, relation.description, function(e,statusCode,b) {
                    var row = JSON.parse(b);
                    if (201 != statusCode) {
                        res.send(e, statusCode)
                        return;
                    }
                    relation.id = row.id;
                    relation.rev = row.rev;
                    addPair();
                });
            }

            function addPair() {
                kcmModel.addOrderedPairToBinaryRelation(relation.id, relation.rev, pair[0], pair[1], function(e,statusCode,updatedRelation) {
                    res.send(e || updatedRelation, statusCode || 500);
                });
            }
        }
        , removePairFromBinaryRelation: function(req, res) {
            kcmModel.removeOrderedPairFromBinaryRelation(req.params.binaryRelationId, req.body.rev, req.body.pair, function(e, statusCode, rev) {
                res.send(e || rev, statusCode || 500);
            });
        }
    };
};

function pipelineSequenceViewData(plId, callback) {
    var incl = []
      , excl = []
      , pl
      , doCallback = function() {
          callback({
              pipelineId: plId
              , includedProblems:incl   , includedProblemIds:pl && pl.problems || []
              , excludedProblems:excl   , excludedProblemIds:_.map(excl, function(p) { return p._id; })
          });
      }
    ;

    if (!plId) {
        doCallback();
        return;
    }

    kcmModel.getDoc(plId, function(e,r,b) {
        if (r.statusCode != 200) {
            doCallback();
            return;
        }

        pl = JSON.parse(b);

        kcmModel.queryView(encodeURI('by-type?include_docs=true&key="problem"'), function(e,r,b) {
            var probs = _.map(JSON.parse(b).rows, function(row) { return row.doc; });

            incl = _.sortBy(
                  _.filter(probs, function(p) { return pl.problems.indexOf(p._id) > -1; })
                  , function(p) { return pl.problems.indexOf(p._id); }
            );
            excl = _.sortBy(
                  _.filter(probs, function(p) { return pl.problems.indexOf(p._id) == -1; })
                  , function(p) { return p.problemDescription; }
            );

            _.each(incl, function(p) { p.orderOn = incl.indexOf(p); });
            _.each(excl, function(p) { p.orderOn = excl.indexOf(p); });

            doCallback();
        });
    });
}

function decompileFormPList(plist, callback) {
    var path = plist && plist.path || undefined
      , command = util.format('perl %s/routes/plutil.pl %s.plist', process.cwd(), path)
      , isBinary
    ;

    if (!path) {
        callback(util.format('plist not found at path "%s".\n\tplist:\n', path, plist));
        return;
    }

    fs.rename(path, path + '.plist', function(e) {
        if (e) {
            callback(util.format('error renaming file: "%s"',e));
            return;
        }

        fs.readFile(path+'.plist', function(e,data) {
            if (e) {
                callback(util.format('error reading file - error reported: "%s"',e));
                return;
            }

            isBinary = data.toString('utf8').substring(0,6) == 'bplist';
            if (!isBinary) {
                // not a compiled plist
                callback(null, path+'.plist');
                return;
            }

            exec(command, function(err, stdout, stderr) {
                if (err) {
                    callback(util.format('plutil was unable to process the file. error: "%s"', err));
                    return;
                }

                if (!/BinaryToXML/.test(stdout)) {
                    callback('error decompiling plist: plutil output format unrecognised');
                    return;
                }

                callback(null, path+'.text.plist');
            });
        });
    });
}
