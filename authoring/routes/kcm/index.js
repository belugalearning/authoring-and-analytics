var fs = require('fs')
  , _ = require('underscore')
  , util = require('util')
  , exec = require('child_process').exec
  , stream = require('stream')
  , zipstream = require('zipstream')
  , kcmController
;

var format = util.format;
var plutilCommand = format('perl %s/../plutil.pl ', __dirname)

module.exports = function(config, legacyKCMController, kcm) {
  kcmController = legacyKCMController

  return {
    pullReplicate: function(req, res) {
      // TODO: ffs, use QueryString or URL modules maybe?
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
            kcmController.pullReplicate(source, filter, continuous, cancel);
            res.send('Replication request received OK. Check stdout for further details');
        } else {
            res.send(500);
        }
    }
    , cannedDatabase: function(req, res) {
      var uuid = kcmController.generateUUID()
        , dbPath = config.couchDatabasesDirectory
        , dbName = kcmController.databaseName
        , zipFile = format('/tmp/canned-db-%s.zip', uuid)

      exec(format('zip %s %s.couch .%s_design', zipFile, dbName, dbName), { cwd:dbPath }, function(e,stdout,stderr) {
        if (e) {
          res.send(e, 500)
          return
        }
        res.download(zipFile, 'canned-db.zip')
      })
    }
    , getAppCannedDatabases: function(req, res) {
      kcmController.getAppCannedDatabases(plutilCommand, req.session.user._id, req.app.config.authoring.pipelineWorkflowStatuses, function(e, statusCode, path) {
        if (200 != statusCode) {
          res.send(e || 'error retrieving canned application content', statusCode || 500)
          return
        }
        exec('zip canned-dbs.zip -r *', { cwd:path }, function(e, stdout, stderr) {
          if (e) res.send(util.format('error zipping databases: "%s"', e), 500)
          else res.download(path+'/canned-dbs.zip')
        })
      })
    }
    , appImportContent: function(req, res) {
      for (var id in kcm.docStores.users) {
        if (kcm.docStores.users[id].loginName == req.params.loginName) {
          kcmController.getAppCannedDatabases(plutilCommand, id, req.app.config.authoring.pipelineWorkflowStatuses, function(e, statusCode, path) {
            if (200 != statusCode) {
              res.send(e, statusCode || 500)
            } else {
              exec('zip ../canned-content.zip -r *', { cwd:path + '/canned-content' }, function(e, stdout, stderr) {
                if (e) res.send(util.format('error zipping content: "%s"', e), 500)
                else res.download(path + '/canned-content.zip')
              })
            }
          })
          return
        }
      }
      res.send(util.format('user with loginName="%s" does not exist', req.params.loginName), 412)
    }
    , downloadToKCMDirs: function(req, res) {
        kcmController.queryView('relations-by-name', 'key', 'Mastery', 'include_docs', true, function(e,r,b) {
            if (!r) {
                res.send('failed to connect to database', 500);
                return;
            }
            if (200 != r.statusCode) {
                res.send(format('error retrieving "mastery" relation. Error: "%s"', e), r.statusCode || 500);
                return;
            }

            var rows = JSON.parse(b).rows
            if (!rows.length) {
                res.send('Mastery relation not found', 500);
                return;
            }

            var mRel = rows[0].doc;

            kcmController.queryView('concept-nodes', 'include_docs', true, function(e,r,b) {
                if (!r) {
                    res.send('failed to connect to database', 500);
                    return;
                }
                if (200 != r.statusCode) {
                    res.send(format('error querying view "concept-nodes". Error: "%s"', e), r.statusCode || 500);
                    return;
                }

                var nodes = JSON.parse(b).rows;
                var masteryNodes = {};
                var nonMasteryNodes = {};
                var regions = {};
                var regionRE = /^region:\s*(\S(?:.*\S)?)\s*$/;

                var makeDirName = function(s) { return s.trim().substring(0,128).replace(/[^a-z0-9\-]+/ig, '_'); }

                nodes.forEach(function(r) {
                    var n = r.doc;

                    if (!~n.tags.indexOf('mastery')) {
                        nonMasteryNodes[n._id] = n;
                        return;
                    }

                    // mastery node
                    masteryNodes[n._id] = n;

                    var numTags = n.tags.length;
                    var region;
                    for (var i=0; i<numTags; i++) {
                        if (regionRE.test(n.tags[i])) {
                            region = n.tags[i].match(regionRE)[1];
                            break;
                        }
                    }

                    if (!region) region = '_REGIONLESS_MASTERY_NODES_';

                    if (!regions[region]) regions[region] = [];

                    n.nodes = [];
                    regions[region].push(n);
                });

                mRel.members.forEach(function(mbr) {
                    var nm = nonMasteryNodes[mbr[0]];
                    var m = masteryNodes[mbr[1]];

                    if (nm && m) {
                        m.nodes.push(nm);
                        delete nonMasteryNodes[mbr[0]];
                    }
                });

                var uuid = kcmController.generateUUID()
                var uuidPath = '/tmp/' + uuid;
                var rootPath = format('%s/tokcm/Number', uuidPath);

                var errors = false;
                var onError = function(e, path, isDir) {
                    if (!errors) {
                        errors = true;
                        res.send(format('error creating %s at path: "%s"  -->  "%s"', (isDir ? 'directory' : 'file'), path, e));
                    }
                };

                exec(format('mkdir -p /tmp/%s/tokcm/Number/', uuid), function(e) {
                    if (e) {
                        onError(e, rootPath, true);
                        return;
                    }

                    var regionNames = _.keys(regions);
                    var regionsOutstanding = regionNames.length;
                    var orphanedNodeIds = _.keys(nonMasteryNodes);
                    var orphansOutstanding =  orphanedNodeIds.length;
                    var numTopDirsOutstanding = regionsOutstanding + (orphansOutstanding > 0 ? 1 : 0);

                    var onAllCreated = function() {
                        var zipFile = format('%s/tokcm.zip', uuidPath);
                        exec(format('zip -r %s tokcm', zipFile), { cwd:uuidPath }, function(e,stdout,stderr) {
                            if (e) {
                                res.send(e, 500);
                                return;
                            }
                            res.download(zipFile, 'tokcm.zip');
                        });
                    };

                    if (numTopDirsOutstanding == 0) onAllCreated();

                    if (orphansOutstanding) {
                        var path = rootPath + '/_NODES_WITHOUT_MASTERY_';
                        fs.mkdir(path, function(e) {
                            if (e) {
                                onError(e, path, true);
                                return;
                            }

                            orphanedNodeIds.forEach(function(id) {
                                var orphanDirPath = format('%s/%s', path, id);
                                fs.mkdir(orphanDirPath, function(e) {
                                    if (e) {
                                        onError(e, orphanDirPath, true);
                                        return;
                                    }
                                    var filePath = orphanDirPath + '/.gitseedir';
                                    fs.writeFile(filePath, function(e) {
                                        if (e) {
                                            onError(e, filePath, true);
                                            return;
                                        }
                                        if (--orphansOutstanding == 0 && --numTopDirsOutstanding == 0) onAllCreated();
                                    });
                                });
                            });
                        });
                    }

                    if (regionsOutstanding) {
                        regionNames.forEach(function(regName) {
                            var regionPath = format('%s/%s', rootPath, makeDirName(regName));
                            fs.mkdir(regionPath, function(e) {
                                if (e) {
                                    onError(e, regionPath, true);
                                    return;
                                }

                                var regionalMasteryNodes = regions[regName];
                                var numMasteryNodesOutstanding = regionalMasteryNodes.length;

                                regionalMasteryNodes.forEach(function(mastery) {
                                    var masteryPath = format('%s/%s', regionPath, makeDirName(mastery.nodeDescription));
                                    fs.mkdir(masteryPath, function(e) {
                                        if (e) {
                                            onError(e, masteryPath, true);
                                            return;
                                        }
                                        var numNodesOutstanding = mastery.nodes.length;
                                        if (numNodesOutstanding == 0) {
                                            var filePath = masteryPath + '/.gitseedir';
                                            fs.writeFile(filePath, '', function(e) {
                                                if (e) {
                                                    onError(e, filePath, false);
                                                    return;
                                                }
                                                if (--numMasteryNodesOutstanding == 0 && --numTopDirsOutstanding == 0) onAllCreated();
                                            });
                                        } else {
                                            mastery.nodes.forEach(function(node) {
                                                var nodePath = format('%s/%s', masteryPath, node._id);
                                                fs.mkdir(nodePath, function(e) {
                                                    if (e) {
                                                        onError(e, nodePath, true);
                                                        return;
                                                    }
                                                    var filePath = nodePath + '/.gitseedir';
                                                    fs.writeFile(filePath, function(e) {
                                                        if (e) {
                                                            onError(e, filePath, false);
                                                            return;
                                                        }
                                                        if (--numNodesOutstanding == 0 && --numMasteryNodesOutstanding == 0 && --numTopDirsOutstanding == 0) onAllCreated();
                                                    });
                                                });
                                            });
                                        }
                                    });
                                });
                            });
                        });
                    }
                });
            });
        });
    }
    , updateUser: function(req, res) {
      if (req.body.user._id !== req.session.user._id) {
        req.send(401)
        return
      }
      kcmController.updateUser(req.body.user, function(e, statusCode, rev) {
        res.send(e || rev, statusCode || 500)
      })
    }
    , getMap: function(req, res) {
      var map = { user:kcm.docStores.users[req.session.user._id], pipelines:kcm.docStores.pipelines, nodes:kcm.docStores.nodes, binaryRelations:{}, chainedBinaryRelations:{}, update_seq:kcm.update_seq, pipelineWorkflowStatuses:req.app.config.authoring.pipelineWorkflowStatuses }

      _.each(kcm.docStores.relations, function(r, id) {
        if (r.relationType == 'binary') {
          map.binaryRelations[id] = r
        } else if (r.relationType == 'chained-binary') {
          var clone = kcm.getDocClone(r._id, 'relation')
          clone.members = kcm.chainedBinaryRelationMembers(r)
          map.chainedBinaryRelations[id] = clone
        }
      })

      res.render('kcm/map', { title:'Knowledge Concept Map', map:map })
    }
    , insertConceptNode: function(req, res) {
      kcmController.insertConceptNode(
        req.session.user._id
        , { nodeDescription:req.body.nodeDescription
          , x:req.body.x
          , y:req.body.y
          , nodeDescription:req.body.nodeDescription || 'NEW CONCEPT NODE'
          , tags: req.body.tags || []
        }
        , function(e, statusCode) { res.send(e, statusCode || 500) }
      )
    }
    , deleteConceptNode: function(req, res) {
      kcmController.deleteConceptNode(req.session.user._id, req.params.conceptNodeId, req.body.conceptNodeRev, function(e, statusCode) {
        if (201 != statusCode) {
          e += ' You will need to refresh the page to bring back an up-to-date version of the map.'
        }
        res.send(e, statusCode || 500)
      })
    }
    , updateConceptNodeDescription: function(req, res) {
        kcmController.updateConceptNodeDescription(req.session.user._id, req.params.conceptNodeId, req.body.rev, req.body.nodeDescription, function(e, statusCode) {
            res.send(e, statusCode || 500);
        });
    }
    , insertConceptNodeTag: function(req, res) {
      kcmController.insertConceptNodeTag(req.session.user._id, req.body.conceptNodeId, req.body.conceptNodeRev, null, req.body.tag, function(e, statusCode, conceptNodeRevision) {
        res.send(e, statusCode || 500)
      })
    }
    , deleteConceptNodeTag: function(req, res) {
      kcmController.deleteConceptNodeTag(req.session.user._id, req.body.conceptNodeId, req.body.conceptNodeRev, req.body.tagIndex, req.body.tagText, function(e, statusCode) {
          res.send(e, statusCode || 500)
      })
    }
    , editConceptNodeTag: function(req, res) {
        kcmController.editConceptNodeTag(req.session.user._id, req.body.conceptNodeId, req.body.conceptNodeRev, req.body.tagIndex, req.body.currentText, req.body.newText, function(e, statusCode) {
            res.send(e, statusCode || 500);
        })
    }
    , uploadPipelineFolder: function(req, res) {
      var encodedCompiledFileStart = new Buffer('bplist').toString('base64')
      var awaiting = req.body.pdefs.length
      var sentError = false

      var next = function(e) {
        if (!sentError) {
          if (e) {
            sentError = true
            res.send(e, 500)
          } else {
            if (!--awaiting) {
              kcmController.uploadPipelineFolder(req.session.user._id, req.body, function(e, statusCode) {
                res.send(e, statusCode || 500)
              })
            }
          }
        }
      }

      req.body.pdefs.forEach(function(pdef, i) {
        if (pdef.slice(0, encodedCompiledFileStart.length) == encodedCompiledFileStart) {
          var path = '/tmp/' + kcmController.generateUUID()
          fs.writeFile(path, pdef, 'base64', function(e) {
            decompileFormPList({ path: path }, function(e, decompiledPath) {
              if (e) {
                next(e)
              } else {
                fs.readFile(decompiledPath, 'base64', function(e, data) {
                  if (!e) req.body.pdefs[i] = data
                  next(e)
                })
              }
            })
          })
        } else {
          next()
        }
      })
    }
    , downloadPipeline: function(req, res) {
      var plId = req.params.pipelineId
      if (!plId) {
        res.send('requires pipeline id', 500)
        return
      }

      res.header('Content-Disposition', 'attachment; filename=pipeline-' + plId + '.zip')
      res.header('Content-Type', 'application/zip')

      var pl = kcm.getDoc(plId, 'pipeline')
      if (!pl) {
        res.send('could not retrieve pipeline: '+plId, 500)
        return
      }

      var sentError = false
        , plistStreams = []

      var zip = zipstream.createZip({ level:1 })
      zip.pipe(res)

      var PListStream = function PListStream(name) {
        this.name = name + '.plist'
        this.readable = true
        this.writable = true
        this.chunks = []
      }
      util.inherits(PListStream, stream.Stream)
      PListStream.prototype.write = function(chunk) {
        if (!this.chunks) {
          this.emit('data', chunk)
          return
        }
        this.chunks.push(chunk)
      }
      PListStream.prototype.end = function() {
        if (!this.chunks) this.emit('end')
        else this.ended = true
      }
      PListStream.prototype.start = function() {
        var self = this
        this.chunks.forEach(function(c) { self.emit('data', c) })
        delete this.chunks
        if (this.ended) this.emit('end')
      }

      pl.problems.forEach(function(problemId, i) {
        plistStreams[i] = new PListStream('pdef_'+i)
        kcmController.getPDef(problemId).pipe(plistStreams[i])
      })

      var plistStream = plistStreams[pl.problems.length] = new PListStream('#meta-pipeline')
      plistStream.write(
        '<?xml version="1.0" encoding="UTF-8"?>' +
        '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">' +
        '<plist version="1.0">' +
          '<dict>' +
            '<key>NODE_ID</key>' +
            '<string>'+req.params.conceptNodeId+'</string>' +
            '<key>PIPELINE_NAME</key>' +
            '<string>'+pl.name+'</string>' +
            '<key>PROBLEMS</key>' +
            '<array>' +
              pl.problems.map(function(problemId, index) { return '<string>pdef_' + index + '.plist</string>' }).join('') +
            '</array>' +
          '</dict>' +
        '</plist>')
      plistStream.end()

      ;(function recZipAddFile(i) {
        var plistStream = plistStreams[i]
        if (plistStream) {
          zip.addFile(plistStream, { name:plistStream.name }, function() { recZipAddFile(i+1) })
          plistStream.start()
        } else {
          zip.finalize()
        }
      })(0)
    }
    , addNewPipelineToConceptNode: function(req, res) {
        var cnId = req.body.conceptNodeId
          , cnRev = req.body.conceptNodeRev
          , plName = req.body.pipelineName
        ;
        kcmController.addNewPipelineToConceptNode(req.session.user._id, plName, cnId, cnRev, function(e, statusCode) {
            res.send(e, statusCode || 500)
        });
    }
    , deletePipeline: function(req, res) {
        kcmController.deletePipeline(req.session.user._id, req.body.pipelineId, req.body.pipelineRev, req.body.conceptNodeId, req.body.conceptNodeRev, function(e, statusCode) {
            res.send(e, statusCode || 500);
        });
    }
    , reorderConceptNodePipelines: function(req, res) {
        kcmController.reorderConceptNodePipelines(req.session.user._id, req.params.conceptNodeId, req.body.conceptNodeRev, req.body.pipelineId, req.body.oldIndex, req.body.newIndex, function(e,statusCode) {
            res.send(e, statusCode || 500)
        })
    }
    , removeProblemFromPipeline: function(req,res) {
        kcmController.removeProblemFromPipeline(req.params.pipelineId, req.params.pipelineRev, req.params.problemId, function(e,statusCode,rev) {
            res.send(e || rev, statusCode || 500);
        });
    }
    , updatePipelineWorkflowStatus: function(req,res) {
        kcmController.updatePipelineWorkflowStatus(req.session.user._id, req.params.id, req.params.rev, req.params.status, function(e, statusCode, rev) {
            res.send(e || rev, statusCode || 500);
        });
    }
    , updatePipelineName: function(req,res) {
        kcmController.updatePipelineName(req.params.id, req.params.rev, req.params.name, function(e, statusCode, rev) {
            res.send(e || rev, statusCode || 500);
        });
    }
    , updatePipelineSequence: function(req, res) {
        kcmController.updatePipelineSequence(req.body.pipelineId, req.body.problems, function(e,statusCode) {
            res.send(e, statusCode || 500);
        });
    }
    , pipelineProblemDetails: function(req, res) {
        kcmController.pipelineProblemDetails(req.body.id, req.body.rev, function(e,statusCode,problemDetails) {
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
        , cnId = req.body['concept-node-id']
        , cnRev = req.body['concept-node-rev']

      if (typeof files == 'undefined') {
        res.send('no files found on form', 500)
        return
      }

      if (typeof files.slice == 'undefined') files = [files] // single pdefs not put in array
      numFiles = files.length

      ;(function decompileNext(file) {
        var numDecompiled

        decompileFormPList(file, function(e, decompiledPDef) {
          if (e) {
            res.send('error decompiling plist: '+e, 500)
            return
          }

          decompiledPDefs.push(decompiledPDef)
          numDecompiled = decompiledPDefs.length

          if (numDecompiled < numFiles) {
            decompileNext(files[numDecompiled])
          } else {
            // all pdefs decompiled fine. use them to create problems
            ;(function createProblem(pdef) {
              var numProblems

              kcmController.insertProblem(pdef, plId, plRev, cnId, cnRev, function(e, statusCode, newProblem) {
                if (201 != statusCode) {
                  res.send('Error creating problem:\n' + e, statusCode || 500)
                  return
                }

                problems.push(newProblem)
                numProblems = problems.length
                if (numProblems < numFiles) {
                  createProblem(decompiledPDefs[numProblems])
                } else {
                  kcmController.appendProblemsToPipeline(plId, _.map(problems, function(p){return p.id;}), function(e, statusCode, plUpdate) {
                    if (201 != statusCode) {
                      res.send(e, statusCode || 500)
                      return
                    }

                    kcmController.pipelineProblemDetails(plId, plUpdate.rev, function(e, statusCode, problemDetails) {
                      res.send(e || { problemDetails:problemDetails, pipelineId:plUpdate.id, pipelineRev:plUpdate.rev }, statusCode || 500)
                    })
                  })
                }
              })
            })(decompiledPDefs[0])
          }
        })
      })(files[0])
    }

    , editProblem: {
      problemDetailPage: function(req, res) {
        var problemId = req.params.problemId
          , problem = problemId && kcm.getDocClone(problemId, 'problem')
          , tool = problem && problem.toolId && kcm.getDocClone(problem.toolId, 'tool')

        if (!problemId) {
          req.send('requires problemId', 412)
          return
        }

        if (!problem) {
          req.send(util.format('problem with id="%s" not found', problemId), 404)
          return
        }

        res.render('problem-detail-page', {
          problemId:problem._id                                   , title:'Edit Problem'
          , problemDescription:problem.problemDescription         , dateCreated:formatDateString(problem.dateCreated)
          , dateModified:formatDateString(problem.dateModified)   , toolName: (tool && tool.name || '')
          , internalDescription:problem.internalDescription || ''
        })
      }
      , sendPList: function(req, res, sendAsAttachment) {
        var problemId = req.params.problemId
        if (!problemId) {
          res.send('requires problemId', 500)
          return
        }

        kcmController.getPDef(problemId, function(e,r,b) {
          if (r.statusCode == 200) {
            if (sendAsAttachment) res.header('Content-Disposition', 'attachment; filename=pdef-' + problemId + '.plist')
            res.header('Content-Type', 'application/xml')
            res.send(b)
          } else {
            res.send('could not retrieve problem plist. statusCode='+r.statusCode, 404)
          }
        })
      }
      , replacePListPage: function(req, res) {
        res.render('upload-plist', { title:'Upload the replacement problem definition PList', problemId:req.params.problemId });
      }
      , performReplacePList: function(req, res) {
        decompileFormPList(req.files.plist, function(e, plist) {
          if (e) {
            res.send(e, 500)
            return
          }

          kcmController.updatePDef(req.params.problemId, plist, function(e) {
            if (e) res.send(e, 500)
            else res.redirect('/kcm/problem/' + req.params.problemId)
          })
        })
      }
    }

    , reorderPipelineProblems: function(req, res) {
        kcmController.reorderPipelineProblems(req.body.pipelineId, req.body.pipelineRev, req.body.problemId, req.body.oldIndex, req.body.newIndex, function(e,statusCode,plRev) {
            res.send(e || plRev, statusCode || 500);
        });
    }
    , updateConceptNodePosition: function(req,res) {
        kcmController.updateConceptNodePosition(req.session.user._id, req.body.id, req.body.rev, req.body.x, req.body.y, function(e, statusCode, nodeRevision) {
            res.send(e || nodeRevision, statusCode || 500);
        });
    }
    , getChainedBinaryRelationsMembers: function(req,res) {
      var cbrsMembers = {}
        , cbr
        , i

      if (Object.prototype.toString.call(req.body.ids) != '[object Array]') {
        res.send('array of strings required for request body.ids', 412)
        return
      }

      for (i=0; i<req.body.ids.length; i++) {
        if (typeof req.body.ids[i] != 'string') {
          res.send('array of strings required for request body.ids', 412)
          return
        }
      }

      for (i=0; i<req.body.ids.length; i++) {
        cbr = kcm.getDocClone(req.body.ids[i], 'relation')

        if (!cbr) {
          res.send(util.format('could not retrieve relation with id="%s"', req.body.ids[i]), 404)
          return
        }

        if (cbr.relationType != 'chained-binary') {
          res.send(util.format('relation with id="%s" does not have relationType="chained-binary"', req.body.ids[i]), 412)
          return
        }

        cbrsMembers[req.body.ids[i]] = kcm.chainedBinaryRelationMembers(cbr)
      }

      res.send(cbrsMembers, 200)
    }
    , addPairToBinaryRelation: function(req,res) {
      // N.B. can add pair to new or existing relation
      var argErrors = []
        , relation = req.body.relation
        , pair = req.body.pair

      var addPair = function() {
        kcmController.addOrderedPairToBinaryRelation(req.session.user._id, relation.id, relation.rev, pair[0], pair[1], function(e, statusCode) {
          res.send(e, statusCode || 500)
        })
      }

      if (!relation) argErrors.push('relation')
      if (!pair) argErrors.push('pair')
      if (argErrors.length) {
        res.send(util.format('Missing Parameters: "%s"', argErrors.join('" and "')), 412)
        return
      }

      if (relation.type != 'new' && relation.type != 'existing') {
        res.send('Bad Arguments - either "new" or "existing" required for relation.type', 412)
        return
      }

      if (relation.type == 'existing') {
        addPair()
      } else {
        // Update kcmController.insertBinaryRelation() to take req.session.user._id as first parameter - and make it comply with document versioning
        res.send('CREATION OF BINARY RELATIONS FROM THE KCM IS CURRENTLY DISABLED. - See Code Comments for instructions to enable.', 500)
        return

        if ('string' != typeof relation.name) argErrors.push('relation.name')
        if ('string' != typeof relation.description) argErrors.push('relation.description')
        if (argErrors.length) {
          res.send(util.format('Bad Arguments - strings required for "%s"', argErrors.join('" and "')), 412)
          return
        }

        kcmController.insertBinaryRelation(relation.name, relation.description, function(e, statusCode) {
          var row = JSON.parse(b)

          if (201 != statusCode) {
            res.send(e, statusCode || 500)
            return
          }

          relation.id = row.id
          relation.rev = row.rev
          addPair()
        })
      }
    }
    , removePairFromBinaryRelation: function(req, res) {
      kcmController.removeOrderedPairFromBinaryRelation(req.session.user._id, req.params.binaryRelationId, req.body.rev, req.body.pair, function(e, statusCode) {
        res.send(e, statusCode || 500)
      })
    }
  }
}

function decompileFormPList(plist, callback) {
  var path = plist && plist.path || undefined
    , command = plutilCommand + path
    , isBinary

  if (!path) {
    callback(format('plist not found at path "%s".\n\tplist:\n', path, plist))
    return
  }

  fs.rename(path, path + '.plist', function(e) {
    if (e) {
      callback(format('error renaming file: "%s"',e))
      return
    }

    fs.readFile(path+'.plist', function(e,data) {
      if (e) {
        callback(format('error reading file - error reported: "%s"',e))
        return
      }

      isBinary = data.toString('utf8').substring(0,6) == 'bplist'
      if (!isBinary) {
        // not a compiled plist
        callback(null, path+'.plist')
        return
      }

      exec(command, function(err, stdout, stderr) {
        if (err) {
          callback(format('plutil was unable to process the file. error: "%s"', err))
          return
        }

        if (!/BinaryToXML/.test(stdout)) {
          callback('plutil output format unrecognised')
          return
        }

        callback(null, path+'.text.plist')
      })
    })
  })
}

function formatDateString(jsonDate) {
    if (!jsonDate) return '';
    return jsonDate.replace(/^([0-9]{4}-[0-9]{2}-[0-9]{2})T(([0-9]{2}:){2}[0-9]{2}).+$/, "$1 $2");
}
