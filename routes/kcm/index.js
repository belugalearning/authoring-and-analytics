var fs = require('fs')
  , _ = require('underscore')
  , util = require('util')
  , kcmModel
;

module.exports = function(model) {
    kcmModel = model.kcm;

    return {
        getMap: function(req, res) {
            var map = { pipelines:{}, nodes:[], prerequisites:[] };
            kcmModel.queryView(encodeURI('pipelines-by-name?include_docs=true'), function(e,r,b) {
                _.each(JSON.parse(b).rows, function(row) { map.pipelines[row.id] = row.doc; });
                kcmModel.queryView(encodeURI('concept-nodes?include_docs=true'), function(e,r,b) {
                    map.nodes = _.map(JSON.parse(b).rows, function(row) { return row.doc; });

                    kcmModel.queryView(encodeURI('relations-by-name?key="Prerequisite"&include_docs=true'), function(e,r,b) {
                        map.prerequisites = JSON.parse(b).rows[0].doc.members;
                        res.render('kcm/map', { title:'Knowledge Concept Map', map:map });
                    });
                });
            });
        }
        , insertConceptNodeTag: function(req, res) {
            kcmModel.insertConceptNodeTag(req.body.conceptNodeId, req.body.conceptNodeRev, req.body.tag, function(e, statusCode, conceptNodeRevision) {
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
        , updateConceptNodePosition: function(req,res) {
            kcmModel.updateConceptNodePosition(req.body.id, req.body.rev, req.body.x, req.body.y, function(e, statusCode, nodeRevision) {
                res.send(e || nodeRevision, statusCode || 500);
            });
        }
        , pullReplicate: function(req, res) {
            var sourceMatch = req.url.match(/(?:\?|\&)source\=([^&]+)/)
              , filterMatch = req.url.match(/(?:\?|\&)filter\=([^&]+)/)
              , continuousMatch = req.url.match(/(?:\?|\&)continuous\=([^&]+)/)
              , source = (sourceMatch && sourceMatch.length > 1 && sourceMatch[1]).replace(/^%22/, '').replace(/%22$/, '')
              , filter = (filterMatch && filterMatch.length > 1 && filterMatch[1]).replace(/^%22/, '').replace(/%22$/, '')
              , continuous = continuousMatch && continuousMatch.length > 1 && continuousMatch[1] == 'true'
            ;
            
            if (source.length > 1) {
                console.log('PULL REPLICATION: source:%s, filter:%s, continuous:%s', source, filter, continuous?'true':'false');
                kcmModel.pullReplicate(source, filter, continuous);
                res.send(200);
            } else {
                res.send(500);
            }
        }
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
};
