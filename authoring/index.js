var express = require('express')
  , _ = require('underscore')
  , urlParser = require('url')
  , crypto = require('crypto')
  , initOldModel = require('./old-model')
  , sessionService = require('./session-service')
  , routeHandlers = require('./routes')
  , KCM = require('./kcm')
  , KCMWebSocketServer = require('./kcm-websocket-server')

var server = express.createServer()
var noAuthRequired = [/^\/kcm\/app-import-content\/.*/]

server.guid = function() {
  return crypto.randomBytes(16)
    .toString('hex')
    .replace(/^(.{8})(.{4})(.{4})(.{4})/i, '$1-$2-$3-')
    .toUpperCase()
}

module.exports = function(config) {
  server.kcm = new KCM(config)

  server.kcm.once('initialised', function() {
    var oldModel = initOldModel(config, server.kcm)
    server.routeHandlers = routeHandlers.init(config, oldModel, server.kcm)
    server.sessionService = sessionService.createService(oldModel.kcm, noAuthRequired)
    server.kcmWSS = new KCMWebSocketServer(server.kcm, server.sessionService, {
      server: server
      , verifyClient: function(ws, callback) {
        server.sessionService.get(ws.req, function(e, session) {
          callback(session)
        })
      }
    })
    server.config = config

    configServer()
    setupRoutes()

    var port = parseInt(config.authoring.port, 10) || 3001
    server.listen(port)
    console.log("Authoring Express server listening on port %d in %s mode", server.address().port, server.settings.env)
  })
  return server
}

function configServer() {
  server.configure(function() {
    server.set('views', __dirname + '/views')
    server.set('view engine', 'jade')
    server.set('view options', { layout: false })

    server.use(express.favicon())
    server.use(express.bodyParser())
    server.use(express.cookieParser())
    server.use(server.sessionService.httpRequestHandler)
    server.use(express.methodOverride())
    server.use(server.router)
    server.use(express.static(__dirname + '/public'))
  })
  server.configure('development', function(){
    server.use(express.errorHandler({ dumpExceptions: true, showStack: true }))
  })
  server.configure('production', function(){
    server.use(express.errorHandler())
  })
}

// routes
function setupRoutes() {
  server.get('/', server.routeHandlers.index)
  server.get('/sitemap', server.routeHandlers.siteMap)

  server.get('/content/element-assessment-criteria', server.routeHandlers.content.elementAssessmentCriteriaPage)
  server.get('/content/assessment-criterion-problems/:criterionId', server.routeHandlers.content.assessmentCriterionProblemsTable)
  server.get('/content/element-assessment-criteria/new-criterion/', server.routeHandlers.content.newAssessmentCriterion)
  server.post('/content/element-assessment-criteria/delete-criterion', server.routeHandlers.content.deleteAssessmentCriterion)
  server.post('/content/element-assessment-criteria/save-criterion', server.routeHandlers.content.saveAssessmentCriterion)
  server.get('/content/element-assessment-criteria/:elementId', server.routeHandlers.content.assessmentCriteriaForElement)
  server.get('/content/element-options/:moduleId', server.routeHandlers.content.elementOptionsForModule)
  server.get('/content/module-options/:topicId', server.routeHandlers.content.moduleOptionsForTopic)
  server.get('/content/problems', server.routeHandlers.content.problemsPage)
  server.get('/content/problem/:problemId', server.routeHandlers.content.editProblem.problemDetailPage)
  server.get('/content/problem/:problemId/upload-mathml-expression', server.routeHandlers.content.editProblem.uploadMathMLExpressionPage)
  server.post('/content/problem/:problemId/upload-mathml-expression', server.routeHandlers.content.editProblem.performUploadMathMLExpression)
  server.get('/content/problem/:problemId/download-mathml-expression', function(req,res) { server.routeHandlers.content.editProblem.sendMathMLExpression(req, res, true) })
  server.get('/content/problem/:problemId/view-mathml-expression', function(req,res) { server.routeHandlers.content.editProblem.sendMathMLExpression(req, res, false) })
  server.get('/content/problem/:problemId/delete-mathml-expression', server.routeHandlers.content.editProblem.deleteMathMLExpression)
  server.post('/content/problem/assign-assessment-criteria-table', server.routeHandlers.content.editProblem.problemAssignAssessmentCriteriaTable)
  server.get('/content/problem-sequence', server.routeHandlers.content.problemSequence)
  server.get('/content/problem-sequence-table/:elementId', server.routeHandlers.content.problemSequenceTable)
  server.post('/content/problem-sequence/update', server.routeHandlers.content.updateProblemSequence)
  server.get('/content/create-problem', server.routeHandlers.content.createProblem.page)
  server.post('/content/create-problem', server.routeHandlers.content.createProblem.uploadPlist)


  server.get('/user-portal', server.routeHandlers.userPortal.userList)
  server.get('/user-portal/:userId/activity-feed', server.routeHandlers.userPortal.activityFeedPage)
  server.get('/user-portal/all-activity-feed', server.routeHandlers.userPortal.allActivitiesReversed)

  // TODO: improve uri's below - more in the uri less in req.body
  server.get('/kcm', server.routeHandlers.kcm.getMap)
  server.get('/kcm/pull-replicate', server.routeHandlers.kcm.pullReplicate) //TODO: Get request shouldn't have side-effects - create replication page with post request to initiate/cancel replications
  server.get('/kcm/get-app-content', server.routeHandlers.kcm.getAppContent)
  server.get('/kcm/app-import-content/:loginName', server.routeHandlers.kcm.appImportContent)
  server.get('/kcm/download-tokcm-dirs', server.routeHandlers.kcm.downloadToKCMDirs)
  server.post('/kcm/update-view-settings', server.routeHandlers.kcm.updateViewSettings)
  server.post('/kcm/update-export-settings', server.routeHandlers.kcm.updateExportSettings)
  server.get('/kcm/canned-database', server.routeHandlers.kcm.cannedDatabase)
  server.post('/kcm/binary-relations/:binaryRelationId/remove-pair', server.routeHandlers.kcm.removePairFromBinaryRelation)
  server.post('/kcm/add-pair-to-binary-relation', server.routeHandlers.kcm.addPairToBinaryRelation)
  server.post('/kcm/chained-binary-relations/members', server.routeHandlers.kcm.getChainedBinaryRelationsMembers)
  server.post('/kcm/concept-nodes/insert', server.routeHandlers.kcm.insertConceptNode)
  server.post('/kcm/concept-nodes/:conceptNodeId/delete', server.routeHandlers.kcm.deleteConceptNode)
  server.post('/kcm/concept-nodes/:conceptNodeId/update-description', server.routeHandlers.kcm.updateConceptNodeDescription)
  server.post('/kcm/concept-nodes/:conceptNodeId/reorder-pipelines', server.routeHandlers.kcm.reorderConceptNodePipelines)
  server.post('/kcm/insert-concept-node-tag', server.routeHandlers.kcm.insertConceptNodeTag)
  server.post('/kcm/delete-concept-node-tag', server.routeHandlers.kcm.deleteConceptNodeTag)
  server.post('/kcm/edit-concept-node-tag', server.routeHandlers.kcm.editConceptNodeTag)
  server.get('/kcm/concept-nodes/:conceptNodeId/pipeline/:pipelineId/download', server.routeHandlers.kcm.downloadPipeline)
  server.post('/kcm/pipelines/upload-pipeline-folder', server.routeHandlers.kcm.uploadPipelineFolder)
  server.post('/kcm/insert-pipeline', server.routeHandlers.kcm.addNewPipelineToConceptNode)
  server.post('/kcm/delete-pipeline', server.routeHandlers.kcm.deletePipeline)
  server.put('/kcm/pipeline/:id/:rev/update-workflow-status/:status', server.routeHandlers.kcm.updatePipelineWorkflowStatus)
  server.put('/kcm/pipeline/:id/:rev/update-name/:name', server.routeHandlers.kcm.updatePipelineName)
  server.get('/kcm/pipelines/:pipelineId?', server.routeHandlers.kcm.pipelinePage)
  server.get('/kcm/pipeline-sequence-tables/', server.routeHandlers.kcm.pipelineSequenceTables)
  server.post('/kcm/pipeline-problem-details', server.routeHandlers.kcm.pipelineProblemDetails)
  server.post('/kcm/pipeline-sequence/update', server.routeHandlers.kcm.updatePipelineSequence)
  server.post('/kcm/reorder-pipeline-problems', server.routeHandlers.kcm.reorderPipelineProblems)
  server.post('/kcm/update-concept-node-position', server.routeHandlers.kcm.updateConceptNodePosition)
  server.post('/kcm/add-problems-to-pipeline', server.routeHandlers.kcm.uploadProblems)
  server.post('/kcm/pipeline/:pipelineId/:pipelineRev/problem/:problemId/remove', server.routeHandlers.kcm.removeProblemFromPipeline)
  server.get('/kcm/problem/:problemId', server.routeHandlers.kcm.editProblem.problemDetailPage)
  server.get('/kcm/problem/:problemId/download-pdef', function(req,res) { server.routeHandlers.kcm.editProblem.sendPList(req, res, true) })
  server.get('/kcm/problem/:problemId/view-pdef', function(req,res) { server.routeHandlers.kcm.editProblem.sendPList(req, res, false) })
  server.get('/kcm/problem/:problemId/replace-pdef', server.routeHandlers.kcm.editProblem.replacePListPage)
  server.post('/kcm/problem/:problemId/replace-pdef', server.routeHandlers.kcm.editProblem.performReplacePList)
}
