var express = require('express')
  , _ = require('underscore')
  , urlParser = require('url')
  , crypto = require('crypto')
  , initLegacyKCMController = require('./old-model/kcm')
  , sessionService = require('./session-service')
  , initKCMRouteHandlers = require('./routes/kcm')
  , KCM = require('./kcm')
  , KCMWebSocketServer = require('./kcm-websocket-server')

var server = express.createServer()
var noAuthRequired = [/^\/kcm\/app-import-content\/.*/, /^\/kcm\/app-edit-pdef(\/.*)?$/]

server.guid = function() {
  return crypto.randomBytes(16)
    .toString('hex')
    .replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})/i, '$1-$2-$3-$4-$5')
    .toUpperCase()
}

module.exports = function(config) {
  server.kcm = new KCM(config)

  server.kcm.once('initialised', function() {
    var legacyKCMController = initLegacyKCMController(config, server.kcm)
    server.kcmRouteHandlers = initKCMRouteHandlers(config, legacyKCMController, server.kcm)
    server.sessionService = sessionService.createService(legacyKCMController, noAuthRequired)
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
    /*server.use(function(req,res,next) {
      console.log(new Date(), req.url)
      next()
    })//*/
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
  server.get('/', function(req,res) { res.redirect('/kcm') })

  // TODO: improve paths below - more in the uri less in req.body
  server.get('/kcm', server.kcmRouteHandlers.getMap)
  server.get('/kcm/pull-replicate', server.kcmRouteHandlers.pullReplicate) //TODO: Get request shouldn't have side-effects - create replication page with post request to initiate/cancel replications
  server.get('/kcm/get-app-canned-databases', server.kcmRouteHandlers.getAppCannedDatabases)

  server.get('/kcm/app-import-content/:loginName', server.kcmRouteHandlers.appImportContent)
  server.post('/kcm/app-edit-pdef', server.kcmRouteHandlers.appEditPDef)

  server.get('/kcm/download-tokcm-dirs', server.kcmRouteHandlers.downloadToKCMDirs)
  server.post('/kcm/update-user', server.kcmRouteHandlers.updateUser)
  server.get('/kcm/canned-database', server.kcmRouteHandlers.cannedDatabase)
  server.post('/kcm/binary-relations/:binaryRelationId/remove-pair', server.kcmRouteHandlers.removePairFromBinaryRelation)
  server.post('/kcm/add-pair-to-binary-relation', server.kcmRouteHandlers.addPairToBinaryRelation)
  server.post('/kcm/chained-binary-relations/members', server.kcmRouteHandlers.getChainedBinaryRelationsMembers)
  server.post('/kcm/concept-nodes/insert', server.kcmRouteHandlers.insertConceptNode)
  server.post('/kcm/concept-nodes/:conceptNodeId/delete', server.kcmRouteHandlers.deleteConceptNode)
  server.post('/kcm/concept-nodes/:conceptNodeId/update-description', server.kcmRouteHandlers.updateConceptNodeDescription)
  server.post('/kcm/concept-nodes/:conceptNodeId/reorder-pipelines', server.kcmRouteHandlers.reorderConceptNodePipelines)
  server.post('/kcm/insert-concept-node-tag', server.kcmRouteHandlers.insertConceptNodeTag)
  server.post('/kcm/delete-concept-node-tag', server.kcmRouteHandlers.deleteConceptNodeTag)
  server.post('/kcm/edit-concept-node-tag', server.kcmRouteHandlers.editConceptNodeTag)
  server.get('/kcm/concept-nodes/:conceptNodeId/pipeline/:pipelineId/download', server.kcmRouteHandlers.downloadPipeline)
  server.post('/kcm/pipelines/upload-pipeline-folder', server.kcmRouteHandlers.uploadPipelineFolder)
  server.post('/kcm/insert-pipeline', server.kcmRouteHandlers.addNewPipelineToConceptNode)
  server.post('/kcm/delete-pipeline', server.kcmRouteHandlers.deletePipeline)
  server.put('/kcm/pipeline/:id/:rev/update-workflow-status/:status', server.kcmRouteHandlers.updatePipelineWorkflowStatus)
  server.put('/kcm/pipeline/:id/:rev/update-name/:name', server.kcmRouteHandlers.updatePipelineName)
  server.get('/kcm/pipelines/:pipelineId?', server.kcmRouteHandlers.pipelinePage)  // TODO: *********** Is this still used? Does it still work?
  server.get('/kcm/pipeline-sequence-tables/', server.kcmRouteHandlers.pipelineSequenceTables)
  server.post('/kcm/pipeline-problem-details', server.kcmRouteHandlers.pipelineProblemDetails)
  server.post('/kcm/pipeline-sequence/update', server.kcmRouteHandlers.updatePipelineSequence)
  server.post('/kcm/reorder-pipeline-problems', server.kcmRouteHandlers.reorderPipelineProblems)
  server.post('/kcm/update-concept-node-position', server.kcmRouteHandlers.updateConceptNodePosition)
  server.post('/kcm/add-problems-to-pipeline', server.kcmRouteHandlers.uploadProblems)
  server.post('/kcm/pipeline/:pipelineId/:pipelineRev/problem/:problemId/remove', server.kcmRouteHandlers.removeProblemFromPipeline)
  server.get('/kcm/problem/:problemId', server.kcmRouteHandlers.editProblem.problemDetailPage)
  server.get('/kcm/problem/:problemId/download-pdef', function(req,res) { server.kcmRouteHandlers.editProblem.sendPList(req, res, true) })
  server.get('/kcm/problem/:problemId/view-pdef', function(req,res) { server.kcmRouteHandlers.editProblem.sendPList(req, res, false) })
  server.get('/kcm/problem/:problemId/replace-pdef', server.kcmRouteHandlers.editProblem.replacePListPage)
  server.post('/kcm/problem/:problemId/replace-pdef', server.kcmRouteHandlers.editProblem.performReplacePList)
}
