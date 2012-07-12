var express = require('express')
  , _ = require('underscore')
  , urlParser = require('url')
  , model, routes
;

var noAuthenticationPathnames = ['/login', '/logout']
  , server = express.createServer()
  , config
;

module.exports = function(conf) {
    config = conf;
    model = require('./model')(config)
    routes = require('./routes')(config, model)
    setupRoutes();

    var port = parseInt(config.authoring.port, 10) || 3001;
    server.listen(port);
    console.log("Authoring Express server listening on port %d in %s mode", server.address().port, server.settings.env);

    return server;
};

// Configuration
server.configure(function() {
    server.set('views', __dirname + '/views');
    server.set('view engine', 'jade');
    server.set('view options', { layout: false });
    
    server.use(express.favicon());
    server.use(express.bodyParser());
    server.use(express.cookieParser());
    server.use(express.session({ cookie:{ maxAge:600000 }, secret: '1a4eca939f8e54fd41e3d74d64aa9187d9951aed50599986418d96180716579c1ec776fc17a96640e579e76481677c87' }));
    server.use(checkAuthentication);
    server.use(express.methodOverride());
    server.use(server.router);
    server.use(express.static(__dirname + '/public'));
});
server.configure('development', function(){
    server.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 
});
server.configure('production', function(){
    server.use(express.errorHandler()); 
});

// auth
function checkAuthentication(req, res, next) {
    var pathname = urlParser.parse(req.url).pathname
      , cache = arguments.callee.cache
    ;

    if (!cache) {
        var regexps = _.filter(noAuthenticationPathnames, function(pn) { return _.isRegExp(pn); });
        var strings = _.difference(noAuthenticationPathnames, regexps);
        cache = arguments.callee.cache = { regexps:regexps, strings:strings };
    }
    
    if (req.session.isAuthenticated || ~cache.strings.indexOf(pathname) || _.find(cache.regexps, function(re) { return re.test(pathname); })) {
        next();
        return;
    }

    if (req.isXMLHttpRequest) {
        res.send('You have been logged out', 401);
    } else {
        res.redirect('/login?redir=' + req.url);
    }
}

// routes
function setupRoutes() {
    server.get('/login', function(req,res) {
        res.render('sessions/login', { redir: req.query.redir || req.body.redir });
    });
    server.post('/login', function(req,res) {
        if ('blauthors' == req.body.user && '1935-Bourbaki' == req.body.password) { 
            req.session.isAuthenticated = true;
            res.redirect(req.body.redir || '/');
        } else {
            if (req.session) req.session.destroy();
            res.render('sessions/login', { redir:req.body.redir });
        }
    });
    server.get('/logout', function(req,res) {
        if (req.session) req.session.destroy();
        res.redirect('/login');
    });


    server.get('/', routes.index);

    server.get('/sitemap', routes.siteMap);

    server.get('/content/element-assessment-criteria', routes.content.elementAssessmentCriteriaPage);
    server.get('/content/assessment-criterion-problems/:criterionId', routes.content.assessmentCriterionProblemsTable);
    server.get('/content/element-assessment-criteria/new-criterion/', routes.content.newAssessmentCriterion);
    server.post('/content/element-assessment-criteria/delete-criterion', routes.content.deleteAssessmentCriterion);
    server.post('/content/element-assessment-criteria/save-criterion', routes.content.saveAssessmentCriterion);
    server.get('/content/element-assessment-criteria/:elementId', routes.content.assessmentCriteriaForElement);
    server.get('/content/element-options/:moduleId', routes.content.elementOptionsForModule);
    server.get('/content/module-options/:topicId', routes.content.moduleOptionsForTopic);
    server.get('/content/problems', routes.content.problemsPage);
    server.get('/content/problem/:problemId', routes.content.editProblem.problemDetailPage);
    server.post('/content/problem/:problemId/perform-update', routes.content.editProblem.performUpdate);
    server.get('/content/problem/:problemId/download-pdef-plist', function(req,res) { routes.content.editProblem.sendPList(req, res, true); });
    server.get('/content/problem/:problemId/view-pdef-plist', function(req,res) { routes.content.editProblem.sendPList(req, res, false); });
    server.get('/content/problem/:problemId/replace-pdef-plist', routes.content.editProblem.replacePListPage);
    server.post('/content/problem/:problemId/replace-pdef-plist', routes.content.editProblem.performReplacePList);
    server.get('/content/problem/:problemId/upload-mathml-expression', routes.content.editProblem.uploadMathMLExpressionPage);
    server.post('/content/problem/:problemId/upload-mathml-expression', routes.content.editProblem.performUploadMathMLExpression);
    server.get('/content/problem/:problemId/download-mathml-expression', function(req,res) { routes.content.editProblem.sendMathMLExpression(req, res, true); });
    server.get('/content/problem/:problemId/view-mathml-expression', function(req,res) { routes.content.editProblem.sendMathMLExpression(req, res, false); });
    server.get('/content/problem/:problemId/delete-mathml-expression', routes.content.editProblem.deleteMathMLExpression);
    server.post('/content/problem/assign-assessment-criteria-table', routes.content.editProblem.problemAssignAssessmentCriteriaTable);
    server.get('/content/problem-sequence', routes.content.problemSequence);
    server.get('/content/problem-sequence-table/:elementId', routes.content.problemSequenceTable);
    server.post('/content/problem-sequence/update', routes.content.updateProblemSequence);
    server.get('/content/create-problem', routes.content.createProblem.page);
    server.post('/content/create-problem', routes.content.createProblem.uploadPlist);


    server.get('/user-portal', routes.userPortal.userList);
    server.get('/user-portal/:userId/activity-feed', routes.userPortal.activityFeedPage);
    server.get('/user-portal/all-activity-feed', routes.userPortal.allActivitiesReversed);

    // TODO: improve uri's below - more in the uri less in req.body
    server.get('/kcm', routes.kcm.getMap);
    server.get('/kcm/pull-replicate', routes.kcm.pullReplicate); //TODO: Get request shouldn't have side-effects - create replication page with post request to initiate/cancel replications
    server.get('/kcm/get-app-content', routes.kcm.getAppContent);
    server.post('/kcm/update-view-settings', routes.kcm.updateViewSettings);
    server.post('/kcm/update-export-settings', routes.kcm.updateExportSettings);
    server.get('/kcm/canned-database', routes.kcm.cannedDatabase);
    server.post('/kcm/binary-relations/:binaryRelationId/remove-pair', routes.kcm.removePairFromBinaryRelation);
    server.post('/kcm/add-pair-to-binary-relation', routes.kcm.addPairToBinaryRelation);
    server.post('/kcm/concept-nodes/insert', routes.kcm.insertConceptNode);
    server.post('/kcm/concept-nodes/:conceptNodeId/delete', routes.kcm.deleteConceptNode);
    server.post('/kcm/concept-nodes/:conceptNodeId/update-description', routes.kcm.updateConceptNodeDescription);
    server.post('/kcm/concept-nodes/:conceptNodeId/reorder-pipelines', routes.kcm.reorderConceptNodePipelines);
    server.post('/kcm/insert-concept-node-tag', routes.kcm.insertConceptNodeTag);
    server.post('/kcm/delete-concept-node-tag', routes.kcm.deleteConceptNodeTag);
    server.post('/kcm/edit-concept-node-tag', routes.kcm.editConceptNodeTag);
    server.post('/kcm/insert-pipeline', routes.kcm.addNewPipelineToConceptNode);
    server.post('/kcm/delete-pipeline', routes.kcm.deletePipeline);
    server.post('/kcm/remove-problem-from-pipeline', routes.kcm.removeProblemFromPipeline);
    server.get('/kcm/pipelines/:pipelineId?', routes.kcm.pipelinePage);
    server.get('/kcm/pipeline-sequence-tables/', routes.kcm.pipelineSequenceTables);
    server.post('/kcm/pipeline-problem-details', routes.kcm.pipelineProblemDetails);
    server.post('/kcm/pipeline-sequence/update', routes.kcm.updatePipelineSequence);
    server.post('/kcm/reorder-pipeline-problems', routes.kcm.reorderPipelineProblems);
    server.post('/kcm/update-concept-node-position', routes.kcm.updateConceptNodePosition);
    server.post('/kcm/add-problems-to-pipeline', routes.kcm.uploadProblems);
    server.get('/kcm/problem/:problemId', routes.content.editProblem.problemDetailPage);
}
