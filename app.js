// ******* make directory in which this file sits (application root directory) the working directory so that files in subdirectories can use paths relative to application root - i.e. they don't need to know their own path
// ******* to find all instances where this convenience is used, search app files for 'process.cwd()'
process.chdir(__dirname);

var noAuthenticationPathnames = ['/login', '/logout']
  , express = require('express')
  , routes = require('./routes')
  , model = require('./model')
  , fs = require('fs')
  , _ = require('underscore')
  , urlParser = require('url')
  , app = module.exports = express.createServer();
;

// Configuration
app.configure(function() {
    app.set('views', __dirname + '/views');
    app.set('view engine', 'jade');
    app.set('view options', { layout: false });
    
    app.use(express.favicon());
    app.use(express.bodyParser());
    app.use(express.cookieParser());
    app.use(express.session({ cookie:{ maxAge:600000 }, secret: '1a4eca939f8e54fd41e3d74d64aa9187d9951aed50599986418d96180716579c1ec776fc17a96640e579e76481677c87' }));
    app.use(checkAuthentication);
    app.use(express.methodOverride());
    app.use(app.router);
    app.use(express.static(__dirname + '/public'));
});

app.configure('development', function(){
    app.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 
});

app.configure('production', function(){
    app.use(express.errorHandler()); 
});

function checkAuthentication(req, res, next) {
    var url = urlParser.parse(req.url);
    if (~noAuthenticationPathnames.indexOf(url.pathname) || req.session.isAuthenticated) {
        next();
        return;
    }
    res.redirect('/login?redir=' + req.url);
}


// Routes
app.get('/login', function(req,res) {
    res.render('sessions/login', { redir: req.query.redir || req.body.redir });
});
app.post('/login', function(req,res) {
   if ('blauthors' == req.body.user && '1935-Bourbaki' == req.body.password) { 
       req.session.isAuthenticated = true;
       res.redirect(req.body.redir || '/');
   } else {
       if (req.session) req.session.destroy();
       res.render('sessions/login', { redir:req.body.redir });
   }
});
app.get('/logout', function(req,res) {
    if (req.session) req.session.destroy();
    res.redirect('/login');
});


app.get('/', routes.index);
//app.get('/images/zubi/:id', routes.zubiImage);

app.get('/sitemap', routes.siteMap);

app.get('/content/element-assessment-criteria', routes.content.elementAssessmentCriteriaPage);
app.get('/content/assessment-criterion-problems/:criterionId', routes.content.assessmentCriterionProblemsTable);
app.get('/content/element-assessment-criteria/new-criterion/', routes.content.newAssessmentCriterion);
app.post('/content/element-assessment-criteria/delete-criterion', routes.content.deleteAssessmentCriterion);
app.post('/content/element-assessment-criteria/save-criterion', routes.content.saveAssessmentCriterion);
app.get('/content/element-assessment-criteria/:elementId', routes.content.assessmentCriteriaForElement);
app.get('/content/element-options/:moduleId', routes.content.elementOptionsForModule);
app.get('/content/module-options/:topicId', routes.content.moduleOptionsForTopic);
app.get('/content/problems', routes.content.problemsPage);
app.get('/content/problem/:problemId', routes.content.editProblem.problemDetailPage);
app.post('/content/problem/:problemId/perform-update', routes.content.editProblem.performUpdate);
app.get('/content/problem/:problemId/download-pdef-plist', function(req,res) { routes.content.editProblem.sendPList(req, res, true); });
app.get('/content/problem/:problemId/view-pdef-plist', function(req,res) { routes.content.editProblem.sendPList(req, res, false); });
app.get('/content/problem/:problemId/replace-pdef-plist', routes.content.editProblem.replacePListPage);
app.post('/content/problem/:problemId/replace-pdef-plist', routes.content.editProblem.performReplacePList);
app.get('/content/problem/:problemId/upload-mathml-expression', routes.content.editProblem.uploadMathMLExpressionPage);
app.post('/content/problem/:problemId/upload-mathml-expression', routes.content.editProblem.performUploadMathMLExpression);
app.get('/content/problem/:problemId/download-mathml-expression', function(req,res) { routes.content.editProblem.sendMathMLExpression(req, res, true); });
app.get('/content/problem/:problemId/view-mathml-expression', function(req,res) { routes.content.editProblem.sendMathMLExpression(req, res, false); });
app.get('/content/problem/:problemId/delete-mathml-expression', routes.content.editProblem.deleteMathMLExpression);
app.post('/content/problem/assign-assessment-criteria-table', routes.content.editProblem.problemAssignAssessmentCriteriaTable);
app.get('/content/problem-sequence', routes.content.problemSequence);
app.get('/content/problem-sequence-table/:elementId', routes.content.problemSequenceTable);
app.post('/content/problem-sequence/update', routes.content.updateProblemSequence);
app.get('/content/create-problem', routes.content.createProblem.page);
app.post('/content/create-problem', routes.content.createProblem.uploadPlist);


app.get('/user-portal', routes.userPortal.userList);
app.get('/user-portal/:userId/activity-feed', routes.userPortal.activityFeedPage);
app.get('/user-portal/all-activity-feed', routes.userPortal.allActivitiesReversed);

app.get('/kcm', routes.kcm.getMap);
app.get('/kcm/pull-replicate', routes.kcm.pullReplicate);
app.get('/kcm/canned-database', routes.kcm.cannedDatabase);
app.post('/kcm/insert-concept-node-tag', routes.kcm.insertConceptNodeTag);
app.post('/kcm/delete-concept-node-tag', routes.kcm.deleteConceptNodeTag);
app.post('/kcm/edit-concept-node-tag', routes.kcm.editConceptNodeTag);
app.post('/kcm/insert-pipeline', routes.kcm.addNewPipelineToConceptNode);
app.post('/kcm/delete-pipeline', routes.kcm.deletePipeline);
app.post('/kcm/remove-problem-from-pipeline', routes.kcm.removeProblemFromPipeline);
app.get('/kcm/pipelines/:pipelineId?', routes.kcm.pipelinePage);
app.get('/kcm/pipeline-sequence-tables/', routes.kcm.pipelineSequenceTables);
app.post('/kcm/pipeline-problem-details', routes.kcm.pipelineProblemDetails);
app.post('/kcm/pipeline-sequence/update', routes.kcm.updatePipelineSequence);
app.post('/kcm/update-concept-node-position', routes.kcm.updateConceptNodePosition);
app.post('/kcm/add-pair-to-binary-relation', routes.kcm.addPairToBinaryRelation);

var options = _.map(
    _.filter(process.argv, function(arg) { return /^-/.test(arg); })
    , function(arg) {
        var i = 1 + process.argv.indexOf(arg);
        var value;
        if (process.argv.length > i && /^[^\-]/.test(process.argv[i])) value = process.argv[i];
        return { name:arg.substring(1), value:value }
    }
);
var getOption = function(name) {
    return _.find(options, function(o) { return o.name == name; });
};
var portOption = getOption('port');
var port = portOption && parseInt(portOption.value) || 3000;
var listen = function() {
    app.listen(port);
    console.log("Express server listening on port %d in %s mode", app.address().port, app.settings.env);
};

if (getOption('createdb')) {
    console.log('creating content database');
    model.content.createDB(listen);
} else if (getOption('initdb')) {
    console.log('initialising content database');
    model.content.populateDBWithInitialContent(listen);
} else {
    if (getOption('updateviews')) {
        console.log('updating views');
        model.content.updateViews(function() {
            model.kcm.updateViews(function() {
                model.users.updateViews(listen);
            });
        });
    } else {
        listen();
    }
}
