var  _ = require('underscore')
  , exec = require('child_process').exec
  , fs = require('fs')
  , util = require('util')
  , model
;

module.exports = function(m) {
    model = m;
    exports.kcm = require('./kcm')(model.kcm);
    exports.appUsersService = require('./app-users-service')(model.appUsersService);
    return exports;
};

exports.index = function(req, res){
    res.redirect('/sitemap');
};

exports.siteMap = function(req, res) {
    res.render('sitemap', { title:'Site Map' });
};

exports.zubiImage = function(req, res){
    var userId = req.params.id.match(/^zubi(.+)\.png$/)[1];
    model.getZubiForDocId(userId, function(e,r,b) {
      if (e) {
          res.send(404);
      } else {
          res.contentType('image/png');
          res.end(b, 'binary');
      }
    });
};  

exports.content = {
    moduleOptionsForTopic: function(req, res) {
        var topicId = req.params.topicId;
        model.content.getDoc(topicId, function(e,r,b) {
            if (e || !r || r.statusCode !== 200) {
                console.log('error retrieving topic id:', topicId);
                console.log('\terror:', e);
                console.log('\tstatusCode:', r && r.statusCode || '');
                console.log('\tbody:', b);
                res.send(r && r.statusCode || 500);
                return;
            }

            var queryString = encodeURI('?keys=' + JSON.stringify(JSON.parse(b).modules));
            model.content.queryView('modules' + queryString, function(e,r,b) {
                if (e || !r || r.statusCode !== 200) {
                    console.log('error retrieving modules for topic id=', topicId);
                    console.log('\terror:', e);
                    console.log('\tstatusCode:', r && r.statusCode || '');
                    console.log('\tbody:', b);
                    res.send(r && r.statusCode || 500);
                    return;
                }
                var modules = JSON.parse(b).rows;
                res.contentType('html');
                res.render('select-module-options', { modules:modules });
            });
        });
    }
    , elementOptionsForModule: function(req, res) {
        var moduleId = req.params.moduleId;
        model.content.getDoc(moduleId, function(e,r,b) {
            if (e || !r || r.statusCode !== 200) {
                console.log('error retrieving module id:', moduleId);
                console.log('\terror:', e);
                console.log('\tstatusCode:', r && r.statusCode || '');
                console.log('\tbody:', b);
                res.send(r && r.statusCode || 500);
                return;
            }

            var queryString = encodeURI('?keys=' + JSON.stringify(JSON.parse(b).elements));
            model.content.queryView('elements' + queryString, function(e,r,b) {
                var elements = JSON.parse(b).rows;
                res.contentType('html');
                res.render('select-element-options', { layout:false, elements:elements });
            });
        });
    }

    , problemsPage: function(req, res) {
        model.content.queryView('problems-by-description?include_docs=true', function(e,r,b) {
            var problems = JSON.parse(b).rows;
            _.each(problems, function(problem) {
                problem.doc.dateCreated = formatDateString(problem.doc.dateCreated);
                problem.doc.dateModified = formatDateString(problem.doc.dateModified);
            });

            model.content.queryView('tools-by-name', function(e,r,b) {
                var tools = JSON.parse(b).rows;
                res.render('problems-page', { title:'Problems', problems:problems, tools:tools });
            });
        });
    }

    , problemSequence: function(req, res) {
        model.content.queryView(encodeURI('syllabi-by-name?key="Default"&include_docs=true'), function(e,r,b) {
            var syllabi = JSON.parse(b).rows
              , topicIds = syllabi && syllabi.length ? syllabi[0].doc.topics : []
            ;
            model.content.queryView(encodeURI('names-types-by-id?keys=' + JSON.stringify(topicIds)), function(e,r,b) {
                var topics = _.filter(JSON.parse(b).rows, function(nameType) { return nameType.value[1] == 'topic'; });
                res.render('problem-sequence-page', {
                    title:'Problem Sequence', includedProblems:[], excludedProblems:[], includedProblemIds:'', excludedProblemIds:'', topics:topics, topicId:null, modules:null, moduleId:null, elements:null, elementId:null, topicModuleElementInDataset:false });
            });
        });
    }
    , problemSequenceTable: function(req, res) {
        var includedProblems = []
          , excludedProblems = []
          , includedProblemIds = ''
          , excludedProblemIds = ''
          , elementId = req.params.elementId
        ;

        var renderProblemSequenceTables = function() {
            res.render('includes/problem-sequence-tables', {
                elementId:elementId
                , includedProblems:includedProblems     , includedProblemIds:includedProblemIds
                , excludedProblems:excludedProblems     , excludedProblemIds:excludedProblemIds
            });
        };

        model.content.getDoc(req.params.elementId, function(e,r,b) {
            if (e || r.statusCode !== 200) {
                renderProblemSequenceTables();
                return;
            }

            var element = JSON.parse(b)
              , queryIncludedURI = encodeURI('problem-descriptions-and-notes?keys=' + JSON.stringify(element.includedProblems))
              , queryExcludedURI = encodeURI('problem-descriptions-and-notes?keys=' + JSON.stringify(element.excludedProblems))
            ;

            includedProblemIds = element.includedProblems.join(',')
            excludedProblemIds = element.excludedProblems.join(',')

            model.content.queryView(queryIncludedURI, function(e,r,b) {
                includedProblems = JSON.parse(b).rows;
                _.each(includedProblems, function(problem) {
                    problem.orderOn = includedProblems.indexOf(problem);
                });

                model.content.queryView(queryExcludedURI, function(e,r,b) {
                    excludedProblems = JSON.parse(b).rows;
                    _.each(excludedProblems, function(problem) {
                        problem.orderOn = problem.value;
                    });
                    renderProblemSequenceTables();
                });
            });
        });
    }
    , updateProblemSequence: function(req, res) {
        model.content.getDoc(req.body._id, function(e,r,b) {
            var element = JSON.parse(b);
            element.includedProblems = req.body.includedProblems;
            element.excludedProblems = req.body.excludedProblems;
            model.content.updateDoc(element, function(e,r,b) {
                res.send(r && r.statusCode || 500);
            });
        });
    }

    , elementAssessmentCriteriaPage: function(req, res) {
        model.content.queryView(encodeURI('syllabi-by-name?key="Default"&include_docs=true'), function(e,r,b) {
            var syllabi = JSON.parse(b).rows
              , topicIds = syllabi && syllabi.length ? syllabi[0].doc.topics : []
            ;
            model.content.queryView(encodeURI('names-types-by-id?keys=' + JSON.stringify(topicIds)), function(e,r,b) {
                var topics = _.filter(JSON.parse(b).rows, function(nameType) { return nameType.value[1] == 'topic'; });
                res.render('element-assessment-criteria-page', {
                    title:'Element Assessment Criteria', topics:topics, topicId:null, modules:null, moduleId:null, elements:null, elementId:null, topicModuleElementInDataset:false });
            });
        });
    }
    , assessmentCriteriaForElement: function(req, res) {
        model.content.getDoc(req.params.elementId, function(e,r,b) {
            var ids = JSON.parse(b).assessmentCriteria
              , queryString = encodeURI('?keys=' + JSON.stringify(ids) + '&include_docs=true');

            model.content.queryView('assessment-criteria' + queryString, function(e,r,b) {
                var ac = _.sortBy(JSON.parse(b).rows, function(ac) { return ac.doc.name; });
                res.contentType('html');
                res.render('element-assessment-criteria-table', { layout:false, criteria:ac });
            });
        });
    }
    , assessmentCriterionProblemsTable: function(req, res) {
        var criterionId = req.params.criterionId
          , queryURI = '/assessment-criteria-problems?startkey=' + JSON.stringify([criterionId]) + '&endkey=' + JSON.stringify([criterionId,{}]) + '&include_docs=true';

        model.content.queryView(encodeURI(queryURI), function(e,r,b) {
            var rows = JSON.parse(b).rows;
            res.render('assessment-criterion-problems-table', { problems:rows });
        });
    }
    , newAssessmentCriterion: function(req, res) {
        res.contentType('html');
        res.render('includes/assessment-criterion-rows', { criterion:null });
    }
    , deleteAssessmentCriterion: function(req, res) {
        model.content.deleteAssessmentCriterion(req.body.id, function(e,r,b) {
            if (e || !r || r.statusCode !== 200) {
                console.log('error deleting assessment criterion');
                console.log('\terror:', e);
                console.log('\tstatusCode:', r && r.statusCode || '');
                console.log('\tbody:', b);
                res.send(r && r.statusCode || 500);
                return;
            }
            res.send(200);
        });
    }
    , saveAssessmentCriterion: function(req, res) {
        var critProperties = req.body
          , critId = critProperties.id;

        delete critProperties.id;
        
        if (critId == '') {
            // insert new criterion
            model.content.insertAssessmentCriterion(critProperties.name, critProperties.elementId, critProperties.pointsRequired, function(newAC) {
                model.content.getDoc(newAC.id, function(e,r,b) {
                    res.render('includes/assessment-criterion-rows', { criterion:JSON.parse(b) });
                });
            });
        } else {
            // update criterion
            model.content.getDoc(critId, function(e,r,b) {
                var doc = JSON.parse(b);
                for (var key in critProperties) {
                    doc[key] = critProperties[key];
                }

                model.content.updateAndGetDoc(doc, function(e,r,b) {
                    res.render('includes/assessment-criterion-rows', { criterion:JSON.parse(b) });
                });
            });
        }
    }

    , createProblem: {
        page: function(req, res) {
            res.render('upload-plist', { title:'Upload the problem definition PList', probemId:'' });
        }
        , uploadPlist: function(req, res) {
            decompilePList(req.files.plist, function(e, plist) {
                if (e) {
                    res.send(e, 500);
                    return;
                }

                model.content.insertProblem(plist, function(e, newProblem) {
                    if (e) res.send(e, 500);
                    else res.redirect('/content/problem/'+newProblem.id);
                });
            });
        }
    }

    , editProblem: (function() {
        return {
            problemDetailPage: function(req, res) {
                var problemId = req.params.problemId;
                if (!problemId) res.redirect('/content/problems');

                model.content.getDoc(problemId, function(e,r,b) {
                    var problem = JSON.parse(b);

                    if (r.statusCode != 200) {
                        res.send(util.format('Document with id="%s" not found', req.params.problemId));
                        return;
                    }
                    if (problem.type != 'problem') {
                        res.send(util.format('Document with id="%s" is not a valid problem document. Expected type:"problem", actual type="%s"', req.params.problemId, problem.type));
                        return;
                    }

                    model.content.queryView(encodeURI('topics-modules-elements?keys=' + JSON.stringify([problem.syllabusId, problem.topicId, problem.moduleId])), function(e,r,b) {
                        var ids = _.flatten(
                            _.map(JSON.parse(b).rows, function(collection) { return collection.value; })
                        );

                        if (problem.toolId) ids.push(problem.toolId);

                        model.content.queryView(encodeURI('names-types-by-id?keys=' + JSON.stringify(ids)), function(e,r,b) {
                            var namesTypes = JSON.parse(b).rows
                            , topics = _.filter(namesTypes, function(nameType) { return nameType.value[1] == 'topic'; })
                            , modules = _.filter(namesTypes, function(nameType) { return nameType.value[1] == 'module'; })
                            , elements = _.filter(namesTypes, function(nameType) { return nameType.value[1] == 'element'; })
                            , tool = _.filter(namesTypes, function(nameType) { return nameType.value[1] == 'tool'; })[0]
                            ;

                            getPotentialAssessmentCriteria(problem, problem.elementId, function(criteria, assignedCriteriaIds) {
                                res.render('problem-detail-page', {
                                    problemId:problem._id                                   , title:'Edit Problem'
                                    , topics:topics                                         , topicId:problem.topicId
                                    , modules:modules                                       , moduleId:problem.moduleId
                                    , elements:elements                                     , elementId:problem.elementId
                                    , topicModuleElementInDataset:true                      , problemDescription:problem.problemDescription
                                    , assessmentCriteria:criteria                           , assignedCriteriaIds:assignedCriteriaIds
                                    , dateCreated:formatDateString(problem.dateCreated)     , dateModified:formatDateString(problem.dateModified)
                                    , toolName: (tool && tool.value[0] || '')               , hasExpression:(problem._attachments && problem._attachments['expression.mathml'] !== undefined)
                                    , problemNotes:(problem.problemNotes || '')             , internalDescription:problem.internalDescription
                                });
                            });
                        });
                    });
                });
            }
            , performUpdate: function(req, res) {
                model.content.getDoc(req.params.problemId, function(e,r,b) {
                    var problem = JSON.parse(b);

                    var removeFromElement = function() {
                        model.content.getDoc(problem.elementId, function(e,r,b) {
                            if (r.statusCode !== 200) {
                                res.send(util.format('error: could not retrieve old element for problem. (statusCode:%d, error:%s)', r.statusCode, e), 500);
                                return;
                            }

                            var element = JSON.parse(b);

                            var i = element.excludedProblems.indexOf(problem._id);
                            if (i >= 0) element.excludedProblems.splice(i, 1);

                            i = element.includedProblems.indexOf(problem._id);
                            if (i >= 0) element.includedProblems.splice(i, 1);

                            model.content.updateDoc(element, function(e,r,b) {
                                if (r.statusCode !== 201) {
                                    res.send(util.format('error: could not remove problem for element. The problem was not updated. (statusCode:%d, error:%s)', r.statusCode, e), 500);
                                    return;
                                }
                                addToElement();
                            });
                        });
                    }

                    var addToElement = function() {
                        model.content.getDoc(req.body.elementId, function(e,r,b) {
                            if (r.statusCode !== 200) {
                                res.send(util.format('error: could not retrieve new element for problem. The problem was not updated. (statusCode:%d, error:%s)', r.statusCode, e), 500);
                                return;
                            }

                            var element = JSON.parse(b);
                            element.excludedProblems.push(problem._id);

                            model.content.updateDoc(element, function(e,r,b) {
                                if (r.statusCode !== 201) {
                                    res.send(util.format('error: could not add problem to new element. If the problem was previously allocated an element, it was removed from that element. The problem was not updated. (statusCode:%d, error:%s)', r.statusCode, e), 500);
                                    return;
                                }
                                updateProblem();
                            });
                        });
                    }

                    var updateProblem = function() {
                        for (var key in req.body) problem[key] = req.body[key];
                        problem.dateModified = new Date().toJSON();

                        model.content.updateDoc(problem, function(e,r,b) {
                            if (r.statusCode !== 201) res.send(util.format('Error updating problem. (statusCode:$d, error:e)', r.statusCode, e), 500);
                            else res.send(201);
                        });
                    };

                    if (!problem.elementId) {
                        addToElement();// add to element, then update
                    } else if (problem.elementId != req.body.elementId) {
                        removeFromElement(); // remove from element, then add to new element, then update
                    } else {
                        updateProblem(); // just update the problem (i.e. it's staying on same element)
                    }
                });
            }
            , sendPList: function(req, res, sendAsAttachment) {
                var problemId = req.params.problemId;

                model.content.getProblemDefinitionPList(problemId, function(e,r,b) {
                    if (r.statusCode == 200) {
                        if (sendAsAttachment) res.header('Content-Disposition', 'attachment; filename=pdef-' + problemId + '.plist');
                        res.header('Content-Type', 'application/xml');
                        res.send(b);
                    } else {
                        res.send('could not retrieve problem plist. statusCode='+r.statusCode, 500);
                    }
                });
            }
            , sendMathMLExpression: function(req, res, sendAsAttachment) {
                var problemId = req.params.problemId;

                model.content.getProblemExpression(problemId, function(e,r,b) {
                    if (r.statusCode == 200) {
                        if (sendAsAttachment) res.header('Content-Disposition', 'attachment; filename=problem-' + problemId + '-expression.mathml');
                        res.header('Content-Type', 'application/xml');
                        res.send(b);
                    } else {
                        res.send('could not retrieve problem expression. statusCode='+r.statusCode, 500);
                    }
                });
            }
            , deleteMathMLExpression: function(req, res, sendAsAttachment) {
                var problemId = req.params.problemId;

                model.content.deleteProblemExpression(problemId, function(e,r,b) {
                    if (e) {
                        res.send(e, 500);
                    } else if (r.statusCode != 200) {
                        res.send('error deleting expression for problem, statusCode='+r.statusCode, 500);
                    } else {
                        res.redirect('/content/problem/' + problemId);
                    }
                });
            }
            , replacePListPage: function(req, res) {
                res.render('upload-plist', { title:'Upload the replacement problem definition PList', problemId:req.params.problemId });
            }
            , performReplacePList: function(req, res) {
                decompilePList(req.files.plist, function(e, plist) {
                    if (e) {
                        res.send(e, 500);
                        return;
                    }

                    model.content.updatePDefPList(req.params.problemId, plist, function(e) {
                        if (e) res.send(e, 500);
                        else res.redirect('/content/problem/' + req.params.problemId);
                    });
                });
            }
            , uploadMathMLExpressionPage: function(req, res) {
                res.render('upload-mathml-expression', { title:'Upload the MathML expression file', problemId:req.params.problemId });
            }
            , performUploadMathMLExpression: function(req, res) {
                var expression = req.files.expression
                  , path = expression && expression.path

                if (!expression) {
                    res.send('expression not found on form', 500);
                    return;
                }
               
                fs.readFile(path, function(e, expression) {
                    if (e) {
                        res.send('error reading expression into memory: Error=' + e, 500);
                        return;
                    }

                    model.content.updateProblemExpression(req.params.problemId, expression, function(e) {
                        if (e) res.send(e, 500);
                        else res.redirect('/content/problem/' + req.params.problemId);
                    });
                });
            }
            , problemAssignAssessmentCriteriaTable: function(req, res) {
                model.content.getDoc(req.body.problemId, function(e,r,b) {
                    if (r.statusCode != 200) {
                        res.send(util.format('error: could not retrieve problem. statusCode:%d, database error:%s', r.statusCode, e));
                        return;
                    }

                    getPotentialAssessmentCriteria(JSON.parse(b), req.body.elementId, function(criteria, assignedCriteriaIds) {
                        res.render('includes/assign-assessment-criteria-table.jade', { assessmentCriteria:criteria, assignedCriteriaIds:assignedCriteriaIds });
                    });
                });
            }
        };
        function getPotentialAssessmentCriteria(problem, elementId, callback) {
            // N.B. elementId does not necessarily correspond to the element that the problem is currently saved against: problem.elementId
            // When editing a problem, its element may be changed, at which point this function is called to retrieve the assessment criteria for the new element, i.e. before the new element is saved against the problem

            var queryURI = 'assessment-criteria-by-element?startkey=' + JSON.stringify([ elementId ]) + '&endkey=' + JSON.stringify([ elementId, {} ]) + '&include_docs=true';

            model.content.queryView(encodeURI(queryURI), function(e,r,b) {
                var criteria = _.map(JSON.parse(b).rows, function(row) {

                    // match will always be undedfined when elementId is different from problem.elementId
                    var match = _.find(problem.assessmentCriteria, function(assignedCriterion) { return assignedCriterion.id == row.id; });
                    return {
                        id:row.id
                        , name:row.key[1]
                        , assigned: (match && true || false)
                        , minScore:(match && match.minScore || 0)
                        , maxScore:(match && match.maxScore || 0)
                        , problemIsMandatory:(match && match.problemIsMandatory || false)
                    };
                });

                // none of the element's assessment criteria will be assigned to the problem if problem.elementId != elementId
                var assignedCriteriaIds = _.map(_.filter(criteria, function(c) { return c.assigned; }), function(c) { return c.id; }).join(',');

                callback(criteria, assignedCriteriaIds);
            });
        }
    })()
};

exports.userPortal = {
    userList: function(req, res) {
        var queryURI = '/users-by-nick-name?include_docs=true';
        model.users.queryView(queryURI, function(e,r,b) {
            if (r.statusCode != 200) {
                res.send(util.format('error retrieving users list. (error:%s, statusCode:%d)', e, r.statusCode), 500);
                return;
            }

            var rows = JSON.parse(b).rows;
            res.render('user-list-page', { title:'App Users', users:rows });
        });
    }
    , activityFeedPage: function(req, res) {
        var userId = req.params.userId
          //, queryURI = util.format('/activity-feed-events-by-user-date?startkey=%s&endkey=%s&include_docs=true&descending=true', JSON.stringify([req.params.userId]), JSON.stringify([req.params.userId,{}]));
          , queryURI = util.format('/activity-feed-events-by-user-date?include_docs=true&descending=true&startkey=' + JSON.stringify([req.params.userId,{}]) + '&endkey=' + JSON.stringify([req.params.userId]))
        ;

        model.users.queryView(encodeURI(queryURI), function(e,r,b) {
            if (r.statusCode != 200) {
                res.send(util.format('error retrieving activity feed. (error:%s, statusCode:%d)', e, r.statusCode), 500);
                return;
            }

            var rows = JSON.parse(b).rows;
            for (var i=0; i<rows.length; i++) rows[i].doc.dateTime = formatDateString(rows[i].doc.dateTime);
            res.render('activity-feed-page', { title:'User Activity Feed', events:rows });
        });
    }
    , allActivitiesReversed: function(req, res) {

        var queryURI = '/activity-feed-events-by-date?descending=true&include_docs=true';

        model.users.queryView(encodeURI(queryURI), function(e,r,b) {
            if (r.statusCode != 200) {
                res.send(util.format('error retrieving activity feed. (error:%s, statusCode:%d)', e, r.statusCode), 500);
                return;
            }

            var rows = JSON.parse(b).rows;
            for (var i=0; i<rows.length; i++) rows[i].doc.dateTime = formatDateString(rows[i].doc.dateTime);
            res.render('activity-feed-page', { title:'All Users Activity Feed', events:rows });
        });
    }
}

function formatDateString(jsonDate) {
    if (!jsonDate) return '';
    return jsonDate.replace(/^([0-9]{4}-[0-9]{2}-[0-9]{2})T(([0-9]{2}:){2}[0-9]{2}).+$/, "$1 $2");
}

function decompilePList(plist, callback) {
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
