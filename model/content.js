var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
;

var contentDatabase = 'tmp-blm-kcm3/'
  , designDoc = 'kcm-views'
//var contentDatabase = 'temp-blm-content/'
//  , designDoc = 'content-views'
  , couchServerURI
  , databaseURI
;

module.exports = function(serverURI) {
    couchServerURI = serverURI;
    databaseURI = serverURI + contentDatabase;
    console.log("content module: databaseURI =", databaseURI);
    
    return {
        queryView: queryView
        , insertProblem: insertProblem
        , getProblemDefinitionPList: getProblemDefinitionPList
        , getProblemExpression: getProblemExpression
        , updatePDefPList: updatePDefPList
        , updateProblemExpression: updateProblemExpression
        , deleteProblemExpression: deleteProblemExpression
        , insertAssessmentCriterion: insertAssessmentCriterion
        , deleteAssessmentCriterion: deleteAssessmentCriterion
        , getDoc: getDoc
        , insertDoc: insertDoc
        , updateDoc: updateDoc
        , updateAndGetDoc: updateAndGetDoc
        , deleteDoc: deleteDoc
        , deleteAllDocs: deleteAllDocs
        , updateViews: updateViews
        , populateDBWithInitialContent: populateDBWithInitialContent
        , createDB: createDB
    };
};

function createDB(callback) {
    console.log('>>> NOT CREATING DATABASE for model.content. Content module now uses kcm database');
    callback();
    return;

    request({
        method: 'PUT'
        , uri: databaseURI
        , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
    }, function(e,r,b) {
        if (r.statusCode == 412) console.log('database already exists');
        else populateDBWithInitialContent(callback);
        callback();
    })
}

function updateViews(callback) {
    console.log('>>> NOT UPDATING VIEWS FOR model.content. Content module now uses kcm database');
    callback();
    return;


    console.log('updating views on', databaseURI);

    var contentViews = {
        _id: '_design/' + designDoc
        , filters: {
            'only-problems': (function(doc, req) { return doc.type && doc.type == 'problem'; }).toString()
            , 'only-tools': (function(doc, req) { return doc.type && doc.type == 'tool'; }).toString()
        }
        , views: {
            'any-by-type-name': {
                map: (function(doc) { if (doc.type && doc.name) emit([doc.type, doc.name], null); }).toString()
            }
            , 'by-type': {
                map: (function(doc) { emit(doc.type, null); }).toString()
            }
            , 'syllabi-by-name': {
                map: (function(doc) { if (doc.type == 'syllabus') emit(doc.name, null); }).toString()
            }
            , 'tools-by-name': {
                map: (function(doc) { if (doc.type == 'tool') emit(doc.name, null); }).toString()
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

    getDoc(contentViews._id, validatedResponseCallback([200,404], function(e,r,b) {
        var writeViews = r.statusCode === 404 ? insertDoc : updateDoc;
        if (r.statusCode === 200) contentViews._rev = JSON.parse(b)._rev;
        
        writeViews(contentViews, validatedResponseCallback(201, callback));
    }));
}

function queryView(view, callback) {
    getDoc('_design/' + designDoc + '/_view/' + view, callback);
};

function insertTopic(name, syllabusId, callback) {
    insertDoc({
        type:'topic'
        , name: name 
        , syllabusId: syllabusId
        , modules: []
    }, validatedResponseCallback(201, function(e,r,b) {
        var newTopic = JSON.parse(b);
        
        getDoc(syllabusId, validatedResponseCallback(200, function(e,r,b) {
            var syllabus = JSON.parse(b);
            syllabus.topics.push(newTopic.id);
            updateDoc(syllabus, validatedResponseCallback(201, function(e,r,b) {
                callback(newTopic);
            }));
        }));
    }));
}

function insertModule(name, topicId, syllabusId, callback) {
    insertDoc({
        type: 'module'
        , name: name
        , topicId: topicId
        , syllabusId: syllabusId
        , elements: []
    }, validatedResponseCallback(201, function(e,r,b) {
        var newModule = JSON.parse(b);

        getDoc(topicId, validatedResponseCallback(200, function(e,r,b) {
            var topic = JSON.parse(b);
            topic.modules.push(newModule.id);
            updateDoc(topic, validatedResponseCallback(201, function(e,r,b) {
                callback(newModule);
            }));
        }));
    }));
}

function insertElement(name, moduleId, topicId, syllabusId, callback) {
    insertDoc({
        type: 'element'
        , name: name
        , moduleId: moduleId
        , topicId: topicId
        , syllabusId: syllabusId
        , assessmentCriteria: []
        , includedProblems: []
        , excludedProblems: []
    }, validatedResponseCallback(201, function(e,r,b) {
        var newElement = JSON.parse(b);
        
        getDoc(moduleId, validatedResponseCallback(200, function(e,r,b) {
            var module = JSON.parse(b);
            module.elements.push(newElement.id);
            updateDoc(module, validatedResponseCallback(201, function(e,r,b) {
                callback(newElement);
            }));
        }));
    }));
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
          , isMetaQuestion = matchMETA_QUESTION && matchMETA_QUESTION.length > 0
          , toolName = (matchValTOOL_KEY && matchValTOOL_KEY.length > 1 && matchValTOOL_KEY[1]) || (isMetaQuestion && 'NONE') // Meta Questions optionally have TOOL_KEY. Other questions must have TOOL_KEY
          , problemDescription = (isMetaQuestion
                                ? matchValMETA_QUESTION_TITLE && matchValMETA_QUESTION_TITLE.length > 1 && matchValMETA_QUESTION_TITLE[1]
                                : matchValPROBLEM_DESCRIPTION && matchValPROBLEM_DESCRIPTION.length > 1 && matchValPROBLEM_DESCRIPTION[1]) // use META_QUESTION_TITLE for Meta Questions
        ;

        if (!toolName || !problemDescription) {
            if (typeof callback == 'function') callback(
                'invalid plist - missing values for keys: '
                    + (toolName ? '' : ' TOOL_KEY')
                    + (toolName || problemDescription ? '' : ' and')
                    + (problemDescription ? '' : (isMetaQuestion ? ' META_QUESTION_TITLE' : ' PROBLEM_DESCRIPTION'))
            );
            return;
        }
        queryView(encodeURI('any-by-type-name?key=' + JSON.stringify(['tool',toolName])), function(e,r,b) {
            var rows = !e && r && r.statusCode == 200 && JSON.parse(b).rows
              , toolId = rows && rows.length && rows[0].id
            ;

            if (toolId || isMetaQuestion) callback(null, plistString, problemDescription, toolId);
            else callback(util.format('invalid plist. Could not retrieve tool with name %s. -- Database callback: (error:%s, statusCode:%d)', toolName, e, r.statusCode));
        });
    });
}

function insertProblem(plist, element, callback) {
    // valid args:
    // 1: plist, callback               - problem created without syllabusId,topicId,moduleId,elementId
    // 2: plist, element, callback
    if (typeof element == 'function') {
        callback = element;
        element = null;
    }

    getProblemInfoFromPList(plist, function(e, plistString, problemDescription, toolId) {
        if (e) {
            callback(e);
            return;
        }

        request({ method:'GET', uri:couchServerURI+'_uuids' }, function(e,r,b) {
            if (r.statusCode != 200) {
                callback(util.format('could not generate uuid -- Database callback: (error:%s, statusCode:%d, body:%s)', e, r.statusCode, b));
                return;
            }

            var performInsert = function(syllabusId) {
                var now = (new Date()).toJSON();
                request({
                    method: 'PUT'
                    , uri: databaseURI + JSON.parse(b).uuids[0]
                    , multipart: [
                        { 'Content-Type': 'application/json'
                            , 'body': JSON.stringify({
                                type: 'problem'
                                , problemDescription: problemDescription
                                , problemNotes: ''
                                , elementId: element && element._id || null
                                , moduleId: element && element.moduleId || null
                                , topicId: element && element.topicId || null
                                , syllabusId: syllabusId
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
                    if (e || r.statusCode != 201) {
                        if (typeof callback == 'function') callback(util.format('Error creating problem -- Database callback: (error:%s, statusCode:%d, body:%s)', e, r.statusCode, b));
                        return;
                    }

                    var newProblem = JSON.parse(b);

                    if (element) {
                        element.excludedProblems.push(newProblem.id);

                        updateDoc(element, function(e,r,b) {
                            if (typeof callback == 'function') {
                                if (e || r.statusCode != 201) callback(util.format('Error adding new problem id=%s to element -- Database callback: (error:%s, statusCode:%d, body:%s)', newProblem.id,  e, r.statusCode, b), newProblem);
                                else callback(null, newProblem);
                            }
                        });
                    } else {
                        if (typeof callback == 'function') callback(null, newProblem);
                        return;
                    }
                });
            };
            
            performInsert(null);
            // TODO: The following block (as with much of this module) is no longer relevant now that we're not using syllabus->topic->module->element hierarchy, line above used instead.
//            if (element) {
//                performInsert(e.element.syllabusId);
//            } else {
//                queryView(encodeURI('syllabi-by-name?key="Default"'), function(e,r,b) {
//                    var rows = r.statusCode == 200 && b && JSON.parse(b).rows || null;
//                    if (!rows || !rows.length) {
//                        callback(util.format('could not retrieve default syllabus. Database callback: (statusCode:%d, error:%s, body:%s)', r.statusCode, e, b));
//                        return;
//                    }
//                    performInsert(rows[0].id);
//                });
//            }

        });
    });
}

function updatePDefPList(problemId, plist, callback) {
    getProblemInfoFromPList(plist, function(e, plistString, problemDescription, toolId) {
        if (e) {
            callback(e);
            return;
        }

        getDoc(problemId, function(e,r,b) { 
            if (r.statusCode != 200) {
                callback('could not retrieve problem statusCode: ' + r.statusCode);
                return;
            }

            var problem = JSON.parse(b);
            problem.problemDescription = problemDescription;
            problem.toolId = toolId;
            problem.dateModified = (new Date()).toJSON();

            if (!problem._attachments) problem._attachments = {};
            problem._attachments['pdef.plist'] = {
                follows: true
                , length: plistString.length
                , 'Content-Type': 'application/xml'
            };

            request({
                method: 'PUT'
                , uri: databaseURI + problemId
                , multipart: [
                    { 'Content-Type': 'application/json', 'body': JSON.stringify(problem) }
                    , { 'body': plistString }
                ]
            }, function(e,r,b) {
                callback(r.statusCode != 201 && util.format('failed to update plist. statusCode:%s, database error:%s', r.statusCode, e) || null);
            });
        });
    });
}

function updateProblemExpression(problemId, expressionMathML, callback) {
    getDoc(problemId, function(e,r,b) { 
        if (r.statusCode != 200) {
            callback('could not retrieve problem statusCode: ' + r.statusCode);
            return;
        }

        var problem = JSON.parse(b);
        problem._attachments['expression.mathml'] = {
            follows: true
            , length: expressionMathML.length
            , 'Content-Type': 'application/xml'
        };

        request({
            method: 'PUT'
            , uri: databaseURI + problemId
            , multipart: [
                { 'Content-Type': 'application/json', 'body': JSON.stringify(problem) }
                , { 'body': expressionMathML }
            ]
        }, function(e,r,b) {
            callback(r.statusCode != 201 && util.format('failed to update MathML expression. statusCode:%s, database error:%s', r.statusCode, e) || null);
        });
    });
}

function deleteProblemExpression(problemId, callback) {
    getDoc(problemId, function(e,r,b) {
        if (r.statusCode != 200) {
            callback(util.format('error retrieving document with id=%s. statusCode=%d. Database Error:"%s"', problemId, r.statusCode, e));
            return;
        }

        var problem = JSON.parse(b);
        
        if (problem.type != 'problem') {
            callback(util.format('error. Document with id="%s" is not a problem', problemId));
            return;
        }

        request({
            method: 'DELETE'
            , uri: databaseURI + problemId + '/expression.mathml?rev=' + problem._rev
            , headers: { 'content-type':'application/json', accepts:'application/xml' }
        }, callback);
    });
}

function getProblemDefinitionPList(problemId, callback) {
    request({
        method: 'GET'
        , uri: databaseURI + problemId + '/pdef.plist'
        , headers: { 'content-type':'application/json', accepts:'application/xml' }
    }, callback);
}

function getProblemExpression(problemId, callback) {
    request({
        method: 'GET'
        , uri: databaseURI + problemId + '/expression.mathml'
        , headers: { 'content-type':'application/json', accepts:'application/xml' }
    }, callback);
}

function insertAssessmentCriterion(name, elementId, pointsRequired, callback) {
    insertDoc({
        type: 'assessment criterion'
        , name: name
        , elementId: elementId
        , pointsRequired: pointsRequired
        , problems: []
    }, validatedResponseCallback(201, function(e,r,b) {
        var newAC = JSON.parse(b);
        getDoc(elementId, validatedResponseCallback(200, function(e,r,b) {
            var element = JSON.parse(b);
            element.assessmentCriteria.push(newAC.id);
            updateDoc(element, validatedResponseCallback(201, function(e,r,b) {
                if (typeof callback == 'function') callback(newAC);
            }));
        }));
    }));
};

function deleteAssessmentCriterion(id, callback) {
    getDoc(id, function(e,r,b) {
        if (e || !r || r.statusCode !== 200) {
            if (typeof callback == 'function') callback('error retrieving criterion', r || { statusCode:500 });
            return;
        }
        var criterion = JSON.parse(b)
          , elemId = criterion.elementId;

        getDoc(elemId, function(e,r,b) {
            if (e || !r || r.statusCode !== 200) {
                if (typeof callback == 'function') callback('error retrieving element associated with criterion', r || { statusCode:500 });
                return;
            }
            var elem = JSON.parse(b)
              , isValidElement = elem.type == 'element'
              , elemCrit = elem.assessmentCriteria;


            if (!isValidElement) callback('error: Bad data. Criterion elementId refers to document not of type="element"', r || { statusCode:500 });
            elemCrit.splice(elemCrit.indexOf(elemId), 1);
            
            updateDoc(elem, function(e,r,b) {
                if (e || !r || r.statusCode !== 201) {
                    if (typeof callback == 'function') callback('error updating element associated with element', r || { statusCode:500 });
                    return;
                }

                deleteDoc(criterion, function(e,r,b) {
                    if (typeof callback == 'function') callback(e,r,b);
                });
            });
        });
    });
};

function getDoc(id, callback) {
    request({
        uri: databaseURI + id
        , headers: { 'content-type':'application/json', accepts:'application/json' }
    }, callback);
};

function insertDoc(doc, callback) {
    request({
        method: 'POST'
        , uri: databaseURI
        , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
        , body: JSON.stringify(doc)
    }, callback);
}

function updateDoc(doc, callback) {
    //console.log('updateDoc:', JSON.stringify(doc,null,2));
    request({
        method:'PUT'
        , uri: databaseURI + doc._id 
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify(doc)
    }, callback);
};

function updateAndGetDoc(doc, callback) {
    request({
        method:'PUT'
        , uri: databaseURI + doc._id 
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify(doc)
    }, validatedResponseCallback(201, function(e,r,b) {
        getDoc(JSON.parse(b).id, validatedResponseCallback(200, function(e,r,b) {
            if (typeof callback == 'function') callback(e,r,b);
        }));
    }));
};

function deleteDoc(doc, callback) {
    request({
        method:'DELETE'
        , uri: databaseURI + doc._id + '?rev=' + doc._rev
    }, function(e,r,b) {
        if (typeof callback == 'function') callback(e,r,b);
    });
};

function deleteAllDocs(callback) {
    request.get(databaseURI + '_all_docs', function(err, res, body) {
        var bodyObj = JSON.parse(body)
          , rows = bodyObj.rows
          , todo = bodyObj.total_rows
          , done = 0;

        if (!todo && typeof callback == 'function') { callback(0, 0); }

        while (rows.length) { 
            var row = rows.pop();
            request({
                method: "DELETE"
                , uri: databaseURI + row.id + '?rev=' + row.value.rev
            }, validatedResponseCallback(200, function(e,r,b) {
                ++done;
                if (!--todo && typeof callback == 'function') { callback(bodyObj.total_rows - done, done);  }
            }));
        }
    });
};

function validatedResponseCallback(validStatusCodes, callback) {
    if (typeof validStatusCodes == 'number') validStatusCodes = [ validStatusCodes ];
    return function(e, r, b) {
        if (!e && r && validStatusCodes.indexOf(r.statusCode) > -1) {
            if (typeof callback == 'function') callback(e,r,b);
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

function logResponse(message, err, res, body) {
    console.log('\nResponse Log');
    console.log('\tmessage:', message);
    console.log('\tError:', err);
    console.log('\tStatus Code:', (res && res.statusCode || ''));
    console.log('\tBody:', body);
}

// Populate DB With Initial Content
function populateDBWithInitialContent(callback) {
    console.log('populate database with initial content (syllabus/topics/modules/elements):');
    addInitialTopicsModulesElements(function(syllabus) {
        var tools = [
            { type:'tool', name:'BlockFloating' }
            , { type:'tool', name:'PlaceValue' }
            , { type:'tool', name:'NLine' }
            , { type:'tool', name:'ColumnAddition' }
            , { type:'tool', name:'ExprScene' }
        ];

        var modules = _.flatten(_.map(syllabus.topics, function(topic) { return topic.modules; }));
        var elements = _.flatten(_.map(modules, function(module) { return module.elements; }));

        console.log('insert tools...');
        (function insertToolsFromIndex(index) {
            if (index === tools.length) {
                console.log('initial population of database complete!');
                if (typeof callback == 'function') callback();
                return;
            }

            var tool = tools[index];
            console.log('insert tools['+index+']:', JSON.stringify(tool));
            insertDoc(tool, validatedResponseCallback(201, function(e,r,b) {
                tool.id = JSON.parse(b).id;
                insertToolsFromIndex(1+index);
            }));
        })(0);
    });
};

function addInitialTopicsModulesElements(callback) {
    console.log('delete all docs...');
    deleteAllDocs(function(errorCount) {
        if (errorCount) {
            console.log('error deleting all documents. errorCount:'+errorCount);
            if (typeof callback == 'function') callback('error deleting documenta. error count =', errorCount);
            return;
        }
        console.log('all docs deleted');
        console.log('insert/update views...');
        updateViews(function(e,r,b) {
            console.log('views inserted/updated');
            console.log('insert "Default" syllabus...');
            insertDoc({ type:'syllabus', name:'Default', topics:[] }, validatedResponseCallback(201, function(e,r,b) {
                console.log('"Default" syllabus inserted');

                var syllabus = { id:JSON.parse(b).id, topics:[] }
                  , syllabusId = syllabus.id
                  , topics = syllabus.topics
                  , s = fs.readFileSync(process.cwd() + '/resources/TopicsModulesElements.txt', 'UTF-8').trim();
                
                _.each(
                    _.map(s.split('\n'), function(line) {
                        var data = line.split('\t');
                        return { topic:data[0], module:data[1], element:data[2] };
                    })
                , function(elemData) {
                    var topic = _.find(topics, function(t) { return t.name == elemData.topic; });
                    if (!topic)
                        topic = topics[-1 + topics.push({ id:null, name:elemData.topic, modules:[] })];

                    var module = _.find(topic.modules, function(m) { return m.name == elemData.module; });
                    if (!module)
                        module = topic.modules[-1 + topic.modules.push({ id:null, name:elemData.module, elements:[] })];

                    module.elements.push({ id:null, name:elemData.element });
                });

                console.log('Syllabus: ' + JSON.stringify(syllabus, null, 2));

                (function insertTopicsFromIndex(topicIndex) {
                    if (topicIndex == topics.length) {
                        console.log(util.format('all %d topics inserted', topics.length));
                        if (typeof callback == 'function') callback(syllabus);
                        return;
                    }

                    var topic = topics[topicIndex]
                      , modules = topic.modules;

                    console.log(util.format('insert topic %d/%d: "%s"', 1+topicIndex, topics.length, topic.name));

                    insertTopic(topic.name, syllabusId, function(topicData) {
                        console.log('topic inserted, insert modules for topic...');
                        topic.id = topicData.id;
                    
                        (function insertModulesFromIndex(moduleIndex) {
                            if (moduleIndex == modules.length) {
                                console.log(util.format('all %d modules inserted for topic:"%s"', topics.length, topic.name));
                                insertTopicsFromIndex(1+topicIndex, callback);
                                return;
                            }
                            
                            var module = modules[moduleIndex]
                              , elements = module.elements;

                              console.log(util.format('insert module %d/%d: "%s::%s"', 1+moduleIndex, modules.length, topic.name, module.name));

                            insertModule(module.name, topic.id, syllabusId, function(moduleData) {
                                console.log('module inserted, insert elements for module...');
                                module.id = moduleData.id;

                                (function insertElementsFromIndex(elementIndex, callback) {
                                    if (elementIndex == elements.length) {
                                        console.log(util.format('all %d elements inserted for module:"%s::%s"', elements.length, topic.name, module.name));
                                        insertModulesFromIndex(1+moduleIndex);
                                        return;
                                    }
                                    
                                    var element = elements[elementIndex];
                                    element.moduleId = module.id;
                                    element.topicId = topic.id;
                                    element.syllabusId = syllabusId;

                                    console.log(util.format('insert element %d/%d: "%s::%s::%s"', 1+elementIndex, elements.length, topic.name, module.name, element.name));
                                    
                                    insertElement(element.name, module.id, topic.id, syllabusId, function(elementData) {
                                        element.id = elementData.id;
                                        insertElementsFromIndex(1+elementIndex);
                                    });
                                })(0);
                            });
                        })(0);
                    });
                })(0);
            }));
        });
    });
};

// THIS FUNCTION NEEDS UPDATING OR DELETING (DELETION PROB BEST)
function addInitialProblems(tools, elements, callback) {
    var s = fs.readFileSync(process.cwd() + '/resources/Problems.txt', 'UTF-8').trim()
      , problems = []
    ;

    problems = _.map(s.split('\n'), function(line) {
        var data = line.split('\t')
        return {
            element: _.find(elements, function(el) { return el.name == data[0] })
            , toolId: (_.find(tools, function(tool) { return tool.name == data[1] })).id
            , problemDescription:data[2]
        };
    });

    (function insertProblemsFromIndex(problemIndex) {
        if (problemIndex == problems.length) {
            if (typeof callback == 'function') callback(problems);
            return;
        }

        var problem = problems[problemIndex]
          , elem = problem.element;
        console.log('problem:', JSON.stringify(problem));

        insertProblem({ problemDescription:problemDescription, elementId:elem.id, moduleId:elem.moduleId, topicId:elem.topicId, syllabusId:elem.syllabusId, toolId:problem.toolId }, function(e, doc) {
            problem.doc = doc;
            insertProblemsFromIndex(1+problemIndex);
        });
    })(0);
}

// THIS FUNCTION NEEDS UPDATING OR DELETING (DELETION PROB BEST)
function addInitialAssessmentCriteria(callback) {
    var elementNames = ["Numbers", "Matching", "Sorting", "Recognising Fractions"];
    queryView('elements-by-moduleid-name', function(e,r,b) {
        var elements = _.filter(JSON.parse(b).rows, function(element) {
            return elementNames.indexOf(element.key[1]) > -1;
        });

        (function insertAssessmentCriteriaFromIndex(index) {
            if (index == elements.length) {
                callback();
                return;
            }

            var element = elements[index]
              , elementId = element.id
              , elementName = element.key[1]
              , name = ({
                'Numbers': 'count up to 10 objects'
                , 'Matching': 'sort and classify objects'
                , 'Sorting': 'smallest and largest'
                , 'Recognising Fractions': 'understanding halving'
            })[elementName];

            insertAssessmentCriterion(name, elementId, 1000, function(e,r,b) {
                insertAssessmentCriteriaFromIndex(1+index);
            });
        })(0);
        //console.log(elements);
    });
}
