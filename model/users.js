var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
;

var designDocName
  , couchServerURI
  , databaseURI
;

module.exports = function(config) {
    couchServerURI = config.couchServerURI.replace(/^(.+[^/])\/*$/, '$1/');
    databaseURI = couchServerURI + config.usersDatabaseName + '/';
    designDocName = config.usersDatabaseDesignDoc;
    console.log(util.format('users module:\tdesignDoc="%s"\tdatabaseURI="%s"', designDocName, databaseURI));
    
    return {
        queryView: queryView
        , updateViews: updateViews
    };
};

function queryView(view, callback) {
    getDoc('_design/' + designDoc + '/_view/' + view, callback);
};

// Views
function updateViews(callback) {
    console.log('updating views on database:', databaseURI);

    var designDoc = {
        _id: '_design/' + designDocName
        , views: {
            'devices': {
                map: (function(doc) { if (doc.type == 'device') emit(doc.firstLaunchDateTime, null); }).toString()
            }
            , 'launch-timestamps-by-date-device': {
                map: (function(doc) {
                    var deviceTS = doc.dbLaunchTimestamps
                      , len = deviceTS && deviceTS.length
                      , i, launchTS;

                    if (doc.type == 'device' && len) {
                        for (i=0; i<len; i++) {
                            launchTS = deviceTS[i];
                            emit([launchTS[0].timestamp, doc._id], launchTS);
                        }
                    }
                }).toString().replace(/\n/g, '')
            }
            , 'users-by-nick-name': {
                map: (function(doc) { if (doc.type == 'user') emit(doc.nickName, null); }).toString()
            }
            , 'users-by-nick-name-password': {
                map: (function(doc) { if (doc.type == 'user') emit([doc.nickName, doc.password], null); }).toString()
            }
            , 'users-by-device': {
                map: (function(doc) { if (doc.type == 'device') emit(doc.name, null); }).toString()
            }
            , 'problem-attempts-by-start': {
                map: (function(doc) { if (doc.type == 'problem attempt') emit(doc.startDateTime, null); }).toString()
            }
            , 'activity-feed-events-by-user-date': {
                map: (function(doc) { if (doc.type == 'activity feed event') emit([doc.userId, doc.dateTime], null); }).toString()
            }
            , 'activity-feed-events-by-date': {
                map: (function(doc) { if (doc.type == 'activity feed event') emit([doc.dateTime, doc.userId], null); }).toString()
            }
            , 'most-recent-session-start-per-device-user': {
                map: (function(doc) {
                    if (doc.type == 'user session') {
                        emit([doc.device, doc.user], doc.dateStart);
                    }
                }).toString().replace(/\n/g, '')
                , reduce: (function(keys, values, rereduce) {
                    var count = values.length
                      , sessionStart
                      , latest
                    ;
                    for (var i=0; i<count; i++) {
                        sessionStart = values[i];
                        if (!latest || sessionStart > latest) latest = sessionStart;
                    }
                    return latest;
                }).toString().replace(/\n/g, '')
            }
            // TODO: legacy - delete after May 25th release
            , 'device-users-last-session': {
                map: (function(doc) {
                    if (doc.type == 'device') {
                        var len = doc.userSessions.length, i;
                        for (i=0; i < len; i++) {
                            var session = doc.userSessions[i];
                            emit([doc._id, session.userId], session.startDateTime);
                        }
                    }
                }).toString().replace(/\n/g, '')
                , reduce: (function(keys, values, rereduce) {
                    var latest;
                    for (var value in values) {
                        if (!latest || value > latest) latest = value;
                    }
                    return latest;
                }).toString().replace(/\n/g, '')
            }
            // TODO: legacy - delete after May 25th release
            , 'device-sessions-by-date': {
                map: (function(doc) {
                    if (doc.type == 'device') {
                        var len = doc.userSessions.length, i;
                        for (i=0; i<len; i++) {
                            var session = doc.userSessions[i];
                            emit([session.startDateTime, doc._id], session.userId);
                        }
                    }
                }).toString().replace(/\n/g, '')
            }
            // query with group level 2
            , 'problems-completed-by-user': {
                map: (function(doc) {
                    if (doc.type == 'problem attempt' && doc.success) {
                        emit([doc.userId, doc.problemId], doc.problemId);
                    }
                }).toString().replace(/\n/g, '')
                , reduce: (function(keys, values, rereduce) {
                    return values[0];
                }).toString().replace(/\n/g, '')
            }
            // query with group level 1
            , 'total-exp-by-user': {
                map: (function(doc) {
                    if (doc.type == 'problem attempt' && doc.success) {
                        var aCP = doc.awardedAssessmentCriteriaPoints
                          , len = aCP.length
                          , i
                        ;
                        for (i=0; i<len; i++) emit(doc.userId, aCP[i].points);
                    }
                }).toString().replace(/\n/g, '')
                , reduce: (function(keys, values, rereduce) {
                    var sum = 0, len = values.length, i;
                    for (i=0; i<len; i++) sum += values[i];
                    return sum;
                }).toString().replace(/\n/g, '')
            }
            // query view with group level=1 (all time in play for user), or group level=2 (time in play on per element per user)
            , 'users-time-in-play': {
                map: (function(doc) {
                    if (doc.type == 'problem attempt') {
                        emit([doc.userId, doc.elementId], doc.timeInPlay);
                    }
                }).toString().replace(/\n/g, '')
                , reduce: (function(keys, values, rereduce) {
                    var sum = 0, len = values.length, i;
                    for (i=0; i<len; i++) sum += values[i];
                    return sum;
                }).toString().replace(/\n/g, '')
            }
        }
    };

    getDoc(designDoc._id, function(e,r,b) {
        var sc = r.statusCode
          , writeViewsFunc = sc == 200 ? updateDoc : insertDoc
        ;
        if (200 != sc && 404 != sc) {
            callback('error retrieving design doc from database', sc);
            return;
        }

        if (200 == sc) designDoc._rev = JSON.parse(b)._rev;

        writeViewsFunc(designDoc, function(e,r,b) {
            callback(e, r.statusCode);
        });
    });
}

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
    request({
        method:'PUT'
        , uri: databaseURI + doc._id 
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify(doc)
    }, callback);
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
