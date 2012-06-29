var util = require('util')
  , request = require('request')
  , _ = require('underscore')
;

var couchServerURI
  , dbName
  , designDoc
  , databaseURI
  , viewsPath
;

module.exports = function(config) {
    couchServerURI = config.couchServerURI.replace(/^(.+[^/])\/*$/, '$1/');
    dbName = config.usersDatabaseName;
    designDoc = config.usersDatabaseDesignDoc;
    databaseURI = couchServerURI + dbName + '/';
    viewsPath = databaseURI + '_design/' + designDoc + '/_view/'; 
    console.log(util.format('appUsersService\t\tdesignDoc="%s"\tdatabaseURI="%s"', designDoc, databaseURI));

    request({
        method: 'PUT'
        , uri: databaseURI
        , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
    }, function(e,r,b) {
        if (201 != r.statusCode && 412 != r.statusCode) {
            console.log('Error other than "already exists" when attempting to create database:"%s". Error:"%s" StatusCode:%d', databaseURI, e, r.statusCode);
            return;
        }
        updateDesignDoc(function(e,statusCode) {
            if (e || 201 != statusCode) {
                console.log('error updating %s design doc. error="%s", statusCode=%d', databaseURI, e, r.statusCode);
            }
        });
    });
    return {
        syncUsers: syncUsers
        , userMatchingNick: userMatchingNick
        , userMatchingNickAndPassword: userMatchingNickAndPassword
    };
};

function syncUsers(clientDeviceUsers, device, callback) {
    var log = false;

    if (log) console.log('\n----------------------------------------------------------------');
    if (log) console.log('clientDeviceUsers:%s', JSON.stringify(clientDeviceUsers,null,2));
    var urIds = _.pluck(clientDeviceUsers, 'id');

    request(encodeURI(databaseURI + '_all_docs?include_docs=true&keys=' + JSON.stringify(urIds)), function(e,r,b) {
        if (200 != r.statusCode) {
            callback(e, r.statusCode);
            return;
        }

        var existingUsers = _.filter(_.pluck(JSON.parse(b).rows, 'doc'), function(d) { return d != null; }); // users that already exist on the server
        var newUserIds = _.difference(urIds, _.pluck(existingUsers, '_id')); // users that need adding to the server
        var newUsers = _.map(newUserIds, function(id) { return _.find(clientDeviceUsers, function(ur) { return ur.id == id; }) });

        if (log) console.log('Server versions of client users:', JSON.stringify(existingUsers,null,2));
        if (log) console.log('\nnewUserIds:', JSON.stringify(newUserIds,null,2));
        if (log) console.log('newUsers:', JSON.stringify(newUsers,null,2));

        // mapped array with client/server update status for each user that already exists on the server and the concatenated completed nodes of client/server
        var existingUsersData = _.map(existingUsers, function(serverUr) {
            var clientUr = _.find(clientDeviceUsers, function(u) { return u.id == serverUr._id; });

            if (!clientUr.nodesCompleted) clientUr.nodesCompleted = [];
            if (!serverUr.nodesCompleted) serverUr.nodesCompleted = [];

            var nodesCompleted = _.uniq(serverUr.nodesCompleted.concat(clientUr.nodesCompleted))
              , numNodesCompleted = nodesCompleted.length
            ;
            return {
                clientUr: clientUr
                , serverUr: serverUr
                , updateOnClient: clientUr.nodesCompleted.length < numNodesCompleted
                , updateOnServer: serverUr.nodesCompleted.length < numNodesCompleted
                , nodesCompleted: nodesCompleted
            };
        });
        _.each(existingUsersData, function(urData) {
            urData.clientUr.nodesCompleted = urData.nodesCompleted;
            urData.serverUr.nodesCompleted = urData.nodesCompleted;
        });
        if (log) console.log('\nexistingUsersData:', JSON.stringify(existingUsersData,null,2));

        var clientUpdates = _.pluck( _.filter(existingUsersData, function(urData) { return urData.updateOnClient; }), 'clientUr' );
        var serverUpdates = _.pluck( _.filter(existingUsersData, function(urData) { return urData.updateOnServer; }), 'serverUr' );

        if (log) console.log('\nclientUpdates:', JSON.stringify(clientUpdates,null,2));
        if (log) console.log('\nserverUpdates:', JSON.stringify(serverUpdates,null,2));

        // call the callback function with the client updates
        callback(null, 200, clientUpdates);

        // update couch db if required
        var bulkUpdateDocs = 
            _.map(newUsers, function(ur) { return { _id:ur.id, nick:ur.nick, password:ur.password, nodesCompleted:ur.nodesCompleted, type:'USER' }; })
            .concat(serverUpdates)
        ;
        if (log) console.log('\nbulkUpdateDocs:',JSON.stringify(bulkUpdateDocs,null,2));

        if (bulkUpdateDocs.length) {
            request({
                uri: databaseURI + '_bulk_docs'
                , method: 'POST'
                , headers: { 'content-type':'application/json', accepts:'application/json' }
                , body: JSON.stringify({ docs: bulkUpdateDocs })
            }, function(e,r,b) {
                if (log) console.log('bulk update response:\nerror:%s\nstatusCode: %s\nbulk update response body:\n%s', e, r.statusCode, JSON.stringify(b,null,2));
                if (log) console.log('------------------------------------------------');
            });
        } else {
                if (log) console.log('------------------------------------------------');
        }
    });
};

function userMatchingNick(nick, callback) {
    queryView(util.format('users-by-nick?key="%s"&include_docs=true', nick), callback);
};

function userMatchingNickAndPassword(nick, password, callback) {
    queryView(util.format('users-by-nick-password?key=%s&include_docs=true', JSON.stringify([nick,password])), callback);
};

function updateDesignDoc(callback) {
    console.log('updating design doc:\t%s_design/%s', databaseURI, designDoc);

    var dd = {
        _id: '_design/' + designDoc
        , views: {
            'users-by-nick' : {
                map: (function(doc) { if ('USER' == doc.type) emit(doc.nick, null); }).toString()
            }
            , 'users-by-nick-password' : {
                map: (function(doc) { if ('USER' == doc.type) emit([doc.nick, doc.password], null); }).toString()
            }
        }
    };

    getDoc(dd._id, function(e,r,b) {
        var requestObj = { headers: { 'content-type': 'application/json', 'accepts': 'application/json' } };

        switch (r.statusCode) {
            case 404:
                requestObj.method = 'POST';
                requestObj.uri = databaseURI;
                requestObj.body = JSON.stringify(dd);
            break;
            case 200:
                requestObj.method = 'PUT';
                requestObj.uri = databaseURI + dd._id;
                dd._rev = JSON.parse(b)._rev;
                requestObj.body = JSON.stringify(dd);
            break;
            default:
                callback('error retrieving design doc from database', r.statusCode);
                return;
            break;
        }
        request(requestObj, function(e,r,b) { callback(e,r.statusCode); });
    });
}

function getDoc(id, callback) {
    request({
        uri: databaseURI + id
        , headers: { 'content-type':'application/json', accepts:'application/json' }
    }, callback);
}

function queryView(viewNameAndQueryString, callback) {
    request({
        uri: encodeURI(util.format('%s%s', viewsPath, viewNameAndQueryString))
        , headers: { 'content-type':'application/json', accepts:'application/json' }
    }, callback);
}
