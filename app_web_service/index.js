var express = require('express');

var server = express.createServer()
  , usersService
;

module.exports = function(config) {
    server.post('/app-logging/upload', require('./logging_service')(config))

    usersService = require('./users_service')(config);
    server.post('/app-users/sync-users', usersService.syncUsers);
    server.post('/app-users/check-nick-available', usersService.checkNickAvailable);
    server.post('/app-users/get-user-matching-nick-password', usersService.getUserMatchingNickAndPassword);

    var port = parseInt(config.appWebService.port, 10) || 3000;
    server.listen(port);
    console.log("AppWebService Express server listening on port %d in %s mode", server.address().port, server.settings.env);

    return server;
};

// server config
server.configure(function() {
    server.use(express.bodyParser());
    server.use(express.cookieParser());
    server.use(express.methodOverride());
    server.use(server.router);
});
server.configure('development', function(){
    server.use(express.errorHandler({ dumpExceptions: true, showStack: true })); 
});
server.configure('production', function(){
    server.use(express.errorHandler()); 
});
