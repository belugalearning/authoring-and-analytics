var fs = require('fs');

module.exports = function(config) {
    return {
        kcm: require('./kcm')(config)
        , appUsersService: require('./app-users-service')(config)
        , content: require('./content')(config)
        , users: require('./users')(config)
    };
};
