var fs = require('fs')
  ,  config = JSON.parse(fs.readFileSync(process.cwd() + '/config.json'))
;

module.exports = {
    kcm: require('./kcm')(config)
    , appUsersService: require('./app-users-service')(config)
    , content: require('./content')(config)
    , users: require('./users')(config)
};
