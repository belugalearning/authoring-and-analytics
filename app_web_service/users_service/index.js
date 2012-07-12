module.exports = function(config) {
    var model = require('./model')(config)
    return require('./route_handlers')(model);
};
