module.exports = function(config) {
    return {
        kcm: require('./kcm')(config)
        , content: require('./content')(config)
    };
};
