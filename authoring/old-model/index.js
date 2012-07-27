module.exports = function(config, kcm) {
    return {
        kcm: require('./kcm')(config, kcm)
        , content: require('./content')(config)
    };
};
