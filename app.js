var fs = require('fs')
  , _ = require('underscore')
;

var config = JSON.parse(fs.readFileSync(__dirname + '/config.json'))
  , appWebService = require('./app_web_service')(config)
  , authoring = require('./authoring')(config)
;

/*
 * NO LAUNCH OPTIONS AT THE MOMENT
 *
var launchOptions = _.map(
    _.filter(
        process.argv
        , function(arg) { return /^-/.test(arg); }
    )
    , function(arg) {
        var i = 1 + process.argv.indexOf(arg)
          ,  value
        ;
        if (process.argv.length > i && /^[^\-]/.test(process.argv[i])) {
            value = process.argv[i];
        }
        return { name:arg.substring(1), value:value }
    }
);

function getAppLaunchOption(name) {
    return _.find(launchOptions, function(o) { return o.name == name; });
}
*/
