var fs = require('fs')
  , _ = require('underscore')
  , WebPortal = require('./web_portal')

console.log('\nBLWebApp Launched at', new Date())

var config = JSON.parse(fs.readFileSync(__dirname + '/config.json'))

if (!!~process.argv.indexOf('--localise-couchdb-uris')) {
  config.couchServerURI = config.couchServerURI.replace(/^http:\/\/.*:/, 'http://127.0.0.1:')
}

var appWebService = require('./app_web_service')(config)
  , authoring = require('./authoring')(config)
  , webPortal = new WebPortal(config)

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
