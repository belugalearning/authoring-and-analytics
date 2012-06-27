var model;

module.exports = function(m) {
    model = m;
    return exports;
};

exports.syncUsers = function(req, res) {
    // send success straight away. Will get data sent again pretty regularly
    var users = JSON.parse(req.body.users)
      , device = req.body.device
    ;
    
    model.syncUsers(users, device, function(e, statusCode, updates) {
        res.send(e || updates, statusCode || 500);
    });
};


/*
app.post('/app-users/sync-users', function(req,res) {
    var users = JSON.parse(req.body.users);

    request('');

    res.send(['this','is','only',{a:'test'}], 200);
});

*/
