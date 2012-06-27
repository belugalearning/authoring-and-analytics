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

exports.checkNickAvailable = function(req, res) {
    var nick = req.body.nick;
    model.userMatchingNick(nick, function(e,r,b) {
        if (200 != r.statusCode) {
            res.send(e, 500);
            return;
        }
        res.send(JSON.parse(b).rows.length == 0, 200);
    });
};
