var model

module.exports = function(m) {
  model = m
  return exports
}

exports.syncUsers = function(req, res) {
  // send success straight away. Will get data sent again pretty regularly
  var users = JSON.parse(req.body.users)

  model.syncUsers(users, function(e, statusCode, updates) {
    res.send(e || updates, statusCode || 500)
  })
}

exports.checkNickAvailable = function(req, res) {
  var nick = req.body.nick

  model.userMatchingNick(nick, function(e,r,b) {
    if (200 != r.statusCode) {
      res.send(e, r.statusCode || 500)
      return
    }

    res.send(JSON.parse(b).rows.length == 0, 200)
  })
}

exports.getUserMatchingNickAndPassword = function(req, res) {
  var nick = req.body.nick
    , password = req.body.password

  model.userMatchingNickAndPassword(nick, password, function(e,r,b) {
    if (200 != r.statusCode) {
      console.log(nick, password, e, r.statusCode)
      res.send(e, r.statusCode || 500)
      return
    }

    var rows = JSON.parse(b).rows

    if (!rows.length) {
      res.send(404)
      return
    }

    var urData = rows[0].doc
      , ur = { id:urData._id, nick:urData.nick, password:urData.password, nodesCompleted:urData.nodesCompleted }

    res.send(ur, 200)
  })
}
