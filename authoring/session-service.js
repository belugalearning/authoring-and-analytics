var urlParser = require('url')
  , _ = require('underscore')
  , util = require('util')
  , connect = require('connect')

var sidKey = 'bl.authoring.sid'
  , sessionStore = new connect.session.MemoryStore
  , parseCookie = connect.cookieParser()

var connectSession = connect.session({
  store: sessionStore
  , key: sidKey
  , cookie: { maxAge:3600000 } // 1 hour
  , secret: '1a4eca939f8e54fd41e3d74d64aa9187d9951aed50599986418d96180716579c1ec776fc17a96640e579e76481677c87'
})

exports.createService = function createSessionService(kcm, noAuthenticatePathnames) {
  var noAuthRegexps = noAuthenticatePathnames ? _.filter(noAuthenticatePathnames, function(pn) { return _.isRegExp(pn) }) : []
    , noAuthStrings = noAuthenticatePathnames ? _.difference(noAuthenticatePathnames, noAuthRegexps) : []

  var get = function(req, fn) {
    parseCookie(req, null, function(e) {
      if (e) fn(e)
      else sessionStore.get(req.cookies[sidKey], fn)
    })
  }

  var httpRequestHandler = function httpRequestHandler(req, res, next) {
    connectSession(req, res, function() {
      var pathname = urlParser.parse(req.url).pathname
      
      switch (pathname) {
        case '/logout':
          if (req.session) req.session.destroy()
          res.redirect('/login')
          break
        case '/login':
          if ('POST' == req.method) {
            // validate credentials
            kcm.queryView('users-by-credentials', 'key', [req.body.user, req.body.password], 'include_docs', true, function(e,r,b) {
              var rows = JSON.parse(b).rows
                , now = new Date
                , allSessions = req.sessionStore.sessions
                , preExistingUserSession

              if (!rows.length) { // incorrect credentials
                if (req.session) req.session.destroy()
                res.render('sessions/login', { redir:req.body.redir })
              } else { // correct credentials
                req.session.user = rows[0].doc

                // is user already logged in?
                var now = new Date()
                  , allSessions = req.sessionStore.sessions

                for (var id in allSessions) {
                  if (id != req.sessionID) {
                    var sess = JSON.parse(allSessions[id])
                    if (sess.user && sess.user._id == req.session.user._id && new Date(sess.cookie.expires) > now) {
                      preExistingUserSession = sess
                      break
                    }
                  }
                }

                if (preExistingUserSession) {
                  // user is already logged on. To continue (s)he needs to force end other session.
                  req.session.pendingLoginConfirmation = true
                  res.render('sessions/confirm-login', { redir:req.body.redir, loggedInUser:preExistingUserSession.user.name })
                } else {
                  // nobody else logged in, continue
                  req.session.isAuthenticated = true
                  res.redirect(req.body.redir || '/')
                }
              }
            })
          } else {
            res.render('sessions/login', { redir: req.query.redir || req.body.redir })
          }
          break
        case '/confirm-login':
          if (req.body.confirm == 'on') {
            req.session.pendingLoginConfirmation = false

            // log off the other user if (s)he is still logged on
            var allSessions = req.sessionStore.sessions
            var authenticatedSession
            for (var id in allSessions) {
              if (id != req.sessionID) {
                var s = JSON.parse(allSessions[id])
                if (s.isAuthenticated) req.sessionStore.destroy(id)
              }
            }
            req.session.isAuthenticated = true
            res.redirect(req.body.redir || '/')
          } else {
            // login cancelled - redirect to login page (& session will be destroyed)
            var url = '/login'
            if (req.body.redir) url += '?redir=' + req.body.redir
            res.redirect(url)
          }
          break
        default:
          if (
            !req.session.isAuthenticated && !~noAuthStrings.indexOf(pathname) && !_.find(noAuthRegexps, function(re) { return re.test(pathname) })
            || req.session.pendingLoginConfirmation
          ) {
            if (req.isXMLHttpRequest) res.send('You have been logged out', 401)
            else res.redirect('/login?redir='+req.url)
          } else {
            next()
          }
          break
      }
    })
  }

  return {
    get: get
    , httpRequestHandler: httpRequestHandler
  }
}
