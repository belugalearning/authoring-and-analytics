var urlParser = require('url')
  , _ = require('underscore')
  , util = require('util')
  , ConnectSession = require('express').session
;

module.exports = function session(kcm, noAuthenticatePathnames) {
    var connectSession = ConnectSession({ key:'bl.authoring.sid', cookie:{ maxAge:600000 }, secret: '1a4eca939f8e54fd41e3d74d64aa9187d9951aed50599986418d96180716579c1ec776fc17a96640e579e76481677c87' });
    var noAuthRegexps = noAuthenticatePathnames ? _.filter(noAuthenticationPathnames, function(pn) { return _.isRegExp(pn); }) : [];
    var noAuthStrings = noAuthenticatePathnames ? _.difference(noAuthenticationPathnames, regexps) : [];

    return function session(req, res, next) {
        connectSession(req, res, function() {
            var pathname = urlParser.parse(req.url).pathname
              , session = req.session
            ;
            switch (pathname) {
                case '/logout':
                    if (session) session.destroy();
                    res.redirect('/login');
                break;
                case '/login':
                    if ('POST' == req.method) {
                        // validate credentials
                        kcm.queryView('users-by-credentials', 'key', [req.body.user, req.body.password], 'include_docs', true, function(e,r,b) {
                            var rows = JSON.parse(b).rows;

                            if (!rows.length) {
                                // incorrect credentials
                                if (session) session.destroy();
                                res.render('sessions/login', { redir:req.body.redir });
                            } else {
                                // correct credentials
                                session.user = lastLoggedInUser = rows[0].doc;

                                // is anyone else logged into the kcm?
                                var now = new Date();
                                var allSessions = req.sessionStore.sessions;
                                var authenticatedSession;
                                for (var id in allSessions) {
                                    if (id != req.sessionID) {
                                        var s = JSON.parse(allSessions[id]);
                                        if (s.isAuthenticated && new Date(s.cookie.expires) > now) {
                                            authenticatedSession = s;
                                            break;
                                        }
                                    }
                                }

                                if (authenticatedSession) {
                                    // there's someone else logged in. For current user to continue (s)he needs to force log out the other user logged in.
                                    session.pendingLoginConfirmation = true;
                                    res.render('sessions/confirm-login', { redir:req.body.redir, loggedInUser:authenticatedSession.user.name });
                                } else {
                                    // nobody else logged in, continue
                                    session.isAuthenticated = true;
                                    res.redirect(req.body.redir || '/');
                                }
                            }
                        });
                    } else {
                        res.render('sessions/login', { redir: req.query.redir || req.body.redir });
                    }
                break;
                case '/confirm-login':
                    if (req.body.confirm == 'on') {
                        session.pendingLoginConfirmation = false;

                        // log off the other user if (s)he is still logged on
                        var allSessions = req.sessionStore.sessions;
                        var authenticatedSession;
                        for (var id in allSessions) {
                            if (id != req.sessionID) {
                                var s = JSON.parse(allSessions[id]);
                                if (s.isAuthenticated) req.sessionStore.destroy(id);
                            }
                        }
                        session.isAuthenticated = true;
                        res.redirect(req.body.redir || '/');
                    } else {
                        // login cancelled - redirect to login page (& session will be destroyed)
                        var url = '/login';
                        if (req.body.redir) url += '?redir=' + req.body.redir;
                        res.redirect(url);
                    }
                break;
                default:
                    if (
                        !session.isAuthenticated && !~noAuthStrings.indexOf(pathname) && !_.find(noAuthRegexps, function(re) { return re.test(pathname); })
                        || session.pendingLoginConfirmation
                    ) {
                        if (req.isXMLHttpRequest) res.send('You have been logged out', 401);
                        else res.redirect('/login?redir='+req.url);
                    } else {
                        next();
                    }
                break;
            };
        });
    };
};
