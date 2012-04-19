var fs = require('fs')
  , _ = require('underscore')
  , util = require('util')
  , kcmModel
;

module.exports = function(model) {
    kcmModel = model.kcm;


    return function(req, res) {
        kcmModel.queryView(encodeURI('concept-nodes?include_docs=true'), function(e,r,b) {
            var rows = JSON.parse(b).rows
              , map = {
                    nodes: _.map(rows, function(row) {
                        var doc = row.doc;
                        return {
                            id: doc._id
                            , nodeDescription: doc.nodeDescription
                            , x: doc.x
                            , y: doc.y
                            , width: doc.width
                            , height: doc.height
                        };
                    })
              }
            ;

            var nodematch = _.filter(map.nodes, function(n) {
                return /Find all factor pairs/i.test(n.nodeDescription);
            });

            kcmModel.queryView(encodeURI('relations-by-name?key="Prerequisite"&include_docs=true'), function(e,r,b) {
                map.prerequisites = JSON.parse(b).rows[0].doc.members;

                res.render('kcm/layout', { title:'Knowledge Concept Map', map:map });
            });
        });
    };
};
