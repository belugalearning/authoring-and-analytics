//*
var fs = require('fs')
  , _ = require('underscore')
  , util = require('util')
  , libxmljs = require('libxmljs')
  , kcmModel
  , map = {}
;//*/

module.exports = function(model) {
    kcmModel = model.kcm;

    // TEMPORARY: import kcm graffle doc on load
    importGraffle();

    return function(req, res) {
        res.render('kcm/layout', { title:'Knowledge Concept Map', map:map });
    };
};

function importGraffle() {
    kcmModel.createDB(function(e,r,b) {
        if (e || r.statusCode != 201) {
            console.log(util.format('Error creating database. error="%s", statusCode="%d"', e, r.statusCode));
            return;
        }

        console.log('created kcm database');

        var xmlDoc = libxmljs.parseXmlString(fs.readFileSync(process.cwd()+'/resources/kcm.graffle', 'utf8'))
          , graphicsElm = xmlDoc.get('//key[text()="GraphicsList"]/following-sibling::array')
          , conceptElms = graphicsElm.find('./dict[child::key[text()="Class"]/following-sibling::string[text()="ShapedGraphic"]][child::key[text()="Shape"]/following-sibling::string[text()="Rectangle"]]')
          , linkElms = graphicsElm.find('./dict[child::key[text()="Class"]/following-sibling::string[text()="LineGraphic"]]')
          , processNodeTextRegExp = /\\cf0\s*([\s\S]+)\}$/ // (mostly) matches the actual text entered (i.e. the portion of the string that comes after the formatting)
        ;

        function getElmId(el) {
            return parseInt(el.get('./key[text()="ID"]/following-sibling::*').text());
        } 

        map.nodes = _.map(conceptElms, function(el) {
            var bounds = el.get('./key[text()="Bounds"]/following-sibling::string').text().match(/\d+\.\d+?/g)
              , nodeDescription = el.get('./key[text()="Text"]/following-sibling::dict/key[text()="Text"]/following-sibling::string').text()
            ;
            nodeDescription = nodeDescription && nodeDescription.match(processNodeTextRegExp).length > 1 && nodeDescription.match(processNodeTextRegExp)[1] || "";

            return {
                id: getElmId(el)
                , graffleId: getElmId(el) // will be overwritten by db uuid
                , nodeDescription: nodeDescription
                , x: bounds[0]
                , y: bounds[1]
                , width: bounds[2]
                , height: bounds[3]
            };
        });


        // translate graph so that top-left node is at (0,0)
        var firstX = _.sortBy(map.nodes, function(n) { return n.x })[0].x
          , firstY = _.sortBy(map.nodes, function(n) { return n.y })[0].y
        ;
        console.log('firstX:', firstX, 'firstY:', firstY);
        _.each(map.nodes, function(n) {
            n.x -= firstX;
            n.y -= firstY;
        });

        var prerequisitePairs = _.filter(
            _.map(linkElms, function(el) {
                return [ getElmId(el.get('./key[text()="Tail"]/following-sibling::dict')), getElmId(el.get('./key[text()="Head"]/following-sibling::dict')) ];
            })
            , function(pair) { 
                return _.find(map.nodes, function(n) { return n.id == pair[0]; }) && _.find(map.nodes, function(n) { return n.id == pair[1]; })
            }
        );
        
        //insert into db
        if (map.nodes.length) {
            kcmModel.queryView(encodeURI('relations-by-name?key="Prerequisite"'), function(e,r,b) {
                var prerequisiteRelationId = r.statusCode == 200 && JSON.parse(b).rows[0].id;
                if (!prerequisiteRelationId) {
                    console.log('error - could not retrieve prerequisite relation.');
                    return;
                }

                (function insertConceptNode(nodeIndex) {
                    console.log(util.format('inserting concept node  %d of %d', 1+nodeIndex, map.nodes.length));

                    var node = map.nodes[nodeIndex];
                    kcmModel.insertConceptNode({
                        nodeDescription: node.nodeDescription
                        , x: node.x
                        , y: node.y
                        , width: node.width
                        , height: node.height
                    }, function(e,r,b) {
                        if (r.statusCode != 201) {
                            console.log(util.format('error inserting concept node: (e:"%s", statusCode:%d, b:"%s")', e, r.statusCode, b));
                            return;
                        }
                        var dbId = JSON.parse(b)._id
                          , graffleId = node.id
                        ;
                        node.id = dbId;
                        _.each(prerequisitePairs, function(pair) {
                            if (pair[0] == graffleId) pair[0] = dbId;
                            else if (pair[1] == graffleId) pair[1] = dbId;
                        });

                        if (++nodeIndex < map.nodes.length) {
                            insertConceptNode(nodeIndex);
                        } else if (prerequisitePairs.length) {

                            (function insertPrerequisitePair(pairIndex) {
                                console.log(util.format('inserting prerequisite pair %d of %d', pairIndex, prerequisitePairs.length));

                                var pair = prerequisitePairs[pairIndex];
                                kcmModel.addOrderedPairToBinaryRelation(prerequisiteRelationId, pair[0], pair[1], function(e,r,b) {
                                    if (r.statusCode != 201) {
                                        console.log(util.format('error inserting prerequisite relation member: (e:"%s", statusCode:%d)', e, r.statusCode));
                                        var nodes = _.filter(map.nodes, function(n) { return n.id == pair[0] || n.id == pair[1]; });
                                        return;
                                    }
                                    if (++pairIndex < prerequisitePairs.length) {
                                        insertPrerequisitePair(pairIndex);
                                    } else {
                                        console.log('all prerequisite pairs inserted');
                                    }
                                });
                            })(0);
                        } else {
                            console.log('no prerequisite links on graph');
                            return;
                        }
                    });
                })(0);
            });
        } else {
            console.log('no concept nodes on graph.');
            return;
        }
    });
}
