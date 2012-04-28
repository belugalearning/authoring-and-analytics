//(function($) {
    var svg, gZoom, gWrapper, gPrereqs, gNodes
      , inFocus = null
      , l = 400
      , nodeTextPadding = 5
      , rControlsWidth = 350
      , dragMouseStart = { x:null, y:null }
    ;

     $.template("cnTagDIV",
        '<div class="cn-tag">\
            <div class="del-btn del-tag"/>\
            {{html tag}}\
        </div>'.replace(/(>|}})\s+/g, '$1'));

   $.template("cnEditTagDIV",
        '<div class="cn-tag edit">\
            <input type="text" value="{{html tag}}"/>\
        </div>'.replace(/(>|}})\s+/g, '$1'));

    $.template("cnPipelineTR",
        '<tr data-id="{{html _id}}">\
            <td class="del-pipeline">\
                <div class="del-btn"/>\
            </td>\
            <td class="txt">\
                <a href="/kcm/pipelines/{{html _id}}" target="_blank">{{html _id}}</a>\
                <br/>\
                <input type="text", value="{{html name}}"/>\
            </td>\
        </tr>'.replace(/(>|}})\s+/g, '$1'));

    $.fn.arrowRedraw = function() {
        $.each(this, function() {
            var gArrow = d3.select(this)
              , gHead = d3.select( $('g#' + $(this).attr('data-head-node'))[0] )
              , gTail = d3.select( $('g#' + $(this).attr('data-tail-node'))[0] )
              , head = gHead.select('rect')
              , tail = gTail.select('rect')
              , hw = parseInt(head.attr("width"))
              , hh = parseInt(head.attr("height"))
              , hx = d3.transform(gHead.attr("transform")).translate[0]
              , hy = d3.transform(gHead.attr("transform")).translate[1]
              , tx = d3.transform(gTail.attr("transform")).translate[0]
              , ty = d3.transform(gTail.attr("transform")).translate[1]
              , len = Math.sqrt(Math.pow(tx-hx,2) + Math.pow(ty-hy,2))
              , grad = (ty-hy) / (tx-hx)
              , theta = hx == tx
                  ? (hy > ty ? 1.5 : 0.5) * Math.PI
                  : Math.atan(grad) + (hx > tx ? Math.PI : 0)
              , ahr // arrow-head radius polar coord
            ;

            if (Math.abs(grad) < Math.abs(hh/hw)) {
                // arrow-head against left or right side of head rect
                var adj = 0.5 * hw * (hx < tx ? 1 : -1);
                ahr = adj / Math.cos(theta);
            } else {
                // arrow-head against top or bottom of head rect
                var opp = 0.5 * hh * (hy < ty ? 1 : -1);
                ahr = opp / Math.sin(theta);
            }

            gArrow
                .attr("transform", "translate("+hx+","+hy+")rotate("+(theta*180/Math.PI)+")")
                .select(".arrow-stem")
                .attr("transform", "scale("+len+",1)")
            ;
            gArrow
                .select(".arrow-head")
                .attr("transform", "translate("+ahr+",0)")
            ;
            //*/
        });
    }

    $(function() {
        $('#insert-pipeline-btn').click(addNewPipelineToConceptNode);
        $('#concept-node-data').on('click', 'td.del-pipeline > div.del-btn', deleteConceptNodePipeline);

        $('#insert-tag-btn').click(addNewTagToConceptNode);
        $('#concept-node-data').on('click', '.del-tag', deleteConceptNodeTag);
        $('#concept-node-data').on('dblclick', '.cn-tag', editConceptNodeTag);

        svg = d3.select("#wrapper")
            .append("svg")
                .attr("data-type", "svg")
                .attr("data-focusable", "true")
                .on("mousedown", gainFocus)
                .call(d3.behavior.zoom()/*.scaleExtent([1, 8])*/.on("zoom", zoom))
        ;

        $(window).resize(layoutControls).resize();

        gZoom = svg.append("g");
            
        gWrapper = gZoom
            .append("g")
            .attr("data-type", "wrapper")
            .attr("data-focusable", "true")
            .on("mousedown", gainFocus)
        ;

        gPrereqs = gWrapper.append("g");
        gNodes = gWrapper.append("g");

        var nodes = gNodes.selectAll('g.node')
            .data(kcm.nodes)
            .enter()
            .append('g')
                .attr('id', function(d) { return d._id; })
                .attr('data-type', 'concept-node')
                .attr('class', function(d) {
                    return 'node' + (d.processed ? ' processed' : '');
                })
                .attr("transform", function(d) { return "translate("+ d.x +","+ d.y +")"; })
                .attr("data-focusable", "true")
                .on("mousedown", gainFocus)
                .call(d3.behavior.drag()
                      .on("dragstart", nodeDragStart)
                      .on("drag", nodeDrag)
                      .on("dragend", nodeDragEnd)
                )
                .each(function(d,i) {
                    d3.select(this).append('rect')
                        .attr('class', 'node-bg')
                })
                .each(function(d,i) {
                    d3.select(this).append('text')
                        .attr('class', 'node-description')
                        .each(function(d,i) {
                            d3.select(this).selectAll('tspan')
                                .data(d.nodeDescription.match(/\S[\s\S]{0,59}(\s|$)/g))
                                .enter()
                                .append('tspan')
                                    .attr('x', 0)
                                    .attr('y', function(d, i) { return 12 + 14 * i; }) // text height 12px, 2px padding between lines
                                    .text(String)
                            ;
                        })
                        .attr('transform', function() {
                            var bbox = d3.select(this).node().getBBox();
                            return 'translate('+(-bbox.width/2)+','+(-bbox.height/2)+')';
                        })
                    ;
                })
                .each(function(d,i) {
                    var bbox = d3.select(this).select('text.node-description').node().getBBox();
                    d3.select(this).select('rect.node-bg')
                        .attr('width', bbox.width + 2 * nodeTextPadding)
                        .attr('height', bbox.height + 2 * nodeTextPadding)
                        .attr('transform', 'translate('+(-(nodeTextPadding+bbox.width/2))+','+(-(nodeTextPadding+bbox.height/2))+')')
                    ;
                })
        ;

        gPrereqs.selectAll('g.link')
            .data(kcm.prerequisites)
            .enter()
            .append('g')
                .attr('id', function(d) { return d.id; })
                .attr('class', 'link')
                .attr("data-focusable", "true")
                .attr("data-head-node", function(d) { return d[1]; })
                .attr("data-tail-node", function(d) { return d[0]; })                
        ;
        gPrereqs.selectAll('g.link').selectAll('rect.arrow-stem')
            .data(function(d) { return [d]; })
            .enter()
            .append("rect")
                .attr("class", "arrow-stem")
                .attr("x", 0)
                .attr("y", -0.5)
                .attr("width", 1)
                .attr("height", 1)
                .attr("style", "fill:#000;")
        ;
        gPrereqs.selectAll('g.link').selectAll('path.arrow-head')
            .data(function(d) { return [d]; })
            .enter()
            .append('path') 
                .attr('d', 'M 0 -0.5 l 0 1 l 16 7.5 l 0 -16 z')
                .attr("class", "arrow-head")
                .attr('style', 'fill:#222; stroke-width:0;')
        ;
        $('g.link').arrowRedraw();

        // scale map to fit screen and translate to centre it
        var w = $('svg').width()
          , h = $('svg').height()
          , bbox = gWrapper.node().getBBox()
          , scale = 1 / Math.max(bbox.width/w, bbox.height/h)
          , tx = w/(2*scale) - (bbox.x + bbox.width/2)
          , ty = h/(2*scale) - (bbox.y + bbox.height/2)
        ;
        gZoom.attr('transform', 'scale('+scale+')translate('+tx+','+ty+')');
        svg.calibrateToTransform(d3.transform(gZoom.attr('transform')));
    });

    function zoom() {
        gZoom.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
    }

    function layoutControls() {
        var windowPadding = 10
          , genPadding = 6
          , w = $(window).width() - 2 * windowPadding
          , h = $(window).height() - 2 * windowPadding
          , mapWidth = w - rControlsWidth - genPadding
          , mapHeight = h
        ;

        $('#wrapper')
            .css('left', windowPadding)
            .css('top', windowPadding)
        ;

        $('#right-panel')
            .css('left', w - rControlsWidth)
            .css('width', rControlsWidth)
            .css('height', mapHeight)
        ;

        svg.attr('style', 'width:'+mapWidth+'px; height:'+mapHeight+'px;');

        setMapBorder();
    }

    function setMapBorder() {
        var mapFocus = $(inFocus).attr('data-type') == 'svg';
        d3.select('svg').attr('class', mapFocus ? 'emphasis' : '');
    }

    function nodeWithId(id) {
        var len = kcm.nodes.length, i;
        for (i=0; i<len; i++) {
            if (kcm.nodes[i].id == id) return kcm.nodes[i];
        }
        return null;
    }

    function gainFocus(event) {
        var e = d3.event || event
          , focusTarget = $(e.target).closest('[data-focusable="true"]')[0]
          , newType = $(this).attr('data-type')
          , oldType = $(inFocus).attr('data-type')
        ;

        if (focusTarget && focusTarget != this || inFocus == this) return;

        svg.setPanEnabled(['svg','wrapper'].indexOf(newType) != -1);

        if (newType == 'svg') e.stopPropagation();
        e.preventDefault();

        if (oldType) {
            d3.select(inFocus).attr('class', function() {
                return d3.select(inFocus).attr('class').replace(/in\-focus */, '');
            });
        }
        if (newType) {
            d3.select(this).attr('class', function() {
                return 'in-focus ' + d3.select(this).attr('class');
            });
        }

        selectConceptNode(newType == 'concept-node' ? d3.select(this).data()[0] : null);
        inFocus = this;
        setMapBorder();
    }

    function selectConceptNode(data) {
        $('#pipelines').html('');

        if (data) {
            $('#concept-node-data').removeClass('none-selected');
            $('#concept-db-id').text(data._id);
            $('#concept-graffle-id').text(data.graffleId || '');
            $('#concept-description').val(data.nodeDescription).prop('disabled', '');
            $('#concept-notes').val(data.notes).prop('disabled', '');
            $('#concept-tags').html($.tmpl('cnTagDIV', $.map(data.tags, function(tag) { return { tag:tag }; })));
            $('#pipelines').html($.tmpl('cnPipelineTR', $.map(data.pipelines, function(plId) { return kcm.pipelines[plId]; })));
 
        } else {
            $('#concept-node-data').addClass('none-selected');
            $('#concept-db-id').text('');
            $('#concept-graffle-id').text('');
            $('#concept-description').val('').prop('disabled', 'disabled');
            $('#concept-notes').val('').prop('disabled', 'disabled');
            $('#concept-tags').html('');
        }
    }

    function nodeDragStart() {
        var e = d3.event.sourceEvent;
        dragMouseStart = { x:e.pageX, y:e.pageY };
    }

    function nodeDrag() {
        var e = d3.event.sourceEvent
          , n = d3.select(this)
          , zoomMultiplier = 1 / d3.transform(gZoom.attr('transform')).scale[0]
          , dx = zoomMultiplier * (e.pageX - dragMouseStart.x)
          , dy = zoomMultiplier * (e.pageY - dragMouseStart.y)
          , data = n.data()[0]
          , x = parseInt(data.x + dx)
          , y = parseInt(data.y + dy)
        ;
        n.attr("transform", "translate("+x+","+y+")");
        $('g.link[data-head-node='+data._id+'], g.link[data-tail-node='+data._id+']').arrowRedraw();
    }

    function nodeDragEnd() {
        var n = d3.select(this)
          , data = n.data()[0]
          , pos = d3.transform(n.attr('transform')).translate
        ;

        if (data.x == pos[0] && data.y == pos[1]) return;

        data.x = pos[0];
        data.y = pos[1];

        $.ajax({
            url: '/kcm/update-concept-node-position'
            , type: 'POST'
            , contentType: 'application/json'
            , data: JSON.stringify({ id:data._id, rev:data._rev, x:data.x, y:data.y })
            , success: function(rev) {
                data._rev = rev;
            }
            , error: ajaxErrorAlerter('Error saving updated concept node position')
        });
    }

    function showSingleInputModal(instruction, callback) {
        var modalBG = $('<div id="modal-bg"/>').appendTo('body')
          , modalDialog = $('<div id="modal-dialog" class="single-input-no-buttons"><h1/><h2>(hit return/escape to confirm/cancel)</h2></div>').appendTo('body')
          , input = $('<input type="text"/>').appendTo(modalDialog).focus()
        ;

        modalDialog.children('h1').text(instruction);
        $('body').css('overflow', 'hidden');

        $(window).on('keydown', function(e) {
            switch (e.keyCode) {
                case 13:
                    if (!input.val().length) return;
                    callback(input.val());
                    break;
                case 27:
                    callback(false);
                    break;
                default:
                    return;
            }
            modalBG.remove();
            modalDialog.remove();
            $(window).off('keydown', arguments.callee);
            $('body').css('overflow', 'auto');
        });
    }

    function showConfirmCancelModal(warning, callback) {
        var modalBG = $('<div id="modal-bg"/>').appendTo('body')
          , modalDialog = $('<div id="modal-dialog" class="warning-no-buttons"><h1/><h2>(hit return/escape to confirm/cancel)</h2></div>').appendTo('body')
        ;

        modalDialog.children('h1').text(warning);
        $('body').css('overflow', 'hidden');

        $(window).on('keydown', function(e) {
            var kc = e.keyCode;
            if (kc == 13 || kc == 27) callback(kc == 13);
            else return;
            modalBG.remove();
            modalDialog.remove();
            $(window).off('keydown', arguments.callee);
            $('body').css('overflow', 'auto');
        });
    }

    function ajaxErrorAlerter(firstline) {
        return function(e) {
            alert(firstline + '\n' + e.responseText + '\n' + [].slice.call(arguments, 1).join('\n'));
        }
    }

    function addNewTagToConceptNode(e) {
        var cn = d3.select(inFocus).data()[0];
        
        e.stopPropagation();
        e.preventDefault();

        showSingleInputModal('Enter Tag:', function(tag) {
            if (tag) {
                $.ajax({
                    url: '/kcm/insert-concept-node-tag'
                    , type: 'POST'
                    , contentType: 'application/json'
                    , data: JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, tag:tag })
                    , success: function(cnRev) {
                        cn.tags.push(tag);
                        cn._rev = cnRev;
                        selectConceptNode(cn);
                    }
                    , error: ajaxErrorAlerter('Error adding new tag to concept node')
                });
            }
        });
    }

    function deleteConceptNodeTag(e) {
        var tag = $(this).closest('div.cn-tag')
          , cn = d3.select(inFocus).data()[0]
        ;
        
        e.stopPropagation();
        e.preventDefault();

        showConfirmCancelModal('You are about to delete a tag from a concept node. Are you sure?', function(confirmation) {
            if (!confirmation) return;

            $.ajax({
                url: '/kcm/delete-concept-node-tag'
                , type: 'POST'
                , contentType: 'application/json'
                , data: JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, tagIndex:tag.index(), tagText:tag.text() })
                , success: function(cnRev) {
                    cn._rev = cnRev;
                    cn.tags.splice(tag.index(), 1);
                    selectConceptNode(cn);
                }
                , error: ajaxErrorAlerter('error deleting concept node tag')
            });
        });
    }

    function editConceptNodeTag(e) {
        var $tag = $(this).closest('div.cn-tag')
          , tagIx = $tag.index()
          , cn = d3.select(inFocus).data()[0]
          , $editTag = $.tmpl('cnEditTagDIV', { tag:cn.tags[tagIx] })
          , $input = $editTag.children('input')
        ;

        $tag.replaceWith($editTag);

        $input
            .focus()
            .on('focusout', function() {
                $editTag.replaceWith($tag);
            })
            .keydown(function(e) {
                switch(e.keyCode) {
                    case 27:
                        cancelEdit();
                        break;
                    case 13:
                        if ($input.val() == $tag.text()) cancelEdit();
                        else if ($input.val().length) saveEdit();
                        break;
                }
            })
        ;

        function cancelEdit() {
            $editTag.replaceWith($tag);
        }

        function saveEdit() {
            $editTag.remove();
            $.ajax({
                url: '/kcm/edit-concept-node-tag'
                , type: 'POST'
                , contentType: 'application/json'
                , data: JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, tagIndex:tagIx, currentText:$tag.text(), newText:$input.val() })
                , success: function(cnRev) {
                    cn._rev = cnRev;
                    cn.tags.splice(tagIx, 1, $input.val());
                    selectConceptNode(cn);
                }
                , error: function() {
                    if (tagIx == 0) $('#concept-tags').prepend($tag);
                    else $('#concept-tags').children().eq(tagIx-1).after($tag);
                    ajaxErrorAlerter('error editing concept node tag').apply(null, [].slice.call(arguments));
                }
            });
        }

        e.stopPropagation();
        e.preventDefault();
    }

    function addNewPipelineToConceptNode(e) {
        var cn = d3.select(inFocus).data()[0];
        
        e.stopPropagation();
        e.preventDefault();

        showSingleInputModal('Enter name for new pipeline:', function(name) {
            if (name) {
                $.ajax({
                    url: '/kcm/insert-pipeline'
                    , type: 'POST'
                    , contentType: 'application/json'
                    , data: JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, pipelineName:name })
                    , success: function(d) {
                        var pl = d.pipeline
                          , cnRev = d.conceptNodeRev
                        ;
                        kcm.pipelines[pl._id] = pl;
                        cn.pipelines.push(pl._id);
                        cn._rev = cnRev;
                        selectConceptNode(cn);
                    }
                    , error: ajaxErrorAlerter('Error adding new pipeline')
                });
            }
        });
    }

    function deleteConceptNodePipeline(e) {
        var plId = $(this).closest('tr').attr('data-id')
          , pl = kcm.pipelines[plId]
          , cn = d3.select(inFocus).data()[0]
        ;
        
        e.stopPropagation();
        e.preventDefault();

        showConfirmCancelModal('You are about to delete a pipeline. Are you sure?', function(confirmation) {
            if (!confirmation) return;

            $.ajax({
                url: '/kcm/delete-pipeline'
                , type: 'POST'
                , contentType: 'application/json'
                , data: JSON.stringify({ pipelineId:plId, pipelineRev:pl._rev, conceptNodeId:cn._id, conceptNodeRev:cn._rev })
                , success: function(cnRev) {
                    cn._rev = cnRev;
                    cn.pipelines.splice(cn.pipelines.indexOf(plId),1);
                    delete kcm.pipelines[plId];
                    selectConceptNode(cn);
                }
                , error: ajaxErrorAlerter('error deleting pipeline')
            });
        });
    }
//})(jQuery);
