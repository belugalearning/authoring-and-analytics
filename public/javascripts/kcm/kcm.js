//(function($) {
    var nodeEnoughProblems = 5
      , windowPadding = 4, genPadding = 6
      , svg, gZoom, gWrapper, gLinks, gPrereqs, gNodes
      , inFocus = null
      , l = 400
      , nodeTextPadding = 5
      , rControlsWidth = 350
      , cnInteractionModifiers = ''
      , cnInteractionInProgress = false
      , mouse = { x:null, y:null, isOverMap:null, xmap:null, ymap:null, overNodes:[] }
      , mapPos = { x:windowPadding, y:windowPadding, width:null, height:null }
    ;

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
        });
    }

    $(function() {
        $('form#upload-pdefs').ajaxForm();

        $(window)
            .resize(layoutControls)
            .on('click', '#insert-tag-btn', addNewTagToConceptNode)
            .on('click', 'table[data-panel="concept-node-data"] .del-tag', deleteConceptNodeTag)
            .on('dblclick', 'table[data-panel="concept-node-data"] .cn-tag', editConceptNodeTag)
            .on('click', '#insert-pipeline-btn', addNewPipelineToConceptNode)
            .on('click', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] tr.pipeline > td.expand-collapse > div', expandCollapsePipeline)
            .on('click', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] tr.pipeline > td > div.del-btn', deleteConceptNodePipeline)
            .on('click', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] td.remove-problem > div.del-btn', removeProblemFromPipeline)
            .on('mousedown', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] tr.pipeline > td > div.gripper', dragReorderPipelines)
            .on('mousedown', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] tr.problem div.gripper', dragReorderPipelineProblems)
            .on('click', 'tr.add-problems div.add-btn', populateHiddenUploadProblemInputs)
            .on('change', 'input[type="file"][name="pdefs"]', uploadProblemsToPipeline)
            .on('mouseover mouseout', '[data-type="concept-node"]', updateMouseOverNodes)
            .on('mousedown', '[data-type="concept-node"]', beginMouseInteractionWithNode)
            .on('mousemove', updateMousePos)
            .on('keydown keyup', keyListener)
            .on('focusin', '#concept-description', onFocusConceptDescriptionTA)
        ;

        svg = d3.select("#wrapper")
            .append("svg")
                .attr("data-type", "svg")
                .attr("data-focusable", "true")
                .on("mousedown", clickGainFocus)
                .call(d3.behavior.zoom().on("zoom", zoom))
        ;
        gZoom = svg.append("g");
        gWrapper = gZoom
            .append("g")
            .attr("data-type", "wrapper")
            .attr("data-focusable", "true")
            .on("mousedown", clickGainFocus)
        ;
        gLinks = gWrapper.append("g");
        gPrereqs = gLinks.append("g");
        gNodes = gWrapper.append("g");

        $(window).resize();

        updateMapNodes();
        updateMapLinks();

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
        var w = $(window).width() - 2 * windowPadding
          , h = $(window).height() - 2 * windowPadding
        ;

        mapPos.width = w - rControlsWidth - genPadding
        mapPos.height = h

        $('#wrapper')
            .css('left', windowPadding)
            .css('top', windowPadding)
        ;

        $('#right-panel')
            .css('left', w - rControlsWidth)
            .css('width', rControlsWidth)
            .css('height', mapPos.height)
        ;

        svg.attr('style', 'width:'+mapPos.width+'px; height:'+mapPos.height+'px;');

        setMapBorder();
    }

    function setMapBorder() {
        var mapFocus = $(inFocus).attr('data-type') == 'svg';
        d3.select('svg').attr('class', mapFocus ? 'emphasis' : '');
    }

    function updateMapNodes() {
        // TODO: Find out what's wrong with (my understanding of) d3.js. This line should not be necessary. Without next line, nodes do not update after changes, incorrect nodes removed after deletions
        gNodes.selectAll('g.node').data([]).exit().remove();

        var nodes = gNodes.selectAll('g.node')
            .data(kcm.nodes)
        ;
        nodes
            .enter()
            .append('g')
                .attr('id', function(d) { return d._id; })
                .attr('data-type', 'concept-node')
                .attr('class', 'node')
                .attr('class', setNodeProblemClasses)
                .attr("transform", function(d) { return "translate("+ d.x +","+ d.y +")"; })
                .attr("data-focusable", "true")
                .on("mousedown", clickGainFocus)
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

        nodes.exit().remove();
    }

    function updateMapLinks() {
        var prereqs = $.grep(kcm.binaryRelations, function(rel) {
            return rel.name == 'Prerequisite';
        });

        if (prereqs.length) {
            var links = gPrereqs.selectAll('g.link')
                .data(prereqs[0].members)
            ;

            links
                .enter()
                .append('g')
                    .attr('id', function(d) { return d.id; })
                    .attr('class', 'link')
                    .attr("data-focusable", "true")
                    .attr("data-head-node", function(d) { return d[1]; })
                    .attr("data-tail-node", function(d) { return d[0]; })                
            ;

            links.exit().remove();

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
        }
        $('g.link').arrowRedraw();
    }

    function nodeWithId(id) {
        var len = kcm.nodes.length, i;
        for (i=0; i<len; i++) {
            if (kcm.nodes[i].id == id) return kcm.nodes[i];
        }
        return null;
    }

    function setNodeProblemClasses(cn) {
        var classes = d3.select(this).attr('class').replace(/\s*(no-problems|enough-problems)\b/g, '')
          , numProblems = 0
        ;

        $.each(cn.pipelines, function(i,plId) { numProblems += kcm.pipelines[plId].problems.length; });

        if (numProblems == 0) {
            return 'no-problems ' + classes;
        } else if (numProblems >= 5) {
            return 'enough-problems ' + classes;
        }
        return classes;
    }

    function clickGainFocus(event) {
        if (cnInteractionInProgress) return;

        var e = d3.event || event
          , focusTarget = $(e.target).closest('[data-focusable="true"]')[0]
          , oldType = $(inFocus).attr('data-type')
          , newType = $(this).attr('data-type')
        ;

        if (focusTarget && focusTarget != this || inFocus == this) return;

        if (newType == 'svg') e.stopPropagation();
        e.preventDefault();

        setFocus(focusTarget);
    }

    function setFocus(focusTarget) {
        var newType = $(focusTarget).attr('data-type')
          , oldType = $(inFocus).attr('data-type')
        ;

        svg.setPanEnabled(['svg','wrapper'].indexOf(newType) != -1);

        if (oldType) {
            $("*:focus").blur();
            d3.select(inFocus).attr('class', function() {
                return d3.select(inFocus).attr('class').replace(/\bin\-focus */, '');
            });
        }
        if (newType) {
            d3.select(focusTarget).attr('class', function() {
                return 'in-focus ' + d3.select(focusTarget).attr('class');
            });
        }

        displayConceptNodeProperties(newType == 'concept-node' ? d3.select(focusTarget).data()[0] : null);
        inFocus = focusTarget;
        setMapBorder();
    }

    function displayConceptNodeProperties(data) {
        $('[data-section="pipelines"]').html('');

        if (data) {
            $('table[data-panel="concept-node-data"]').removeClass('none-selected');
            $('#concept-db-id').text(data._id);
            $('#concept-graffle-id').text(data.graffleId || '');
            $('#concept-description').val(data.nodeDescription).prop('disabled', '');
            $('#concept-notes').val(data.notes).prop('disabled', '');
            $('#concept-tags').html($.tmpl('cnTagDIV', $.map(data.tags, function(tag) { return { tag:tag }; })));
            $('[data-section="pipelines"]').html($.tmpl('cnPipelineTR', $.map(data.pipelines, function(plId) { return kcm.pipelines[plId]; })));
 
        } else {
            $('table[data-panel="concept-node-data"]').addClass('none-selected');
            $('#concept-db-id').text('');
            $('#concept-graffle-id').text('');
            $('#concept-description').val('').prop('disabled', 'disabled');
            $('#concept-notes').val('').prop('disabled', 'disabled');
            $('#concept-tags').html('');
        }
    }

    function showModalDialog(dialogClasses, h1Text, appendToDialog, initialFocusEl, confirmDialogTest, callback) {
        var $bg = $('<div id="modal-bg"/>').appendTo('body')
          , $dialog = $('<div id="modal-dialog"><h1/><h2>(hit return/escape to confirm/cancel)</h2></div>').appendTo('body')
          , $body = $('body')
        ;
        $dialog
            .addClass(dialogClasses)
            .append(appendToDialog)
            .children('h1')
                .text(h1Text)
        ;
        if (initialFocusEl) {
            setTimeout(function() { $(initialFocusEl).focus(); }, 0);
        }
        $body.css('overflow', 'hidden');

        $(window).on('keydown', function(e) {
            switch (e.keyCode) {
                case 13:
                    e.preventDefault(); // if focus is in textarea, prevents new line from being inserted
                    if ('function' == typeof confirmDialogTest && !confirmDialogTest()) return;
                    callback(true);
                    break;
                case 27:
                    callback(false);
                    break;
                default:
                    return;
            }
            $bg.remove();
            $dialog.remove();
            $body.css('overflow', 'auto');
            $(window).off('keydown', arguments.callee);
        });
    }
    function showSingleInputModal(instruction, callback) {
        var $input = $('<input type="text"/>');
        showModalDialog('single-input-no-buttons'
                        , instruction
                        , $input
                        , $input
                        , function() { return $input.val().length; }
                        , function(ok) { callback(ok ? $input.val() : false); }
        );
    }
    function showConfirmCancelModal(warning, callback) {
        showModalDialog('warning-no-buttons'
                        , warning
                        , null
                        , null
                        , null
                        , callback
        );
    }

    function showNewLinkDetailsModal(cn1Data, cn2Data, callback) {
        var br = kcm.binaryRelations
          , $div = $.tmpl('newLinkConfigDIV', { cn1Data:cn1Data, cn2Data:cn2Data, relations:kcm.binaryRelations })
        ;
        $div
            .on('change', 'input[type="radio"][name="mode"]', function() {
                var newRel = $div.find('input[type="radio"][name="mode"]:checked').val() == 'new';
                if (newRel) {
                    $div.addClass('new-relation');
                    $div.find('input#new-binary-relation-name').focus();
                    setWarning(true, true);
                } else {
                    $div.removeClass('new-relation');
                    $div.find('select#binary-relations').focus();
                    setWarning();
                }
            })
            .on('change', 'select#binary-relations', function() {
                var br = binaryRelationWithId($(this).val());
                $div.find('#binary-relation-desc').text(br.relationDescription);
            })
            .on('click', 'div.reverse-btn > input[type="button"]', function() {
                var $l = $div.find('.new-link-node-l')
                  , $r = $div.find('.new-link-node-r')
                  , $toR = $l.children('div').remove()
                  , $toL = $r.children('div').remove()
                ;
                $toL.appendTo($l);
                $toR.appendTo($r);
                setWarning();
            })
            .on('focusout', 'input[type="text"]', function(e) {
                setWarning(false, 'new-binary-relation-name' == $(e.currentTarget).prop('id'));
            })
        ;
        showModalDialog('new-link-details'
            , 'New Link Details:'
            , $div
            , $div.find('select#binary-relations')
            , function() {
                setWarning();
                // TODO: see alert below
                if ($div.find('input[type="radio"][name="mode"]:checked').val() == 'new') {
                    alert('Creation of new relations disabled until map can display relations other than prerequisites. New relation functionalty tested and passed.');
                    return false;
                }
                return $div.find('.warning').text().length == 0;
            }
            , function(doAdd) { 
                if (!doAdd) return;

                var newRel = $div.find('input[name="mode"]:checked').val() == 'new'
                  , pair = [ $div.find('.new-link-node-l').children('div').attr('data-id'), $div.find('.new-link-node-r').children('div').attr('data-id') ]
                  , rel = !newRel && binaryRelationWithId($('select#binary-relations').val())
                  , relData
                ;

                if (newRel) {
                    relData = { type:'new', name:$div.find('input#new-binary-relation-name').val(), description:$div.find('input#new-binary-relation-desc').val() };
                } else {
                    relData = { type:'existing', id:rel._id, rev:rel._rev }
                }

                $.ajax({
                    url:'/kcm/add-pair-to-binary-relation'
                    , type:'POST'
                    , contentType:'application/json'
                    , data:JSON.stringify({ relation:relData, pair:pair })
                    , success:function(relation) {
                        if (newRel) {
                            kcm.binaryRelations.push(relation);
                        } else {
                            kcm.binaryRelations.splice(kcm.binaryRelations.indexOf(rel), 1, relation);
                        }
                        updateMapLinks();
                    }
                    , error: ajaxErrorHandler('Error adding new pair to binary relation')
                });
            }
        );

        setWarning();
        function setWarning(exemptNewRelName, exemptNewRelDesc) {
            var s = '';

            if ($div.find('input[name="mode"]:checked').val() == 'new') {
                if (!exemptNewRelName && !$div.find('input#new-binary-relation-name').val().length) s = 'New relation requires a name';
                if (!exemptNewRelDesc && !$div.find('input#new-binary-relation-desc').val().length) s += s.length ? ' and a description' : 'New relation requires a description'; 
            } else {
                var cnId0 = $div.find('.new-link-node-l').children('div').attr('data-id')
                  , cnId1 = $div.find('.new-link-node-r').children('div').attr('data-id')
                  , relId = $('select#binary-relations').val()
                  , rel = binaryRelationWithId(relId)
                  , matches = $.grep(rel.members, function(pair) { return pair[0] == cnId0 && pair[1] == cnId1; })
                ;
                if (matches.length) {
                    s = 'Ordered pair is already a member of the selected binary relation.';
                }
            }
            $div.find('.warning').text(s);
            s.length ? $div.addClass('show-warning') : $div.removeClass('show-warning');
        }
    }

    function ajaxErrorHandler(firstline) {
        return function(e) {
            if (401 == e.status) {
                self.location = '/login?redir=' + self.location.pathname + self.location.search
            } else {
                alert(firstline + '\n' + e.responseText + '\n' + [].slice.call(arguments, 1).join('\n'));
            }
        }
    }

    function updateMousePos(e) {
        var tf = d3.transform(gZoom.attr('transform'))
          , scale = tf.scale[0];

        mouse.x = e.pageX;
        mouse.y = e.pageY;

        mouse.isOverMap = mouse.x > mapPos.x && mouse.x < (mapPos.x + mapPos.width) && mouse.y > mapPos.y && mouse.y < (mapPos.y + mapPos.height);
        if (mouse.isOverMap) {
            mouse.xmap = (mouse.x - mapPos.x - tf.translate[0]) / scale;
            mouse.ymap = (mouse.y - mapPos.y - tf.translate[1]) / scale;
        } else {
            mouse.xmap = null;
            mouse.ymap = null;
        }
    }

    function updateMouseOverNodes(e) {
        var ix = mouse.overNodes.indexOf(this);
        switch (e.type) {
            case 'mouseover':
                if (!~ix) mouse.overNodes.push(this);
            break;
            case 'mouseout':
                if (~ix) mouse.overNodes.splice(ix,1);
            break;
        }
    }

    function keyListener(e) {
        switch(String.fromCharCode(e.keyCode).toLowerCase()) {
            case 'b':
                switch (e.type) {
                    case 'keydown':
                        if (!~cnInteractionModifiers.indexOf('(drawlink)')) cnInteractionModifiers += '(drawlink)';
                    break;
                    case 'keyup':
                        cnInteractionModifiers = cnInteractionModifiers.replace(/\(drawlink\)/g, '');
                    break;
                }
            break;
            case 'd':
                switch (e.type) {
                    case 'keydown':
                        if (!~cnInteractionModifiers.indexOf('(delete)')) cnInteractionModifiers += '(delete)';
                    break;
                    case 'keyup':
                        cnInteractionModifiers = cnInteractionModifiers.replace(/\(delete\)/g, '');
                    break;
                }
            break;
            case 'n':
                if (e.type=='keyup' && mouse.isOverMap && !mouse.overNodes.length && $(inFocus).attr('data-type') == 'svg') {
                    insertConceptNode(mouse.xmap, mouse.ymap);
                }
            break;
        }
    }

    function onFocusConceptDescriptionTA() {
        var $ta = $(this)
          , gnode = inFocus
          , cn = d3.select(gnode).data()[0]
          , savedDesc = cn.nodeDescription
        ;

        $ta
            .on('focusout', onDescFocusOut)
            .on('keyup', onDescKeyUp)
        ;

        function saveCancelChanges(forceCancel) {
            var text = $ta.val();

            $ta
                .off('focusout', onDescFocusOut)
                .off('keyup', onDescKeyUp)
            ;

            if (inFocus == gnode) {
                if (text == savedDesc) return;

                if (!text.length || forceCancel) {
                    $ta.val(savedDesc);
                    return;
                }

                showConfirmCancelModal('Save changes to concept node description?', function(saveChange) {
                    if (saveChange) {
                        $.ajax({
                            url:'/kcm/concept-nodes/' + cn._id + '/update-description'
                            , type:'POST'
                            , contentType:'application/json'
                            , data:JSON.stringify({ nodeDescription:text, rev:cn._rev })
                            , success:function(cnRev) {
                                console.log('desc updated:',cnRev);
                                cn._rev = cnRev;
                                cn.nodeDescription = text;
                                updateMapNodes();
                            }
                            , error:ajaxErrorHandler('Error updating concept node description')
                        });
                    } else {
                        $ta.val(savedDesc);
                    }
                });
            }
        }

        function onDescFocusOut() {
            saveCancelChanges(false);
        }

        function onDescKeyUp(e) {
            if (e.keyCode == 27) {
                saveCancelChanges(true);
                $('#concept-description').blur();
            }
        }
    }

    function insertConceptNode(x, y) {
        $.ajax({
            url:'/kcm/concept-nodes/insert'
            , type:'POST'
            , contentType:'application/json'
            , data:JSON.stringify({ nodeDescription:'[NEW CONCEPT NODE]', x:x, y:y })
            , success:function(cn) {
                kcm.nodes.push(cn);
                updateMapNodes();
                setFocus($('g#'+cn._id)[0]);
                $('#concept-description').select();
            }
            , error:ajaxErrorHandler('Error inserting new concept node')
        });
    }

    function addNewTagToConceptNode(e) {
        var cn = d3.select(inFocus).data()[0];
        
        e.stopPropagation();
        e.preventDefault();

        showSingleInputModal('Enter Tag:', function(tag) {
            if (tag) {
                $.ajax({
                    url:'/kcm/insert-concept-node-tag'
                    , type:'POST'
                    , contentType:'application/json'
                    , data: JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, tag:tag })
                    , success: function(cnRev) {
                        cn.tags.push(tag);
                        cn._rev = cnRev;
                        displayConceptNodeProperties(cn);
                    }
                    , error: ajaxErrorHandler('Error adding new tag to concept node')
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
                    displayConceptNodeProperties(cn);
                }
                , error: ajaxErrorHandler('error deleting concept node tag')
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
                    displayConceptNodeProperties(cn);
                }
                , error: function() {
                    if (tagIx == 0) $('#concept-tags').prepend($tag);
                    else $('#concept-tags').children().eq(tagIx-1).after($tag);
                    ajaxErrorHandler('error editing concept node tag').apply(null, [].slice.call(arguments));
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
                    url:'/kcm/insert-pipeline'
                    , type:'POST'
                    , contentType:'application/json'
                    , data:JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, pipelineName:name })
                    , success:function(d) {
                        var pl = d.pipeline
                          , cnRev = d.conceptNodeRev
                        ;
                        kcm.pipelines[pl._id] = pl;
                        cn.pipelines.push(pl._id);
                        cn._rev = cnRev;
                        displayConceptNodeProperties(cn);
                    }
                    , error:ajaxErrorHandler('Error adding new pipeline')
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
                , type:'POST'
                , contentType:'application/json'
                , data:JSON.stringify({ pipelineId:plId, pipelineRev:pl._rev, conceptNodeId:cn._id, conceptNodeRev:cn._rev })
                , success:function(cnRev) {
                    cn._rev = cnRev;
                    cn.pipelines.splice(cn.pipelines.indexOf(plId),1);
                    delete kcm.pipelines[plId];
                    displayConceptNodeProperties(cn);

                    var numProblems = 0;
                    $.each(cn.pipelines, function(i,plId) { numProblems += kcm.pipelines[plId].problems.length; });

                    if (!numProblems) {
                        d3.select($('g#'+cn._id)[0]).attr('class', function(d) {
                            return updateNodeClassStringForProblems(d, d3.select(this).attr('class'));
                        });
                    } else if (numProblems < nodeEnoughProblems) {
                    } else {
                    }

                    var plWithProbs = $.grep(cn.pipelines, function(plId) { return kcm.pipelines[plId].problems.length; })
                    if (!plWithProbs.length) {
                    }
                }
                , error:ajaxErrorHandler('error deleting pipeline')
            });
        });
    }

    function expandCollapsePipeline(e) {
        var $trPL = $(this).closest('tr.pipeline')
          , $trPLProbs = $trPL.next()
        ;

        if ($trPL.hasClass('expanded')) {
            $trPL.add($trPLProbs).removeClass('expanded');
        } else {
            $trPL.add($trPLProbs).addClass('expanded');

            if ($trPLProbs.hasClass('not-loaded')) {
                var pl = kcm.pipelines[$trPL.attr('data-id')];

                $.ajax({
                    url:'/kcm/pipeline-problem-details'
                    , type:'POST'
                    , contentType:'application/json'
                    , data:JSON.stringify({ id:pl._id, rev:pl._rev })
                    , success:function(problemDetails) {
                        $trPLProbs
                          .removeClass('not-loaded')
                          .find('tr.loading-message').replaceWith($.tmpl('plProblemTR', problemDetails));
                    }
                    , error: ajaxErrorHandler('Error retrieving pipeline problems')
                });
            }
        }
    }

    function populateHiddenUploadProblemInputs(e) {
        var plId = $(e.currentTarget).closest('tr.pipeline-problems').attr('data-id')
          , plRev = kcm.pipelines[plId]._rev
        ;
        $('input[name="pipeline-id"]').val(plId);
        $('input[name="pipeline-rev"]').val(plRev);
        $('input[type="file"][name="pdefs"]').click();
    }

    function uploadProblemsToPipeline(e) {
        $('form#upload-pdefs').ajaxSubmit({
            success: function(o) { 
                var $trPLProbs = $('tr.pipeline-problems.expanded[data-id="'+o.pipelineId+'"]')
                  , pl = kcm.pipelines[o.pipelineId]
                  , cn = $.grep(kcm.nodes, function(n) { return ~n.pipelines.indexOf(pl._id); })[0]
                ;

                pl._rev = o.pipelineRev;
                pl.problems = $.map(o.problemDetails, function(p) { return p.id; });

                if ($trPLProbs.length) {
                    $trPLProbs
                        .find('tr.problem')
                            .remove()
                            .end()
                        .find('tr.add-problems')
                            .before($.tmpl('plProblemTR', o.problemDetails));
                }

                d3.select($('g#'+cn._id)[0]).attr('class', setNodeProblemClasses);
            }
            , error: ajaxErrorHandler('Error uploading problems to pipeline.')

        });
        $('form#upload-pdefs').clearForm();
    }

    function removeProblemFromPipeline(e) {
        showConfirmCancelModal('You are about to remove a problem from a pipeline. Are you sure?', function(confirmation) {
            if (!confirmation) return;

            var $trProblem = $(e.currentTarget).closest('tr.problem')
              , problemId = $trProblem.attr('data-id')
              , pl = kcm.pipelines[$(e.currentTarget).closest('tr.pipeline-problems').attr('data-id')]
            ;

            $.ajax({
                url: '/kcm/remove-problem-from-pipeline'
                , type:'POST'
                , contentType:'application/json'
                , data:JSON.stringify({ pipelineId:pl._id, pipelineRev:pl._rev, problemId:problemId })
                , success:function(plRev) {
                    var probIx = pl.problems.indexOf(problemId);
                    pl.problems.splice(probIx, 1);
                    pl._rev = plRev;
                    $trProblem.remove();

                    if (!pl.problems.length) {
                        var cn = $.grep(kcm.nodes, function(n) { return ~n.pipelines.indexOf(pl._id); })[0]
                          , plWithProbs = $.grep(cn.pipelines, function(plId) { return kcm.pipelines[plId].problems.length; })
                        ;
                        if (!plWithProbs.length) {
                            d3.select($('g#'+cn._id)[0]).attr('class', setNodeProblemClasses);
                        }
                    }
                }
                , error:ajaxErrorHandler('error removing problem from pipeline')
            });
        });
    }

    function dragReorderPipelines(e) {
        var $nosel = $('body, input, textarea').not('.no-select').addClass('no-select')
          , $table = $(e.currentTarget).closest('table[data-section="pipelines"]')
          , $tbody = $table.children('tbody')
          , $tr = $(e.target).closest('tr.pipeline').add($(e.target).closest('tr.pipeline').next('tr.pipeline-problems'))
          , $proxy = $table.clone()
          , $proxyWrap = $('<div data-panel="concept-node-data" class="drag-proxy-wrap"/>')
          , $proxyBorder = $('<div class="drag-zone-marker"/>')
          , id = $tr.attr('data-id')
          , tableOffset = $table.offset()
          , trOffset = $tr.offset()
          , trHeight = $tr.outerHeight() + ($tr.hasClass('expanded') ? $($tr[1]).outerHeight() : 0)
          , $lastVisTR = $tbody.children(':last').hasClass('expanded') ? $tbody.children(':last') : $tbody.children('.pipeline:last')
          , yEndLastVisTR = $lastVisTR.offset().top + $lastVisTR.outerHeight()
          , yEndTR = $tr.offset().top + trHeight
          , ymin = tableOffset.top - (trOffset.top - tableOffset.top)
          , ymax = tableOffset.top + yEndLastVisTR - yEndTR
          , ymouseInit = e.pageY
          , startIndex = Math.floor($tr.index() / 2)
        ;
        $proxy
            .css('margin', 0)
            .width($table.outerWidth())
        ;
        $proxyWrap
            .css('left', tableOffset.left)
            .css('top', tableOffset.top)
            .append($proxy)
            .appendTo('body')
        ;
        $proxyBorder
            .width($table.width())
            .height($table.height())
            .css('left', tableOffset.left)
            .css('top', tableOffset.top)
            .appendTo('body')
        ;

        $table.find('tr[data-id='+id+']').css('visibility', 'hidden');
        $proxy.find('tr.pipeline[data-id!='+id+'], tr.pipeline-problems[data-id!='+id+']').css('visibility', 'hidden');

        $(window).on('mouseup mousemove', function(e) {
            var ix = Math.floor($tr.index() / 2);

            if (e.type == 'mousemove') {
                var y = Math.min(ymax, Math.max(ymin, tableOffset.top + e.pageY - ymouseInit))
                  , $proxyTR = $proxyWrap.find('tr.pipeline[data-id="'+id+'"]')
                  , yProxyTop = $proxyTR.offset().top
                  , yProxyBottom = $tr.hasClass('expanded') ? ($proxyTR.next().offset().top + $proxyTR.next().outerHeight()) : (yProxyTop + $proxyTR.outerHeight())
                  , $prevPl = $tbody.children('.pipeline:lt('+ix+'):last')
                  , $nextPl = $tbody.children('.pipeline:gt('+ix+'):first')
                  , yMidPrev = $prevPl.length && $prevPl.offset().top + 0.5 * ($prevPl.outerHeight() + ($prevPl.hasClass('expanded') ? $prevPl.next().outerHeight() : 0))
                  , yMidNext = $nextPl.length && $nextPl.offset().top + 0.5 * ($nextPl.outerHeight() + ($nextPl.hasClass('expanded') ? $nextPl.next().outerHeight() : 0))
                ;
                $proxyWrap.css('top', y);

                if ($nextPl.length && yProxyBottom > yMidNext) {
                    $tr.remove().insertAfter($nextPl.next());
                } else if ($prevPl.length && yProxyTop < yMidPrev) {
                    $tr.remove().insertBefore($prevPl);
                }
            } else {
                $nosel.removeClass('no-select');
                $proxyWrap.remove();
                $proxyBorder.remove();
                $table.find('tr[data-id='+id+']').css('visibility', '');
                $(window).off('mouseup mousemove', arguments.callee);
                
                if (ix != startIndex) {
                    var cnId = $('#concept-db-id').text()
                      , cn = d3.select($('g#'+cnId)[0]).data()[0]
                      , plId = $tr.attr('data-id')
                    ;
                    $.ajax({
                        url: '/kcm/concept-nodes/'+cnId+'/reorder-pipelines'
                        , type:'POST'
                        , contentType:'application/json'
                        , data:JSON.stringify({ conceptNodeRev:cn._rev, pipelineId:plId, oldIndex:startIndex, newIndex:ix })
                        , success:function(cnRev) {
                            cn._rev = cnRev;
                            cn.pipelines.splice(startIndex,1);
                            cn.pipelines.splice(ix,0,plId);
                        }
                        , error:ajaxErrorHandler('error reordering concept node pipelines')
                    });
                }
            }
        });
    }

    function dragReorderPipelineProblems(e) {
        var $nosel = $('body, input, textarea').not('.no-select').addClass('no-select')
          , $table = $(e.currentTarget).closest('table[data-type="pipeline-problems"]')
          , $tr = $(e.target).closest('tr.problem')
          , $proxy = $table.clone()
          , $proxyWrap = $('<div data-panel="concept-node-data" class="drag-proxy-wrap"/>')
          , $proxyBorder = $('<div class="drag-zone-marker"/>')
          , id = $tr.attr('data-id')
          , trOffset = $tr.offset()
          , tableOffset = $table.offset()
          , ymin = tableOffset.top - (trOffset.top - tableOffset.top)
          , ymax = tableOffset.top + $tr.parent().children('tr.problem:last').offset().top - trOffset.top
          , ymouseInit = e.pageY
          , index = $tr.index()
        ;
        $proxy
            .width($table.outerWidth())
        ;
        $proxyWrap
            .css('left', tableOffset.left)
            .css('top', tableOffset.top)
            .append($proxy)
            .appendTo('body')
        ;
        $proxyBorder
            .width($table.width())
            .height($table.height())
            .css('left', tableOffset.left)
            .css('top', tableOffset.top)
            .appendTo('body')
        ;

        $table.find('tr[data-id='+id+']').css('visibility', 'hidden');
        $proxy.find('tr[data-id!='+id+']').css('visibility', 'hidden');

        $(window).on('mouseup mousemove', function(e) {
            if (e.type == 'mousemove') {
                var y = Math.min(ymax, Math.max(ymin, tableOffset.top + e.pageY - ymouseInit));
                $proxyWrap.css('top', y);

                var prev = $tr.prev('.problem')
                  , next = $tr.next('.problem')
                  , yProxy = $proxyWrap.find('tr.problem[data-id="'+id+'"]').offset().top
                ;
                
                if (next.length && (yProxy + $tr.height()) > (next.offset().top + 0.5 * next.height())) {
                    $tr.remove().insertAfter(next);
                } else if (prev.length && yProxy < (prev.offset().top + 0.5 * prev.height())) {
                    $tr.remove().insertBefore(prev);
                }
            } else {
                $nosel.removeClass('no-select');
                $proxyWrap.remove();
                $proxyBorder.remove();
                $table.find('tr[data-id='+id+']').css('visibility', '');
                $(window).off('mouseup mousemove', arguments.callee);
                
                if ($tr.index() != index) {
                    var plId = $('tr.problem').closest('.pipeline-problems').attr('data-id')
                      , pl = kcm.pipelines[plId];
                    $.ajax({
                        url: '/kcm/reorder-pipeline-problems'
                        , type:'POST'
                        , contentType:'application/json'
                        , data:JSON.stringify({ pipelineId:pl._id, pipelineRev:pl._rev, problemId:$tr.attr('data-id'), oldIndex:index, newIndex:$tr.index() })
                        , success:function(plRev) {
                            pl._rev = plRev;
                        }
                        , error:ajaxErrorHandler('error reordering pipeline problems')
                    });
                }
            }
        });
    }

    function binaryRelationWithId(id) {
        return $.grep(kcm.binaryRelations, function(br) { return id == br._id; })[0];
    }

    function beginMouseInteractionWithNode(e) {
        var n = d3.select(e.currentTarget)
          , data = n.data()[0]
          , mouseStart = { x:e.pageX, y:e.pageY }
          , mapTransform
          , zoomMultiplier
          , x, y
          , startInteraction = arguments.callee
          , mousemoveNext, endInteractionNext
          , event = e
        ;
        var endInteractionEventType = ~cnInteractionModifiers.indexOf('(drawlink)') ? 'mousedown' : 'mouseup';
        var mousemove = function(e) {
            var dx, dy;

            mapTransform = d3.transform(gZoom.attr('transform'));
            event = e;

            zoomMultiplier = 1 / mapTransform.scale[0];
            dx = zoomMultiplier * (e.pageX - mouseStart.x);
            dy = zoomMultiplier * (e.pageY - mouseStart.y)
            x = parseInt(data.x + dx)
            y = parseInt(data.y + dy)

            if (typeof mousemoveNext == 'function') mousemoveNext();
        };
        var endInteraction = function(e) {
            event = e;

            $(window)
                .on('keydown keyup', keyListener)
                .on('mousedown', '[data-type="concept-node"]', startInteraction)
                .off('mousemove', mousemove)
                .off(endInteractionEventType, arguments.callee)
            ;
            if (typeof endInteractionNext == 'function') endInteractionNext();
            cnInteractionInProgress = false;
        };

        cnInteractionInProgress = true;
        $(window)
            .off('keydown keyup', keyListener)
            .off('mousedown', '[data-type="concept-node"]', startInteraction)
            .on('mousemove', mousemove)
            .on(endInteractionEventType, endInteraction)
        ;

        if (~cnInteractionModifiers.indexOf('(drawlink)')) {
            // LINK DRAWING
            var linkTarget
              , link
            ;
            function setLinkTarget(cn) {
                var classes = cn.attr('class');
                cn.attr('class', (classes + ' link-target').replace(/^\s+/, ''));
                linkTarget = cn;
            }
            function unsetLinkTarget() {
                if (!linkTarget) return;
                var classes = linkTarget.attr('class');
                linkTarget.attr('class', classes.replace(/\s*link-target/, ''));
                linkTarget = null;
            }
            function mouseoverCN (e) {
                var cn = d3.select(e.currentTarget)
                if (cn.node() == n.node()) return;
                if (linkTarget) unsetLinkTarget(linkTarget);
                setLinkTarget(cn);

                var ltData = linkTarget.data()[0];
                drawLinkTo(ltData.x, ltData.y);
            }
            function mouseoutCN (e) {
                var cn = d3.select(e.currentTarget);
                if (!linkTarget || cn.node() != linkTarget.node()) return;
                unsetLinkTarget(linkTarget);
            }
            function drawLinkTo(x,y) {
                var dx = x - data.x
                  , dy = y - data.y
                  , len = parseInt(Math.sqrt(dx*dx + dy*dy))
                  , theta = dx == 0
                            ? (dy<0 ? 1.5 : 0.5) * Math.PI
                            : Math.atan(dy/dx) + (dx<0 ? Math.PI : 0)
                ;
                link.attr('transform', 'translate('+data.x+','+data.y+')rotate('+(theta*180/Math.PI)+')');
                link.select('rect').attr('transform', 'scale('+len+',1)');
            }
            $(window)
                .on('mouseover', '[data-type="concept-node"]', mouseoverCN)
                .on('mouseout', '[data-type="concept-node"]', mouseoutCN)
            ;

            link = gLinks.append('g')
                .attr('class', 'link-create')
            ;
            link.append('rect')
                .attr('class', 'arrow-stem')
                .attr('x', 0)
                .attr('y', -2)
                .attr('width', 1)
                .attr('height', 4)
            ;

            mousemoveNext = function() {
                if (!linkTarget) {
                    // #wrapper has zero padding, thus its offset is that of its child: the svg map
                    var mx = (event.pageX - $('#wrapper').offset().left - mapTransform.translate[0]) / mapTransform.scale[0]
                      , my = (event.pageY - $('#wrapper').offset().top - mapTransform.translate[1]) / mapTransform.scale[0]
                    ;
                    drawLinkTo(mx,my);
                }
            };
            endInteractionNext = function() {
                $(window)
                    .off('mouseover', '[data-type="concept-node"]', mouseoverCN)
                    .off('mouseout', '[data-type="concept-node"]', mouseoutCN)
                ;

                if (linkTarget) {
                    showNewLinkDetailsModal(data, linkTarget.data()[0], function() {
                    });
                    unsetLinkTarget();
                }
                link.remove();
            };
        } else if (~cnInteractionModifiers.indexOf('(delete)')) {
            // DELETE NODE
            if (data.pipelines.length) {
                alert('Delete all pipelines from node before deleting node');
                return;
            }
            showSingleInputModal('Are you sure you want to delete the node? Type "yes" and press return to confirm.', function(response) {
                if (response == 'yes') {
                    $.ajax({
                        url:'/kcm/concept-nodes/' + data._id + '/delete'
                        , type:'POST'
                        , contentType:'application/json'
                        , data: JSON.stringify({ conceptNodeId:data._id, conceptNodeRev:data._rev })
                        , success: function(updatedBinaryRelations) {
                            setFocus(null);

                            $.each(updatedBinaryRelations, function(i,updated) {
                                var old = $.grep(kcm.binaryRelations, function(br) { return br._id == updated._id; })[0]
                                  , ix = kcm.binaryRelations.indexOf(old)
                                ;
                                kcm.binaryRelations.splice(ix, 1, updated);
                            });
                            
                            updateMapLinks();

                            ix = mouse.overNodes.indexOf($('g#'+data._id)[0]);
                            if (~ix) mouse.overNodes.splice(ix,1);

                            kcm.nodes.splice(kcm.nodes.indexOf(data), 1);
                            updateMapNodes();
                        }
                        , error: ajaxErrorHandler('Error deleting concept node.')
                    });
                }
            });
        } else {
            // NODE DRAGGING
            mousemoveNext = function() {
                n.attr("transform", "translate("+x+","+y+")");
                $('g.link[data-head-node='+data._id+'], g.link[data-tail-node='+data._id+']').arrowRedraw();
            };
            endInteractionNext = function() {
                var pos = d3.transform(n.attr('transform')).translate;
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
                    , error: ajaxErrorHandler('Error saving updated concept node position')
                });
            };
        }
        cnInteractionModifiers = '';
    }

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
        '<tr class="pipeline" data-id="{{html _id}}">\
            <td class="expand-collapse">\
                <div/>\
            </td>\
            <td class="txt">\
                <a href="/kcm/pipelines/{{html _id}}" target="_blank">{{html _id}}</a>\
                <br/>\
                <input type="text", value="{{html name}}"/>\
            </td>\
            <td class="controls">\
                <div class="gripper"/>\
                <div class="del-btn"/>\
            </td>\
        </tr>\
        <tr class="pipeline-problems not-loaded" data-id="{{html _id}}">\
            <td colspan="3">\
                <table data-type="pipeline-problems">\
                    <tbody>\
                        <tr class="loading-message">\
                            <td>[Loading Problems]</td>\
                        </tr>\
                        <tr class="add-problems">\
                            <td/>\
                            <td>\
                                <div class="add-btn"/>\
                            </td>\
                            <td class="txt">\
                                Upload problems to pipeline\
                            </td>\
                        </tr>\
                    </tbody>\
                </table>\
            </td>\
        </tr>'.replace(/(>|}})\s+/g, '$1'));

    $.template("plProblemTR",
        '<tr class="problem" data-id="{{html id}}" title="last modified: {{html lastModified}}">\
            <td class="drag-problem">\
                <div class="gripper"/>\
            </td>\
            <td class="remove-problem">\
                <div class="del-btn"/>\
            </td>\
            <td class="txt">\
                <div class="last-modified">Mod: {{html lastModified}}</div>\
                <a href="/kcm/problem/{{html id}}">{{html id}}</a>\
                <div class="problem-desc">{{html desc}}</div>\
            </td>\
        </tr>'.replace(/(>|}})\s+/g, '$1'));

    $.template("newLinkConfigDIV",
        '<div>\
            <div class="warning"/>\
            <input type="radio" name="mode" value="existing" checked="checked" /><span class="radio-label">Existing Relation</span>\
            <input type="radio" name="mode" value="new" /><span class="radio-label">New Relation</span><br/>\
            <div class = "relation-name">\
                <select id="binary-relations">\
                    {{each relations}}\
                        <option value="${$value._id}">${$value.name}</option>\
                    {{/each}}\
                </select>\
                <input type="text" id="new-binary-relation-name"/>\
            </div>\
            <div class="new-link-node new-link-node-l">\
                <h3>Concept Node with description:</h3>\
                <div data-id="{{html cn1Data._id}}">{{html cn1Data.nodeDescription}}</div>\
            </div>\
            <div class="new-link-node new-link-node-r">\
                <h3>Concept Node with description:</h3>\
                <div data-id="{{html cn2Data._id}}">{{html cn2Data.nodeDescription}}</div>\
            </div>\
            <div class="relation-desc">\
                <span id="binary-relation-desc">{{html relations[0].relationDescription}}</span>\
                <input type="text" id="new-binary-relation-desc"/>\
            </div>\
            <div class="reverse-btn"><input type="button" value="<- reverse relationship ->"/> </div>\
        </div>'.replace(/(>|}})\s+/g, '$1'));
//})(jQuery);
