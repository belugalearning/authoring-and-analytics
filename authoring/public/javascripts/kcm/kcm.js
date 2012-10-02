//(function($) {
  var nodeEnoughProblems = 5
    , windowPadding = 4, genPadding = 6
    , svg, gZoom, gWrapper, gLinks, gNodes
    , relIdContainerDict = {}
    , inFocus = null
    , l = 400
    , nodeTextPadding = 5
    , rControlsWidth = 350
    , mouseInteractionModifiers = ''
    , cnInteractionInProgress = false
    , mouse = { x:null, y:null, isOverMap:null, xmap:null, ymap:null, overNodes:[], overLink:null }
    , mapPos = { x:windowPadding, y:windowPadding, width:null, height:null }

      $.expr[':'].texteq = function(a, i, m) {
        return $(a).text() === m[3]
      }

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

      if (Math.abs(grad) < Math.abs(hh/hw)) {
        // arrow-head against left or right side of head rect
        var adj = 0.5 * hw * (hx < tx ? 1 : -1)
        ahr = adj / Math.cos(theta)
      } else {
        // arrow-head against top or bottom of head rect
        var opp = 0.5 * hh * (hy < ty ? 1 : -1)
        ahr = opp / Math.sin(theta)
      }

      gArrow
        .attr("transform", "translate("+hx+","+hy+")rotate("+(theta*180/Math.PI)+")")
        .select(".arrow-stem")
        .attr("transform", "scale("+len+",1)")
      
      gArrow
        .select(".arrow-stem-bg")
        .attr("transform", "scale("+len+",1)")
      
      gArrow
        .select(".arrow-head")
        .attr("transform", "translate("+ahr+",0)")
      
    })
  }

  $(function() {
    var ws = new WebSocket('ws://' + window.document.location.host)
    ws.onopen = function(event) {
      ws.send(JSON.stringify({ event:'subscribe-kcm-changes', update_seq:kcm.update_seq }))
    }
    ws.onmessage = function(message) {
      var data = JSON.parse(message.data)
      console.log('message data:', data) // ----------- !!!!!!!!!!!!!!!!!!!!!!!!
      
      if (data.seq <= kcm.update_seq) return
      kcm.update_seq = data.seq

      switch(data.doc.type) {
        case 'concept node':
          if (data.deleted) {
            delete kcm.nodes[data.id]
            for (var i=0; i<mouse.overNodes; i++) {
              if ($(mouse.overNodes[i]).attr('id') == data.id) {
                mouse.overNodes.splice(i,1)
                break
              }
            }
            updateMapNodes()
            updateMapLinks()
            if ($(inFocus).attr('id') == data.id) setFocus(null)
          } else if (kcm.nodes[data.id]) {
            kcm.nodes[data.id] = data.doc
            updateMapNodes()
            updateMapLinks()
          } else {
            kcm.nodes[data.id] = data.doc
            updateMapNodes()
            if (data.doc.currentVersion.user == kcm.user) {
              setFocus($('g#'+data.id)[0])
              $('#concept-description').select()
            }
          }
          break
        case 'pipeline':
          if (data.deleted) {
            delete kcm.pipelines[data.id]
            updateMapNodes()
          } else {
            if (typeof data.doc.workflowStatus === 'undefined') data.doc.workflowStatus = 0 
            kcm.pipelines[data.id] = data.doc
            if (inFocus && $(inFocus).attr('data-type') == 'concept-node') {
              var n = d3.select(inFocus).data()[0]
              if (~n.pipelines.indexOf(data.id)) {
                updateMapNodes()
              }
            }
          }
          break
        case 'relation':
          if (data.doc.relationType == 'binary') {
            if (data.deleted) delete kcm.binaryRelations[data.id]
            else kcm.binaryRelations[data.id] = data.doc

            var cbrsToUpdate = []
            $.each(kcm.chainedBinaryRelations, function(id, cbr) {
              var relations = cbr.chain.map(function(link) { return link.relation })
              if (~relations.indexOf(data.id)) cbrsToUpdate.push(id)
            })
            updateChainedBinaryRelationsMembers(cbrsToUpdate, function(updates) {
              console.log('updates:', updates)
              for (var cbrId in updates) {
                kcm.chainedBinaryRelations[cbrId].members = updates[cbrId]
              }
              updateMapNodes()
              updateMapLinks()
            })
          } 
          break
      }
    }

      $('form#upload-pdefs').ajaxForm();

      // add texteq selector to jquery
      $('#file-url')
        .on('dragover mousedown', function(e) {
          $('#file-holder').addClass('highlight-folder-upload')
          $('#file-url').val('')
        })
        .on('mouseup dragleave dragend drop', function(e) {
          $('#file-holder').removeClass('highlight-folder-upload')
        })
        .on('change', function(e) {
          var metafile = '#meta-pipeline.plist'
            , files = []
            , numFilesToLoad = 0
            , numFilesLoaded = 0
            , fr
            , cn, plName, pl, problems

          var onanyload = function() {
            var pdefs = []

            if (++numFilesLoaded == numFilesToLoad) {
              if (!cn) {
                alert(metafile + ' required but missing')
                return
              }
              
              for (var i=0, p; p=problems[i]; i++) {
                for (j=0, f; f=files[j]; j++) {
                  if (f.name == p) {
                    pdefs[i] = f.data
                    break
                  }
                }
                if (j == files.length) {
                  alert('missing plist: ' + p)
                  return
                }
              }

              $.ajax({
                url: '/kcm/pipelines/upload-pipeline-folder'
                , type: 'POST'
                , contentType: 'application/json'
                , data: JSON.stringify({
                  cnId: cn._id
                  , cnRev: cn._rev
                  , plId: pl && pl._id
                  , plRev: pl && pl._rev
                  , plName: plName
                  , pdefs: pdefs
                })
                , success: function() {
                  alert('successfully ' + (pl ? 'updated' : 'added new') + ' pipeline')
                }
                , error: ajaxErrorHandler('Error uploading pipeline')
              })
            }
          }

          for (var i=0, f; f = e.target.files[i]; i++) {
            if (/.+\.plist$/.test(f.name)) {
              numFilesToLoad++

              fr = new FileReader()

              if (f.name == metafile) {
                fr.readAsText(f)
                fr.onload = function(e) {
                  var $meta
                    , $problems
                    , cnId

                  try {
                    $meta = $( $.parseXML(e.target.result) )
                      .children('plist:first')
                        .children('dict:first')
                    
                    $problems = $meta.children('key:texteq("PROBLEMS")').next()
                    if (!$problems.length || $problems[0].tagName !== 'array') {
                      alert('required but missing: array for key "PROBLEMS"')
                      return
                    }

                    problems = $.map($problems.children('string'), function(s) {
                      return $(s).text().replace(/^.*\/([^/]+\.plist)$/, '$1')
                    })

                    cnId = $meta.children('key:texteq("NODE_ID")').next().text()
                    if (!cnId) {
                      alert('required but missing: string for key "NODE_ID"')
                      return
                    }

                    if (!kcm.nodes[cnId]) {
                      alert('cannot find node matching string for key NODE_ID: ' + cnId)
                      return
                    }

                    cn = kcm.nodes[cnId]

                    plName = $meta.children('key:texteq("PIPELINE_NAME")').next().text()
                    if (!plName) {
                      alert('required but missing: string for key "PIPELINE_NAME"')
                      return
                    }

                    for (var i=0, plId;  plId=cn.pipelines[i];  i++) {
                      if (kcm.pipelines[plId].name == plName) {
                        pl = kcm.pipelines[plId]
                        break
                      }
                    }
                    onanyload()
                  } catch(e) {
                    alert('invalid file: ' + metafile)
                  }
                }
              } else {
                fr.readAsDataURL(f)
                fr.onload = (function(name) {
                  return function(e) {
                    var base64 = e.target.result.slice(13) // "data:;base64,".length == 13
                    files.push( { name:name, data:base64 } )
                    onanyload()
                  }
                })(f.name)
              }
            }
          }
        })

      $(window)
          .resize(layoutControls)
          .on('change', 'table[data-panel="view-settings"] #links-view-settings input[type="checkbox"]', onLinkViewSettingsEdit)
          .on('keydown keyup focusout', 'table[data-panel="view-settings"] #links-view-settings input[type="text"]', onLinkViewSettingsEdit)
          .on('change', 'table[data-panel="export-settings"] #export-all-nodes', changeExportAllNodes)
          .on('change', 'table[data-panel="export-settings"] tr[data-key="pipelineWorkflowStatus"] select', changePipelineWorkflowStatusSetting)
          .on('click', 'table[data-panel="export-settings"] .panel-right-btn', addExportSettingTag)
          .on('click', 'table[data-panel="export-settings"] .del-tag', deleteExportSettingTag)
          .on('dblclick', 'table[data-panel="export-settings"] .cn-tag', editExportSettingTag)
          .on('click', '#insert-tag-btn', addNewTagToConceptNode)
          .on('click', 'table[data-panel="concept-node-data"] .del-tag', deleteConceptNodeTag)
          .on('dblclick', 'table[data-panel="concept-node-data"] .cn-tag', editConceptNodeTag)
          .on('click', '#insert-pipeline-btn', addNewPipelineToConceptNode)
          .on('click', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] tr.pipeline > td.expand-collapse > div', expandCollapsePipeline)
          .on('click', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] tr.pipeline > td > div.del-btn', deleteConceptNodePipeline)
          .on('click', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] td.remove-problem > div.del-btn', removeProblemFromPipeline)
          .on('mousedown', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] tr.pipeline > td > div.gripper', dragReorderPipelines)
          .on('mousedown', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] tr.problem div.gripper', dragReorderPipelineProblems)
          .on('change', 'table[data-panel="concept-node-data"] table[data-section="pipelines"] tr.pipeline > td > select.workflow-status', updatePipelineWorkflowStatus)
          .on('click', 'tr.add-problems div.add-btn', populateHiddenUploadProblemInputs)
          .on('change', 'input[type="file"][name="pdefs"]', uploadProblemsToPipeline)
          .on('mouseover mouseout', '[data-type="concept-node"]', updateMouseOverNodes)
          .on('mousedown', '[data-type="concept-node"]', beginMouseInteractionWithNode)
          .on('mouseover mouseout', '[data-relation-type="binary"] [data-type="binary-relation-pair"]', updateMouseOverLink)
          .on('mousedown', '[data-relation-type="binary"] [data-type="binary-relation-pair"]', mouseDownLink)
          .on('mousemove', updateMousePos)
          .on('keydown keyup', keyCommandListener)
          .on('focusin', '#concept-description', onFocusConceptDescriptionTA)
          .on('focusin', 'tr.pipeline input[type="text"]', onFocusPipelineNameInput)
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
      gNodes = gWrapper.append("g");

      $(window).resize();

      updateMapNodes();
      updateMapLinks();

      updateExportSettingsDisplay();
      updateViewSettingsDisplay();

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
    gZoom.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")")
  }

  function layoutControls() {
    var w = $(window).width() - 2 * windowPadding
      , h = $(window).height() - 2 * windowPadding

    mapPos.width = w - rControlsWidth - genPadding
    mapPos.height = h

    $('#wrapper')
      .css('left', windowPadding)
      .css('top', windowPadding)

    $('#right-panel')
      .css('left', w - rControlsWidth)
      .css('width', rControlsWidth)
      .css('height', mapPos.height)

    svg.attr('style', 'width:'+mapPos.width+'px; height:'+mapPos.height+'px;')

    setMapBorder()
  }

  function setMapBorder() {
    var mapFocus = $(inFocus).attr('data-type') == 'svg'
    d3.select('svg').attr('class', mapFocus ? 'emphasis' : '')
  }

  function updateMapNodes() {
    // TODO: UPDATE - code below is now dependent on all nodes being removed / recreated
    // TODO: Find out what's wrong with (my understanding of) d3.js. This line should not be necessary. Without next line, nodes do not update after changes, incorrect nodes removed after deletions

    var selectedNodeId
    if ($(inFocus).attr('data-type') == 'concept-node') {
      selectedNodeId = $(inFocus).attr('id')
    }

    gNodes.selectAll('g.node').remove()

    var gN = gNodes
      .selectAll('g.node')
        .data($.map(kcm.nodes, function(n) { return n }))

    gN
      .enter()
      .append('g')
        .attr('id', function(d) { return d._id })
        .attr('data-type', 'concept-node')
        .attr('class', 'node')
        .attr('class', setNodeColour)
        .attr("transform", function(d) { return "translate("+ d.x +","+ d.y +")" })
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
                .attr('y', function(d, i) { return 12 + 14 * i }) // text height 12px, 2px padding between lines
                .text(String)
          })
          .attr('transform', function() {
            var bbox = d3.select(this).node().getBBox()
            return 'translate('+(-bbox.width/2)+','+(-bbox.height/2)+')'
          })
        })
        .each(function(d,i) {
          var bbox = d3.select(this).select('text.node-description').node().getBBox()
          d3.select(this).select('rect.node-bg')
            .attr('width', bbox.width + 2 * nodeTextPadding)
            .attr('height', bbox.height + 2 * nodeTextPadding)
            .attr('transform', 'translate('+(-(nodeTextPadding+bbox.width/2))+','+(-(nodeTextPadding+bbox.height/2))+')')
        })


    if (selectedNodeId) {
      setFocus($('g#'+selectedNodeId)[0])
    }
  }

  function updateMapLinks() {
    $('[data-type="binary-relation-pair"]').remove()

    $.each($.extend({}, kcm.binaryRelations, kcm.chainedBinaryRelations), function(key, br) {
      if (!kcm.viewSettings.links[br._id]) {
        kcm.viewSettings.links[br._id] = { visible:true, colour:'000' }
      }

      var vs = kcm.viewSettings.links[br._id]
        , g = relIdContainerDict[br._id]
        , validMembers = $.grep(br.members, function(p) { return kcm.nodes[p[0]] && kcm.nodes[p[1]] })
        , links

      vs.visible = vs.visible !== false
      vs.color = /^([0-9a-f]{3,3}){1,2}$/i.test(vs.colour) && vs.colour || '000'

      if (!g) {
        g = relIdContainerDict[br._id] = gLinks.append("g")
          .attr('data-type', 'relation')
          .attr('data-relation-type', function() { return br.relationType })
          .attr('data-id', br._id)
      }
      $(g[0]).css('visibility', vs.visible ? 'visible' : 'hidden')

      links = g.selectAll('g.link').data(validMembers)

      links
        .enter()
        .append('g')
          .attr('class', 'link')
          .attr("data-focusable", "true")
          .attr('data-type', function() { return 'binary-relation-pair' })
          .attr("data-head-node", function(d) { return d[1] })
          .attr("data-tail-node", function(d) { return d[0] })                
          .on("mousedown", clickGainFocus)

      g.selectAll('g.link').selectAll('rect.arrow-stem-bg')
        .data(function(d) { return [d] })
        .enter()
          .append("rect")
          .attr("class", "arrow-stem-bg")
          .attr("x", 0)
          .attr("y", -5)
          .attr("width", 1)
          .attr("height", 10)
          
      g.selectAll('g.link').selectAll('rect.arrow-stem')
        .data(function(d) { return [d] })
        .enter()
          .append("rect")
          .attr("class", "arrow-stem")
          .attr("x", 0)
          .attr("y", -1.5)
          .attr("width", 1)
          .attr("height", 3)
      
      g.selectAll('g.link').selectAll('path.arrow-head')
        .data(function(d) { return [d] })
        .enter()
            .append('path') 
            .attr('d', 'M 0 -0.5 l 0 1 l 16 7.5 l 0 -16 z')
            .attr("class", "arrow-head")
      

      g.selectAll('rect.arrow-stem').attr("style", function() { return "fill:#"+ vs.colour +";" })
      g.selectAll('path.arrow-head').attr('style', function() { return 'fill:#'+ vs.colour +'; stroke-width:0;' })

    })

    $('g.link').arrowRedraw()

    if (inFocus && $(inFocus).attr('data-type') == 'binary-relation-pair') {
      setFocus($('g#'+d3.select(inFocus).attr('id'))[0])
    }
  }

    function setNodeColour(cn) {
        var classes = d3.select(this).attr('class').replace(/\s*(no-problems|enough-problems|mastery)\b/g, '')
          , numProblems = 0
        ;
        
        if (~cn.tags.indexOf('mastery')) {
            return 'mastery ' + classes;
        }

        $.each(cn.pipelines, function(i,plId) { if (kcm.pipelines[plId]) numProblems += kcm.pipelines[plId].problems.length; });

        if (numProblems == 0) {
            return 'no-problems ' + classes;
        } else if (numProblems >= 15) {
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
            $('#concept-description').val(data.nodeDescription).prop('disabled', '');
            $('#concept-tags').html($.tmpl('cnTagDIV', $.map(data.tags, function(tag) { return { tag:tag }; })));
            $('[data-section="pipelines"]').html($.tmpl('cnPipelineTR', $.map(data.pipelines, function(plId) { return kcm.pipelines[plId]; }), { nodeId:data._id}));
 
        } else {
            $('table[data-panel="concept-node-data"]').addClass('none-selected');
            $('#concept-db-id').text('');
            $('#concept-description').val('').prop('disabled', 'disabled');
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
                      , callback)
    }

    function showNewLinkDetailsModal(cn1Data, cn2Data, callback) {
        var $div = $.tmpl('newLinkConfigDIV', {
          cn1Data:cn1Data
          , cn2Data:cn2Data
          , relations:$.map(kcm.binaryRelations, function(br) { return br })
        })
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
                var br = kcm.binaryRelations[$(this).val()];
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
                  , rel = !newRel && kcm.binaryRelations[$('select#binary-relations').val()]
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
                  , rel = kcm.binaryRelations[$('select#binary-relations').val()]
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

    function ajaxErrorHandler(firstline, callback) {
        return function(e) {
            if (401 == e.status) {
                self.location = '/login?redir=' + self.location.pathname + self.location.search
            } else {
                alert(firstline + '\n' + e.responseText + '\n' + [].slice.call(arguments, 1).join('\n'));
                if (typeof callback == 'function') callback()
            }
        }
    }

    function updateChainedBinaryRelationsMembers(ids, callback) {
      $.ajax({
        url:'/kcm/chained-binary-relations/members'
        , type:'POST'
        , contentType:'application/json'
        , data: JSON.stringify({ ids:ids })
        , error: ajaxErrorHandler('Error retrieving chained binary relations members')
        , success: callback
      })
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

    function updateMouseOverLink(e) {
        if (mouse.overLink) {
            d3.select(mouse.overLink).attr('class', function(d,i) { return $(this).attr('class').replace(/\bhighlight\s*/g, ''); });
            mouse.overLink = null;
        }

        if (e) {
            switch(e.type) {
                case 'mouseover':
                    mouse.overLink = this;
                    d3.select(this).attr('class', function(d,i) { return 'highlight ' + $(this).attr('class'); });
                break;
            }
        }
    }
    
    function mouseDownLink(e) {
        if (/\(delete\)/.test(mouseInteractionModifiers)) {
            var $lk = $(e.target).closest('[data-type="binary-relation-pair"]')
              , tId = $lk.attr('data-tail-node')
              , hId = $lk.attr('data-head-node')
              , br = kcm.binaryRelations[$lk.closest('g[data-relation-type="binary"]').attr('data-id')]
            ;  

            $(window)
                .off('mouseover mouseout', '[data-relation-type="binary"] [data-type="binary-relation-pair"]', updateMouseOverLink)
                .off('keydown keyup', keyCommandListener)
            ;

            showConfirmCancelModal('You are about to delete a "' + br.name + '" link. Are you sure?', function(doDelete) {
                mouseInteractionModifiers = '';
                updateMouseOverLink();
                $('#wrapper').removeClass('delete-mode');

                $(window)
                    .on('mouseover mouseout', '[data-relation-type="binary"] [data-type="binary-relation-pair"]', updateMouseOverLink)
                    .on('keydown keyup', keyCommandListener)
                ;

                if (true === doDelete) {
                    var pair = $.grep(br.members, function(p) {
                        return tId == p[0] && hId == p[1];
                    })[0];

                    $.ajax({
                        url:'/kcm/binary-relations/' + br._id + '/remove-pair'
                        , type:'POST'
                        , contentType:'application/json'
                        , data: JSON.stringify({ pair:pair, rev:br._rev })
                        , error: ajaxErrorHandler('Error deleting pair from binary relation.')
                    });
                }
            });
        }
    }

  function keyCommandListener(e) {
    var focus = $(':focus')
    if (e.type == 'keydown' && focus.length && ~['input','textarea'].indexOf(focus[0].tagName.toLowerCase())) return

    switch(e.keyCode) {
      case 66: // 'b'
        switch (e.type) {
          case 'keydown':
            if (!~mouseInteractionModifiers.indexOf('(drawlink)')) mouseInteractionModifiers += '(drawlink)'
            break
          case 'keyup':
            mouseInteractionModifiers = mouseInteractionModifiers.replace(/\(drawlink\)/g, '')
            break
        }
        break
      case 68: // 'd'
        switch (e.type) {
          case 'keydown':
            $('#wrapper').addClass('delete-mode')
            if (!~mouseInteractionModifiers.indexOf('(delete)')) mouseInteractionModifiers += '(delete)'
            break
          case 'keyup':
            $('#wrapper').removeClass('delete-mode')
            mouseInteractionModifiers = mouseInteractionModifiers.replace(/\(delete\)/g, '')
            break
        }
        break
      case 77: // 'm'
        if (e.type=='keyup' && mouse.isOverMap && !mouse.overNodes.length && $(inFocus).attr('data-type') == 'svg') {
          insertConceptNode(mouse.xmap, mouse.ymap, 'NEW MASTERY NODE', ['mastery'])
        }
        break
      case 78: // 'n'
        if (e.type=='keyup' && mouse.isOverMap && !mouse.overNodes.length && $(inFocus).attr('data-type') == 'svg') {
          insertConceptNode(mouse.xmap, mouse.ymap, 'NEW CONCEPT NODE')
        }
        break
      case 70:
        if (e.ctrlKey && e.type == 'keydown' && !$('body > #modal-bg').length) displayMapSearch()
        break // ctrl + 'f'
      case 191:
        if (!e.shiftKey) { // '/'
          if (e.type == 'keyup' && !$('body > #modal-bg').length) displayMapSearch()
        } else { // '?'
        }
        break
    }
  }

    function onFocusPipelineNameInput() {
      var $input = $(this)
        , pl = kcm.pipelines[$(this).closest('tr').attr('data-id')]

      var keyUp = function(e) {
        if (e.keyCode == 13) {
          endEdit()
          $input.blur()
        } else if (e.keyCode == 27) {
          endEdit(true)
          $input.blur()
        }
      }

      var undoEdit = function() { $input.val(pl.name) }

      var endEdit = function(forceUndoEdit) {
        var name = $input.val().trim()

        $input
          .off('keyup', keyUp)
          .off('focusout', endEdit)

        if (name == pl.name || !name.length || true === forceUndoEdit) {
          undoEdit()
          return
        }
        showConfirmCancelModal('Save changes to pipeline name?', function(saveChange) {
          if (saveChange) {

            $.ajax({
              url: '/kcm/pipeline/' + pl._id + '/' + pl._rev + '/update-name/' + name
              , type: 'PUT'
              , success: function(rev) {
                pl.name = name
                pl._rev = rev
              }
              , error: ajaxErrorHandler('Error updating pipeline name', function() { $input.val(pl.name) })
            });
          } else {
            undoEdit()
          }
        })
      }
        
      $input
        .on('keyup', keyUp)
        .on('focusout', endEdit)
    }

    function onFocusConceptDescriptionTA() {
        var $ta = $(this)
          , gnode = inFocus
          , cn = d3.select(gnode).data()[0]
          , savedDesc = cn.nodeDescription
        ;

        $ta
            .on('keyup', onDescKeyUp)
            .on('focusout', saveUndoEdit)
        ;

        function onDescKeyUp(e) {
            if (e.keyCode == 27) {
                saveUndoEdit(true);
                $('#concept-description').blur();
            }
        }

        function undoEdit() { if (gnode == inFocus) $ta.val(savedDesc); }

        function saveUndoEdit(forceUndoEdit) {
            var text = $ta.val();

            $ta
                .off('focusout', saveUndoEdit)
                .off('keyup', onDescKeyUp)
            ;

            if (gnode == inFocus) {
                if (text == savedDesc) return;

                if (!text.length || true === forceUndoEdit) {
                    if (gnode == inFocus) undoEdit();
                    return;
                }

                showConfirmCancelModal('Save changes to concept node description?', function(saveChange) {
                    if (saveChange) {
                        $.ajax({
                            url:'/kcm/concept-nodes/' + cn._id + '/update-description'
                            , type:'POST'
                            , contentType:'application/json'
                            , data:JSON.stringify({ nodeDescription:text, rev:cn._rev })
                            , error:ajaxErrorHandler('Error updating concept node description')
                        });
                    } else {
                        undoEdit();
                    }
                });
            }
        }
    }

    function insertConceptNode(x, y, nodeDescription, tags) {
      if (typeof nodeDescription != 'string' ) nodeDescription = 'NEW CONCEPT NODE'
      if (Object.prototype.toString.call(tags) != '[object Array]') tags = []
      $.ajax({
        url:'/kcm/concept-nodes/insert'
        , type:'POST'
        , contentType:'application/json'
        , data:JSON.stringify({ nodeDescription:'[NEW CONCEPT NODE]', x:x, y:y, nodeDescription:nodeDescription, tags:tags })
        , error:ajaxErrorHandler('Error inserting new concept node')
      });
    }

    function onLinkViewSettingsEdit(e) {
        var id = $(this).closest('tr').attr('data-id')
          , lVS = kcm.viewSettings.links[id]
        ;
        if (!lVS) {
            kcm.viewSettings.links[id] = { visible:true, colour:'000' };
        }
        switch ($(this).attr('type')) {
            case 'checkbox':
                lVS.visible = $(this).prop('checked');
            break;
            case 'text':
                var txt = $(this).val().replace(/[^0-9a-f]/ig, '');
                if (txt != $(this).val()) $(this).val(txt);

                var isValid = /^([0-9a-f]{3,3}){1,2}$/i.test(txt);
                if (e.type != 'focusout') {
                    $(this).css('color', isValid ? '#'+txt : '#000');
                    return;
                }
                if (!isValid) {
                    $(this).val(lVS.colour).css('#'+lVS.colour);
                    return;
                }
                $(this).css('color', txt);
                if (txt == lVS.colour) return;
                lVS.colour = txt;
            break;
        }
        updateMapLinks();
        saveViewSettings();
    }

  function updateViewSettingsDisplay() {
    var allRelations = $.map($.extend({}, kcm.binaryRelations, kcm.chainedBinaryRelations), function(r) { return r })
    var $trs = $.tmpl('brViewSettingsTR', $.map(allRelations, function(r) { return $.extend({}, r, kcm.viewSettings.links[r._id]) }))
    $('table#links-view-settings')
      .children('tbody')
        .remove()
        .end()
      .append($('<tbody/>').html($trs))
  }

    function saveViewSettings() {
        var fn = arguments.callee;
        if (!fn.cache) fn.cache = { saveInProgress:false, queuedSave:false };

        if (fn.cache.saveInProgress) {
            fn.cache.queuedSave = true;
            return;
        }
        fn.cache.saveInProgress = true;
        
        $.ajax({
            url:'/kcm/update-view-settings'
            , type:'POST'
            , contentType:'application/json'
            , data: JSON.stringify({ viewSettings: kcm.viewSettings })
            , success: function(rev) {
                kcm.viewSettings._rev = rev;
                fn.cache.saveInProgress = false;
                if (fn.cache.queuedSave) {
                    fn.cache.queuedSave = false;
                    fn();
                }
            }
            , error: function() {
                fn.cache.saveInProgress = false;
                if (fn.cache.queuedSave) {
                    fn.cache.queuedSave = false;
                    fn();
                }
                ajaxErrorHandler('Error saving updated view settings').apply(null, [].slice.call(arguments));
            }
        });
    }

    function saveExportSettings() {
        var fn = arguments.callee;
        if (!fn.cache) fn.cache = { saveInProgress:false, queuedSave:false };

        if (fn.cache.saveInProgress) {
            fn.cache.queuedSave = true;
            return;
        }
        fn.cache.saveInProgress = true;
        
        $.ajax({
            url:'/kcm/update-export-settings'
            , type:'POST'
            , contentType:'application/json'
            , data: JSON.stringify({ exportSettings: kcm.exportSettings })
            , success: function(rev) {
                kcm.exportSettings._rev = rev;
                fn.cache.saveInProgress = false;
                if (fn.cache.queuedSave) {
                    fn.cache.queuedSave = false;
                    fn();
                }
            }
            , error: function() {
                fn.cache.saveInProgress = false;
                if (fn.cache.queuedSave) {
                    fn.cache.queuedSave = false;
                    fn();
                }
                ajaxErrorHandler('Error saving updated export settings').apply(null, [].slice.call(arguments));
            }
        });
    }

    function changeExportAllNodes(e) {
        kcm.exportSettings.exportAllNodes = $(e.target).prop('checked');
        saveExportSettings();
    }

    function changePipelineWorkflowStatusSetting(e) {
        var key = $(this).attr('data-key');
        var val = $(this).val();
        kcm.exportSettings[key] = isNaN(val) ? val : Number(val);
        saveExportSettings();
    }

    function updateExportSettingsDisplay() {
        var es = kcm.exportSettings;

        // export all nodes checkbox
        $('#export-all-nodes').prop('checked', es.exportAllNodes === true);

        // 'Export all nodes tagged', 'Tags to bit cols', 'Tag Prefixes to Text Cols', 'Pipeline Names'
        $.each($('.tags-box'), function() {
            $(this).html($.tmpl('cnTagDIV', $.map(es[$(this).closest('tr').attr('data-key')], function(tag) { return { tag:tag }; })))
            $(this).children().length ? $(this).removeClass('empty') : $(this).addClass('empty');
        });

        // Pipeline workflow status
        $('select[data-key="pipelineWorkflowStatusOperator"]').val(es.pipelineWorkflowStatusOperator);
        $('select[data-key="pipelineWorkflowStatusLevel"]').val(es.pipelineWorkflowStatusLevel);
    }

    function addExportSettingTag(e) {
        var $tr = $(this).closest('tr')
          , type = $tr.attr('data-type')
          , key = $tr.attr('data-key')
        ;
        e.stopPropagation();
        e.preventDefault();

        showSingleInputModal('Enter Tag:', function(tag) {
            if (tag) {
                kcm.exportSettings[key].push(tag);
                updateExportSettingsDisplay();
                saveExportSettings();
            }
        });
    }

    function deleteExportSettingTag(e) {
        var $tr = $(this).closest('tr')
          , type = $tr.attr('data-type')
          , key = $tr.attr('data-key')
        ;
        e.stopPropagation();
        e.preventDefault();

        kcm.exportSettings[key].splice($(this).closest('.cn-tag').index(), 1);
        updateExportSettingsDisplay();
        saveExportSettings();
    }

    function editExportSettingTag(e) {
        var $tr = $(this).closest('tr')
          , type = $tr.attr('data-type')
          , key = $tr.attr('data-key')
          , array = kcm.exportSettings[key]
        ;

        editTag(e, array, function(newText, tagIx, undoCallback) {
            array.splice(tagIx, 1, newText);
            updateExportSettingsDisplay();
            saveExportSettings();
        });
    }

  function addNewTagToConceptNode(e) {
    var cn = d3.select(inFocus).data()[0]

    e.stopPropagation()
    e.preventDefault()

    showSingleInputModal('Enter Tag:', function(tag) {
      if (tag) {
        cn = kcm.nodes[cn._id] // may have been edited/deleted by another user whilst adding tag
        if (!cn) {
          alert('node not found. it may have been deleted by another user.')
        } else {
          $.ajax({
            url:'/kcm/insert-concept-node-tag'
            , type:'POST'
            , contentType:'application/json'
            , data: JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, tag:tag })
            , error: ajaxErrorHandler('Error adding new tag to concept node')
          })
        }
      }
    })
  }

  function deleteConceptNodeTag(e) {
    var $tag = $(this).closest('div.cn-tag')
      , cn = d3.select(inFocus).data()[0]

    e.stopPropagation()
    e.preventDefault()

    showConfirmCancelModal('You are about to delete a tag from a concept node. Are you sure?', function(confirmation) {
      if (!confirmation) return

      cn = kcm.nodes[cn._id] // may have been edited/deleted by another user whilst adding tag
      if (!cn) {
        alert('node not found. it may have been deleted by another user.')
      } else {
        $.ajax({
          url: '/kcm/delete-concept-node-tag'
          , type: 'POST'
          , contentType: 'application/json'
          , data: JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, tagIndex:$tag.index(), tagText:$tag.text() })
          , error: ajaxErrorHandler('error deleting concept node tag')
        })
      }
    })
  }
    
  function editConceptNodeTag(e) {
    var cn = d3.select(inFocus).data()[0]
    editTag(e, cn.tags, function(tagText, tagIx, undoCallback) {
      cn = kcm.nodes[cn._id] // may have been edited/deleted by another user whilst adding tag
      if (!cn) {
        alert('node not found. it may have been deleted by another user.')
      } else {
        $.ajax({
          url: '/kcm/edit-concept-node-tag'
          , type: 'POST'
          , contentType: 'application/json'
          , data: JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, tagIndex:tagIx, currentText:cn.tags[tagIx], newText:tagText})
          , error: function() {
            undoCallback()
            ajaxErrorHandler('error editing concept node tag').apply(null, [].slice.call(arguments))
          }
        })
      }
    })
  }

  function editTag(e, array, callback) {
    var $tag = $(e.target).closest('div.cn-tag')
      , tagIx = $tag.index()
      , $parent = $tag.parent()
      , $editTag = $.tmpl('cnEditTagDIV', { tag:array[tagIx] })
      , $input = $editTag.children('input')
    
    e.stopPropagation()
    e.preventDefault()

    $tag.replaceWith($editTag)

    $input
      .focus()
      .on('focusout', function() { $editTag.replaceWith($tag) })
      .keydown(function(e) {
        switch(e.keyCode) {
          case 27:
            $editTag.replaceWith($tag)
            break
          case 13:
            if ($input.val() == $tag.text()) {
              $editTag.replaceWith($tag)
            } else if ($input.val().length) {
              $editTag.remove()
              callback($input.val(), tagIx, function() {
                if (tagIx == 0) $parent.prepend($tag)
                else $parent.children().eq(tagIx-1).after($tag)
              })
            }
            break
        }
      })
  }

  function addNewPipelineToConceptNode(e) {
    var cn = d3.select(inFocus).data()[0]

    e.stopPropagation()
    e.preventDefault()

    showSingleInputModal('Enter name for new pipeline:', function(name) {
      if (name) {
        cn = kcm.nodes[cn._id]
        if (!cn) {
          alert('node not found. it may have been deleted by another user.')
        } else {
          $.ajax({
            url:'/kcm/insert-pipeline'
            , type:'POST'
            , contentType:'application/json'
            , data:JSON.stringify({ conceptNodeId:cn._id, conceptNodeRev:cn._rev, pipelineName:name })
            , error:ajaxErrorHandler('Error adding new pipeline')
          })
        }
      }
    })
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
      // TODO: This is either retarded for not setting cn and pl here, or it's retarded cos there's no comment explaining why. Or I shouldn't TODO my code after drinking
        $('form#upload-pdefs').ajaxSubmit({
            success: function(o) { 
                var $trPLProbs = $('tr.pipeline-problems.expanded[data-id="'+o.pipelineId+'"]')
                  , pl = kcm.pipelines[o.pipelineId]
                  , nodes = $.map(kcm.nodes, function(n) { return n })
                  , cn = $.grep(nodes, function(n) { return ~n.pipelines.indexOf(pl._id); })[0]
                ;

                pl._rev = o.pipelineRev;
                pl.problems = $.map(o.problemDetails, function(p) { return p.id; });
                pl.workflowStatus = 0;

                if ($trPLProbs.length) {
                    $trPLProbs
                        .find('tr.problem')
                            .remove()
                            .end()
                        .find('tr.add-problems')
                            .before($.tmpl('plProblemTR', o.problemDetails));
                }

                // if pipeline still visible (i.e. its node is still selected) update its status select
                $('tr.pipeline[data-id="'+pl._id+'"] > td > select.workflow-status').val(0);

                d3.select($('g#'+cn._id)[0]).attr('class', setNodeColour);
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
                url: '/kcm/pipeline/' + pl._id + '/' + pl._rev + '/problem/' +  problemId + '/remove'
                , type:'POST'
                , success:function(plRev) {
                    var probIx = pl.problems.indexOf(problemId);
                    pl.workflowStatus = 0;
                    pl.problems.splice(probIx, 1);
                    pl._rev = plRev;
                    $trProblem.remove();

                    // if pipeline still visible (i.e. concept its node still selected), reset its workflow status
                    $('tr.pipeline[data-id="'+pl._id+'"] > td > select.workflow-status').val(0);

                    if (!pl.problems.length) {
                        var nodes = $.map(kcm.nodes, function(n) { return n })
                          , cn = $.grep(nodes, function(n) { return ~n.pipelines.indexOf(pl._id); })[0]
                          , plWithProbs = $.grep(cn.pipelines, function(plId) { return kcm.pipelines[plId].problems.length; })
                        ;
                        if (!plWithProbs.length) {
                            d3.select($('g#'+cn._id)[0]).attr('class', setNodeColour);
                        }
                    }
                }
                , error:ajaxErrorHandler('error removing problem from pipeline')
            });
        });
    }

    function updatePipelineWorkflowStatus() {
        var id = $(this).closest('tr').attr('data-id')
          , status = $(this).val()
          , pl = kcm.pipelines[id]
        ;
        $.ajax({
            url: '/kcm/pipeline/' + id + '/' + pl._rev + '/update-workflow-status/' + status
            , type:'PUT'
            , error:ajaxErrorHandler('error updating pipeline workflow status')
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
                            pl.workflowStatus = 0;
                            $('tr.pipeline[data-id="'+pl._id+'"] > td > select.workflow-status').val(0);
                        }
                        , error:ajaxErrorHandler('error reordering pipeline problems')
                    });
                }
            }
        });
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
        var endInteractionEventType = ~mouseInteractionModifiers.indexOf('(drawlink)') ? 'mousedown' : 'mouseup';
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
                .on('keydown keyup', keyCommandListener)
                .on('mousedown', '[data-type="concept-node"]', startInteraction)
                .off('mousemove', mousemove)
                .off(endInteractionEventType, arguments.callee)
            ;
            if (typeof endInteractionNext == 'function') endInteractionNext();
            cnInteractionInProgress = false;
        };

        cnInteractionInProgress = true;
        $(window)
            .off('keydown keyup', keyCommandListener)
            .off('mousedown', '[data-type="concept-node"]', startInteraction)
            .on('mousemove', mousemove)
            .on(endInteractionEventType, endInteraction)
        ;

        if (~mouseInteractionModifiers.indexOf('(drawlink)')) {
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
        } else if (~mouseInteractionModifiers.indexOf('(delete)')) {
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
                pos[0] = parseInt(pos[0], 10);
                pos[1] = parseInt(pos[1], 10);

                if (data.x == pos[0] && data.y == pos[1]) return;
                data.x = pos[0]
                data.y = pos[1]

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
        mouseInteractionModifiers = '';
    }

  function displayMapSearch() {
    var matches = []
      , currMatchIx = -1

    var highlightNode = function() {
      var n = matches[currMatchIx]
        , g = $('g#'+n._id)[0]
        , tx = $('svg').width() / 2 - n.x
        , ty = $('svg').height() / 2 - n.y

      $('#map-search-numbers').text( (1+currMatchIx) + ' / ' + matches.length )

      gZoom.attr('transform', 'scale(1)translate('+tx+','+ty+')')
      svg.calibrateToTransform(d3.transform(gZoom.attr('transform')))

      setFocus(g)
      $('input#map-search').focus()
    }

    var search = function() {
      var currMatch = matches[currMatchIx]
        , s = $('input#map-search').val().toLowerCase()
        , nodes = $.map(kcm.nodes, function(n) { return n })

      if (s.length) {
        matches = $.grep(nodes, function(n) { return ~n._id.toLowerCase().indexOf(s) || ~n.nodeDescription.toLowerCase().indexOf(s) })
      } else {
        matches = []
      }
      
      if (matches.length) {
        if (currMatch) currMatchIx = matches.indexOf(currMatch)
        if (!~currMatchIx) currMatchIx = 0
        highlightNode()
      } else {
        currMatchIx = -1
        $('#map-search-numbers').text('0 / 0')
        setFocus(null)
        $('input#map-search').focus()
      }
    }

    var prevMatch = function() {
      if (!matches.length) return
      currMatchIx = (matches.length + currMatchIx - 1) % matches.length
      highlightNode()
    }
    var nextMatch = function() {
      if (!matches.length) return
      currMatchIx = (currMatchIx + 1) % matches.length
      highlightNode()
    }
    
    var sel = function(e) {
      $('#map-search').select()
      if (e.type == 'mousedown') $(window).on('mouseup', sel)
      else $(window).off('mouseup', sel)
    }

    if ($('input#map-search').length) {
      $('input#map-search').focus()
      return
    }

    $.tmpl('mapSearchDIV').appendTo('body')

    $('#command-strip').on('mousedown', sel)

    $('input#map-search')
      .focus()
      .on('keyup', function(e) {
        switch(e.keyCode) {
          case 27: // ESC
            $('#command-strip').remove()
            break
          case 71: if (!e.ctrlKey) break // CTRL + 'g'
          case 13: // RETURN
            if (!matches.length) search()
            if (e.shiftKey) prevMatch()
            else nextMatch()
            break;
          case 16: // SHIFT
          case 17: // CTRL
          case 18: // ALT
            break
          default:
            search()
            break
        }
      })

    $('#command-strip #prev-match').on('click', prevMatch)
    $('#command-strip #next-match').on('click', nextMatch)
  }

    $.template("brViewSettingsTR",
        '<tr data-id="{{html _id}}">\
            <td class="br-name">{{html name}}</td>\
            <td class="br-visible">\
                <input type="checkbox" {{if visible !== false}}checked="checked"{{/if}}/>\
            </td>\
            <td class="br-colour">\
                <input type="text" value="{{html colour}}" maxlength="6" style="color:#{{html colour}}"/>\
            </td>\
            {{html tag}}\
        </tr>'.replace(/(>|}})\s+/g, '$1'));

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
                <a class="pipeline-id" href="/kcm/concept-nodes/${$item.nodeId}/pipeline/{{html _id}}/download" type="application/zip">{{html _id}}</a>\
                <br/>\
                <input type="text", value="{{html name}}"/>\
                <select class="workflow-status">\
                    <option value="0" {{if workflowStatus == 0}}selected="selected"{{/if}}>U</option>\
                    <option value="32" {{if workflowStatus == 32}}selected="selected"{{/if}}>T</option>\
                    <option value="64" {{if workflowStatus == 64}}selected="selected"{{/if}}>P</option>\
                </select>\
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
                <a href="/kcm/problem/{{html id}}" target="_blank">{{html id}}</a>\
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
    </div>'.replace(/(>|}})\s+/g, '$1'))

  $.template('mapSearchDIV',
    '<div id="command-strip" style="position:absolute; left:0; top:0; width:500px; background-color:#d5d5d5; padding:4px 0 2px 4px;">\
      <table style="width:100%;"><tr>\
        <td style="width:1px; padding-right:5px; vertical-align:middle;">Search:</td>\
        <td><input id="map-search" type="text" style="width:100%"/></td>\
        <td id="map-search-numbers" nowrap style="padding-left:14px; width:1px; text-align:left; vertical-align:middle; white-space:nowrap;">0 / 0</td>\
        <td style="width:70px; text-align:center;"><input id="prev-match" type="button" value="<"><input id="next-match" type="button" value=">"></td>\
      </tr></table>\
    </div>'.replace(/(>|}})\s+/g, '$1'));
//})(jQuery);
