(function($) {
    $(function() {
        $(window).resize(function() {
            var h = $(window).height() - $('div.scroll-div').offset().top - 50;
            $('div.scroll-div').css('height', h);
        }).resize();
        
        $('#wrapper')
            .on('change', '#select-pipeline', function() {
                $(this).children('[value="0"]').remove();
                self.location = '/kcm/pipelines/' + $('#select-pipeline').val();
            })
            .on('click', '.click-across-inner > tbody > tr > td', function(e) {
                if (this != e.target) return;
                selectUnselectClickAcrossItem($(this).parent());
            })
            .on('click', '.click-across-outer.left-selected .click-right', function(e) {
                var $tr = $(e.currentTarget).closest('table.click-across-outer').find('tr.selected');
                clickAcrossRight($tr);
                updatePageControls();
            })
            .on('click', '.click-across-outer.right-selected .click-left', function(e) {
                var $tr = $(e.currentTarget).closest('table.click-across-outer').find('tr.selected');
                clickAcrossLeft($tr, true);
                updatePageControls();
            })
            .on('click', '#cancel-page-changes', function() {
                if ($('#wrapper').hasClass('unsaved-changes')) cancelPageChanges();
            })
            .on('click', '#save-page-changes', function() {
                if ($('#wrapper').hasClass('unsaved-changes') && !$('#wrapper').hasClass('invalid-changes')) savePageChanges();
            })
            .on('mousedown', '.click-across-inner-l', function(e) {
                var y = e.pageY
                  , $tr = $('tr.problem', e.currentTarget)
                        .filter(function(i, problem) {
                            var top = $(problem).offset().top;
                            return top < y && top + $(problem).height() > y;
                        })
                  , height = $tr.height();
                
                if (!$tr.length) return;
                
                var drag = function(e) {
                    if (e.type == 'mouseup') {
                        $(window).off('mousemove mouseup', drag);
                        return;
                    }

                    var top = $tr.offset().top;

                    if (e.pageY < top) {
                        var $prev = $tr.prev();
                        if ($prev.length) {
                            $tr.remove();
                            $tr.insertBefore($prev);
                            updatePageControls();
                        }
                    } else if (e.pageY > top + height) {
                        var $next = $tr.next();
                        if ($next.length) {
                            $tr.remove()
                            $tr.insertAfter($next);
                            updatePageControls();
                        }
                    }
                };
                $(window).on('mousemove mouseup', drag);
            })
        ;
    });

    function selectUnselectClickAcrossItem($tr) {
        var $table = $('.click-across-outer').removeClass('left-selected right-selected');

        if ($tr.hasClass('selected')) {
            $tr.removeClass('selected');
        } else {
            var $caOuter = $tr.closest('.click-across-outer');
            $caOuter.find('.click-across-inner > tbody > tr').removeClass('selected');
            $tr.addClass('selected');
            $table.addClass($tr.closest('table').hasClass('click-across-inner-l') ? 'left-selected' : 'right-selected');
        }
    }

    function clickAcrossRight($tr) {
        //console.log('R');
        var $caOuter = $tr.closest('.click-across-outer')
          , laterTRs = $caOuter.find('.click-across-inner-r > tbody > tr')
                .filter(function(index, otherTR) {
                    var orderVal1 = $(otherTR).attr('data-order-on')
                      , orderVal2 = $tr.attr('data-order-on')
                    ;
                    if (!isNaN(orderVal1) && !isNaN(orderVal2)) {
                        orderVal1 = parseFloat(orderVal1);
                        orderVal2 = parseFloat(orderVal2);
                    }
                    return orderVal1 > orderVal2;
                })
        ;

        $tr.remove();
        if (laterTRs.length) $tr.insertBefore(laterTRs[0]);
        else $caOuter.find('.click-across-inner-r').append($tr);
        
        $('.click-across-outer').removeClass('left-selected').addClass('right-selected');
    }

    function clickAcrossLeft($tr, ignoreDataOrderAndAppend) {
        //console.log('L');
        var $caOuter = $tr.closest('.click-across-outer')
          , after = $caOuter.find('.click-across-inner-l > tbody > tr').filter(function(index, criterion) {
            return $(criterion).attr('data-order-on') > $tr.attr('data-order-on');
        });

        $tr.remove();
        if (!ignoreDataOrderAndAppend && after.length) $tr.insertBefore(after[0]);
        else $caOuter.find('.click-across-inner-l').append($tr);
        $('.click-across-outer').removeClass('right-selected').addClass('left-selected');
    }

    function updatePageControls() {
        var pageState = checkDataForChangesAndValidate();
        //console.log('updatePageControls()', JSON.stringify(pageState,null,2));
        pageState.changes ? $('#wrapper').addClass('unsaved-changes') : $('#wrapper').removeClass('unsaved-changes');
        pageState.invalid ? $('#wrapper').addClass('invalid-changes') : $('#wrapper').removeClass('invalid-changes');
    }

    function checkDataForChangesAndValidate() {
        var nochanges = true
          , valid = true
          , dataArrayElements = $('[data-collection="array"][data-saved-array-element-ids][data-array-elements-selector]').toArray()
          , dataValueElements = $('[data-saved-value]').toArray()
          , savedArrayElementIds, currentArrayElementIds, elementsSelector, $arr, $el, val
        ;
        
        if ($('#select-pipeline').val() == '0' || !$('#select-pipeline').val()) valid = false;

        while(nochanges && dataArrayElements.length) {
            $arr = $(dataArrayElements.pop());
            if ($arr.closest('[data-ignore-branch]').length) continue;

            savedArrayElementIds = $arr.attr('data-saved-array-element-ids');
            elementsSelector = $arr.attr('data-array-elements-selector');
            currentArrayElementIds = $.map(
                $(elementsSelector, $arr)
                , function(el) { return $(el).attr('data-element-id'); })
            .join(',');

            if (savedArrayElementIds != currentArrayElementIds) nochanges = false;
        }

        while((nochanges || valid) && dataValueElements.length) {
            $el = $(dataValueElements.pop());
            if ($el.closest('[data-ignore-branch]').length) continue;
            
            switch ($el[0].tagName.toLowerCase()) {
                case 'input':
                    if ($el.attr('type') == 'checkbox') val = $el.prop('checked') ? 'true' : 'false';
                    else val = $el.val();
                    break;
                case 'select':
                    val = $el.val();
                    break;
                default:
                    val = $el.text();
                    break;
            }
            if ($el.attr('data-saved-value') != val) nochanges = false;
            if (valid && (
                ($el.attr('data-required') && !val.length) ||
                ($el.attr('data-type') == 'number' && isNaN(val)) ||
                ($el.attr('data-type') == 'integer' && (isNaN(val) || Number(val) !== parseInt(val,10)))
            )) {
                valid = false;
            }
        }

        return { changes:!nochanges, invalid:!valid };
    }

    function cancelPageChanges() {
        var $l = $('table#.click-across-inner-l')
        var $r = $('table#.click-across-inner-r')
          , lIds = $l.attr('data-saved-array-element-ids')
          , rIds = $r.attr('data-saved-array-element-ids')
        ;
        
        // by moving all over to excluded, the included problems are guarateed to be restored to their correct order
        $l.children('tbody').children('tr').each(function() {
            clickAcrossRight($(this));
        });

        $r.children('tbody').children('tr').each(function() {
            var $tr = $(this);
            if (rIds.indexOf($tr.attr('data-element-id')) == -1) clickAcrossLeft($tr);
        });

        $('[data-saved-value]').each(function() {
            var $el = $(this)
              , savedVal = $el.attr('data-saved-value');

            switch(this.tagName.toLowerCase()) {
                case 'input':
                    if ($el.attr('type') == 'checkbox') $el.prop('checked', savedVal === 'true');
                    else $el.val(savedVal);
                case 'select':
                    $el.val(savedVal);
                    break;
                default:
                    $el.text(savedVal);
                    break;
            }
        });
        updatePageControls();
    }

    function savePageChanges() {
        $.ajax({
            url:'/kcm/pipeline-sequence/update'
            , type:'POST'
            , contentType:'application/json'
            , processData:false
            , data: JSON.stringify({ pipelineId:$('#select-pipeline').val(), problems:getDataset().includedProblems })
            , success:function() {
                $('[data-collection="array"]').each(function() {
                    var elementsSelector = $(this).attr('data-array-elements-selector')
                      , elements = $(elementsSelector, this).toArray()
                      , elementIds = $.map(elements, function(el) { return $(el).attr('data-element-id'); }).join(',')
                    ;
                    $(this).attr('data-saved-array-element-ids', elementIds);

                    if ($(this).attr('data-user-order') == 'true') {
                        $(elements).each(function(index) {
                            $(this).attr('data-order', index);
                        });
                    }
                });

                $('[data-saved-value]').each(function() {
                    var $el = $(this)
                      , newSavedVal;

                    switch(this.tagName.toLowerCase()) {
                        case 'input':
                            if ($el.attr('type') == 'checkbox') newSavedVal = $el.prop('checked') ? 'true' : 'false';
                            else newSavedVal = $el.val();
                            break;
                        case 'select':
                            newSavedVal = $el.val();
                            break;
                        default:
                            newSavedVal = $el.text();
                            break;
                    }
                    $el.attr('data-saved-value', newSavedVal);
                });
                updatePageControls();
            }
            , error:ajaxErrorAlerter('error saving page changes')
        });
    }

    function ajaxErrorAlerter(firstline) {
        return function(e) {
            alert(firstline + '\n' + e.responseText + '\n' + [].slice.call(arguments, 1).join('\n'));
        }
    }

    function getDataset() {
        var dataset = {};

        (function getDatasetRecursive($el, o) {
            if ($el.attr('data-ignore-branch') == 'true') return;

            switch ($el.attr('data-collection')) {
                case 'array':
                    var a = []
                      , elementsSelector = $el.attr('data-array-elements-selector')
                      , elements = $(elementsSelector, $el); 

                    if (!elementsSelector) throw('attribute "data-array-elements-selector" required');

                    if (o instanceof Array) {
                        o.push(a);
                    } else {
                        var key = $el.attr('data-key');
                        if (!key) throw('attribute "data-key" required');
                        o[key] = a;
                    }

                    elements.each(function() { getDatasetRecursive($(this), a); });
                    return;
                case 'dictionary':
                    var dict = {};
                    
                    if (o instanceof Array) {
                        o.push(dict)
                    } else {
                        var key = $el.attr('data-key');
                        if (!key) throw('attribute "data-key" required');
                        o[key] = dict;
                    }

                    $el.children().each(function() { getDatasetRecursive($(this), dict); });
                    return;
                default:
                    var key = $el.attr('data-key')
                      , val = $el.attr('data-value');
                    if (key || o instanceof Array) {
                        if (!val) {
                            switch($el[0].tagName.toLowerCase()) {
                                case 'input':
                                    if ($el.attr('type') == 'checkbox') val = $el.prop('checked');
                                    else val = $el.val();
                                    break;
                                case 'select':
                                    val = $el.val();
                                    break;
                                default:
                                    val = $el.text();
                                    break;        
                            }
                        }
                        switch ($el.attr('data-type')) {
                            case 'integer':
                                val = parseInt(val,10);
                                break;
                            default:
                                break;
                        }
                        if (o instanceof Array) {
                            o.push(val);
                            break;
                        } else {
                            o[key] = val;
                        }
                    }
                    $el.children().each(function() { getDatasetRecursive($(this), o); });
                    break;
            }
        })($('#wrapper'), dataset);

        return dataset;
    }
})(jQuery);
