(function($) {
    $(function() {
        $('#select-topic').change(function() {
            $(this).children('[value="0"]').remove();
            resetSelectElement();
            populateModulesForTopic($(this).val());
        });

        $('#select-module').change(function() {
            $(this).children('[value="0"]').remove();
            populateElementsForModule($(this).val());
        });

        $('#select-element').change(function() {
            $(this).children('[value="0"]').remove();
            populateAssessmentCriteriaForElement($(this).val());
        });

        $('#wrapper')
            .on('click', '.expandCollapse', function(e) {
                expandCollapseCriterion(
                    $(e.currentTarget).closest('tr'));
            })
            .on('click', '.delete', function(e) {
                deleteCriterion(
                    $(e.currentTarget).closest('tr'));
            })
            .on('click', '.cancel', function(e) {
                cancelCriterionChanges(
                    $(e.currentTarget).closest('tr'));
            })
            .on('click', '.save', function(e) {
                saveCriterion(
                    $(e.currentTarget).closest('tr'));
            })
            .on('focusout', 'input[type="text"][data-type]', autoCorrectOnFocusOut)
            .on('change keyup mouseup focusout', '[data-key]', function(e) {
                checkCriterionChangesAndValidate(
                    $(e.currentTarget).closest('tr'));
            })
            .on('click', '#new-criterion', function(e) {
                newAssessmentCriterion();
            })
        ;
         
        // Temporary!! Pre-populate assessment crit for development
        //populateAssessmentCriteriaForElement('c945f815a4a474143d3090b797d3da2d');
    });

    function populateModulesForTopic(topicId) {
        $.get('/content/module-options/' + topicId, function(data) {
            var $selectModule = $('#select-module')
                .children()
                .remove()
                .end()
                .append(data);

            var disabled = $selectModule.val() == "0" && $selectModule.children().length == 1;
            $selectModule.prop('disabled', disabled ? 'disabled' : '');
        });
        populateAssessmentCriteriaForElement(null);
    }

    function populateElementsForModule(moduleId) {
        $.get('/content/element-options/' + moduleId, function(data) {
            var $selectElement = $('#select-element')
                .children()
                .remove()
                .end()
                .append(data)
                .prop('disabled', ($('#select-element').children().length > 1 ? '' : 'disabled'));

            var disabled = $selectElement.val() == "0" && $selectElement.children().length == 1;
            $selectElement.prop('disabled', disabled ? 'disabled' : '');
        });
        populateAssessmentCriteriaForElement(null);
    }

    function resetSelectElement() {
        $('#select-element')
            .children()
            .remove()
            .end()
            .append('<option value="0">Select An Element</option>')
            .prop('disabled', 'disabled');
    }

    function populateAssessmentCriteriaForElement(elementId) {
        $('table#assessment-criteria').remove();
        if (elementId)
            $.get('/content/element-assessment-criteria/' + elementId, function(data) {
                $(data).appendTo('#wrapper');
            });
    }

    function expandCollapseCriterion($trValues) {
        var $trProblems = $trValues.next()
          , currentState = $trValues.hasClass('collapsed') ? 'collapsed' : 'expanded'
          , nextState = $trValues.hasClass('collapsed') ? 'expanded' : 'collapsed'
        ;

        $trValues.add($trProblems)
            .removeClass(currentState)
            .addClass(nextState);

            
        if ($trProblems.hasClass('not-loaded')) {
            var id = $trValues.children('[data-key="id"]').attr('data-saved-value');
            
            $.get('/content/assessment-criterion-problems/' + id, function(data) {
                $trProblems.find('.loading').replaceWith(data);
            });
        }
    }

    function cancelCriterionChanges($trValues) {
        if ($trValues.hasClass('new-criterion')) {
            $trValues.remove();
        } else {
            $trValues.find('[data-key]').each(function(index, field) {
                $(field).val($(field).attr('data-saved-value'));
            });
            $trValues.removeClass('unsaved-changes invalid');
        }

        updateAnyCriteriaUnsavedChanges();
    }

    function autoCorrectOnFocusOut(e) {
        var $input = $(e.currentTarget)
          , newVal
        ;
        switch ($input.attr('data-type')) {
            case 'integer':
                newVal = parseInt($input.val(), 10) || parseInt($input.attr('data-saved-value'), 10) || 0;
                $input.val(newVal);
                break;
            default:
                break;
        }
    }
    
    function checkCriterionChangesAndValidate($trValues) {
        var nochanges = true
          , valid = true
          , fields = $trValues.find('[data-key]').toArray();
        ;

        while (fields.length && (nochanges || valid)) {
            var $field = $(fields.pop())
              , val;

            switch($field[0].tagName.toLowerCase()) {
                case 'input':
                    val = $field.val();
                    break;
                case 'td':
                    val = $field.html();
                    break;
                default:
                    val = $field.html();
            }
            if (val != $field.attr('data-saved-value')) nochanges = false;
            if ($field.attr('data-required') == 'true' && !val.length) valid = false;
        }

        nochanges ? $trValues.removeClass('unsaved-changes') : $trValues.addClass('unsaved-changes');
        valid ? $trValues.removeClass('invalid') : $trValues.addClass('invalid');

        updateAnyCriteriaUnsavedChanges();
    }

    function updateAnyCriteriaUnsavedChanges() {
        var $ac = $('table#assessment-criteria')
          , unsavedChanges = $ac.find('tr.criterion-values.unsaved-changes, tr.criterion-values.new-criterion').length > 0;
        
        if (unsavedChanges != $ac.hasClass('unsaved-changes')) {
            if (unsavedChanges) {
                $ac.addClass('unsaved-changes');
            } else {
                $ac.removeClass('unsaved-changes');
            }
            updateTopicModuleElementSelectsDisabled();
        }
    }

    function updateTopicModuleElementSelectsDisabled() {
        var $ac = $('table#assessment-criteria')
          , $t = $('#select-topic')
          , $m = $('#select-module')
          , $e = $('#select-element')
        ;

        if ($ac.length && $ac.hasClass('unsaved-changes')) {
            $t.add($m).add($e).prop('disabled', true);
        } else {
            $t.prop('disabled', $t.children('option[value != "0"]').length == 0);
            $m.prop('disabled', $t.prop('disabled') || $m.children('option[value != "0"]').length == 0);
            $e.prop('disabled', $m.prop('disabled') || $e.children('option[value != "0"]').length == 0);
        } 
    }

    function newAssessmentCriterion() {
        $.get('/content/element-assessment-criteria/new-criterion/', function(data) {
            $(data).insertAfter($('table#assessment-criteria tr:first'));
            $('table#assessment-criteria tr:eq(1) > td.criterion-name input').focus();
            updateAnyCriteriaUnsavedChanges();
        });
    }

    function deleteCriterion($trValues) {
        var id = $trValues.find('td[data-key="id"]').html();
        if (!id) return; //error
        $.ajax({
            url: '/content/element-assessment-criteria/delete-criterion'
            , type: 'POST'
            , contentType: 'application/json'
            , processData: false
            , data: JSON.stringify({ id:id })
            , success: function() {
                $trValues
                    .next('tr.criterion-problems').remove()
                    .end()
                    .remove();
                updateAnyCriteriaUnsavedChanges();
            }
            , error: function() {
                //console.log('error:', arguments);
            }
        });
    }

    function saveCriterion($trValues) {
        var properties = {};
        
        $trValues
            .find('[data-key]')
            .each(function(i, el) {
                var val;
                
                switch(el.tagName.toLowerCase()) {
                    case 'input':
                        val = $(el).val();
                        break;
                    case 'td':
                        val = $(el).html();
                        break;
                    default:
                        val = $(el).html();
                }
                if ($(el).attr('data-type') == 'integer') val = parseInt(val, 10);
                
                properties[$(el).attr('data-key')] = val;
            });

        if (!properties.id) {
            properties.elementId = $('#select-element').val();
        }

        $.ajax({
            url: '/content/element-assessment-criteria/save-criterion'
            , type: 'POST'
            , contentType: 'application/json'
            , processData: false
            , data: JSON.stringify(properties)
            , dataType: 'html'
            , success: function(pair) {
                $trValues
                    .next('tr.criterion-problems').remove()
                    .end()
                    .remove();

                var name = $(pair).find('[data-key="name"]').attr('data-saved-value');

                var after = $('#assessment-criteria .criterion-values').filter(function(i, trValues) {
                    return $(trValues).find('[data-key="name"]').attr('data-saved-value') > name;
                });

                if (after.length) $(pair).insertBefore(after[0]);
                else $('#assessment-criteria').append(pair);

                updateAnyCriteriaUnsavedChanges();
            }
            , error: function() {
                //console.log('error:', arguments);
            }
        });
    }
})(jQuery);

