var minTickTime = 5 // time in ms. setInterval() on Chrome has min delay of c.5ms so this value should not be set <5. Increase if required further for performance.
  , selectedUser
  , urPAEvents
  , problemAttempts = []
  , currPAData
  , showEventText = true

function populateProblemAttempts(baseData) {
  var keyIxs = {
    ur: 0
    , pADate: 1
    , pAId: 2
    , eDate: 3
    , docType: 4
  }
  
  var currPA
  var hasPollData = false
  var $div

  baseData.forEach(function(e, i) {
    if (!currPA || currPA.id != e.key[keyIxs.pAId]) {
      if (currPA && !hasPollData) problemAttempts.pop()
      if (hasPollData) $div.appendTo($('#parse-replays-log'))
      hasPollData = false

      currPA = { id:e.key[keyIxs.pAId], date:e.key[keyIxs.pADate], events:[] }
      problemAttempts.push(currPA)

      
      $div = $('<div/>')
      $('<div style="margin:10px 0;"/>')
        .text('Problem Attempt:' + JSON.stringify(currPA))
        .appendTo($div)
    }

    if (e.key[keyIxs.docType] == 'ProblemAttemptGOPoll') hasPollData = true

    currPA.events.push({
      date: e.key[keyIxs.eDate]
      , docType: e.key[keyIxs.docType]
      , value: e.value
    })

    var sp = Array(Math.max(0, 6 - i.toString().length)).join(' ')
    $('<div/>')
      .html(i + sp + [e.key[2], new Date(e.key[3]).toJSON().replace(/[ZT]/g, ' '), e.key[4]].join('  ') + Array(25 - e.key[4].length).join(' ') + (e.key[4]=='ProblemAttempt' ? e.value : ''))
      .appendTo($div)
  })

  if (!hasPollData && problemAttempts.length) problemAttempts.pop()
  else $div.appendTo($('#parse-replays-log'))
}

function loadPA(pAIx) {
  var pa = problemAttempts[pAIx]
  var paData = {
    id: pa.id
    , startTime: new Date(pa.date).getTime()
    , endTime: new Date(pa.events[0].date).getTime()
    , duration: (new Date(pa.events[0].date).getTime() - new Date(pa.date).getTime())
    , currentEventBlockIx: 0
    , numEventBlocks: 0
    , eventBlocks: []
    , absTimes: []
    , relTimes: []
    , gameObjects: []
    , touches: []
    , timerId: null
  }
  paData.formattedDurationString = relTimeToFormattedString(paData.duration)

  var eIx = pa.events.length
    , e
    , evBlockIx
    , eAbsTime

  currPAData && currPAData.timerId && clearTimeout(currPAData.timerId)

  while (e = pa.events[--eIx]) {
    eAbsTime = new Date(e.date).getTime()

    if (!paData.numEventBlocks || eAbsTime > paData.absTimes[evBlockIx] + minTickTime) { 
      evBlockIx = paData.numEventBlocks++
      paData.absTimes[evBlockIx] = eAbsTime
      paData.relTimes[evBlockIx] = eAbsTime - paData.startTime
      paData.gameObjects.forEach(function(go) { go.states[evBlockIx] = JSON.parse( JSON.stringify(go.states[evBlockIx-1]) ) })
      paData.touches.forEach(function(touch) { touch.states[evBlockIx] = JSON.parse( JSON.stringify(touch.states[evBlockIx-1]) ) })
      paData.eventBlocks[evBlockIx] = []
    }
    paData.eventBlocks[evBlockIx].push(e)

    if (e.docType == 'ProblemAttemptGOPoll') {
      e.value.forEach(function(goDelta) {
        var go
        if (goDelta.status == 'ADD') {
          go = { id:goDelta.id, states:[] }
          paData.gameObjects.push(go)

          for (var i=0; i<evBlockIx; i++) go.states[i] = { x:0, y:0, display:'none' }
          go.states[evBlockIx] = { x:goDelta.x, y:768-goDelta.y, display:'' }
        } else {
          go = paData.gameObjects.filter(function(aGO) { return aGO.id == goDelta.id } )[0]
          if (go) {
            if (goDelta.status == 'REMOVE') {
              go.states[evBlockIx].display = 'none'
            } else {
              if (typeof goDelta.x != 'undefined') go.states[evBlockIx].x = goDelta.x
              if (typeof goDelta.y != 'undefined') go.states[evBlockIx].y = 768 - goDelta.y
            }
          }
        }
      })
    } else if (e.docType == 'TouchLog') {
      if (e.value.phase == 'UITouchPhaseEnded' || e.value.phase == 'UITouchPhaseCancelled') {
        paData.touches[e.value.index].states[evBlockIx].display = 'none'
      } else {
        if (e.value.phase == 'UITouchPhaseBegan') {
          paData.touches[e.value.index] = { index:e.value.index, states:[] }
          for (var i=0; i<evBlockIx; i++) paData.touches[e.value.index].states[i] = { x:0, y:0, display:'none' }
        }
        paData.touches[e.value.index].states[evBlockIx] = { x:e.value.x, y:e.value.y, display:'' }
      }
    }
  }

  currPAData = paData
  createPAElements()
  setProgress(0)
  play()
}

function createPAElements() {
  d3.select('div#curr-pa-events').selectAll('div').remove()
  d3.select('g#game-objects').selectAll('g.go').remove()
  d3.select('g#touches').selectAll('g.touch').remove()

  var events = d3.select('div#curr-pa-events').selectAll('div')
    .data(currPAData.eventBlocks.reverse())

  events.enter().append('div')
    .attr('class', 'event-block')
    .attr('style', 'display:none;')
    .each(function(eventBlocks,i) {
      d3.select(this).selectAll('div')
          .data(eventBlocks)
        .enter().append('div')
          .attr('data-doc-type', function(e, i) { return e.docType })
          .attr('class', 'doc-type-color')
          .text(function(e, i) {
            return new Date(e.date).toJSON().match(/T(.+)Z/)[1] + '\t' + JSON.stringify(e.value)
          })
    })

  var go = d3.select('g#game-objects').selectAll('g.go')
    .data(currPAData.gameObjects)

  var touch = d3.select('g#touches').selectAll('g.touch')
    .data(currPAData.touches)
  
  go.enter().append('g')
    .attr('data-doc-type', 'ProblemAttemptGOPoll')
    .attr('class', 'go')
    .attr('style', function(go) { return 'display:' + go.states[0].display + ';' })
    .append('rect')
      .attr('class', 'doc-type-color')
      .attr('width', 20)
      .attr('height', 20)
      .attr('transform', 'translate(-10,-10)')

  touch.enter().append('g')
    .attr('data-doc-type', 'TouchLog')
    .attr('class', 'touch')
    .attr('style', function(touch) { return 'display:' + touch.states[0].display + ';' })
    .each(function(touch) {
      d3.select(this).append('circle')
        .attr('class', 'doc-type-color')
        .attr('r', 15)
    })
    .append("svg:text")
      .text(function(touch) { return touch.index })
}

function showCurrentEventBlock() {
  var evBlockIx = currPAData.currentEventBlockIx

  if (showEventText) {
    d3.select('div#curr-pa-events').selectAll('div.event-block')
    /* --- this block only sets display for divs that will change & makes change with jQuery. Not sure if faster of slower
      .each(function(eventBlocks, i) {
        var display = evBlockIx >= (currPAData.numEventBlocks - i - 1) ? 'block' : 'none'
        $(this).css('display') != display && $(this).css('display', display)
      }) */
      .attr('style', function(eventBlocks, i) { return evBlockIx >= (currPAData.numEventBlocks - i - 1) ? '' : 'display:none;' })
  }

  d3.select('g#game-objects').selectAll('g.go') 
    .attr('transform', function(go) {
      var td = go.states[evBlockIx];
      return 'translate(' + td.x + ',' + td.y + ')'
    })
    .attr('style', function(go) {
      var td = go.states[evBlockIx];
      return 'display:' + td.display + ';'
    })

  d3.select('g#touches').selectAll('g.touch')
    .attr('transform', function(touch) {
      var td = touch.states[evBlockIx];
      return 'translate(' + td.x + ',' + td.y + ')'
    })
    .attr('style', function(touch) {
      var td = touch.states[evBlockIx];
      return 'display:' + td.display + ';'
    })
}

function play() {
  if (!currPAData) return
  if (currPAData.timerId) clearInterval(currPAData.timerId)
  if (getProgress() >= 1) setProgress(0)

  var duration = currPAData.duration
    , startPlayingRelTime = duration * getProgress()
    , startPlayingAbsTime = new Date().getTime()

  currPAData.timerId = setInterval((function() {
    var relTime = startPlayingRelTime + new Date().getTime() - startPlayingAbsTime
    setProgress(relTime / duration)
    if (relTime >= duration) stop()
  }), minTickTime)
}

function stop() {
  if (!currPAData || !currPAData.timerId) return false
  clearInterval(currPAData.timerId)
  currPAData.timerId = null
  return true
}

// N.B. Search for new event block index ebIx herein can be very easily optimised if required
function setProgress(progress) {
  if (!currPAData) return
  progress = Math.max(0, Math.min(1, progress)) || 0

  var duration = currPAData.duration
    , ebIx = progress > (currPAData.relTimes[currPAData.currentEventBlockIx] / duration) ? currPAData.currentEventBlockIx : 0
    , ebRelTime

  while(ebRelTime = currPAData.relTimes[++ebIx]) if (ebRelTime / duration > progress) break
  ebIx--

  if (ebIx !== currPAData.currentEventBlockIx) {
    currPAData.currentEventBlockIx = ebIx
    showCurrentEventBlock()
  }

  $('#progress').width((100*progress) + '%')
  $('#time-display').text( relTimeToFormattedString(progress * duration)  + ' / '  + currPAData.formattedDurationString )
}

function getProgress() {
  return $('#progress').width() / $('#timeline').width()
}

function setEventTextVisibility(b) {
  showEventText = b === true
  $('#curr-pa-events').css('display', showEventText ? '' : 'none')
  if (currPAData) showCurrentEventBlock()
}

function relTimeToFormattedString(ms) {
  var totalS = ms / 1000
    , mins = Math.floor(totalS / 60).toString()
    , secs = (totalS % 60).toFixed(1)
    , m = '00'.slice(mins.length) + mins
    , s = '00'.slice(secs.length - 2) + secs
  return m + ':' + s
}

$(function() {
  $('select#problem-attempts').on('change', function(e) {
    $(this).blur()
    window.location.hash = '/user/' + selectedUser.id + '/problem-attempt/' + $(this).val()
  })

  $('.gripper').on('mousedown', function(e) {
    var f = arguments.callee
    var $gripper = $(e.target)
    var $p = $gripper.parent()
    var sp = $p.position()
    var smX = e.pageX
    var smY = e.pageY
    var mm = function(e) {
      $p.css('left', e.pageX - smX + sp.left)
        .css('top', e.pageY - smY + sp.top)
    }
    var mu = function(e) {
      $(window).off('mousemove', mm).off('mouseup', mu)
      $gripper.on('mousedown', f)
    }

    $(window).on('mousemove', mm).on('mouseup', mu)
  })

  $('#timeline').on('mousedown', function(e) {
    var onMouseD = arguments.callee
      , isPlaying = stop()
      , $tl = $(this)
      , tlX = $tl.offset().left
      , tlW = $tl.width()
      , progress = e.offsetX / tlW
      , onMouseM = function(e) { progress = (e.clientX - tlX) / tlW }
      , timerId = setInterval((function() { setProgress(progress) }), 10)

    setProgress(progress)
    $('*').addClass('no-select')

    $tl.off('mousedown', onMouseD)

    $(window)
      .on('mousemove', onMouseM)
      .on('mouseup', function(e) {
        $(window)
          .off('mousemove', onMouseM)
          .off('mouseup', arguments.callee)
        $tl.on('mousedown', onMouseD)
        $('*').removeClass('no-select')
        clearInterval(timerId)
        if (isPlaying) play()
      })
  })

  $('input#event-text-visibility').on('change', function(e) {
    var f = $(this).prop('checked') ? 'removeClass' : 'addClass'
    $(this).closest('ul')[f]('hide-all')
    setEventTextVisibility($(this).prop('checked'))
  })

  $('ul#filter-events > li[data-doc-type] > input').on('change', function(e) {
    var cssClass = 'hide' + $(this).parent().attr('data-doc-type')
      , f =  $(this).prop('checked') ? 'removeClass' : 'addClass'
    $('div#curr-pa-events')[f](cssClass)
  })

  $('input#user')
    .on('keyup paste cut', function(e) {
      var matches = []
        , $input = $(this)

      if (e.keyCode === 27) return // handled by $(window).on('keydown', 'input#user'....)

      // input value only updated for cut/paste after 0 second delay
      setTimeout(function() {
        var val = $input.val()

        if (val.length) {
          var re = new RegExp('^' + val.replace(/([^0-9a-z])/ig, '\\$1'), 'i')
          matches = users.filter(function(ur) { return re.test(ur.id) || re.test(ur.name) })
        }

        !val.length || matches.length ? $input.removeClass('no-match') : $input.addClass('no-match')

        var sel = d3.select('#matching-users').selectAll('div').data(matches)
        sel.exit().remove()
        sel.enter().append('div')
          .attr('class', 'user-match')
          .attr('tabindex', -1)
        sel.text(function(ur, i) { return ur.name + ' | ' + ur.id })
        $('#matching-users').css('display', matches.length ? 'block' : 'none')
      }, 0)
    })

  $('div#matching-users')
    .on('keydown click', 'div.user-match', function(e) {
      if (e.type == 'click' || e.keyCode == 13) {
        window.location.hash = '/user/' + d3.select(this).data()[0].id
        $('#matching-users').css('display', 'none')
      }
    })

  $(window)
    .on('keydown', 'input#user, div.user-match', function(e) {
      var $input = $('input#user')
        , $match

      switch(e.keyCode) {
        case 13: // return
          break;
        case 38: // up
        case 40: // down
          if (e.keyCode == 38) {
            $match = this == $input[0] ? $('.user-match:last') : $(this).prev()
          } else if (e.keyCode == 40) {
            $match = this == $input[0] ? $('.user-match:first') : $(this).next()
          }
          if (!$match.length) $('input#user').focus()
          else $match.focus()
          break;
        case 27: // escape
          if (this == $input[0]) {
            $input.blur()
          } else {
            $input.focus()
          }
          $input.removeClass('no-match')
          break;
      }
    })
    .on('focusout', 'input#user, div.user-match', function(e) {
      // next element only gets focus after 0 second delay
      setTimeout(function() {
        var $focus = $('*:focus')
          , $input = $('input#user')
        if (!$focus.hasClass('user-match') && $focus[0] != $input[0]) {
          $('div#matching-users').css('display', 'none')
          $input
            .val(selectedUser ? selectedUser.name : '')
            .removeClass('no-match')
        }
      }, 0)
    })
    .on('keydown', function(e) {
      var focusedEl = $('*:focus')[0]
      if (focusedEl && ~['input','select'].indexOf(focusedEl.tagName.toLowerCase())) return
      if ($(focusedEl).hasClass('user-match')) return
      if (e.keyCode == 32 && !stop()) play()
    })
    .on('hashchange', function(e) {
      var hash = window.location.hash
        , baseHash = '/user/'
        , urId
        , matchingUr
        , matchPA
        , paId

      var clearUser = function() {
        selectedUser = null
        problemAttempts = []
        $('select#problem-attempts').children().remove()
        $('input#user').val('')
      }

      stop()
      $('#time-display').html('')
      $('#progress').width(0)
      currPAData = null
      d3.select('div#curr-pa-events').selectAll('div').remove()
      d3.select('g#game-objects').selectAll('g.go').remove()
      d3.select('g#touches').selectAll('g.touch').remove()

      if (!/^#\/user\//.test(hash)) {
        window.location.hash = baseHash
        return
      }

      urId = hash.match(/^#\/user\/([^/]*)/)[1]

      if (!urId.length) {
        clearUser()
        return
      }

      for (var i=0; matchingUr=users[i]; i++) {
        if (matchingUr.id == urId) break
      }

      if (!matchingUr) {
        alert('could not find user with id="' + urId + '"')
        window.location.hash = baseHash
        return
      }

      matchPA = function() {
        var paRE = /^#\/user\/[^/]+\/problem-attempt\/([^/]+)/
          , paREMatch = window.location.hash.match(paRE)
          , paId = paREMatch && paREMatch[1]
          , urBaseHash = baseHash + selectedUser.id + '/'
          , $sel = $('select#problem-attempts') 
          , $option
        
        if (!paId) {
          if (hash != urBaseHash) window.location.hash = baseHash + selectedUser.id + '/'
          return
        }

        $option = $sel.children('[value="'+paId+'"]')

        if (!$option.length) {
          $sel.prepend('<option value="0">Select a problem attempt</option>')
          $sel.val(0)
          alert('problem attempt with id="' + paId + '" not found for user with id="' + selectedUser.id + '"') 
          window.location.hash = urBaseHash
          return
        }

        console.log($sel.children('[value="0"]'))

        $sel
          .val(paId)
          .children('[value="0"]').remove()
        loadPA($option.index())
      }

      if (selectedUser == matchingUr) {
        matchPA()
      } else {
        clearUser()
        $.ajax({
          url: '/pa-events-for-user/' + matchingUr.id
          , method: 'GET'
          , error: function(e) {
            alert('error retrieving problem attempt events for user with id="' + matchingUr.id + '"\n' + e)
          }
          , success: function(events) {
            var $sel =  $('select#problem-attempts')

            selectedUser = matchingUr
            $('input#user').val(selectedUser.name)

            populateProblemAttempts(events)

            $sel.children().remove()
            $sel.append('<option value="0">Select a problem attempt</option>')
            problemAttempts.forEach(function(pa) {
              $sel.append($('<option value="'+pa.id+'">' + (pa.date || 'null').replace(/T|(\.\d{3}Z)/g, ' &nbsp; ') + '</option>'))
            })

            matchPA()
          }
        })
      }
    })
    .trigger('hashchange')
})
