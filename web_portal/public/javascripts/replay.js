var minTickTime = 5 // time in ms. setInterval() on Chrome has min delay of c.5ms so this value should not be set <5. Increase if required further for performance.
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

  problemAttempts.forEach(function(pa) {
    $('select#problem-attempts').append($('<option value="'+pa.id+'">' + (pa.date || 'null').replace(/T|(\.\d{3}Z)/g, ' &nbsp; ') + '</option>'))
  })
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
    .append('rect')
      .attr('class', 'doc-type-color')
      .attr('width', 20)
      .attr('height', 20)
      .attr('transform', 'translate(-10,-10)')

  touch.enter().append('g')
    .attr('data-doc-type', 'TouchLog')
    .attr('class', 'touch')
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

  var duration = currPAData.duration
    , startPlayingRelTime = duration * getProgress()
    , startPlayingAbsTime = new Date().getTime()

  currPAData.timerId = setInterval((function() {
    var relTime = startPlayingRelTime + new Date().getTime() - startPlayingAbsTime
    setProgress(relTime / duration)
    if (relTime >= duration) {
      clearInterval(currPAData.timerId)
      currPAData.timerId = null
    }
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

  var relTimeS = progress * duration / 1000
  var m = Math.floor(relTimeS / 60).toString()
  var s = (relTimeS % 60).toFixed(1)
  $('#time-display').text(relTimeToFormattedString(progress * duration) + ' / ' + currPAData.formattedDurationString)
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
  populateProblemAttempts(replayData)

  $('select#problem-attempts').on('change', function(e) {
    $(e.target)
      .blur()
      .children('[value="0"]').remove()
    loadPA($(e.target).children('[selected]').index())
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

  $('input#event-text-visibility').on('change', function(e) { setEventTextVisibility($(this).prop('checked')) })

  $(window).on('keydown', function(e) {
    var focusedEl = $('*:focus')[0]
    if (focusedEl && ~['input','select'].indexOf(focusedEl.tagName.toLowerCase())) return
    if (e.keyCode == 32 && !stop()) play()
  })
})
