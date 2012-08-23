var problemAttempts = []
var typeTxtCols = {
  'ProblemAttempt': '#494949'
  , 'ProblemAttemptGOPoll': '#269FF5'
}

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

function playPAIndex(pAIx) {
  var cache = arguments.callee.cache
    , pa = problemAttempts[pAIx]
    , eIx = pa.events.length - 1
    , startTime = new Date().getTime()
    , firstETime = new Date(pa.events[eIx].date).getTime()

  if (!cache) cache = arguments.callee.cache = {}
  else if (cache.toId) clearTimeout(cache.toId)

  $('#curr-pa-events').html('')
  $('<div>').text(pa.id).css('color', '#8fa').appendTo('#curr-pa-events')

  ;(function nextEvent() {
    var e = pa.events[eIx]
      , eText = JSON.stringify(e.value)
      , eDate = new Date(e.date)
      , eTimeAfterFirstE = eDate.getTime() - firstETime
      , timeSinceStart = new Date().getTime() - startTime
      , timeUntilE = eTimeAfterFirstE - timeSinceStart
  
    if (timeUntilE > 10000) {
      startTime -= (timeUntilE - 10000)
      nextEvent()
      return
    }

    cache.toId = setTimeout((function() {
      delete cache.toId
      $('<div style="padding:2px 0;"/>').text(eText).insertAfter($('#curr-pa-events').children(':first')).css('color', typeTxtCols[e.docType])
      if (eIx--) nextEvent()
    }), timeUntilE)
  })()
}

$(function() {
  populateProblemAttempts(replayData)

  $('select#problem-attempts').on('change', function(e) {
    $(e.target).children('[value="0"]').remove()
    playPAIndex($(e.target).children('[selected]').index())
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
})
