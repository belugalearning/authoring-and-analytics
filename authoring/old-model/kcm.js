var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
  , libxmljs = require('libxmljs')
  , exec = require('child_process').exec
  , sqlite3 = require('sqlite3').verbose()

var kcmDatabaseName
  , kcm // new model - gradually transition to retrieving docs from here & updating/deleting via its controllers
  , designDoc
  , couchServerURI
  , databaseURI
  , relationTypes = ['binary']

module.exports = function(config, kcmModel) {
  kcm = kcmModel
  couchServerURI = config.couchServerURI.replace(/^(.+[^/])\/*$/, '$1/')
  kcmDatabaseName = config.authoring.kcmDatabaseName
  designDoc = config.authoring.kcmDatabaseDesignDoc
  databaseURI = couchServerURI + kcmDatabaseName + '/'
  console.log(util.format('Authoring -> kcm\t\tdesignDoc="%s"\tdatabaseURI="%s"', designDoc, databaseURI))

  request({
    method: 'PUT'
    , uri: databaseURI
    , headers: { 'content-type': 'application/json', 'accepts': 'application/json' } 
  }, function(e,r,b) {
    if (!r) {
      console.log('Error - could not connect to Couch Server testing for existence of database:"%s"', databaseURI)
      return
    }
    if (201 != r.statusCode && 412 != r.statusCode) {
      console.log('Error other than "already exists" when attempting to create database:"%s". Error:"%s" StatusCode:%d', databaseURI, e, r.statusCode)
      return
    }
    if (false) {
              updateDesignDoc(function(e, statusCode) {
                if (201 != statusCode) {
                  console.log('error updating design doc on database:"%s". Error:"%s". StatusCode:%d', databaseURI, e, statusCode)
                }
              })
    }
  })

  if (false) {
  queryView('by-type', 'keys', ['concept node','pipeline','problem','relation','tool'], 'include_docs', true, 'reduce', false, function(e,r,b) {
    console.log('get docs by-type: e="%s" statusCode="%d"', e, r.statusCode)
    var docs = _.pluck(JSON.parse(b).rows, 'doc')
    console.log('docs len:', docs.length)
    var updates = []
    docs.forEach(function(d) {
      var needsUpdate = false
      if (d.revisions) {
        delete d.revisions
        needsUpdate = true
      }
      if (!d.versions) {
        d.versions = []
        needsUpdate = true
      }
      if (needsUpdate) updates.push(d)
    })
    console.log('updates.length =', updates.length)
    if (updates.length > 0) {
      request({
        method:'POST'
        , uri: databaseURI + '_bulk_docs'
        , headers: { 'content-type':'application/json', accepts:'application/json' }
        , body: JSON.stringify({ docs:updates })
      }, function(e,r,b) {
        console.log('bulk update add revisions array callback. e="%s" statusCode=%d', e, r.statusCode)
      })
    }
  })
  }

  return {
    databaseName: kcmDatabaseName
    , generateUUID: generateUUID
    , importGraffleMapIntoNewDB: importGraffleMapIntoNewDB
    , createDB: createDB
    , pullReplicate: pullReplicate
    , updateDesignDoc: updateDesignDoc
    , queryView: queryView
    , updateViewSettings: updateViewSettings
    , updateExportSettings: updateExportSettings
    , insertProblem: insertProblem
    , insertConceptNode: insertConceptNode
    , deleteConceptNode: deleteConceptNode
    , updateConceptNodePosition: updateConceptNodePosition
    , updateConceptNodeDescription: updateConceptNodeDescription
    , insertConceptNodeTag: insertConceptNodeTag
    , deleteConceptNodeTag: deleteConceptNodeTag
    , editConceptNodeTag: editConceptNodeTag
    , insertBinaryRelation: insertBinaryRelation
    , addOrderedPairToBinaryRelation: addOrderedPairToBinaryRelation
    , removeOrderedPairFromBinaryRelation: removeOrderedPairFromBinaryRelation
    , addNewPipelineToConceptNode: addNewPipelineToConceptNode
    , deletePipeline: deletePipeline
    , reorderConceptNodePipelines: reorderConceptNodePipelines
    , appendProblemsToPipeline: appendProblemsToPipeline
    , removeProblemFromPipeline: removeProblemFromPipeline
    , updatePipelineSequence: updatePipelineSequence
    , updatePipelineWorkflowStatus: updatePipelineWorkflowStatus
    , updatePipelineName: updatePipelineName
    , pipelineProblemDetails: pipelineProblemDetails
    , reorderPipelineProblems: reorderPipelineProblems
    , getAppContent: getAppContent
    , getDoc: getDoc
    , getDocs: getDocs
    , updateDoc: updateDoc // TODO: Shoud not give direct access to this function if we're doing undo functionality.
  }
}

function firstVersion(user, action, actionData) {
  return {
    currentVersion: {
      user: user
      , date: new Date
      , action: action
      , actionData: actionData
    }
    , versions: []
  }
}

function nextVersion(doc, user, action, actionData) {
    var b64 = new Buffer(JSON.stringify(doc)).toString('base64')

    if (!doc._attachments) doc._attachments = {}
    doc._attachments[doc._rev] = { "content_type":"application\/json", "data":new Buffer(JSON.stringify(doc)).toString('base64') }

    if (!doc.versions) doc.versions = []
    var lastVersion = doc.currentVersion || {}
    doc.versions.push({ rev:doc._rev, user:lastVersion.user, date:lastVersion.date, action:lastVersion.action })

    doc.currentVersion = {
      user: user
      , date: new Date
      , action: action
      , actionData: actionData
    }
}

function replaceUUIDWithGraffleId() {
  var file = __dirname + '/../resources/nc-graffle-import-notes.csv'
    , notes = fs.readFileSync(file, 'UTF8')
    , graffleIdNodeMap = {}

  queryView('concept-nodes', 'include_docs', true, function(e,r,b) {
    var nodes = JSON.parse(b).rows
      , docNodeIds = _.uniq(notes.match(/2e203[a-z0-9]+/ig))


    _.each(nodes, function(n) {
      graffleIdNodeMap[n.doc.graffleId] = n.doc
      idMap[n.doc.graffleId] = n.id
    })

    _.each(docNodeIds, function(id) {
      notes = notes.replace(new RegExp(id,'ig'), 'GraffleId:'+idMap[id])
    })

    fs.writeFileSync(file, notes, 'UTF8')
  })
}

function generateUUID() {
  var chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
  var buff = []
  for (var i=0; i<32; i++) {
    buff[i] = chars[Math.floor(chars.length * Math.random())]
  }
  return buff
    .join('')
    .replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, '$1-$2-$3-$4-$5')
    .toUpperCase()
}

function importGraffleMapIntoNewDB(conceptNodeLayer, toolsLayer, dummyRun) {
  // files from which to import
  var graffleDocPath = __dirname + '/../resources/kcm-120420/kcm-120420-tools.graffle'
    , svgDocPath = __dirname + '/../resources/kcm-120420/kcm-120420-tools.svg'


  // check args
  var errors = false
  if (parseInt(conceptNodeLayer) !== conceptNodeLayer) {
    console.log('BAD ARG: integer must be provided for parameter: conceptNodeLayer. The map was not imported.')
    errors = true
  }
  if (parseInt(toolsLayer) !== toolsLayer) {
    console.log('BAD ARG: integer must be provided for parameter: toolsLayer. The map was not imported.')
    errors = true
  }
  if (errors) return

    var map = {}
      , graffle = libxmljs.parseXmlString(fs.readFileSync(graffleDocPath, 'utf8'))
      , graffleGraphicsElm = graffle.get('//key[text()="GraphicsList"]/following-sibling::array')
      , graffleConceptElms = graffleGraphicsElm.find('./dict' + 
                                                   '[child::key[text()="Layer"]/following-sibling::integer[text()="'+conceptNodeLayer+'"]]' +
                                                   '[child::key[text()="Class"]/following-sibling::string[text()="ShapedGraphic"]]' +
                                                   '[child::key[text()="Shape"]/following-sibling::string[text()="Rectangle"]]')
      , graffleLinkElms = graffleGraphicsElm.find('./dict' + 
                                                '[child::key[text()="Layer"]/following-sibling::integer[text()="'+conceptNodeLayer+'"]]' +
                                                '[child::key[text()="Class"]/following-sibling::string[text()="LineGraphic"]]')
      , svg = libxmljs.parseXmlString(fs.readFileSync(svgDocPath, 'utf8'))
      , svgConceptElms = svg.find('./g/g[child::title[text()="Concepts"]]/g[@id]')
      , svgToolElms = svg.find('./g/g[child::title[text()="Tool Coverage"]]/g[@id]')


    // function takes text element, orders tspans inside it according to positon, joins text inside tspans into single string
    // N.B. line-breaks on map are automated and are thus ignored below
    function svgTextString(textEl) {
      var orderedTSpans = textEl.find('./tspan').sort(function(a,b) {
        var ax = parseFloat(a.attr('x').value()) || 0
          , ay = parseFloat(a.attr('y').value()) || 0
          , bx = parseFloat(b.attr('x').value()) || 0
          , by = parseFloat(b.attr('y').value()) || 0

        return ay == by ? ax - bx : ay - by
      })

      var s = ''
        , i = 0
        , tspan, lastY, y


      while (tspan = orderedTSpans[i++]) {
        y = tspan.attr('y').value()
        if (y != lastY) {
          if (lastY) s += '\n' // temporarily insert newline
            lastY = y
        }
        s += tspan.text()
      }

      // replace newlines without an adjacent space with a space. Replace newlines with an adjacent space with nothing.
      return s.replace(/(\S)\n(\S)/g, '$1 $2').replace(/\n/g, '')
    }

    function getGraffleElmId(el) {
      return parseInt(el.get('./key[text()="ID"]/following-sibling::*').text())
    } 

    // finds rectangles on the tools layer that overlap with the concept node
    function conceptToolsText(svgConceptElm) {
      var cRect = svgConceptElm.get('./rect')
        , cl = parseFloat(cRect.attr('x').value())
        , ct = parseFloat(cRect.attr('y').value())
        , cr = parseFloat(cRect.attr('width').value()) + cl
        , cb = parseFloat(cRect.attr('height').value()) + ct


      var toolElms = _.filter(svgToolElms, function(elm) {
        var tRect = elm.get('./rect')
          , tl = parseFloat(tRect.attr('x').value())
          , tt = parseFloat(tRect.attr('y').value())
          , tr = parseFloat(tRect.attr('width').value()) + tl
          , tb = parseFloat(tRect.attr('height').value()) + tt

        return tl <= cr && tr >= cl && tt <= cb && tb >= ct
      })

      return _.map(toolElms, function(elm) {
        return 'Suggested Tool: ' + svgTextString(elm.get('./text'))
      }).join('\n\n')
    }

    map.nodes = _.filter(
      _.map(graffleConceptElms, function(el) {
      var bounds = el.get('./key[text()="Bounds"]/following-sibling::string').text().match(/\d+\.\d+?/g)
        , coordMultiplier = 1.6 // used to generate additional spacing between nodes enabling consistent automated sizing of node description rectangles without overlap
        , nodeId = getGraffleElmId(el)
        , svgEl = _.find(svgConceptElms, function(elm) { return elm.attr('id').value() == 'id'+nodeId+'_Graphic' })
        , desc = svgEl && svgTextString(svgEl.get('./text'))
        , syallabusTags
        , toolsText


      if (!svgEl) {
        console.log('NO CORRESPONDING NODE IN SVG IMPORT. NODE NOT IMPORTED\t graffle node id:%d\t svgElSelector: %s', nodeId, svgElSelector)
      } else {
        toolsText = conceptToolsText(svgEl)
        ccssTagRegExp = new RegExp(/\([0-9a-z]+\.[a-z]+\.[0-9a-z]+\)/ig)
        syllabusTags = _.map(desc.match(ccssTagRegExp), function(ccssCode) { return 'CCSS:' + ccssCode.match(/[^()]+/)[0] })
        desc = desc.replace(ccssTagRegExp, '')
      }

      return {
        id: nodeId // will be overwritten with db uuid
        , graffleId: nodeId
        , nodeDescription: desc
        , x: parseInt(coordMultiplier * (parseFloat(bounds[0]) + 0.5 * parseFloat(bounds[2]))) // x coord + half width = horizontal centre
        , y: parseInt(coordMultiplier * (parseFloat(bounds[1]) + 0.5 * parseFloat(bounds[3]))) // y coord + half height = vertical centre
        , valid: svgEl != undefined
        , tags: syllabusTags
        , notes: toolsText

      }
    })
    , function(n) { return n.valid }
    )

    // position leftmost node at x=0, topmost node at y=0. Not important.
    var minX = _.min(map.nodes, function(n) { return n.x }).x
    var minY = _.min(map.nodes, function(n) { return n.y }).y
    _.each(map.nodes, function(n) {
      n.x -= minX
      n.y -= minY
    })

    var prerequisitePairs = _.filter(
      _.map(graffleLinkElms
            , function(el) {
              var tailId = getGraffleElmId(el.get('./key[text()="Tail"]/following-sibling::dict'))
                , headId = getGraffleElmId(el.get('./key[text()="Head"]/following-sibling::dict'))

              return [ tailId, headId ]
            })
            , function(pair) { 
              var tail = _.find(map.nodes, function(n) { return n.id == pair[0] })
                , head = _.find(map.nodes, function(n) { return n.id == pair[1] })


              if (tail == undefined || head == undefined) {
                console.log('INVALID LINK - NODE(S) NOT FOUND. LINK NOT IMPORTED.\t Tail Id="%s" is %s.\t Head Id="%s" is %s', pair[0], (tail ? 'valid' : 'invalid'), pair[1], (head ? 'valid' : 'invalid'))
              }

              return tail != undefined && head != undefined
            }
    )

    console.log('\nvalid nodes: %d', map.nodes.length)
    console.log('valid prerequisite pairs: %d', prerequisitePairs.length)

    if (dummyRun === true) return

      //insert into db
      if (map.nodes.length) {
        queryView('relations-by-name', 'key', 'Prerequisite', function(e,r,b) {
          var rows = r.statusCode == 200 && JSON.parse(b).rows
            , prereqId = rows && rows.length && rows[0].id
            , prereqRev = rows && rows.length && rows[0].rev

          if (!prereqId) {
            console.log('error - could not retrieve prerequisite relation. \ne:"%s", \nstatusCode:%d, \nb:"%s"', e, r.statusCode, b)
            return
          }

          ;(function insertCNode(nodeIndex) {
            console.log('inserting concept node at index:%d of array length:%d', nodeIndex, map.nodes.length)

            var node = map.nodes[nodeIndex]
            delete node.id
            delete node.valid
            insertConceptNode(node, function(e,statusCode,conceptNode) {
              if (statusCode != 201) {
                console.log('error inserting concept node: (e:"%s", statusCode:%d, b:"%s")', e, r.statusCode, b)
                return
              }

              _.each(prerequisitePairs, function(pair) {
                if (pair[0] == node.graffleId) pair[0] = conceptNode._id
                  else if (pair[1] == node.graffleId) pair[1] = conceptNode._id
              })

          if (++nodeIndex < map.nodes.length) {
            insertCNode(nodeIndex)
          } else if (prerequisitePairs.length) {

            ;(function insertPrerequisitePair(pairIndex) {
              console.log('inserting prerequisite pair at index:%d of array length:%d', pairIndex, prerequisitePairs.length)

              var pair = prerequisitePairs[pairIndex]
              addOrderedPairToBinaryRelation(prereqId, prereqRev, pair[0], pair[1], function(e,statusCode,rev) {
                if (201 != statusCode) {
                  console.log('error inserting prerequisite relation member: (e:"%s", statusCode:%d)', e, statusCode)
                  var nodes = _.filter(map.nodes, function(n) { return n.id == pair[0] || n.id == pair[1] })
                  return
                }
                prereqRev = rev
                if (++pairIndex < prerequisitePairs.length) {
                  insertPrerequisitePair(pairIndex)
                } else {
                  console.log('all prerequisite pairs inserted')
                }
              })
            })(0)
          } else {
            console.log('no prerequisite links on graph')
            return
          }
            })
          })(0)
        })
      } else {
        console.log('no concept nodes on graph.')
        return
      }
}

function createDB(callback) {
  request({
    method: 'PUT'
    , uri: databaseURI
    , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
  }
  , function(e,r,b) {
    // ********
    // Don't do anything more - get everything else from pull replication
    callback(e,r,b)
    return
    // ********

    var insertDBCallbackArgs = [e,r,b]

    if (r.statusCode == 412) {
      console.log('could not create database. Database with name "%s" already exists', databaseURI)
      updateDesignDoc(callback)
    } else if (r.statusCode != 201) {
      if (!e) e = 'error creating database'
      if ('function' == typeof callback) callback(e,r,b)
      else console.log('%s with uri "%s". StatusCode=%d', e, databaseURI, r.statusCode)
      return
    } else {
      console.log('created database at uri:\nupdate views...', databaseURI)
      updateDesignDoc(function(e,r,b) {
        console.log('insert "Prerequisite" binary relation...')
        insertBinaryRelation('Prerequisite', 'is a prerequisite of', function(e,statusCode,b) {
          if ('function' == typeof callback) {
            if (statusCode == 201) callback.apply(null, insertDBCallbackArgs)
            else callback('eror inserting prerequisite relation', statusCode)
          }
        })
      })
    }
  })
}

function pullReplicate(source, filter, continuous, cancel) {
  var target = kcmDatabaseName.replace(/\/$/,'')

  filter = filter || undefined
  continuous = continuous === true
  cancel = cancel === true

  console.log('-----\nPULL REPLICATION\nsource: %s\ntarget: %s\nfilter: %s\ncontinuous: %s\ncancel: %s\n-----', source, target, filter, continuous, cancel)

  request({
    method:'POST'
    , uri: couchServerURI + '_replicate'
    , headers: { 'content-type':'application/json', accepts:'application/json' }
    , body: JSON.stringify({ source:source, target:target, filter:filter, continuous:continuous===true, cancel:cancel===true })
  }, function() {
    console.log(arguments, '\n\n\n')
  })
}

function updateExportSettings(updatedSettings, callback) {
  if (!updatedSettings || 'ExportSettings' != updatedSettings.type) {
    callback('invalid export settings document', 500)
    return
  }
  updateDoc(updatedSettings, function(e,r,b) {
    if (201 != r.statusCode) {
      callback(util.format('error updating export settings. Database reported error: "%s"', e), r.statusCode)
      return
    }
    callback(null,201,JSON.parse(b).rev)
  })
}

function updateViewSettings(updatedSettings, callback) {
  if (!updatedSettings || 'ViewSettings' != updatedSettings.type) {
    callback('invalid view settings document', 500)
    return
  }
  updateDoc(updatedSettings, function(e,r,b) {
    if (201 != r.statusCode) {
      callback(util.format('error updating view settings. Database reported error: "%s"', e), r.statusCode)
      return
    }
    callback(null,201,JSON.parse(b).rev)
  })
}

// Views
function updateDesignDoc(callback) {
  console.log('Authoring -> kcm\t\tupdating design doc:\t%s_design/', databaseURI, designDoc)

  var kcmViews = {
    _id: '_design/' + designDoc
    , views: {
      'any-by-type-name': {
        map: (function (doc) { if (doc.type && doc.name) emit([doc.type, doc.name], null); }).toString()
      }
      , 'by-type': {
        map: (function(doc) { if (doc.type) emit(doc.type, null) }).toString()
        , reduce: (function(keys, values, rereduce) {
          if (!rereduce) return values.length
            return sum(values)
        }).toString()
      }
      , 'by-user-type': {
        map: (function(doc) { if (doc.type && doc.user) emit([doc.user,doc.type], null) }).toString()
      }
      , 'users-by-credentials': {
        map: (function(doc) { if ('User' == doc.type) emit([doc.loginName, doc.password], null) }).toString()
      }
      , 'concept-nodes': {
        map: (function(doc) { if (doc.type == 'concept node') emit(doc._id, null) }).toString()
      }
      , 'concept-nodes-by-graffle-id': {
        map: (function(doc) { if (doc.type == 'concept node') emit(doc.graffleId, null) }).toString()
      }
      , 'concept-nodes-by-pipeline': {
        map: (function(doc) {
          if (doc.type == 'concept node') {
            var len = doc.pipelines.length
            for (var i=0; i<len; i++) emit (doc.pipelines[i], doc._id)
          }
        }).toString()
      }
      , 'concept-node-pipelines': {
        map: (function(doc) {
          if (doc.type == 'concept node') {
            var len = doc.pipelines.length
            for (var i=0; i<len; i++) emit (doc._id, doc.pipelines[i])
          }
        }).toString()
      }

      // Reduce off / Group-Level >= 2:   key: <tag>          value: <node_id>
      // Group-Level 1:                   key: <tag>          value: <total_number_of_instances_of_tag>
      // Group-Level 0:                   key: null           value: <total_number_of_tags>
      , 'concept-node-tags': {
        map: (function(doc) {
          if ('concept node' == doc.type) {
            doc.tags.forEach(function(tag) { emit(tag, doc._id) })
          }
        }).toString()
        , reduce: (function(keys, values, rereduce) {
          if (!rereduce) return values.length
            else return sum(values)
        }).toString()
      }

      , 'pipelines-by-name': {
        map: (function(doc) { if (doc.type == 'pipeline') emit(doc.name, null) }).toString()
      }
      , 'pipeline-problems': {
        map: (function(doc) {
          if (doc.type == 'pipeline') {
            var len = doc.problems.length
            for (var i=0; i<len; i++) emit (doc._id, doc.problems[i])
          }
        }).toString()
      }

      // N.B. only emits results for pipelines with >=1 problem
      // Reduce off:                  key: [<pipeline_name>, <pipeline_id>, <pipeline_rev>]       value: <pipeline_problems>
      // Group-Level >=2:             key: [<pipeline_name>, <pipeline_id>, <pipeline_rev>]       value: <num_problems_in_pipeline>
      // Group-Level 1:               key: [<pipeline_name>, <pipeline_rev>]                      value: <num_problems_in_pipelines_of_name>
      // group-level none:            key: null,                                                  value: <total_number_of_problems_in_all_pipelines>
      , 'pipelines-with-problems-by-name': {
        map: (function(doc) {
          if (doc.type == 'pipeline' && doc.problems.length) {
            emit([doc.name, doc._id, doc._rev, doc.workflowStatus], doc.problems)
          }
        }).toString()
        , reduce: (function(keys, values, rereduce) {
          if (!rereduce) {
            var numProblems = 0
            , i, len = values.length

            for (i=0; i<len; i++) {
              numProblems += values[i].length
            }
            return numProblems
          } else {
            return sum(values)
          }
        }).toString()
      }

      , 'problems-by-tool': {
        map: (function(doc) { if (doc.type == 'problem') emit(doc.toolId, null) }).toString()
      }
      , 'relations-by-name': {
        map: (function(doc) { if (doc.type == 'relation') emit(doc.name, null) }).toString()
      }
      , 'relations-by-relation-type': {
        map: (function(doc) { if (doc.type == 'relation') emit(doc.relationType, null) }).toString()
      }
      , 'relations-by-relation-type-name': {
        map: (function(doc) { if (doc.type == 'relation') emit([doc.relationType, doc.name], null) }).toString()
      }
      , 'tools-by-name': {
        map: (function(doc) { if (doc.type == 'tool') emit(doc.name, null) }).toString()
      }
      , 'names-types-by-id': {
        map: (function(doc) { emit(doc._id, [doc.name, doc.type]) }).toString()
      }
      , 'problem-descriptions': {
        map: (function(doc) { if (doc.type == 'problem') emit(doc._id, doc.problemDescription) }).toString()
      }
      , 'problem-descriptions-and-notes': {
        map: (function(doc) { if (doc.type == 'problem') emit(doc._id, [doc.problemDescription, doc.problemNotes]) }).toString()
      }
      , 'problems-by-description': {
        map: (function(doc) { if (doc.type == 'problem') emit(doc.problemDescription, null) }).toString()
      }
    }
  }

  getDoc(kcmViews._id, function(e,r,b) {
    var sc = r.statusCode
    , writeViewsFunc = sc == 200 ? updateDoc : insertDoc

    if (200 != sc && 404 != sc) {
      callback('error retrieving design doc from database', sc)
      return
    }

    if (200 == sc) kcmViews._rev = JSON.parse(b)._rev

      writeViewsFunc(kcmViews, function(e,r,b) {
        callback(e, r.statusCode)
      })
  })
}

function queryView(view) {
  var uri = util.format('%s_design/%s/_view/%s', databaseURI, designDoc, view)
  var callbackIx = arguments.length - 1
  var callback = arguments[callbackIx]

  if ('function' != typeof callback) return

  for (var i=1; i<callbackIx; i+=2) {
    uri += i == 1 ? '?' : '&'

    var field = arguments[i]
    if ('string' != typeof field) {
      callback(util.format('argument at index %d is not a string.', i), 412)
      return
    }
    uri += field + '='

    var value = arguments[i+1]
    uri += (function rec(val) {
      var valType = Object.prototype.toString.call(val).match(/^\[object ([a-z]+)\]$/i)[1]

      switch (valType) {
        case 'Null':
          case 'Undefined':
          return 'null'
        case 'Number':
          case 'Boolean':
          return val.toString()
        case 'String':
          return '%22' + encodeURIComponent(val) + '%22'
        case 'Array':
          return '%5B' + _.map(val, function(el) { return rec(el) }).join(',') + '%5D'
        case 'Object':
          // Empty object comes after everything else, so is used for endkey. e.g. startkey=['A']&endkey=['A',{}] will return all keys with 'A' as first element
          return Object.keys(val).length === 0 ? '%7B%7D' : '#'
        default:
          return '#'
      }
    })(value)

    if (~uri.indexOf('#')) {
      callback(util.format('argument at index %d is invalid.', i+1), 412)
      return
    }
  }
  request.get(uri, callback)
}

function insertProblem(plist, callback) {
  getProblemInfoFromPList(plist, function(e, plistString, problemDescription, toolId, internalDescription) {
    if (e) {
      callback(e)
      return
    }
    request({ method:'GET', uri:couchServerURI+'_uuids' }, function(e,r,b) {
      if (r.statusCode != 200) {
        callback(util.format('could not generate uuid -- Database callback: (error:%s, statusCode:%d, body:%s)', e, r.statusCode, b), r.statusCode)
        return
      }
      var now = (new Date()).toJSON()

      request({
        method: 'PUT'
        , uri: databaseURI + JSON.parse(b).uuids[0]
        , multipart: [
          { 'Content-Type': 'application/json'
            , 'body': JSON.stringify({
              type: 'problem'
              , problemDescription: problemDescription
              , internalDescription: internalDescription
              , problemNotes: ''
              , toolId: toolId
              , dateCreated: now
              , dateModified: now
              , assessmentCriteria: []
              , _attachments: {
                'pdef.plist': {
                  follows: true
                  , length: plistString.length
                  , 'Content-Type': 'application/xml'
                }
              }
            })
          }
          , { 'body': plistString }
        ]
      }
      , function(e,r,b) {
        if (r.statusCode != 201) {
          if (typeof callback == 'function') callback(util.format('Error uploading problem to database\n -- Database callback: (error:"%s", statusCode:%d, body:"%s")', e, r.statusCode, b), r.statusCode)
          return
        }
        callback(null, 201, JSON.parse(b))
      })
    })
  })
}

function getProblemInfoFromPList(plist, callback) {
  fs.readFile(plist, 'utf8', function(e, plistString) {
    if (e) {
      if (typeof callback == 'function') callback('could not load plist. error: ' + e)
      return
    }

    var matchMETA_QUESTION = plistString.match(/<key>META_QUESTION<\/key>/i)
      , matchValMETA_QUESTION_TITLE = plistString.match(/META_QUESTION_TITLE<\/key>\s*<string>([^<]+)/i)
      , matchValTOOL_KEY = plistString.match(/TOOL_KEY<\/key>\s*<string>([^<]+)/i)
      , matchValPROBLEM_DESCRIPTION = plistString.match(/PROBLEM_DESCRIPTION<\/key>\s*<string>([^<]+)/i)
      , matchValINTERNAL_DESCRIPTION = plistString.match(/INTERNAL_DESCRIPTION<\/key>\s*<string>([^<]+)/i)
      , matchNUMBER_PICKER = plistString.match(/<key>NUMBER_PICKER<\/key>/i)
      , matchValNUMBER_PICKER_DESCRIPTION = plistString.match(/NUMBER_PICKER_DESCRIPTION<\/key>\s*<string>([^<]+)/i)
      , isMetaQuestion = matchMETA_QUESTION && matchMETA_QUESTION.length > 0
      , isNumberPicker = matchNUMBER_PICKER && matchNUMBER_PICKER.length > 0
      , toolName = (matchValTOOL_KEY && matchValTOOL_KEY.length > 1 && matchValTOOL_KEY[1]) || ((isMetaQuestion || isNumberPicker) && 'NONE') // Meta Questions optionally have TOOL_KEY. Other questions must have TOOL_KEY
      , internalDescription = matchValINTERNAL_DESCRIPTION && matchValINTERNAL_DESCRIPTION.length > 1 && matchValINTERNAL_DESCRIPTION[1] || ''
      , problemDescription


    if (isMetaQuestion) {
      problemDescription = matchValMETA_QUESTION_TITLE && matchValMETA_QUESTION_TITLE.length > 1 && matchValMETA_QUESTION_TITLE[1]
    } else if (isNumberPicker) {
      problemDescription = matchValNUMBER_PICKER_DESCRIPTION && matchValNUMBER_PICKER_DESCRIPTION.length > 1 && matchValNUMBER_PICKER_DESCRIPTION[1]
    } else {
      problemDescription = matchValPROBLEM_DESCRIPTION && matchValPROBLEM_DESCRIPTION.length > 1 && matchValPROBLEM_DESCRIPTION[1]
    }

    if (!toolName || !problemDescription) {
      if (typeof callback == 'function') callback(
        'invalid plist - missing values for keys: '
          + (toolName ? '' : ' TOOL_KEY')
        + (toolName || problemDescription ? '' : ' and')
        + (problemDescription ? '' : (isMetaQuestion ? ' META_QUESTION_TITLE' : (toolName == 'NUMBER_PICKER' ? 'NUMBER_PICKER_DESCRIPTION' : ' PROBLEM_DESCRIPTION')))
      )
      return
    }
    queryView('tools-by-name', 'key', toolName, function(e,r,b) {
      var rows = !e && r && r.statusCode == 200 && JSON.parse(b).rows
      , toolId = rows && rows.length && rows[0].id


      if (toolId || isMetaQuestion || isNumberPicker) callback(null, plistString, problemDescription, toolId, internalDescription)
      else callback(util.format('invalid plist. Could not retrieve tool with name "%s". -- Database callback: (error:%s, statusCode:%d)', toolName, e, r.statusCode), r.statusCode)
    })
  })
}

function insertConceptNode(user, o, callback) {
  var errors = ''
  if (typeof o.nodeDescription != 'string' || !o.nodeDescription.length) errors += 'String value required for "nodeDescription". "'
  if (isNaN(o.x)) errors += 'Float value required for "x". '
  if (isNaN(o.y)) errors += 'Float value required for "y". '

  if (errors.length) {
    callback('Error. Could not create concept node. Bad argument. ' + errors, 500)
    return
  }

  var doc = firstVersion(user, 'insertConceptNode', null)
  doc.nodeDescription = o.nodeDescription
  doc.x = o.x
  doc.y = o.y
  doc.tags = o.tags || []
  doc.type = 'concept node'
  doc.pipelines = []

  insertDoc(doc, function(e,r,b) {
    if (!r || 201 != r.statusCode) {
      e = util.format('Error inserting concept node. Database reported error:"%s"', e)
    }
    callback(e, r && r.statusCode)
  })
}

function deleteConceptNode(user, id, rev, callback) {
  var node = kcm.getDocClone(id, 'concept node')
  var relUpdates = []
  var relations = _.chain(kcm.docStores.relations)
    .values()
    .filter(function(r) { return r.relationType == 'binary' })
    .map(function(r) { return JSON.parse(JSON.stringify(r)) })
    .value()

  if (!node) {
    callback(util.format('concept node with id="%s" not found. The concept node was not deleted.', id), 404)
    return
  }

  if (node._rev != rev) {
    callback(util.format('Concept node revisions do not correspond. supplied:"%s", database:"%s". The concept node was not deleted.', rev, node._rev), 409)
    return
  }

  if (node.pipelines.length) {
    callback('Concept node contains pipelines. The concept node was not deleted.', 412)
    return
  }

  relations.forEach(function(r) {
    var pair
      , deletedPairs = []

    for (var i=0, numPairs=r.members.length; i<numPairs; i++) {
      if (~r.members[i].indexOf(id)) {
        deletedPairs.push(r.members[i])
        r.members.splice(i,1)
        numPairs--
      }
    }

    if (deletedPairs.length) {
      nextVersion(r, user, 'deleteConceptNode:'+id, deletedPairs)
      relUpdates.push(r)
    }
  })

  nextVersion(node, user, 'deleteConceptNode')
  node._deleted = true
      
  request({
    method:'POST'
    , uri: databaseURI + '_bulk_docs'
    , headers: { 'content-type':'application/json', accepts:'application/json' }
    , body: JSON.stringify({ docs:relUpdates.concat(node) })
  }, function(e,r,b) {
    if (!r || 201 != r.statusCode) {
      e = util.format('Error deleting concept node / updating binary relations featuring concept node. Database reported error: "%s"', e)
    }
    callback(e, r && r.statusCode)
  })
}

function updateConceptNodePosition(user, id, rev, x, y, callback) {
  var node = kcm.getDocClone(id, 'concept node')

  if (isNaN(x) || isNaN(y)) {
    callback(util.format('BAD ARGS: numeric values required for x and y. Supplied: "%s" and "%s". The concept node position was not updated.', x, y), 412)
    return
  }

  if (!node) {
    callback(util.format('concept node with id="%s" not found. The concept node position was not updated.', id), 404)
    return
  }

  if (node._rev != rev) {
      callback(util.format('concept node revisions do not correspond. supplied:"%s", database:"%s". The concept node position was not updated.', rev, node._rev), 409)
      return
  }

  node.x = parseInt(x, 10)
  node.y = parseInt(y, 10)

  nextVersion(node, user, 'updateConceptNodePosition')

  updateDoc(node, function(e,r,b) {
    if (!r || r.statusCode != 201) e = util.format('Error updating concept node position. Database Error: "%s"', e)
      callback(e, r && r.statusCode || null, JSON.parse(b).rev)
  })
}

function updateConceptNodeDescription(user, id, rev, desc, callback) {
  if ('string' != typeof(desc) || !desc.length) {
    callback('BAD ARGS: string required for concept node description', 412)
    return
  }

  var cn = kcm.getDocClone(id, 'concept node')

  if (!cn) {
    callback(util.format('Error: Concept node id="%s" was not found. The concept node description was not updated.', id), 404)
    return
  }

  if (cn._rev !== rev) {
    callback(util.format('Error: Concept node revisions do not correspond. Supplied:"%s", Database:"%s". The concept node description was not updated', rev, cn._rev), 409)
    return
  }

  nextVersion(cn, user, 'updateConceptNodeDescription')
  cn.nodeDescription = desc

  updateDoc(cn, function(e,r,b) {
    if (!r || 201 != r.statusCode) e = util.format('Error updating concept node description. Database Error: "%s"', e)
    callback(e, r && r.statusCode)
  })
}

function insertConceptNodeTag(user, conceptNodeId, conceptNodeRev, tagIndex, tagText, callback) {
  if ('string' != typeof tagText) {
    callback('tag supplied is not a string', 412)
    return
  }

  if (tagIndex && !/^\d+$/.test(tagIndex)) {
    callback(util.format('Integer required for tagIndex, %s supplied. The tag was not added', tagIndex), 412)
    return
  }

  var cn = kcm.getDocClone(conceptNodeId, 'concept node')

  if (!cn) {
    callback(util.format('Error: Concept node id="%s" was not found. The tag was not added.', conceptNodeId), 404)
    return
  }

  if (cn._rev !== conceptNodeRev) {
    callback(util.format('Error: Concept node revisions do not correspond. Supplied:"%s", Database:"%s". The tag was not added', conceptNodeRev, cn._rev), 409)
    return
  }

  if (~cn.tags.indexOf(tagText)) {
    callback(util.format('Error: Concept node with id="%s" already contains tag="%s". The tag was not added.', conceptNodeId, tagText), 412)
    return
  }

  // TODO: This is ad hoc consideration of mastery tags in combo with prereqs. Genericise
  if ('mastery' == tagText) {
    for (var id in kcm.docStores.relations) {
      if (kcm.docStores.relations[id].name == 'Prerequisite') {
        if (_.find(kcm.docStores.relations[id].members, function(pair) { return ~pair.indexOf(conceptNodeId) })) {
          callback('node cannot be tagged mastery because it features in a "Prerequisite" relationship', 412)
          return
        }
        break;
      }
    }
  }

  nextVersion(cn, user, 'insertConceptNodeTag', tagText)

  if (tagIndex < cn.tags.length) {
    cn.tags.splice(tagIndex, 0, tagText)
  } else {
    cn.tags.push(tagText)
  }

  updateDoc(cn, function(e,r,b) {
    callback(e, r && r.statusCode)
  })
}

function deleteConceptNodeTag(user, conceptNodeId, conceptNodeRev, tagIndex, tagText, callback) {
  if ('string' != typeof tagText) {
    callback('tag supplied is not a string - tag was not deleted', 412)
    return
  }
  if (!/^\d+$/.test(tagIndex)) {
    callback('tagIndex is not an integer - tag was not deleted', 412)
    return
  }

  var cn = kcm.getDocClone(conceptNodeId, 'concept node')

  if (!cn) {
    callback(util.format('Error: Concept node id="%s" was not found. The tag was not deleted.', conceptNodeId), 404)
    return
  }

  if (cn._rev !== conceptNodeRev) {
    callback(util.format('Error: Concept node revisions do not correspond. Supplied:"%s", Database:"%s". The tag was not deleted', conceptNodeRev, cn._rev), 409)
    return
  }

  if (!cn.tags || cn.tags[tagIndex] != tagText) {
    callback(util.format('tag with text="%s" not found at index=%d on concept node with id="%s". The tag was not deleted.', tagText, tagIndex, conceptNodeId), 412)
  }

  // TODO: This is ad hoc consideration of mastery tags in combo with prereqs. Genericise
  if ('mastery' == tagText) {
    for (var id in kcm.docStores.relations) {
      if (kcm.docStores.relations[id].name == 'Mastery') {
        if (_.find(kcm.docStores.relations[id].members, function(pair) { return pair[1] == conceptNodeId })) {
          callback(util.format('Could not remove tag "mastery" from concept node id="%s" as it is the mastery node of another concept node', conceptNodeId), 412)
          return
        }
        break;
      }
    }
  }

  nextVersion(cn, user, 'deleteConceptNodeTag', tagText)
  cn.tags.splice(tagIndex, 1)

  updateDoc(cn, function(e,r,b) {
    callback(e, r && r.statusCode)
  })
}

function editConceptNodeTag(user, conceptNodeId, conceptNodeRev, tagIndex, currentText, newText, callback) {
  if ('string' != typeof newText) {
    callback('tag supplied is not a string - tag was not edited', 412)
    return
  }
  if (!/^\d+$/.test(tagIndex)) {
    callback('tagIndex is not an integer - tag was not edited', 412)
    return
  }

  var cn = kcm.getDocClone(conceptNodeId, 'concept node')

  if (!cn) {
    callback(util.format('Error: Concept node id="%s" was not found. The tag was not edited.', conceptNodeId), 404)
    return
  }

  if (cn._rev !== conceptNodeRev) {
    callback(util.format('Error: Concept node revisions do not correspond. Supplied:"%s", Database:"%s". The tag was not edited', conceptNodeRev, cn._rev), 409)
    return
  }

  if (!cn.tags || cn.tags[tagIndex] != currentText) {
    callback(util.format('tag with text="%s" not found at index=%d on concept node with id="%s". The tag was not edited.', currentText, tagIndex, conceptNodeId), 412)
  }

  // TODO: This is ad hoc consideration of mastery tags in combo with prereqs. Genericise
  if ('mastery' == currentText) {
    for (var id in kcm.docStores.relations) {
      if (kcm.docStores.relations[id].name == 'Mastery') {
        if (_.find(kcm.docStores.relations[id].members, function(pair) { return pair[1] == conceptNodeId })) {
          callback(util.format('Could not remove tag "mastery" from concept node id="%s" as it is the mastery node of another concept node', conceptNodeId), 412)
          return
        }
        break;
      }
    }
  }
  if ('mastery' == newText) {
    for (var id in kcm.docStores.relations) {
      if (kcm.docStores.relations[id].name == 'Prerequisite') {
        if (_.find(kcm.docStores.relations[id].members, function(pair) { return ~pair.indexOf(conceptNodeId) })) {
          callback('node cannot be tagged mastery because it features in a "Prerequisite" relationship', 412)
          return
        }
        break;
      }
    }
  }

  nextVersion(cn, user, 'editConceptNodeTag', newText)
  cn.tags.splice(tagIndex, 1, newText)

  updateDoc(cn, function(e, r, b) {
    callback(e, r && r.statusCode)
  })
}

function insertBinaryRelation(name, description, callback) {
  if ('string' != typeof name) {
    if ('function' == typeof callback) callback('could not insert relation - name required', 500)
    return
  }
  if ('string' != typeof description) {
    if ('function' == typeof callback) callback('could not insert relation - description required', 500)
    return
  }

  queryView('relations-by-name', 'key', o.name, function(e,r,b) {
    if (200 == r.statusCode) {
      if ('function' == typeof callback) callback(util.format('could not insert relation - a relation with name "%s" already exists', name), 409)
      return
    }
    insertDoc({
      type:'relation'
      , relationType: 'binary'
      , name: name
      , relationDescription: description
      , members: []
    }, function(e,r,b) {
      if (201 != r.statusCode) {
        if ('function' == typeof callback) callback(util.format('Error inserting binary relation. Database Error: "%s"', e), r.statusCode)
        return
      }
      callback(null,201,b)
    })
  })
}

function addOrderedPairToBinaryRelation(user, relationId, relationRev, cn1Id, cn2Id, callback) {
  var noStringErrors = []
  if (typeof relationId != 'string' || !relationId.length) noStringErrors.push('relationId')
  if (typeof relationRev != 'string' || !relationRev.length) noStringErrors.push('relationRev')
  if (typeof cn1Id != 'string' || !cn1Id.length) noStringErrors.push('cn1Id')
  if (typeof cn2Id != 'string' || !cn2Id.length) noStringErrors.push('cn2Id')
  if (noStringErrors.length) {
    callback('BAD ARGS - strings required for: ' + noStringErrors.join(), 412)
    return
  }

  var r = kcm.getDocClone(relationId, 'relation')
  var cn1 = kcm.getDocClone(cn1Id, 'concept node')
  var cn2 = kcm.getDocClone(cn2Id, 'concept node')

  if (!r) {
    callback(util.format('could not find relation with id="%s"', relationId), 404)
    return
  }

  if (!r.relationType == 'binary') {
    callback(util.format('relation with id="%s" has relationType="%s". Binary required', relationId, r.relationType), 412)
    return
  }

  if (r._rev != relationRev) {
    callback(util.format('relation revisions do not correspond. supplied:"%s", database:"%s"', relationRev, r._rev), 409)
    return
  }

  if (_.find(r.members, function(p) { p[0] == cn1Id && p[1] == cn2Id })) {
    callback(util.format('ordered pair ["%s", "%s"] is already a member of binary relation id="%s"', cn1Id, cn2Id, relationId), 412)
    return
  }

  if (!cn1) {
    callback(util.format('could not find concept node corresponding to cn1Id="%s"', cn1Id), 412)
    return
  }

  if (!cn2) {
    callback(util.format('could not find concept node corresponding to cn2Id="%s"', cn2Id), 412)
    return
  }

  // TODO: This block is an ugly ad hoc hack, get generic asap
  if (r.name == 'Prerequisite') {
    // prerequisite relationship does not apply to mastery nodes. (See InterMastery relation)
    if (~cn1.tags.indexOf('mastery') || ~cn2.tags.indexOf('mastery')) {
      callback('One or both concept nodes are tagged "mastery". The prerequisite relationship is invalid for mastery nodes.', 412)
      return
    }

    // need to ensure that creation of new link is not going to implicitly create bi-directional InterMastery link between mastery nodes of cn1 and cn2
    var mastery = kcm.cloneRelationNamed('Mastery')
    if (!mastery) {
      callback('Could not retrieve relation named "Mastery" to check for illegal implicit creation of bi-directional InterMastery link between mastery nodes', 500)
      return
    }
    
    var cn1MasteryLink = _.find(mastery.members, function(l) { return l[0] == cn1Id })
      , cn2MasteryLink = _.find(mastery.members, function(l) { return l[0] == cn2Id })
      , cn1MasteryId = cn1MasteryLink && cn1MasteryLink[1]
      , cn2MasteryId = cn2MasteryLink && cn2MasteryLink[1]

    // unless cn1 and cn2 have distinct mastery nodes, no chance of implicit creation of bi-directional InterMastery link
    if (cn1MasteryId && cn2MasteryId && cn1MasteryId != cn2MasteryId) {
      var cn1MasteryChildren = mastery.members.filter(function(p) { return p[1] == cn1MasteryId }).map(function(p) { return p[0] })
      var cn2MasteryChildren = mastery.members.filter(function(p) { return p[1] == cn2MasteryId }).map(function(p) { return p[0] })
      var oppDirIMLink = _.find(r.members, function(p) { return ~cn2MasteryChildren.indexOf(p[0]) && ~cn1MasteryChildren.indexOf(p[1]) })

      if (oppDirIMLink) {
        callback(util.format('Prerequisite relationship would implicitly create illegal bi-directional InterMastery link between mastery nodes with ids "%s" and "%s"', cn1MasteryId, cn2MasteryId), 412)
        return
      }
    }
  } else if (r.name == 'Mastery') {
    if (~cn1.tags.indexOf('mastery') || !~cn2.tags.indexOf('mastery')) {
      callback('Invalid "mastery" relationship. Link tail node MUST NOT be tagged "mastery" and link head node MUST be tagged "mastery"', 412)
      return
    }

    if (_.find(r.members, function(p) { return p[0] == cn1Id })) {
      callback(util.format('node id="%s" already has a mastery node', cn1Id), 412)
      return
    }

    var prereq = kcm.cloneRelationNamed('Prerequisite')
    if (!prereq) {
      callback('Could not retrieve relation named "Prerequisite" to check for illegal implicit creation of bi-directional InterMastery link between mastery nodes', 500)
      return
    }

    var interMastery = kcm.cloneRelationNamed('InterMastery')
    if (!interMastery) {
      callback('Could not retrieve relation named "InterMastery" to check for illegal implicit creation of bi-directional InterMastery link between mastery nodes', 500)
      return
    }
    
    var mastersOfNodesDependentOnCN1 = [] // new preq link will create InterMastery links from cn2 up to these nodes
      , mastersOfNodesCN1DependentOn = [] // new preq link will create InterMastery links from these nodes up to cn2
      , cn2InterMasteryLinkedUpNodes = [] // cn2 has InterMastery link up to these nodes
      , cn2InterMasteryLinkedDnNodes = [] // cn2 has InterMastery link down to these nodes

    // any intersection between
    //  either: mastersOfNodesDependentOnCN1 AND cn2InterMasteryLinkedDnNodes
    //  or:     mastersOfNodesCN1DepednentOn AND cn2InterMasteryLinkedUpNodes
    // implies illegal implicit creation of bi-directional InterMastery link would result from addition of ordered pair [cn1Id,cn2Id] to the Mastery relation

    prereq.members.forEach(function(p) {
      var masteryLink
      if (p[0] == cn1Id) {
        masteryLink = _.find(r.members, function(l) { return l[0] == p[1] })
        if (masteryLink && masteryLink[1] != cn2Id && !~mastersOfNodesDependentOnCN1.indexOf(masteryLink[1])) {
          mastersOfNodesDependentOnCN1.push(masteryLink[1])
        }
      } else if (p[1] == cn1Id) {
        masteryLink = _.find(r.members, function(l) { return l[0] == p[0] })
        if (masteryLink && masteryLink[1] != cn2Id && !~mastersOfNodesCN1DependentOn.indexOf(masteryLink[1])) {
          mastersOfNodesCN1DependentOn.push(masteryLink[1])
        }
      }
    })

    var illegalIntersection = _.intersection(mastersOfNodesDependentOnCN1, mastersOfNodesCN1DependentOn)
    if (illegalIntersection.length) {
      callback(util.format('Adding link would implicitly create illegal bi-directional InterMastery links between mastery node id="%s" and mastery nodes with ids: "%s".', cn2Id, illegalIntersection.join('","')), 412)
      return
    }

    kcm.chainedBinaryRelationMembers(interMastery)
      .forEach(function(link) {
        if (link[0] == cn2Id) {
          cn2InterMasteryLinkedUpNodes.push(link[1])
        } else if (link[1] == cn2Id) {
          cn2InterMasteryLinkedDnNodes.push(link[0])
        }
      })

    var illegalIntersections =
      _.intersection(mastersOfNodesDependentOnCN1, cn2InterMasteryLinkedDnNodes)
        .concat(
          _.intersection(mastersOfNodesCN1DependentOn, cn2InterMasteryLinkedUpNodes))

    if (illegalIntersections.length) {
      callback(util.format('Adding link would implicitly create illegal bi-directional InterMastery links between mastery node id="%s" and mastery nodes with ids: "%s".', cn2Id, illegalIntersections.join('","')), 412)
      return
    }
  }

  nextVersion(r, user, 'addOrderedPairToBinaryRelation', [cn1Id, cn2Id])
  r.members.push( [ cn1Id, cn2Id] )

  updateDoc(r, function(e,r,b) {
    callback(e, r && r.statusCode)
  })
}

function removeOrderedPairFromBinaryRelation(relationId, relationRev, pair, callback) {
  getDoc(relationId, function(e,r,b) {
    if (200 != r.statusCode)  {
      if ('function' == typeof callback) callback(util.format('Error retrieving relation. Database reported error:"%s". The pair was not removed from the relation', e),r.statusCode)
        return
    }

    var relation = JSON.parse(b)

    if ('relation' != relation.type || 'binary' != relation.relationType) {
      callback(util.format('document with id "%s" does not represent a binary relation - pair not removed from relation', relationId), r.statusCode)
      return
    }

    if (relation._rev != relationRev) {
      callback(util.format('supplied revision:"%s" does not match latest document revision:"%s" on document id:"%s"', relationRev, relation._rev, relationId), 409)
      return
    }

    var match = _.find(relation.members, function(p) {
      return pair[0] == p[0] && pair[1] == p[1]
    })

    if (!match) {
      callback(util.format('ordered pair ["%s", "%s"] not a member of binary relation id="%s"', pair[0], pair[1], relationId), 500)
      return
    }

    relation.members.splice(relation.members.indexOf(match), 1)

    updateDoc(relation, function(e,r,b) {
      if (201 != r.statusCode) {
        callback(util.format('Error updating relation. Database reported error:"%s". The pair was not removed from the relation', e), r.statusCode)
        return
      }

      var rev = JSON.parse(b).rev

      queryView('relations-by-relation-type', 'include_docs', true, 'key', 'chained-binary', function(e,r,b) {
        if (200 != r.statusCode) { 
          // don't throw error as it's not especially important
          // TODO: this error however should be logged somewhere for developer attention
          callback(null, 201, { rev:rev })
          return
        }

        var cRIds = _.chain(JSON.parse(b).rows)
          .map(function(r) { return r.doc })
          .filter(function(r) {
            return ~_.pluck(r.chain, 'relation').indexOf(relation._id)
          })
          .pluck('_id')
          .value()

        if (!cRIds.length) {
          callback(null, 201, { rev:rev })
          return
        }

        getChainedBinaryRelationsWithMembers(cRIds, function(e, statusCode, cRs) {
          callback(null, 201, { rev:rev, chainedBinaryRelations:cRs })
        })
      })
    })
  })
}

function addNewPipelineToConceptNode(user, pipelineName, conceptNodeId, conceptNodeRev, callback) {
  if (typeof pipelineName != 'string' || !pipelineName.length) {
    if ('function' == typeof callback) callback('BAD ARGS - pipelineName requires string', 412)
    return
  }
  if (typeof conceptNodeId != 'string' || !conceptNodeId.length) {
    if ('function' == typeof callback) callback('BAD ARGS - conceptNodeId requires string', 412)
    return
  }

  var cn = kcm.getDocClone(conceptNodeId, 'concept node')
  if (!cn.pipelines) conceptNode.pipelines = []

  if (!cn) {
    callback('could not find concept node', 404)
    return
  }

  if (cn._rev != conceptNodeRev) {
    callback(util.format('concept node revisions do not correspond. supplied:"%s", database:"%s". The pipeline was not created.', conceptNodeRev, cn._rev), 409)
    return
  }

  var pl = firstVersion(user, 'addNewPiplineToConceptNode', null)
  pl._id = generateUUID()
  pl.type = 'pipeline'
  pl.name = pipelineName
  pl.problems = []
  pl.workflowStatus = 0
  pl.revisions = []

  nextVersion(cn, user, 'addNewPipelineToConceptNode', pl._id)
  cn.pipelines.push(pl._id)

  request({
    method:'POST'
    , uri: databaseURI + '_bulk_docs'
    , headers: { 'content-type':'application/json', accepts:'application/json' }
    , body: JSON.stringify({ docs:[pl,cn] })
  }, function(e,r,b) {
    if (!r || 201 != r.statusCode) {
      e = util.format('Error creating pipeline and adding it to node. Database reported error: "%s"', e)
    }
    callback(e, r && r.statusCode)
  })
}

function deletePipeline(user, plId, plRev, cnId, cnRev, callback) {
  var pl = kcm.getDocClone(plId, 'pipeline')
    , cn = kcm.getDocClone(cnId, 'concept node')
    , ix = cn && cn.pipelines.indexOf(plId)

  if (!pl) {
    callback(util.format('could not retrieve pipeline id="%s". The pipeline was not deleted.', plId), 404)
    return
  }

  if (!cn) {
    callback(util.format('could not retrieve concept node with id="%s". Pipeline with id="%s" was not deleted.', cnId, plId), 404)
    return
  }

  if (pl._rev !== plRev) {
    callback(util.format('Error: Pipeline revisions do not correspond. Supplied:"%s", Database:"%s". The pipeline was not deleted', plRev, pl._rev), 409)
    return
  }

  if (cnRev != cn._rev) {
    callback(util.format('Error: concept node revisions do not correspond. Supplied:"%s", Database:"%s", The pipeline was not deleted', cnRev, cn._rev), 409)
    return
  }

  if (ix === -1) {
    callback(util.format('Error: Pipeline with id="%s" not found on concept node with id="%s". The pipeline was not deleted.', plId, cnId), 412)
    return
  }

  nextVersion(cn, user, 'deletePipeline', plId)
  nextVersion(pl, user, 'deletePipeline')
  cn.pipelines.splice(ix, 1)
  pl._deleted = true

  request({
    method:'POST'
    , uri: databaseURI + '_bulk_docs'
    , headers: { 'content-type':'application/json', accepts:'application/json' }
    , body: JSON.stringify({ docs:[pl,cn] })
  }, function(e,r,b) {
    if (!r || 201 != r.statusCode) {
      e = util.format('Error deleting creating pipeline and adding it to node. Database reported error: "%s"', e)
    }
    callback(e, r && r.statusCode)
  })

  getDoc(plId, function(e,r,b) {
    if (r.statusCode != 200) {
      callback(util.format('could not retrieve pipeline. (Database Error:"%s"). The pipeline was not deleted.',e), r.statusCode)
      return
    }
    var pl = JSON.parse(b)

    if (pl._rev != plRev) {
      callback(util.format('Error: Pipeline revisions do not correspond. Supplied:"%s", Database:"%s". The pipeline was not deleted', plRev, pl._rev), 500)
      return
    }

    if ('pipeline' != pl.type) {
      callback(util.format('Error: Document with id="%s" is not a pipeline. The document was not deleted.',plId), 500)
      return
    }

    getDoc(cnId, function(e,r,b) {
      var cn, plIx

      if (r.statusCode != 200) {
        callback(util.format('Error: could not retrieve concept node. (Database Error:"%s"). The pipeline was not deleted.',e), r.statusCode)
        return
      }

      cn = JSON.parse(b)

      if ('concept node' != cn.type) {
        callback(util.format('Error: Document with id="%s" is not a concept node. The pipeline was not deleted.',cnId), 500)
        return
      }
      if (cnRev != cn._rev) {
        callback(util.format('Error: concept node revisions do not correspond. Supplied:"%s", Database:"%s", The pipeline was not deleted', cnRev, cn._rev), 500)
        return
      }

      plIx = cn.pipelines.indexOf(plId)

      if (-1 == plIx) {
        callback(util.format('Error: Pipeline with id="%s" not found on concept node with id="%s". The pipeline was not deleted.', plId, cnId), 500)
        return
      }

      cn.pipelines.splice(plIx, 1)

      updateDoc(cn, function(e,r,b) {
        if (r.statusCode != 201) {
          callback('Error: Failed to remove pipeline from concept node. Database Error:"%s". The pipeline was not deleted.', r.statusCode)
          return
        }

        var cnUpdatedRev = JSON.parse(b).rev

        deleteDoc(plId, plRev, function(e,r,b) {
          if (r.statusCode != 200) {
            callback(util.format('Error: Failed to delete pipeline. Database Error:"%s". The pipeline was removed from its concept node but not deleted.', e), r.statusCode)
            return
          }
          callback(null, 200, cnUpdatedRev)
        })
      })
    })
  })
}

function reorderConceptNodePipelines(user, conceptNodeId, conceptNodeRev, pipelineId, oldIndex, newIndex, callback) {
  if (!/^\d+$/.test(oldIndex)) {
    callback('bad argument suppied for oldIndex:"%s" - positive integer required', oldIndex)
    return
  }
  if (!/^\d+$/.test(newIndex)) {
    callback('bad argument suppied for newIndex:"%s" - postive integer required', newIndex)
    return
  }

  var cn = kcm.getDocClone(conceptNodeId, 'concept node')
    , pl = kcm.getDocClone(pipelineId, 'pipeline')

  if (!cn) {
    callback(util.format('Concept Node with id="%s" not found. Its pipelines were not reordered', conceptNodeId), 404)
    return
  }

  if (!pl) {
    callback(util.format('Pipeline with id="%s" not found. The pipelines of node with id="%s" were not reordered', pipelineId, conceptNodeId), 404)
    return
  }

  if (cn._rev !== conceptNodeRev) {
    callback(util.format('Error: Concept node revisions do not correspond. Supplied:"%s", Database:"%s". The pipelines were not reordered', conceptNodeRev, cn._rev), 409)
    return
  }

  if (cn.pipelines.indexOf(pipelineId) !== oldIndex) {
    callback(util.format('Concept node id="%s" does not have pipeline id="%s" at index="%s". The pipelines were not reordered', conceptNodeId, pipelineId, oldIndex), 412)
    return
  }

  if (cn.pipelines.length <= newIndex) {
    callback(util.format('Invalid new index:"%s" supplied for pipeline id="%s" on concept node id="%s" which contains %s pipelines.', newIndex, pipelineId, cn._id, cn.pipelines.length),500)
    return
  }

  nextVersion(cn, user, 'reorderConceptNodePipelines', { pipeline:pipelineId, newIndex:newIndex })
  cn.pipelines.splice(oldIndex,1)
  cn.pipelines.splice(newIndex,0,pipelineId)

  updateDoc(cn, function(e,r,b) { callback(e, r && r.statusCode) })
}

function removeProblemFromPipeline(pipelineId, pipelineRev, problemId, callback) {
  var argErrors = []
  if ('string' != typeof pipelineId) argErrors.push('pipelineId')
  if ('string' != typeof pipelineRev) argErrors.push('pipelineRev')
  if ('string' != typeof problemId) argErrors.push('problemId')
  if (argErrors.length) {
    callback('BAD ARGS: strings required for ' + argErrors.join(' and ') +'. The problem was not removed from the pipeline', 500)
    return
  }

  getDoc(pipelineId, function(e,r,b) {
    if (200 != r.statusCode) {
      callback(util.format('could not retrieve pipeline. (Database Error:"%s"). The pipeline was not deleted.',e), r.statusCode)
      return
    }
    var pl = JSON.parse(b)

    if (pl._rev != pipelineRev) {
      callback(util.format('Error: Pipeline revisions do not correspond. Supplied:"%s", Database:"%s". The pipeline was not deleted', pipelineRev, pl._rev), 500)
      return
    }

    if ('pipeline' != pl.type) {
      callback(util.format('Error: Document with id="%s" is not a pipeline. The document was not deleted.',plId), 500)
      return
    }

    var problemIx = pl.problems.indexOf(problemId)

    if (!~problemIx) {
      callback(util.format('Problem with id="%s" not found on pipeline with id="%s". The problem was not deleted from the pipeline.', problemId, pipelineId), 500)
      return
    }

    pl.problems.splice(problemIx, 1)
    pl.workflowStatus = 0

    updateDoc(pl, function(e,r,b) {
      if (201 != r.statusCode) {
        callback(util.format('Error removing problem with id="%s" from pipeline with id="%s". Database reported error:"%s"', problemId, pipelineId, e), r.statusCode)
        return
      }
      callback(null,201,JSON.parse(b).rev)
    })
  })
}

function updatePipelineWorkflowStatus(user, pipelineId, pipelineRev, status, callback) {
  var argErrors = []
  if ('string' != typeof pipelineId) argErrors.push('pipelineId')
  if ('string' != typeof pipelineRev) argErrors.push('pipelineRev')
  if (argErrors.length) {
    callback('BAD ARGS: strings required for ' + argErrors.join(' and ') +'. The pipeline workflow status was not updated.', 512)
    return
  }

  if (!/^\d+$/.test(status)) {
    callback('BAD ARG: integer string required for status. The pipeline workflow status was not updated.', 412)
    return
  }

  var pl = kcm.getDocClone(pipelineId, 'pipeline')

  if (pl._rev != pipelineRev) {
    callback(util.format('Error: Pipeline revisions do not correspond. Supplied:"%s", Database:"%s". The pipeline workflow status was not updated.', pipelineRev, pl._rev), 409)
    return
  }

  nextVersion(pl, user, 'updatePipelineWorkflowStatus')

  pl.workflowStatus = parseInt(status,10)

  updateDoc(pl, function(e,r,b) {
    if (201 != r.statusCode) {
      callback(util.format('Error updating workflow status for pipeline with id="%s". Database reported error:"%s"', pipelineId, e), r.statusCode)
      return
    }
    callback(null, 201)
  })
}

function updatePipelineName(id, rev, name, callback) {
  var argErrors = []
  if ('string' != typeof id) argErrors.push('id')
  if ('string' != typeof rev) argErrors.push('rev')
  if ('string' != typeof name) argErrors.push('name')
  if (argErrors.length) {
    callback('BAD ARGS: strings required for ' + argErrors.join(' and ') +'. The pipeline name was not updated.', 500)
    return
  }

  getDoc(id, function(e,r,b) {
    if (200 != r.statusCode) {
      callback(util.format('could not retrieve pipeline. (Database Error:"%s"). The pipeline name was not updated.',e), r.statusCode)
      return
    }
    var pl = JSON.parse(b)

    if (pl._rev != rev) {
      callback(util.format('Error: Pipeline revisions do not correspond. Supplied:"%s", Database:"%s". The pipeline name was not updated.', rev, pl._rev), 500)
      return
    }

    if ('pipeline' != pl.type) {
      callback(util.format('Error: Document with id="%s" is not a pipeline. The pipeline name was not updated.',plId), 500)
      return
    }

    pl.name = name

    updateDoc(pl, function(e,r,b) {
      if (201 != r.statusCode) {
        callback(util.format('Error updating name for pipeline with id="%s". Database reported error:"%s"', id, e), r.statusCode)
        return
      }
      callback(null,201,JSON.parse(b).rev)
    })
  })
}

function appendProblemsToPipeline(plId, problemIds, callback) {
  getDoc(plId, function(e,r,b) {
    if (200 != r.statusCode) {
      callback('could not retrieve pipeline. Problems not added to pipeline.', r.statusCode)
      return
    }

    var pl = JSON.parse(b)
    pl.problems = pl.problems.concat(problemIds)
    pl.workflowStatus = 0

    updateDoc(pl, function(e,r,b) {
      if (201 != r.statusCode) {
        callback(util.format('could not add problems to pipeline. DB error: "%s"', e), r.statusCode)
        return
      }
      callback(null, 201, JSON.parse(b))
    })
  })
}

function updatePipelineSequence(pipelineId, problemSequence, callback) {
  var errors = ''
  if (typeof pipelineId != 'string' || !pipelineId.length) errors += 'BAD ARG: String value required for "pipelineId"'
  if (problemSequence.slice == undefined) errors += 'BAD ARG: Array required for "problemSequence"'
  if (errors.length) {
    callback('Error. Could not update pipeline sequence.\n' + errors, 500)
    return
  }

  getDoc(pipelineId, function(e,r,b) {
    if (200 != r.statusCode) {
      callback('Could not retrieve pipeline from database. The problem sequence was not updated.', r.statusCode)
      return
    }
    var pl = JSON.parse(b)

    if ('pipeline' != pl.type) {
      callback(util.format('invalid pipeline - id="%s" does not correspond to pipeline document.', pipelineId), 500)
      return
    }

    pl.problems = problemSequence
    pl.workflowStatus = 0

    updateDoc(pl, function(e,r,b) {
      if (201 != r.statusCode) callback(util.format('The pipeline problem sequence failed to update. Database Error: "%s"', e), r.statusCode)
      else callback(null,201)
    })
  })
}

function pipelineProblemDetails(id, rev, callback) {
  var errors = []
  if (typeof id != 'string' || !id.length) errors.push('BAD ARG: String value required for "id"')
  if (typeof rev != 'string' || !id.length) errors.push('BAD ARG: String value required for "rev"')
  if (errors.length) {
    callback('Error. Could not update pipeline sequence.\n' + errors.join('\n'), 500)
    return
  }

  getDoc(id, function(e,r,b) {
    if (200 != r.statusCode) {
      callback(util.format('Error could not retrieve document with id="%s". Database reported error:"%s"',id,e), r.statusCode)
      return
    }

    var pl = JSON.parse(b)

    if (pl._rev != rev) {
      callback(util.format('pipeline revisions do not correspond. supplied:"%s", database:"%s". The pipeline problems could not be retrieved.', rev, pl._rev), 500)
      return
    }

    if ('pipeline' != pl.type) {
      callback(util.format('Document with id="%s" does not have type "pipeline"', id), 500)
      return
    }

    request({
      uri: encodeURI(util.format('%s_all_docs?keys=%s&include_docs=true', databaseURI, JSON.stringify(pl.problems)))
      , headers: { 'content-type':'application/json', accepts:'application/json' }
    }, function(e,r,b) {
      if (200 != r.statusCode) {
        callback(util.format('Error retrieving pipeline problems. Database reported error:"%s"', e), r.statusCode)
        return
      }

      var problems = _.map(JSON.parse(b).rows, function(p) {
        return {
          id: p.id
          , desc: typeof p.doc.internalDescription == 'string' && p.doc.internalDescription.length ? p.doc.internalDescription : p.doc.problemDescription
          , lastModified: typeof p.doc.dateModified == 'string' ? p.doc.dateModified.replace(/.{8}$/, '').replace(/[a-z]/gi, ' ') : ''
        }
      })

      callback(null, 200, problems)
    })
  })
}

function reorderPipelineProblems(pipelineId, pipelineRev, problemId, oldIndex, newIndex, callback) {
  if (!/^\d+$/.test(oldIndex)) {
    callback('bad argument suppied for oldIndex:"%s" - positive integer required', oldIndex)
    return
  }
  if (!/^\d+$/.test(newIndex)) {
    callback('bad argument suppied for newIndex:"%s" - postive integer required', newIndex)
    return
  }

  getDoc(pipelineId, function(e,r,b) {
    if (200 != r.statusCode) {
      callback(util.format('could not retrieve document with id="%s" from database. Error reported: "%s"', pipelineId, e), r.statusCode)
      return
    }
    var pl = JSON.parse(b)

    if (pl._rev != pipelineRev) {
      callback(util.format('Error: Pipeline revisions do not correspond. Supplied:"%s", Database:"%s". The pipeline problems were not reordered', pipelineRev, pl._rev), 500)
      return
    }
    if ('pipeline' != pl.type) {
      callback(util.format('Error: Document with id="%s" is not a pipeline. The pipeline problems were not reordered.',pipelineId), 500)
      return
    }
    if (problemId != pl.problems[oldIndex]) {
      callback(util.format('Pipeline id="%s" does not have problem id="%s" at index="%s". The pipeline problems were not reordered', pipelineId, problemId, oldIndex), 412)
      return
    }
    if (newIndex >= pl.problems.length) {
      callback(util.format('Invalid new index:"%s" supplied for problem id="%s" on pipeline id="%s". The pipeline contains %s problems.', newIndex, problemId, pl._id, pl.problems.length),500)
      return
    }

    pl.problems.splice(oldIndex,1)
    pl.problems.splice(newIndex,0,problemId)
    pl.workflowStatus = 0

    updateDoc(pl, function(e,r,b) {
      if (201 != r.statusCode) {
        callback(util.format('error reordering pipeline. Database reported error:"%s".',e), r.statusCode)
        return
      }
      callback(null,201,JSON.parse(b).rev)
    })
  })
}

function getAppContent(userId, callback) {
  var writeLog = true

  queryView('by-user-type', 'key', [userId, 'ExportSettings'], 'include_docs', true, function(e,r,b) {
    var rows = r && r.statusCode == 200 && JSON.parse(b).rows

    if (!rows || !rows.length) {
      callback(util.format('Could not retrieve export settings. Database error:"%s"', e), r && r.statusCode != 200 ? r.statusCode : 500)
      return
    }

    var exportSettings = rows[0].doc
      , exportAllNodes  = exportSettings.exportAllNodes === true
      , nodeInclusionTags = exportSettings.nodeInclusionTags
      , nodeTagsToBitCols = exportSettings.nodeTagsToBitCols
      , nodeTagPrefixesToTextCols = exportSettings.nodeTagPrefixesToTextCols
      , pipelineNames = exportSettings.pipelineNames
      , pipelineWorkflowStatusLevels
      , path
      , dbPath
      , pdefsPath
      , exportLogWriteStream


    var allWfStatusLevels = [0,32,64]
    switch (exportSettings.pipelineWorkflowStatusOperator) {
      case '<=':
        pipelineWorkflowStatusLevels = _.filter(allWfStatusLevels, function(l) { return l <= exportSettings.pipelineWorkflowStatusLevel })
      break
      case '==':
        pipelineWorkflowStatusLevels = _.filter(allWfStatusLevels, function(l) { return l == exportSettings.pipelineWorkflowStatusLevel })
      break
      case '>=':
        pipelineWorkflowStatusLevels = _.filter(allWfStatusLevels, function(l) { return l >= exportSettings.pipelineWorkflowStatusLevel })
      break
      default:
        callback(util.format('invalid pipeline workflow status operator:"%s"', exportSettings.pipelineWorkflowStatusOperator), 500)
      return
    }

    request(couchServerURI + '_uuids', function(e,r,b) {
      if (200 != r.statusCode) {
        callback(e || 'error generating uuid', statusCode || 500)
        return
      }

      path = '/tmp/' + JSON.parse(b).uuids[0]
      pdefsPath = path + '/pdefs'
      dbPath = path + '/content.db'

      fs.mkdirSync(path)
      fs.mkdirSync(pdefsPath)

      //console.log('content path:', path)

      if (writeLog) exportLogWriteStream = fs.createWriteStream(path + '/export-log.txt') 
      if (writeLog) exportLogWriteStream.write('================================================\nExport Settings Couch Document:\n' + JSON.stringify(exportSettings,null,4) + '\n================================================\n')

      getAppContentJSON(function(e, statusCode, content) {
        if (200 != statusCode) {
          callback(e || 'error retrieving app json content', statusCode || 500)
          return
        }
        if (writeLog) exportLogWriteStream.write('\n================================================\nKCM Content shaped for use as source of SQLite database:\n' + JSON.stringify(content,null,4) + '\n================================================\n')

        //console.log('\n\nJSON:\n%s\n', JSON.stringify(content,null,2))

        createSQLiteDB(content, function(e, statusCode) {
          if (201 != statusCode) {
            callback(e || 'error creating content database', statusCode || 500)
            return
          }
          writePDefs(content.problems, function(e, statusCode, pdefs) {
            if (201 != statusCode) {
              callback(e || 'error writing problem pdefs', statusCode || 500)
              return
            }

            if (writeLog) exportLogWriteStream.end()
            exec('zip content.zip -r pdefs content.db export-log.txt', { cwd:path }, function(e, stdout, stderr) {
              if (e) {
                callback(util.format('error zipping content:"%s"', e), 500)
                return
              }
              callback(null, 200, path + '/content.zip')
            })
          })
        })
      })
    })

    function getAppContentJSON(jsonCallback) {
      getPipelines(function(e, statusCode, pipelines) {
        if (200 != statusCode) {
          jsonCallback(e || 'error retrieving pipelines', statusCode || 500)
          return
        }
        getNodes(pipelines, function(e, statusCode, nodes) {
          if (200 != statusCode) {
            jsonCallback(e || 'error retrieving concept nodes', statusCode || 500)
            return
          }

          var nodeIds = Object.keys(nodes)
            , binaryRelations = []

          kcm.docStores.relations.forEach(function(r) {
            if (!~['binary','chained-binary'].indexOf(r.relationType)) return

            var brMembers = r.relationType == 'binary' ? r.members : kcm.chainedBinaryRelationMembers(r)

            binaryRelations.push({
              id: r._id
              , rev: r._rev
              , name: r.name
              , members: brMembers.filter(function(pair) { return ~nodeIds.indexOf(pair[0]) && ~nodeIds.indexOf(pair[1]) })
            })
          })

          getProblems(pipelines, function(e, statusCode, problems) {
            if (200 != statusCode) {
              jsonCallback(e || 'error retrieving problems', statusCode || 500)
              return
            }
            //console.log('app content JSON generated.')
            jsonCallback(e, statusCode, { conceptNodes:_.values(nodes), pipelines:_.values(pipelines), binaryRelations:binaryRelations, problems:problems })
          })
        })
      })

      function getPipelines(cb) {
        var numPlNames = pipelineNames.length
          , pipelines = {}


        if (!numPlNames) {
          cb('case when pipelineNames.length == 0 not handled', 500)
          return
        }

        ;(function getPipelinesMatchingNameAtIndex(i) {
          var name = pipelineNames[i]

          queryView('pipelines-with-problems-by-name', 'reduce', false, 'startkey', [name], 'endkey', [name,{}], function(e,r,b) {
            if (!r || 200 != r.statusCode) {
              cb(util.format('Error retrieving pipelines with name="%s". db reported error: "%s"', name, e), r && r.statusCode || 500)
              return
            }

            var rows = JSON.parse(b).rows
              , row, key, workflowStatus
              , len = rows.length, i


            for (i=0; i<len; i++) {
              row = rows[i]
              key = row.key
              wfStatus = key[3] || 0

              if (~pipelineWorkflowStatusLevels.indexOf(wfStatus)) {
                pipelines[row.id] = { id:row.id, rev:key[2], name:name, workflowStatus:wfStatus, problems:row.value }
              }
            }

            if (++i < len) {
              getPipelinesMatchingNameAtIndex(i)
            } else {
              cb (null, 200, pipelines)
            }
          })
        })(0)
      }

      function getNodes(pipelines, cb) {
        var plIds = _.keys(pipelines)

        getNodeDocs(function(docs) {
          var nodes = {}
            , tagTextRegExps = _.map(nodeTagPrefixesToTextCols, function(prefix) { return new RegExp(util.format('^%s:\\s*(.+)', prefix)) })

          docs.forEach(function(doc) {
            var n = { id:doc._id, rev:doc._rev, pipelines:_.intersect(doc.pipelines, plIds), workflowStatus:0, x:doc.x, y:doc.y }

            if (n.pipelines.length) {
              n.workflowStatus = Math.min.apply(Math, _.map(n.pipelines, function(plId) { return pipelines[plId].workflowStatus }))
            }

            nodeTagsToBitCols.forEach(function(tag) {
              n[tag] = ~doc.tags.indexOf(tag) ? 1 : 0
            })

            nodeTagPrefixesToTextCols.forEach(function(tagPrefix, i) {
              var re = tagTextRegExps[i]
                , matchingTags = _.filter(doc.tags, function(tag) { return tag.match(re) != null })

              n[tagPrefix] = JSON.stringify(_.map(matchingTags, function(t) { return t.substring(1 + tagPrefix.length) }))
            })

            nodes[n.id] = n
          })
          cb(null,200,nodes)
        })

        function getNodeDocs(successCB) {
          if (exportAllNodes) {
            queryView('concept-nodes', 'include_docs', true, function(e,r,b) {
              if (200 != r.statusCode) {
                cb(util.format('Error retrieving concept nodes. db reported error: "%s"', e), r.statusCode || 500)
                return
              }
              successCB(_.pluck(JSON.parse(b).rows, 'doc'))
            })
          } else {
            getNodesByPipeline(function(plNodeDocs) {
              getNodesByTag(function(tagNodeDocs) {
                var uniqDocs = _.uniq(plNodeDocs.concat(tagNodeDocs), false, function(doc) { return doc._id })
                successCB(uniqDocs)
              })
            })
          }
        }

        function getNodesByPipeline(successCB) {
          queryView('concept-nodes-by-pipeline', 'include_docs', true, 'keys', plIds, function(e,r,b) {
            if (200 != r.statusCode) {
              cb(util.format('Error retrieving concept nodes. db reported error: "%s"', e), r.statusCode || 500)
              return
            }
            successCB(_.map(JSON.parse(b).rows, function(row) { return row.doc }))
          })
        }

        function getNodesByTag(successCB) {
          if (!nodeInclusionTags.length) {
            successCB([])
            return
          }
          queryView('concept-node-tags', 'keys', nodeInclusionTags, 'include_docs', true, 'reduce', false, function(e,r,b) {
            if (200 != r.statusCode) {
              cb(util.format('Error retrieving concept node tags. db reported error: "%s"', e), r.statusCode || 500)
              return
            }
            successCB(_.map(JSON.parse(b).rows, function(row) { return row.doc }))
          })
        }
      }

      function getProblems(pipelines, cb) {
        var problemIds = _.uniq(_.flatten(_.map(pipelines, function(pl) { return pl.problems })))

        request(encodeURI(databaseURI + '_all_docs?keys=' + JSON.stringify(problemIds)), function(e,r,b) {
          if (200 != r.statusCode) {
            cb(util.format('Error retrieving binary relations. db reported error: "%s"', e), r.statusCode || 500)
            return
          }

          var problems = _.map(JSON.parse(b).rows, function(row) { return { id:row.id, rev:row.value.rev } })
          cb(null, 200, problems)
        })
      }
    }

    function writePDefs(problems, cb) {
      var pIds = _.map(problems, function(p) { return p.id })
      var pdefs = []

      ;(function getPDefsRec() {
        var i = pdefs.length
        , id = pIds[i]

        if (i == pIds.length) {
          //console.log('all pdefs retrieved')
          writePDefsRec(0)
        } else {
          //console.log('get pdef at index', i)
          request({
            uri: databaseURI + id + '/pdef.plist'
          }, function(e,r,b) {
            pdefs[i] = b
            getPDefsRec()
          })
        }
      })()

      function writePDefsRec(i) {
        if (i == pdefs.length) {
          //console.log('all pdefs written')
          cb(null, 201)
        } else {
          //console.log('write pdef at index:',i)
          fs.writeFileSync(pdefsPath + '/' + pIds[i] + '.plist', pdefs[i])
          writePDefsRec(i+1)
        }
      }
    }

    function createSQLiteDB(content, cb) {
      var db = new sqlite3.Database(dbPath)
      db.serialize(function() {
        var tagBitColDecs = _.map(nodeTagsToBitCols, function(tag) { return util.format(', %s INTEGER', tag) }).join('')
        var tagTextColDecs = _.map(nodeTagPrefixesToTextCols, function(tagPrefix) { return util.format(', %s TEXT', tagPrefix) }).join('')
        exportLogWriteStream.write('\n================================================\nCREATE TABLE ConceptNodes\n')
        var cnTableCreateScript = util.format("CREATE TABLE ConceptNodes (id TEXT PRIMARY KEY ASC, rev TEXT, pipelines TEXT, workflowSatatus INTEGER, x INTEGER, y INTEGER%s%s)", tagBitColDecs, tagTextColDecs)
        db.run(cnTableCreateScript)

        exportLogWriteStream.write('\n================================================\nINSERT INTO ConceptNodes\n')
        var cnIns = db.prepare(util.format("INSERT INTO ConceptNodes VALUES (?,?,?,?,?,?%s)", new Array(nodeTagsToBitCols.length + nodeTagPrefixesToTextCols.length + 1).join(',?')))
        content.conceptNodes.forEach(function(n) {
          exportLogWriteStream.write('['+n.id+']')
          var tags = _.map(nodeTagsToBitCols.concat(nodeTagPrefixesToTextCols), function(tag) { return n[tag] })
          , cols = [n.id, n.rev, JSON.stringify(n.pipelines), n.workflowStatus, n.x, n.y].concat(tags)

          cnIns.run.apply(cnIns, cols)
        })
        cnIns.finalize()

        exportLogWriteStream.write('\n================================================\nCREATE TABLE Pipelines\n')
        db.run("CREATE TABLE Pipelines (id TEXT PRIMARY KEY ASC, rev TEXT, name TEXT, workflowStatus INTEGER, problems TEXT)")
        exportLogWriteStream.write('\n================================================\nINSERT INTO Pipelines\n')
        var plIns = db.prepare("INSERT INTO Pipelines VALUES (?,?,?,?,?)")
        content.pipelines.forEach(function(pl) {
          exportLogWriteStream.write('['+pl.id+']')
          plIns.run(pl.id, pl.rev, pl.name, pl.workflowStatus, JSON.stringify(pl.problems))
        })
        plIns.finalize()

        exportLogWriteStream.write('\n================================================\nCREATE TABLE Problems\n')
        db.run("CREATE TABLE Problems (id TEXT PRIMARY KEY ASC, rev TEXT)")
        exportLogWriteStream.write('\n================================================\nINSERT INTO Problems\n')
        var probsIns = db.prepare("INSERT INTO Problems VALUES (?,?)")
        content.problems.forEach(function(p) {
          exportLogWriteStream.write('['+p.id+']')
          probsIns.run(p.id, p.rev)
        })
        probsIns.finalize()

        exportLogWriteStream.write('\n================================================\nCREATE TABLE BinaryRelations\n')
        db.run("CREATE TABLE BinaryRelations (id TEXT PRIMARY KEY ASC, rev TEXT, name TEXT, members TEXT)")
        exportLogWriteStream.write('\n================================================\nINSERT INTO BinaryRelations\n')
        var brIns = db.prepare("INSERT INTO BinaryRelations VALUES (?,?,?,?)")
        content.binaryRelations.forEach(function(br) {
          exportLogWriteStream.write('['+br.id+']')
          brIns.run(br.id, br.rev, br.name, JSON.stringify(br.members))
        })
        brIns.finalize()

        if (writeLog) {
          db.all("SELECT * FROM ConceptNodes", function(err, rows) {
            exportLogWriteStream.write('\n================================================\nSELECT * FROM ConceptNodes:\n' + JSON.stringify(rows,null,4) + '\n================================================\n')
          })
          db.all("SELECT * FROM Pipelines", function(err, rows) {
            exportLogWriteStream.write('\n================================================\nSELECT * FROM Pipelines:\n' + JSON.stringify(rows,null,4) + '\n================================================\n')
          })
          db.all("SELECT * FROM Problems", function(err, rows) {
            exportLogWriteStream.write('\n================================================\nSELECT * FROM Problems:\n' + JSON.stringify(rows,null,4) + '\n================================================\n')
          })
          db.all("SELECT * FROM BinaryRelations", function(err, rows) {
            exportLogWriteStream.write('\n================================================\nSELECT * FROM BinaryRelations:\n' + JSON.stringify(rows,null,4) + '\n================================================\n')
          })
        }

        db.close(function() {
          cb(null,201)
        })
      })  
    }
  })
}

// TODO: The following functions should be shared across model modules
function getDoc(id, callback) {
  request({
    uri: databaseURI + id
    , headers: { 'content-type':'application/json', accepts:'application/json' }
  }, callback)
}

function getDocs(ids, callback) {
  request({
    url: encodeURI(util.format('%s_all_docs?keys=%s&include_docs=true', databaseURI, JSON.stringify(ids)))
    , headers: { 'content-type':'application/json', accepts:'application/json' }
  }, callback)
}

function insertDoc(doc, callback) {
  request({
    method: 'post'
    , uri: databaseURI
    , headers: { 'content-type': 'application/json', 'accepts': 'application/json' }
    , body: JSON.stringify(doc)
  }, ('function' == typeof callback && callback || function(){}))
}

function updateDoc(doc, callback) {
  //console.log('updateDoc:', JSON.stringify(doc,null,2))
  request({
    method:'PUT'
    , uri: databaseURI + doc._id 
    , headers: { 'content-type':'application/json', accepts:'application/json' }
    , body: JSON.stringify(doc)
  }, ('function' == typeof callback && callback || function(){}))
}

function deleteDoc(id, rev, callback) {
  request({
    method:'DELETE'
    , uri: databaseURI + id + '?rev=' + rev
  }, callback)
}

//
function validatedResponseCallback(validStatusCodes, callback) {
  if (typeof validStatusCodes == 'number') validStatusCodes = [ validStatusCodes ]
    return function(e, r, b) {
      if (!e && r && validStatusCodes.indexOf(r.statusCode) > -1) {
        if ('function' == typeof callback) callback(e,r,b)
        return
      }
      console.log('\nHTTP RESPONSE ERROR')
      console.log('\tError:', e)
      console.log('\tValid Status Codes:', validStatusCodes)
      console.log('\tActual Status Code:', (r && r.statusCode || ''))
      console.log('\tBody:', b)
      process.exit(1)
    }
}
