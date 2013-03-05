var fs = require('fs')
  , _ = require('underscore')
  , request = require('request')
  , util = require('util')
  , libxmljs = require('libxmljs')
  , exec = require('child_process').exec
  , sqlite3 = require('sqlite3').verbose()
  , plist = require('plist')

var kcmDatabaseName
  , kcm // new model - gradually transition to retrieving docs from here & updating/deleting via its controllers
  , designDoc
  , couchServerURI
  , databaseURI
  , relationTypes = ['binary']

module.exports = function(config, kcm_) {
  kcm = kcm_
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
      console.log('Error (harmless) - could not connect to Couch Server testing for existence of database:"%s"', databaseURI)
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
    , updateUser: updateUser
    , insertProblem: insertProblem
    , getPDef: getPDef
    , updatePDef: updatePDef
    , appEditPDef: appEditPDef
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
    , uploadPipelineFolder: uploadPipelineFolder
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
    , getAppCannedDatabases: getAppCannedDatabases
    , getDoc: getDoc
    , getDocs: getDocs
    , updateDoc: updateDoc // TODO: Shoud not give direct access to this function if we're doing undo functionality.
  }
}

// versioning
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

function updateUser(user, callback) {
  if (!user || 'User' !== user.type) {
    callback('invalid user document', 500)
    return
  }
  updateDoc(user, function(e,r,b) {
    var sc = r && r.statusCode || 500
    if (sc !== 201) {
      callback(util.format('error updating user. Database reported error: "%s"', e || b), r.statusCode)
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

// Problems
function insertProblem(plistPath, plId, plRev, cnId, cnRev, callback) {
  var pl = kcm.getDoc(plId, 'pipeline')
    , cn = kcm.getDoc(cnId, 'concept node')

  if (!pl || !cn) {
    callback((pl ? 'Concept Node': 'Pipeline') + ' node found', 404)
    return
  }
  if (pl._rev !== plRev || cn._rev !== cnRev) {
    callback((pl ? 'Concept Node': 'Pipeline') + ' revision conflict', 409)
    return
  }
  
  fs.readFile(plistPath, 'utf8', function(e, plistString) {
    if (e) {
      if (typeof callback == 'function') callback('could not read pdef. error: ' + e)
      return
    }

    var info = getProblemInfoFromPList(plistString)

    if (info.error) {
      callback(info.error)
      return
    }

    var now = (new Date).toJSON()

    request({
      method: 'PUT'
      , uri: databaseURI + generateUUID()
      , headers: { 'Content-Type': 'application/json' }
      , body: JSON.stringify({
        type: 'problem'
        , pdef: plist.parseStringSync(plistString)
        , pipeline: plId
        , conceptNode: cnId
        , problemDescription: info.problemDescription
        , internalDescription: info.internalDescription
        , problemNotes: ''
        , toolId: info.toolId
        , dateCreated: now
        , dateModified: now
      })
    }
    , callback)
  })
}

function getPDef(problemId, callback) {
  return request({
    uri: databaseURI + problemId + '/pdef.plist'
    , method: 'GET'
    , headers: { 'content-type':'application/json', accepts:'application/xml' }
  }, callback)
}

function updatePDef(user, problemId, plistPath, callback) {
  fs.readFile(plistPath, 'utf8', function(e, plistString) {
    if (e) {
      if (typeof callback == 'function') callback('could not read pdef. error: ' + e)
      return
    }

    var info = getProblemInfoFromPList(plistString)

    if (info.error) {
      callback(info.error)
      return
    }

    var problem = kcm.getDocClone(problemId, 'problem')
    if (!problem) {
      callback('problem not found', 404)
      return
    }

    problem.problemDescription = info.problemDescription;
    problem.internalDescription = info.internalDescription;
    problem.toolId = info.toolId;
    problem.dateModified = (new Date()).toJSON();
    problem.pdef = plist.parseStringSync(plistString)

    nextVersion(problem, user, 'updatePDef')

    if (!problem._attachments) problem._attachments = {};
    problem._attachments['pdef.plist'] = {
      data: new Buffer(plistString).toString('base64')
      , 'Content-Type': 'application/xml'
    }

    request({
      method: 'PUT'
      , uri: databaseURI + problemId
      , body: JSON.stringify(problem)
    }, function(e,r,b) {
      callback(r.statusCode != 201 && util.format('failed to update pdef. statusCode:%s, database error:%s', r.statusCode, e) || null);
    })
  })
}

function getProblemInfoFromPList(plistString, callback) {
  var xmlDoc

  try {
    xmlDoc = libxmljs.parseXmlString(plistString)
  } catch(e) {
    return { error: 'could not parse pdef - is pdef compiled?\n\n' + plistString }
  }

  var stringValueForKey = function(key) {
    // The following does not work for plists with text containing '£' - libxml reads the <string> element as <ustring>. Workaround with keyElm/valElm is ok
    // TODO:investigate
    //var elm = xmlDoc.get(util.format('//string[preceding-sibling::key[1][text() = "%s"]]', key))
    //return elm && elm.text()

    var keyElm = xmlDoc.get(util.format('//key[text() = "%s"]', key))
      , valElm = keyElm && keyElm.nextElement()
    return valElm && valElm.text()
  }

  var toolName = stringValueForKey('TOOL_KEY')
    , tool = toolName && kcm.cloneDocByTypeName('tool', toolName)
    , isMetaQ = xmlDoc.get('//key[text() = "META_QUESTION"]')
    , isNumberPicker = xmlDoc.get('//key[text() = "NUMBER_PICKER"]')
    , descElmName = isMetaQ ? 'META_QUESTION_TITLE' : isNumberPicker ? 'NUMBER_PICKER_DESCRIPTION' : 'PROBLEM_DESCRIPTION'
    , desc = stringValueForKey(descElmName)
    , internalDesc = stringValueForKey('INTERNAL_DESCRIPTION')
    , missingStrings = []

  if (!desc && toolName != 'ExprBuilder') missingStrings.push(descElmName)
  if (!isMetaQ && !isNumberPicker && !toolName) missingStrings.push('TOOL_KEY')

  if (missingStrings.length) {
    return { error:util.format('invalid plist - missing strings for keys: "%s"', missingStrings.join('","')) }
  }

  if (isMetaQ || isNumberPicker || tool) {
    return { problemDescription:desc, internalDescription:internalDesc, toolId:tool && tool._id }
  } else {
    return { error:util.format('invalid plist. Could not retrieve tool with name "%s".\n\n%s', toolName, plistString) }
  }
}

function appEditPDef(userLoginName, pId, pRev, pdef, callback) {
  var user

  for (var urId in kcm.docStores.users) {
    if (kcm.docStores.users[urId].loginName == userLoginName) {
      user = kcm.docStores.users[urId]
      break
    }
  }

  if (!user) {
    callback(util.format('user with loginName "%s" not found', userLoginName), 404)
    return
  }

  var problem = kcm.getDocClone(pId, 'problem')

  if (!problem) {
    callback(util.format('problem with id "%s" not found', pId), 404)
    return
  }

  if (problem._rev !== pRev) {
    // TODO: return details of most recent edits since _rev=rev
    callback(JSON.stringify({ rev:problem._rev }), 409)
    return
  }

  nextVersion(problem, user._id, 'appEditPDef')

  // TODO: delete the conversion to XML after ensuring that all probs have pdef json
  var plistString = plist.build(pdef).toString()
  // N.B. for some reason this module converts all string keys to data keys and base 64 encodes the string
  var pdefXML = libxmljs.parseXmlString(plistString)
  pdefXML.find('//data').forEach(function(dn) {
    var decoded = new Buffer(dn.text(), 'base64').toString()
    dn.text(decoded)
    dn.name('string')
  })
  plistString = pdefXML.toString()

  var info = getProblemInfoFromPList(plistString)
  if (info.error) {
    callback(info.error.match(/^[^\n]*/)[0], 400)
    return
  }
  problem.pdef = pdef
  problem.problemDescription = info.problemDescription
  problem.internalDescription = info.internalDescription
  problem.dataModified = new Date()

  problem._attachments['pdef.plist'] = {
    data: new Buffer(plistString).toString('base64')
    , 'Content-Type': 'application/xml'
  }

  request({
    method: 'PUT'
    , uri: databaseURI + problem._id
    , body: JSON.stringify(problem)
  }, function(e,r,b) {
    var sc = r && r.statusCode || 500
    if (sc !== 201) {
      callback(util.format('failed to update pdef.\nstatusCode:%s\ndatabase error:%s', r.statusCode, e || b), sc)
    } else {
      var newRev = JSON.parse(b).rev
      callback(null, 201, newRev)
    }
  })
}

// Nodes
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

// Relations
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

function removeOrderedPairFromBinaryRelation(user, relationId, relationRev, pair, callback) {
  var noStringErrors = []
  if (typeof relationId != 'string' || !relationId.length) noStringErrors.push('relationId')
  if (typeof relationRev != 'string' || !relationRev.length) noStringErrors.push('relationRev')
  if (noStringErrors.length) {
    callback('BAD ARGS - strings required for: ' + noStringErrors.join(), 412)
    return
  }

  if (Object.prototype.toString.call(pair) != '[object Array]' || typeof pair[0] != 'string' || typeof pair[1] != 'string') {
    callback('BAD ARGS - array of strings required for parameter: pair', 412)
    return
  }

  var br = kcm.getDocClone(relationId, 'relation')

  if (!br) {
    callback(util.format('could not find relation with id="%s"', relationId), 404)
    return
  }

  if (!br.relationType == 'binary') {
    callback(util.format('relation with id="%s" has relationType="%s". Binary required', relationId, br.relationType), 412)
    return
  }

  if (br._rev != relationRev) {
    callback(util.format('relation revisions do not correspond. supplied:"%s", database:"%s"', relationRev, br._rev), 409)
    return
  }

  var ix
  for (ix=0; ix<br.members.length; ix++) {
    if (br.members[ix][0] == pair[0] && br.members[ix][1] == pair[1]) break
  }

  if (ix == br.members.length) {
    callback(util.format('ordered pair ["%s", "%s"] not a member of binary relation id="%s"', pair[0], pair[1], relationId), 412)
    return
  }

  nextVersion(br, user, 'removeOrderedPairFromBinaryRelation', pair)
  br.members.splice(ix, 1)

  updateDoc(br, function(e,r,b) {
    callback(e, r && r.statusCode)
  })
}

// Pipelines
function uploadPipelineFolder(user, o, callback) {
  var cn, pl
    , errors = []
    , docsToSave = []
    , now = (new Date).toJSON()
    , decodedPDef, info

  if (typeof o.cnId != 'string') errors.push('string required for cnId') 
  if (typeof o.cnRev != 'string') errors.push('string required for cnRev')
  if (typeof o.plName !='string') errors.push('string required for plName')
  if (Object.prototype.toString.call(o.pdefs) != '[object Array]') errors.push('array required for pdefs')
  if (errors.length) {
    callback(errors.join('\n'), 412)
  }

  cn = kcm.getDocClone(o.cnId, 'concept node')

  if (!cn) {
    callback(util.format('node with id="%s not found', o.cnId), 404)
    return
  }

  if (cn._rev != o.cnRev) {
    callback('concept node revision conflict', 409)
    return
  }

  if (o.plId) {
    if (typeof o.plRev != 'string') {
      callback('revision required for pipeline', 412)
      return
    }

    pl = kcm.getDocClone(o.plId, 'pipeline')

    if (!pl) {
      callback(util.format('pipeline with id="%s" not found', o.plId), 404)
      return
    }

    if (pl._rev != o.plRev) {
      callback('pipeline revision conflict', 409)
      return
    }

    if (pl.conceptNode !== cn._id) {
      callback('concept node id on pipeline conflicts with supplied pipeline id')
      return
    }

    nextVersion(pl, user, 'uploadPipelineFolder')

    pl.problems.forEach(function(pId) {
      var p = kcm.getDocClone(pId, 'problem')
      if (p) {
        p._deleted = true
        docsToSave.push(p)
      }
    })
  } else {
    pl = firstVersion(user, 'uploadPipelineFolder')
    pl._id = generateUUID()
    pl.type = 'pipeline'
    pl.conceptNode = o.cnId

    docsToSave.push(cn)
    nextVersion(cn, user, 'uploadPipelineFolder', pl._id)
    cn.pipelines.push(pl._id)
  }

  pl.name = o.plName
  pl.workflowStatus = 0
  pl.problems = []

  for (var i=0; i<o.pdefs.length; i++) {
    pl.problems[i] = generateUUID()

    decodedPDef = new Buffer(o.pdefs[i], 'base64').toString()
    info = getProblemInfoFromPList(decodedPDef)

    if (info.error) {
      callback(util.format('error with problem %d of %d in pipeline: %s', i+1, o.pdefs.length, info.error), 500)
      return
    }

    docsToSave.push({
      _id: pl.problems[i]
      , type: 'problem'
      , pipeline: pl._id
      , conceptNode: cn._id
      , problemDescription: info.problemDescription
      , internalDescription: info.internalDescription
      , toolId: info.toolId
      , dateCreated: now
      , dateModified: now
      , pdef: plist.parseStringSync(decodedPDef)
      , _attachments: {
        'pdef.plist': {
          'Content-Type': 'application/xml'
          , data: o.pdefs[i]
        }
      }
    })
  }

  docsToSave.push(pl)

  request({
    method:'POST'
    , uri: databaseURI + '_bulk_docs'
    , headers: { 'content-type':'application/json', accepts:'application/json' }
    , body: JSON.stringify({ docs:docsToSave })
  }, function(e,r,b) {
    if (!r || 201 != r.statusCode) {
      e = util.format('Error uploading docs. Database reported error: "%s"', e)
    }
    callback(e, r && r.statusCode)
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
  if (!cn.pipelines) cn.pipelines = []

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
  pl.conceptNode = conceptNodeId
  pl.name = pipelineName
  pl.problems = []
  pl.workflowStatus = 0

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

  if (!~ix) {
    callback(util.format('Error: Pipeline with id="%s" not found on concept node with id="%s". The pipeline was not deleted.', plId, cnId), 412)
    return
  }

  var docs = [pl, cn].concat(pl.problems.map(function(pId) {
    var p = kcm.getDocClone(pId, 'problem')
    nextVersion(p, user, 'deletePipeline')
    p._deleted = true
  }))

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
  
  var p = kcm.getDocClone(problemId, 'problem')
  var pl = kcm.getDocClone(pipelineId, 'pipeline')

  if (!p) {
    callback(util.format('problem with id="%s" not found', problemId), 404)
    return
  }
  if (!pl) {
    callback(util.format('pipeline with id="%s" not found', pipelineId), 404)
    return
  }
  if (pl._rev != pipelineRev) {
    callback(util.format('Error: Pipeline id="%s" revisions do not correspond. Supplied:"%s", Database:"%s". Problem with id="%s" not removed from pipeline', pl._id, pipelineRev, pl._rev, p._id), 409)
    return
  }

  var pIx = pl.problems.indexOf(p._id)
  if (!~pIx) {
    callback(util.format('Problem with id="%s" not found on pipeline with id="%s". The problem was not deleted from the pipeline.', problemId, pipelineId), 500)
    return
  }
  
  pl.problems.splice(pIx, 1)
  pl.workflowStatus = 0

  updateDoc(pl, function(e,r,b) {
    var sc = r && r.statusCode
    if (201 != sc) {
      callback(util.format('Error removing problem with id="%s" from pipeline with id="%s". Database reported error:"%s"', problemId, pipelineId, e), sc)
      return
    }
    callback(null,201,JSON.parse(b).rev)
    request.del(util.format('%s%s?rev=%s', databaseURI, p._id, p._rev))
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
  var pl = kcm.getDocClone(plId, 'pipeline')

  if (!pl) {
    callback(util.format('pipeline id="%s" not found', plId), 404)
    return
  }

  pl.problems = pl.problems.concat(problemIds)
  pl.workflowStatus = 0

  updateDoc(pl, function(e,r,b) {
    var sc = r && r.statusCode
    callback(sc == 201 ? null : util.format('error appending problems to pipeline. e="%s"  sc=%d', e, sc), sc || 500)
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
    callback('Error retrieving pipeline problem details.\n' + errors.join('\n'), 500)
    return
  }

  var pl = kcm.getDoc(id, 'pipeline')

  if (!pl) {
    callback(util.format('Pipeline with id="%s" not found', id), 404)
    return
  }

  if (pl._rev != rev) {
    callback(util.format('pipeline revisions do not correspond. supplied:"%s", database:"%s". The pipeline problems could not be retrieved.', rev, pl._rev), 500)
    return
  }

  if (Object.prototype.toString.call(pl.problems) != '[object Array]') {
    callback('pipeline badly formed - does not have problems array', 500)
    return
  }

  var problems = []
  var toolsStore = kcm.docStores.tools

  pl.problems.forEach(function(pId) {
    var p = kcm.getDoc(pId, 'problem')
    if (!p) {
      callback(util.format('pipeline problem with id="%s" could not be found', pId), 500)
      return
    }
    
    problems.push({
      id: p._id
      , desc: p.problemDescription
      , lastModified: typeof p.dateModified == 'string' ? p.dateModified.replace(/.{8}$/,'').replace(/[a-z]/gi, ' ') : ''
      , tool: p.toolId && toolsStore[p.toolId] && toolsStore[p.toolId].name || ''
    })
  })

  callback(null,200,problems)
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

// Export KCM
function getAppCannedDatabases(plutilCommand, userId, pipelineWorkflowStatuses, callback) {
  // download includes:
  //  1) all-users.db
  //  2) canned-content/ (i.e. KCM)
  //  3) user-state-template.db
  
  var tempProblemIdDescJSON = []

  var writeLog = true
    , path = '/tmp/' + generateUUID()
    , contentPath = path + '/canned-content'
    , pdefsPath = contentPath + '/pdefs'
    , contentDBPath = contentPath + '/content.db'
    , allUsersDBPath = path + '/all-users.db'
    , userStateTemplateDBPath = path + '/user-state-template.db'
    , exportLogWriteStream
    , content

  fs.mkdirSync(path)
  fs.mkdirSync(contentPath)
  fs.mkdirSync(pdefsPath)

  var origCallback = callback
  callback = function() {
    fs.writeFileSync(contentPath + '/problems-ids-descriptions.json', JSON.stringify(tempProblemIdDescJSON,null,2), 'utf8')
    if (exportLogWriteStream) exportLogWriteStream.end()
    origCallback.apply(null, [].slice.call(arguments))
  }

  var gotoNext = function(next) {
    if (typeof next === 'function') {
      next.apply(null, [].slice.call(arguments, 1))
    } else {
      callback(null, 200, path)
    }
  }

  // 1) all-users.db
  var createAllUsers = function() {
    var args = arguments
      , db = new sqlite3.Database(allUsersDBPath)

    db.serialize(function() {
      db.run("CREATE TABLE users (id TEXT PRIMARY KEY ASC, nick TEXT, password TEXT, flag_remove INTEGER, last_server_process_batch_date REAL, nick_clash INTEGER)")
      db.run("CREATE TABLE NodePlays (episode_id TEXT PRIMARY KEY ASC, batch_id TEXT, user_id TEXT, node_id TEXT, start_date REAL, last_event_date REAL, ended_pauses_time REAL, curr_pause_start_date REAL, score INTEGER)")
      db.run("CREATE TABLE ActivityFeed (batch_id TEXT, user_id, TEXT, event_type TEXT, date REAL, data TEXT)")
      db.run("CREATE TABLE FeatureKeys (batch_id TEXT, user_id TEXT, key TEXT, date REAL)")

      db.close(function() {
        gotoNext.apply(null, args)
      })
    })
  }
  
  // 2) KCM
  var createContent = function() {
    var args = arguments
      , user = kcm.docStores.users[userId]
      , exportSettings = user.exportSettings
      , nodeExclusionTags = exportSettings.nodeExclusionTags
      , conceptNodeRequiredTags = exportSettings.conceptNodeRequiredTags
      , exportAllNodes  = exportSettings.exportAllNodes === true // export all OTHER nodes
      , nodeInclusionTags = exportSettings.nodeInclusionTags
      , nodeTagsToBitCols = exportSettings.nodeTagsToBitCols
      , nodeTagPrefixesToTextCols = exportSettings.nodeTagPrefixesToTextCols
      , pipelineNames = exportSettings.pipelineNames
      , includedPipelineWorkflowStatuses
      , exportLogWriteStream

    var getAppContentJSON = function() {
      var masteryPairs = kcm.cloneDocByTypeName('relation', 'Mastery').members

      // which nodes & pipelines to include
      var incNodes = {}
        , incPipelines = []

      _.each(kcm.docStores.nodes, function(n) {
        if (!~n.tags.indexOf('mastery')) return
        if (_.intersection(exportSettings.nodeExclusionTags, n.tags).length) return

        var conceptNodes = _.map(
          _.filter(masteryPairs, function(p) { return p[1] == n._id })
          , function(p) { return kcm.docStores.nodes[p[0]] })

        conceptNodes = JSON.parse(JSON.stringify(
          conceptNodes.filter(function(cn) {
            if (_.intersection(exportSettings.nodeExclusionTags, cn.tags).length) return false
            return !_.difference(exportSettings.conceptNodeRequiredTags, cn.tags).length
          })
        ))

        conceptNodes.forEach(function(cn) {
          var cnPipelines = cn.pipelines.map(function(plId) { return kcm.docStores.pipelines[plId] })
          cnPipelines = cnPipelines.filter(function(pl) {
            return pl.problems.length && ~includedPipelineWorkflowStatuses.indexOf(pl.workflowStatus) && ~exportSettings.pipelineNames.indexOf(pl.name)
          })

          cn.pipelines = _.pluck(cnPipelines, '_id')

          if (cnPipelines.length) {
            cn.hasPipelineInExport = true 
            incPipelines = incPipelines.concat(cnPipelines)
          }
        })

        conceptNodes = conceptNodes.filter(function(cn) {
          return cn.hasPipelineInExport || exportSettings.exportAllNodes || _.intersection(exportSettings.nodeInclusionTags, cn.tags).length
        })

        if (conceptNodes.length || exportSettings.exportAllNodes || _.intersection(exportSettings.nodeInclusionTags, n.tags).length) {
          incNodes[n._id] = n

          conceptNodes.forEach(function(cn) {
            incNodes[cn._id] = cn
          })
        }
      })

      // pipelines & problems
      var pipelines = {}
        , problems = {}

      incPipelines.forEach(function(pl) {
        pipelines[pl._id] = { id:pl._id, rev:pl._rev, name:pl.name, workflowStatus:pl.workflowStatus, problems:pl.problems }

        pl.problems.forEach(function(prId) {
          var problem = kcm.docStores.problems[prId]
          problems[prId] = { id:problem._id, rev:problem._rev }
        })
      })

      // nodes
      var nodes = {}
      var tagTextRegExps = _.map(nodeTagPrefixesToTextCols, function(prefix) { return new RegExp(util.format('^%s:\\s*(.+)', prefix)) })
      _.each(incNodes, function(n, id) {
        nodes[id] = { id:n._id, rev:n._rev, pipelines:n.pipelines, x:n.x, y:n.y, workflowStatus:0 }
        
        if (n.pipelines.length) {
          nodes[id].workflowStatus = Math.min.apply(Math, _.map(nodes[id].pipelines, function(plId) { return pipelines[plId].workflowStatus }))
        }

        nodeTagsToBitCols.forEach(function(tag) {
          nodes[id][tag] = ~n.tags.indexOf(tag) ? 1 : 0
        })

        nodeTagPrefixesToTextCols.forEach(function(tagPrefix, i) {
          var re = tagTextRegExps[i]
            , matchingTags = _.filter(n.tags, function(tag) { return tag.match(re) != null })
          nodes[id][tagPrefix] = JSON.stringify(_.map(matchingTags, function(t) { return t.substring(1 + tagPrefix.length) }))
        })
      })

      // binary relations
      var binaryRelations = []
      _.each(kcm.docStores.relations, function(r) {
        if (!~['binary','chained-binary'].indexOf(r.relationType)) return

        var pairs = r.relationType == 'binary' ? r.members : kcm.chainedBinaryRelationMembers(r)

        binaryRelations.push({
          id: r._id
          , rev: r._rev
          , name: r.name
          , members: pairs.filter(function(p) { return nodes[p[0]] && nodes[p[1]] })
        })
      })

      return { conceptNodes:_.values(nodes), pipelines:_.values(pipelines), binaryRelations:binaryRelations, problems:_.values(problems) }
    }

    // set problem pdefs as property
    var getPDefs = function(problems, next) {
      var numProblems = problems.length
        , numPDefs = 0
        , sentError = false

      if (!numProblems) {
        next()
        return
      }

      problems.forEach(function(p) {
        request({ uri: databaseURI + p.id + '/pdef.plist' }, function(e,r,b) {
          if (sentError) return
          var sc = r && r.statusCode || 500
          if (e || sc !== 200) {
            callback(util.format('error retrieving pdef for problem id="%s" from database, error="%s", statusCode=%d', p.id, e || b, sc), sc)
            sentError = true
            return
          }

          var onSuccess = function(pdef) {
            // TODO: Remove (probably)
            tempProblemIdDescJSON.push([p.id, pdef.PROBLEM_DESCRIPTION])

            p.pdef = JSON.stringify(pdef)
            if (++numPDefs == numProblems) next()
          }

          if (/^bplist/.test(b)) {
            var pPath = util.format('%s/%s.plist', pdefsPath, p.id)
            fs.writeFile(pPath, b, 'utf8', function(e) {
              if (e) {
                callback(util.format('error writing binary plist id="%s". error="%s"', p.id, e))
                sentError = true
                return
              }

              exec(plutilCommand + pPath, function(e) {
                if (e) {
                  callback(util.format('error decompiling plist id="%s". error="%s"', p.id, e))
                  sentError = true
                  return
                }

                var pdef = plist.parseFileSync(pPath)
                if (!pdef) {
                  callback(util.format('error parsing decompiled plist id="%s". error="%s"', p.id, e))
                  sentError = true
                  return
                }
                onSuccess(pdef)
              })
            })
          } else {
            var pdef = plist.parseStringSync(b)
            if (!pdef) {
              callback(util.format('error parsing plist id="%s". error="%s"', p.id, e))
              sentError = true
              return
            }
            onSuccess(pdef)
          }
        })
      })
    }

    function createSQLiteDB(content, cb) {
      var db = new sqlite3.Database(contentDBPath)
      db.serialize(function() {
        var tagBitColDecs = _.map(nodeTagsToBitCols, function(tag) { return util.format(', %s INTEGER', tag) }).join('')
        var tagTextColDecs = _.map(nodeTagPrefixesToTextCols, function(tagPrefix) { return util.format(', %s TEXT', tagPrefix) }).join('')
        exportLogWriteStream.write('\n================================================\nCREATE TABLE ConceptNodes\n')
        var cnTableCreateScript = util.format("CREATE TABLE ConceptNodes (id TEXT PRIMARY KEY ASC, rev TEXT, pipelines TEXT, workflowStatus INTEGER, x INTEGER, y INTEGER%s%s)", tagBitColDecs, tagTextColDecs)
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
        db.run("CREATE TABLE Problems (id TEXT PRIMARY KEY ASC, rev TEXT, pdef TEXT, last_saved_pdef TEXT, change_stack TEXT, stack_current_index INTEGER, stack_last_save_index INTEGER)")
        exportLogWriteStream.write('\n================================================\nINSERT INTO Problems\n')
        var probsIns = db.prepare("INSERT INTO Problems VALUES (?,?,?, ?,?,?, ?)")
        content.problems.forEach(function(p) {
          exportLogWriteStream.write('['+p.id+']')
          probsIns.run(p.id, p.rev, p.pdef, p.pdef, '[]', 0, 0)
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

    var allWfStatusLevels = _.pluck(pipelineWorkflowStatuses, 'value')
    switch (exportSettings.pipelineWorkflowStatusOperator) {
      case '<=':
        includedPipelineWorkflowStatuses = _.filter(allWfStatusLevels, function(l) { return l <= exportSettings.pipelineWorkflowStatusLevel })
      break
      case '==':
        includedPipelineWorkflowStatuses = _.filter(allWfStatusLevels, function(l) { return l == exportSettings.pipelineWorkflowStatusLevel })
      break
      case '>=':
        includedPipelineWorkflowStatuses = _.filter(allWfStatusLevels, function(l) { return l >= exportSettings.pipelineWorkflowStatusLevel })
      break
      default:
        callback(util.format('invalid pipeline workflow status operator:"%s"', exportSettings.pipelineWorkflowStatusOperator), 500)
      return
    }

    if (writeLog) exportLogWriteStream = fs.createWriteStream(contentPath + '/export-log.txt') 
    if (writeLog) exportLogWriteStream.write('================================================\nExport Settings Couch Document:\n' + JSON.stringify(exportSettings,null,4) + '\n================================================\n')

    content = getAppContentJSON()
    if (writeLog) exportLogWriteStream.write('\n================================================\nKCM Content shaped for use as source of SQLite database:\n' + JSON.stringify(content,null,4) + '\n================================================\n')

    getPDefs(content.problems, function() { // only reachable on getPDefs success
      createSQLiteDB(content, function(e, statusCode) {
        if (201 != statusCode) {
          callback(e || 'error creating content database', statusCode || 500)
        } else {
          gotoNext.apply(null, args)
        }
      })
    })
  }
  
  // 3) user-state-template.db
  var createUserStateTemplate = function() {
    var args = arguments
      , db = new sqlite3.Database(userStateTemplateDBPath)

    db.serialize(function() {
      db.run("CREATE TABLE Nodes (id TEXT PRIMARY KEY ASC, time_played REAL, last_played REAL, last_score INTEGER, total_accumulated_score INTEGER, high_score INTEGER, first_completed REAL, last_completed REAL, \
             artifact_1_last_achieved REAL, artifact_1_curve TEXT, artifact_2_last_achieved REAL, artifact_2_curve TEXT, artifact_3_last_achieved REAL, artifact_3_curve TEXT, \
             artifact_4_last_achieved REAL, artifact_4_curve TEXT, artifact_5_last_achieved REAL, artifact_5_curve TEXT)")

      var cnIns = db.prepare("INSERT INTO Nodes(id, time_played, total_accumulated_score, high_score) VALUES (?,?,?,?)")
      content.conceptNodes.forEach(function(n) {
        cnIns.run.apply(cnIns, [n.id, 0, 0, 0])
      })
      cnIns.finalize()

      db.run("CREATE TABLE ActivityFeed (event_type TEXT, date REAL, data TEXT)")
      db.run("CREATE TABLE FeatureKeys (key TEXT PRIMARY KEY ASC, encounters TEXT)")

      db.close(function() {
        gotoNext.apply(null, args)
      })
    })
  }

  createAllUsers(createContent, createUserStateTemplate)
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


// Unused
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
