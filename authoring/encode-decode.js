exports.encode = getEnDeCoder(true)
exports.decode = getEnDeCoder(false)

function getEnDeCoder(encoder) {
  var pairs = 
    [ ['&lt;', '<']
    , ['&gt;', '>']
    , ['&amp;', '&']
    , ['&apos;', "'"]
    , ['&quot;', '"']
    ]

  var dict = pairs.reduce(function(o, pair) {
    o[pair[Number(encoder)]] = pair[Number(!encoder)]
    return o
  }, {})

  var re = new RegExp(Object.keys(dict).join('|'), 'g')

  return function fn(field) {
    if (Array.isArray(field)) {
      field.forEach(function(item, i) { field[i] = fn(item) })
      return field

    } else if (field && typeof field == 'object') {
      Object.keys(field).forEach(function(key) { field[key] = fn(field[key]) })
      return field

    } else if (typeof field == 'string') {
      return field.replace(re, function(s) { return dict[s] })

    } else {
      return field
    }
  }
}
