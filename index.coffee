request = require 'request'
_       = require 'lodash'
async   = require 'async'
program = require 'commander'
log4js  = require 'log4js'
mqes    = require 'mqes'

logger = log4js.getLogger 'default'
SCROLL_ID = ''

queryDate = (options, callback) ->
  par =
    uri: program.database
    method: 'POST'
    qs:
      size: +program.size or 10
      from: v if v = +program.from
      _source: v if v = program.source
      sort: v if v = program.sort
    json: mqes.convQuery JSON.parse program.query
  if program.scroll
    _.extend par.qs,
      scroll: '1m'
      search_type: 'scan'
  _.extend par.json, JSON.parse(v) if v = program.extend
  logger.debug '%j', par
  request par, (err, resp, body) ->
    return logger.error err if err
    return logger.error '%j', body if body.error
    SCROLL_ID = v if v = body._scroll_id
    result = body.hits.hits
    cc = body.hits.total
    logger.info 'total %d / %d', result.length, cc
    callback null, result


fetch = (callback) ->
  [protocol, __, domain] = program.database.split '/'
  par =
    method: 'GET'
    uri: [protocol, '//', domain, '/_search/scroll'].join ''
    qs:
      scroll_id: SCROLL_ID
      scroll: '1m'
  #logger.debug 'fetch %j', par
  request par, (err, resp, body) ->
    try obj = JSON.parse body
    return callback null, [] if not obj
    body = obj
    result = body.hits.hits
    delete body.hits.hits
    #logger.debug '%j', body
    SCROLL_ID = body._scroll_id if body._scroll_id
    callback null, result

scrollFetch = (iter, callback) ->
  way =
    cc: 0
    ls: []
  step = (cb) ->
    fetch (err, ee) ->
      _.each ee, iter
      way.ls = ee
      logger.info 'fetch %d get %d fin!', way.cc++, ee.length
      cb()
  test = () ->
    way.ls.length > 0
  async.doWhilst step, test, callback

delIter = (e, callback) ->
  [protocol, __, domain] = program.database.split '/'
  par =
    method: 'DELETE'
    uri: [protocol, '', domain, e._index, e._type, e._id].join '/'
  request par, (err, resp, body) ->
    return callback() or logger.error err if err
    logger.debug body
    callback()


main = () ->
  program.version '0.1.1'
    .option '-d --database <i>', 'http://es.api.com/xx/yy/_search'
    .option '-q --query <json of mqes>', 'eg. {"a":1}'
    .option '-f --from [index]', 'from index number; disable when --scroll'
    .option '-s --size [number]', 'size number; the number of results per shard when --scroll'
    .option '--source [f1,f2...]', 'params _source'
    .option '--sort [field:desc/asc]', 'sort by field; disable when --scroll'
    .option '--scroll', 'use scroll & echo with console.error, 使用2>>xx.log将输入重定向'
    .option '--delete [yes/Y]', 'delete query result'
    .option '--extend [json]', 'merge to request body, eg. script_fields'
    .parse process.argv

  if not program.query or not program.database
    return program.help()
  return logger.warn '--database value must endWith /_seach' if not /\/_search$/.test program.database
  queryDate {}, (err, list) ->
    if program.scroll
      wirte = (e) -> console.error '%j', e
      scrollFetch wirte, (err) ->
        logger.info 'scroll fin!!'
    else if program.delete in ['yes', 'Y']
      async.eachLimit list, 10, delIter
    else
      _.each list, (e) ->
        logger.debug '%j', e

module.exports = {main}
