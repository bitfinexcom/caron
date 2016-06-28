'use strict'

const Redis = require('ioredis')
const crypto = require('crypto')
const program = require('commander')

program
  .version('0.0.2')
  .option('-t, --type <type>', 'queue type (sidekiq/bull)')
  .option('-l, --list <list>', 'source redis list')
  .option('-r, --redis <redis>', 'redis url')
  .option('--debug', 'debug')

  .parse(process.argv)

if (!program.type || !program.list || !program.redis) {
  program.help()
  process.exit()
}

console.log('caron(' + program.redis + '/' + program.list + '/' + program.type + ')')

var redis = Redis.createClient(program.redis, {
  dropBufferSupport: true
})

var scripts = {
  prefix: [
    'local cnt = 0',
    'local err = 0',
    'while (redis.call("LLEN", "' + program.list + '") ~= 0) do',
    '  local msg = redis.call("RPOP", "' + program.list + '")',
    '  if not msg then break end',
    '  local cmsg = cjson.decode(msg)',
    '  if not cmsg or type(cmsg) ~= "table" then',
    '    err = -2',
    '    break',
    '  end'
  ].join("\n"),
  suffix: [
    '  cnt = cnt + 1',
    'end',
    'return {err, cnt}'
  ].join("\n"),
  bull: {
    lua: [
      'if not cmsg["queue"] then cmsg["queue"] = "default" end',
      'local jobId = redis.call("INCR", "bull:" .. cmsg["queue"] .. ":id")',
      'redis.call("HMSET", "bull:" .. cmsg["queue"] .. ":" .. jobId, "data", msg, "opts", "{}", "progress", 0, "delay", 0, "timestamp", ARGV[1], "attempts", 1, "attemptsMade", 0, "stacktrace", "[]", "returnvalue", "null")',
      'if redis.call("EXISTS", "bull:" .. cmsg["queue"] .. ":meta-paused") ~= 1 then',
      '  redis.call("LPUSH", "bull:" .. cmsg["queue"] .. ":wait", jobId)',
      'else',
      '  redis.call("LPUSH", "bull:" .. cmsg["queue"] .. ":paused", jobId)',
      'end',
      'redis.call("PUBLISH", "bull:" .. cmsg["queue"] .. ":jobs", jobId)',
    ].join("\n")
  },
  sidekiq: {
    lua: [
      'if not cmsg["queue"] then',
      '  cmsg["queue"] = "default"',
      'end',
      'if not cmsg["class"] then',
      '  cmsg["class"] = "BaseJob"',
      'end',
      'local payload = { queue = cmsg["queue"], jid = ARGV[1], class = cmsg["class"], args = { cmsg } }',
      'redis.call("LPUSH", "queue:" .. cmsg["queue"], cjson.encode(payload))',
      'redis.call("SADD", "queues", cmsg["queue"])',
    ].join("\n")
  }
}

var ptype = program.type

if (!scripts[ptype]) {
  program.help()
  process.exit()
}

var script = [scripts.prefix, scripts[ptype].lua, scripts.suffix].join("\n")

redis.defineCommand('qwork', {
  lua: script,
  numberOfKeys: 0
})

var elapsed_time = (start) => {
  return process.hrtime(start)
}

var work = () => {
  var args = []
  var ts_start = null
  
  if (program.debug) {
    ts_start = process.hrtime()
  }
  
  switch (ptype) {
    case 'bull':
      args.push(Math.floor((new Date()).getTime() * 1000))
      break
    case 'sidekiq':
      args.push(crypto.randomBytes(12).toString('hex'))
      break
  }
  
  args.push(
    (err, res) => {
      if (err) {
        console.error(err)
        process.exit()
        return
      }

      if (program.debug) {
        let elapsed = elapsed_time(ts_start)
        console.log(res[1] + ' jobs processed in ' + elapsed[0] + 's,' + Math.round(elapsed[1] / 1000) + 'µs')
      }

      setTimeout(() => {
        work()
      }, 100)
    }
  )

  redis.qwork.apply(
    redis, args
  )
}

work()
