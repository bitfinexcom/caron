'use strict'

const Redis = require('ioredis')
const crypto = require('crypto')
const program = require('commander')

program
  .version('0.0.6')
  .option('-t, --type <val>', 'queue type [sidekiq | bull]')
  .option('-l, --list <val>', 'source redis list (i.e: global_jobs)')
  .option('-r, --redis <val>', 'redis url (i.e: redis://127.0.0.1:6379)')
  .option('-f, --freq <n>', 'poll frequency (in milliseconds) - default: 10', parseInt)
  .option('-b, --batch <n>', 'max number of jobs created per batch - default: 1000', parseInt)
  .option('--def_queue <val>', 'default dest queue - default: default')
  .option('--def_sk_worker <val>', 'default Job Queue worker - default: BaseJob')
  .option('--def_bl_attempts <val>', 'default Bull Job attempts - default: 1', parseInt)
  .option('--debug', 'debug')
  .parse(process.argv)

if (!program.freq) program.freq = 10
if (!program.batch) program.batch = 1000
if (!program.def_queue) program.def_queue = 'default'
if (!program.def_sk_worker) program.def_sk_worker = 'BaseJob'
if (!program.def_bl_attempts) program.def_bl_attempts = 1

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
    'while ((redis.call("LLEN", "' + program.list + '") ~= 0) and (cnt < ' + program.batch + ')) do',
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
      'if not cmsg["queue"] then cmsg["queue"] = "' + program.def_queue + '" end',
      'local jobId = redis.call("INCR", "bull:" .. cmsg["queue"] .. ":id")',
      'redis.call("HMSET", "bull:" .. cmsg["queue"] .. ":" .. jobId, "data", msg, "opts", "{}", "progress", 0, "delay", 0, "timestamp", ARGV[1], "attempts", ' + program.def_bl_attempts + ', "attemptsMade", 0, "stacktrace", "[]", "returnvalue", "null")',
      'if redis.call("EXISTS", "bull:" .. cmsg["queue"] .. ":meta-paused") ~= 1 then',
      '  redis.call("LPUSH", "bull:" .. cmsg["queue"] .. ":wait", jobId)',
      'else',
      '  redis.call("LPUSH", "bull:" .. cmsg["queue"] .. ":paused", jobId)',
      'end',
      'redis.call("PUBLISH", "bull:" .. cmsg["queue"] .. ":jobs", jobId)'
    ].join("\n")
  },
  sidekiq: {
    lua: [
      'if not cmsg["queue"] then',
      '  cmsg["queue"] = "' + program.def_queue + '"',
      'end',
      'if not cmsg["class"] then',
      '  cmsg["class"] = "' + program.def_sk_worker + '"',
      'end',
      'local payload = { queue = cmsg["queue"], jid = ARGV[1], class = cmsg["class"], args = { cmsg } }',
      'redis.call("LPUSH", "queue:" .. cmsg["queue"], cjson.encode(payload))',
      'redis.call("SADD", "queues", cmsg["queue"])'
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
      }, program.freq)
    }
  )

  redis.qwork.apply(
    redis, args
  )
}

work()
