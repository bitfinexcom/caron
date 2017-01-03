'use strict'

const Redis = require('ioredis')
const crypto = require('crypto')

const program = require('yargs')
  .option('t', {
    describe: 'queue type',
    alias: 'type',
    choices: ['sidekiq', 'bull', 'resque'],
    demand: true
  })
  .option('l', {
    describe: 'source redis list (e.g: `my_job_queue`)',
    alias: 'list',
    demand: true,
    type: 'string'
  })
  .option('r', {
    describe: 'redis url',
    alias: 'redis',
    default: 'redis://127.0.0.1:6379',
    type: 'string'
  })
  .option('f', {
    describe: 'poll frequency (milliseconds)',
    alias: 'freq',
    default: 10,
    type: 'number'
  })
  .option('b', {
    describe: 'max number of jobs processed per batch',
    alias: 'batch',
    default: 100,
    type: 'number'
  })
  .option('q_prefix', {
    describe: 'redis queue prefix (e.g: "production:")',
    default: '',
    type: 'string'
  })
  .option('q_lifo', {
    describe: 'Bull LIFO mode',
    default: false,
    type: 'boolean'
  })
  .option('def_queue', {
    describe: 'default destination queue',
    default: 'default',
    type: 'string'
  })
  .option('def_worker', {
    describe: 'default job queue worker',
    default: 'BaseJob',
    type: 'string'
  })
  .option('def_attempts', {
    describe: 'default job attempts',
    default: 1,
    type: 'number'
  })
  .boolean('debug')
  .help('help')
  .version()
  .usage('Usage: $0 -t <val> -l <val> -r <val>')
  .argv

const ptype = program.type
const debug = program.debug

console.log('caron(' + program.redis + '/' + program.list + '/' + program.type + ')')

if (debug) console.log('started')

const STATUS = {
  active: 1,
  processing: 0,
  rseed: Date.now()
}

Redis.Promise.onPossiblyUnhandledRejection(e => {
  console.error(e)
  STATUS.processing = 0
  stop()
})

const redis = Redis.createClient(program.redis)

redis.on('error', e => {
  STATUS.processing = 0
  console.log(e)
})

const scripts = {
  prefix: [
    'math.randomseed(tonumber(ARGV[3]))',
    'local function get_random_string(length)',
    '  local str = ""',
    '  for i = 1, length do',
    '    local rid = math.random()',
    '    if rid < 0.3333 then',
    '      str = str..string.char(math.random(48, 57))',
    '    else',
    '      str = str..string.char(math.random(97, 122))',
    '    end',
    '  end',
    '  return str',
    'end',
    'local cnt = 0',
    'local err = 0',
    'while ((redis.call("LLEN", "' + program.list + '") ~= 0) and (cnt < ' + program.batch + ')) do',
    '  cnt = cnt + 1',
    '  local msg = redis.call("RPOP", "' + program.list + '")',
    '  if not msg then break end',
    '  local valid_json, cmsg = pcall(cjson.decode, msg)',
    '  if not valid_json or not cmsg or type(cmsg) ~= "table" then',
    '    err = -2',
    '    break',
    '  end',
    '  if not cmsg["$queue"] then',
    '    cmsg["$queue"] = "' + program.def_queue + '"',
    '  end',
    '  local jqueue = cmsg["$queue"]',
    '  cmsg["$queue"] = nil'
  ].join("\n"),
  suffix: [
    'end',
    'return {err, cnt}'
  ].join("\n"),
  ruby_common_1: [
    'local jretry = cmsg["$retry"]',
    'if not jretry then jretry = false end',
    'if not cmsg["$class"] then cmsg["$class"] = "' + program.def_worker + '" end',
    'local payload = { queue = jqueue, class = cmsg["$class"], retry = jretry }',
    'if (ARGV[1] == "sidekiq") or (ARGV[1] == "sidekiq") then',
    '  payload["created_at"] = ARGV[2]',
    '  if not cmsg["$jid"] then cmsg["$jid"] = get_random_string(24) end',
    '  payload["jid"] = cmsg["$jid"]',
    '  if not cmsg["$backtrace"] then cmsg["$backtrace"] = false end',
    '  payload["backtrace"] = cmsg["$backtrace"]',
    'end',
    'cmsg["$class"] = nil',
    'if (not cmsg["$args"]) or (type(cmsg["$args"]) ~= "table") or (next(cmsg["$args"]) == nil) then cmsg["$args"] = "ARRAY_EMPTY" end',
    'payload["args"] = cmsg["$args"]',
    'payload = cjson.encode(payload)',
    'payload = string.gsub(payload, \'"ARRAY_EMPTY"\', "[]")',
    'payload = string.gsub(payload, \':{}\', ":null")',
    'redis.call("SADD", "' + program.q_prefix + 'queues", jqueue)'
  ].join("\n")
}

scripts.bull = {
  lua: [
    'local pushCmd = "' + (program.q_lifo ? 'R' : 'L') + 'PUSH"',
    'local jobId = redis.call("INCR", "bull:" .. jqueue .. ":id")',
    'local jattempts = cmsg["$attempts"]',
    'local jdelay = cmsg["$delay"]',
    'local jobCustomId = cmsg["$jobId"]',
    'if not(jobCustomId == nil or jobCustomId == "") then jobId = jobCustomId end',
    'cmsg["$attempts"] = nil',
    'cmsg["$delay"] = nil',
    'cmsg["$jodId"] = nil',
    'if (type(jattempts) ~= "number") or (jattempts <= 0) then jattempts = ' + program.def_attempts + ' end',
    'if (type(jdelay) ~= "number") or (jdelay < 0) then jdelay = 0 end',
    'local jopt0 = cmsg["$removeOnComplete"]',
    'if (type(jopt0) ~= "boolean") then jopt0 = true end',
    'local jopts = { removeOnComplete = jopt0 }',
    'cmsg["$removeOnComplete"] = nil',
    'local jobIdKey = "bull:" .. jqueue .. ":" .. jobId',
    'if redis.call("EXISTS", jobIdKey) == 0 then',
    '  redis.call("HMSET", jobIdKey, "data", cjson.encode(cmsg), "opts", cjson.encode(jopts), "progress", 0, "delay", jdelay, "timestamp", ARGV[2], "attempts", jattempts, "attemptsMade", 0, "stacktrace", "[]", "returnvalue", "null")',
    '  if jdelay > 0 then',
    '    local timestamp = (tonumber(ARGV[2]) + jdelay) * 0x1000 + bit.band(jobId, 0xfff)',
    '    redis.call("ZADD", "bull:" .. jqueue .. ":delayed", timestamp, jobId)',
    '    redis.call("PUBLISH", "bull:" .. jqueue .. ":delayed", (timestamp / 0x1000))',
    '  else',
    '    if redis.call("EXISTS", "bull:" .. jqueue .. ":meta-paused") ~= 1 then',
    '      redis.call(pushCmd, "bull:" .. jqueue .. ":wait", jobId)',
    '    else',
    '      redis.call(pushCmd, "bull:" .. jqueue .. ":paused", jobId)',
    '    end',
    '    redis.call("PUBLISH", "bull:" .. jqueue .. ":jobs", jobId)',
    '  end',
    'end'
  ].join("\n")
}

scripts.sidekiq = {
  lua: scripts.ruby_common_1 + "\n" + [
    'redis.call("LPUSH", "' + program.q_prefix + 'queue:" .. jqueue, payload)',
  ].join("\n")
}
  
scripts.resque = {
  lua: scripts.ruby_common_1 + "\n" + [
    'redis.call("RPUSH", "' + program.q_prefix + 'queue:" .. jqueue, payload)',
  ].join("\n")
}

var script = [scripts.prefix, scripts[ptype].lua, scripts.suffix].join("\n")

redis.defineCommand('qwork', {
  lua: script,
  numberOfKeys: 0
})

const elapsed_time = (start) => {
  return process.hrtime(start)
}

const work = () => {
  if (!STATUS.active || STATUS.processing) return

  const rseed = Date.now()
  
  if (rseed < STATUS.rseed) {
    setTimeout(work, 5)
    return
  }

  const args = []
  var ts_start = null
  
  if (debug) {
    ts_start = process.hrtime()
  }
  
  args.push(
    ptype,
    Date.now(), //timestamp
    rseed
  )
  
  args.push(
    (err, res) => {
      STATUS.processing = 0

      if (err) {
        console.error(err)
        stop()
        return
      }

      if (debug) {
        let elapsed = elapsed_time(ts_start)
        console.log(res[1] + ' jobs processed in ' + elapsed[0] + 's,' + Math.round(elapsed[1] / 1000) + 'µs')
      }

      setTimeout(work, program.freq)
    }
  )

  STATUS.processing = 1

  redis.qwork.apply(
    redis, args
  )
}

work()

const stop = () => {
  if (!STATUS.active) return

  STATUS.active = 0
  
  if (debug) console.log('stopping...')
  
  setInterval(() => {
    if (STATUS.processing) return
    if (debug) console.log('stopped')
    process.exit()
  }, 250)
}

process.on('SIGINT', () => {
  stop()
})
