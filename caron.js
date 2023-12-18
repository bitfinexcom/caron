'use strict'

const debug = require('debug')('caron:caron')

const Redis = require('ioredis')

const elapsedTime = (start) => {
  return process.hrtime(start)
}

class Caron {
  constructor (opts) {
    this.status = {
      active: true,
      processing: false,
      rseed: Date.now()
    }

    debug('started')

    this.ptype = opts.type
    this.freq = opts.freq
    this.exit = opts.exit || true

    this.redis = Redis.createClient(opts.redis)
    this.registerHandlers()

    const scripts = this.setupScripts(opts)
    this.setupRedis(scripts)
  }

  stop (cb) {
    if (!this.status.active) return

    this.status.active = false

    debug('stopping...')

    this.stopInter = setInterval(() => {
      if (this.status.processing) return
      debug('stopped')

      clearInterval(this.stopInter)
      cb(null)
    }, 250)
  }

  start () {
    this.work()

    return this
  }

  kill () {
    if (!this.exit) return
    process.exit()
  }

  work () {
    if (!this.status.active || this.status.processing) return

    const rseed = Date.now()

    if (rseed < this.status.rseed) {
      setTimeout(() => {
        this.work()
      }, 10)
      return
    }

    const args = []
    let startTs = null

    if (debug) {
      startTs = process.hrtime()
    }

    args.push(
      this.ptype,
      Date.now(), // timestamp
      rseed
    )

    args.push(
      (err, res) => {
        this.status.processing = false

        if (err) {
          console.error(err)
          this.stop(() => {
            this.kill()
          })
          return
        }

        if (debug.enabled) {
          const elapsed = elapsedTime(startTs)
          debug(res[1] + ' jobs processed in ' + elapsed[0] + 's,' + Math.round(elapsed[1] / 1000) + 'µs')
        }

        setTimeout(() => {
          this.work()
        }, this.freq || 50)
      }
    )

    this.status.processing = true

    this.redis.qwork.apply(
      this.redis, args
    )
  }

  registerHandlers () {
    this.redis.on('error', e => {
      this.status.processing = false
      console.log(e)
    })
  }

  setupRedis (scripts) {
    const script = [
      scripts.prefix,
      scripts[this.ptype].lua,
      scripts.suffix
    ].join('\n')

    debug(script)

    this.redis.defineCommand('qwork', {
      lua: script,
      numberOfKeys: 0
    })
  }

  setupScripts (program) {
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
      ].join('\n'),
      suffix: [
        'end',
        'return {err, cnt}'
      ].join('\n'),
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
      ].join('\n')
    }

    scripts.bull = {
      lua: [
        'local pushCmd = "' + (program.q_lifo ? 'R' : 'L') + 'PUSH"',
        'local jobId = redis.call("INCR", "bull:" .. jqueue .. ":id")',
        'local jattempts = cmsg["$attempts"]',
        'local jdelay = cmsg["$delay"]',
        'local jbackoff = cmsg["$backoff"]',
        'local jobCustomId = cmsg["$jobId"]',
        'if not(jobCustomId == nil or jobCustomId == "") then jobId = jobCustomId end',
        'cmsg["$attempts"] = nil',
        'cmsg["$delay"] = nil',
        'cmsg["$backoff"] = nil',
        'cmsg["$jodId"] = nil',
        'if (type(jattempts) ~= "number") or (jattempts <= 0) then jattempts = ' + program.def_attempts + ' end',
        'if (type(jdelay) ~= "number") or (jdelay < 0) then jdelay = 0 end',
        'local jopt0 = cmsg["$removeOnComplete"]',
        'if (type(jopt0) ~= "boolean") then jopt0 = true end',
        'local jopts = { removeOnComplete = jopt0, attempts = jattempts, delay = jdelay, backoff = jbackoff }',
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
        '    redis.call("PUBLISH", "bull:" .. jqueue .. ":waiting@null", jobId)',
        '  end',
        'end'
      ].join('\n')
    }

    scripts.sidekiq = {
      lua: scripts.ruby_common_1 + '\n' + [
        'redis.call("LPUSH", "' + program.q_prefix + 'queue:" .. jqueue, payload)'
      ].join('\n')
    }

    return scripts
  }
}

module.exports = Caron
