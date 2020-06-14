'use strict'

const debug = require('debug')('caron:caron')

const Redis = require('ioredis')
const fs = require('fs')
const { join } = require('path')

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

    this.engines = ['bull', 'sidekiq']

    debug('started')

    this.ptype = opts.type
    this.freq = opts.freq
    this.exit = opts.exit || true
    if (opts.q_lifo) {
      opts.q_lifo = 'R'
    } else {
      opts.q_lifo = 'L'
    }

    if (!this.engines.includes(this.ptype)) {
      console.error(`not a supported engine ${this.ptype}`)
      process.exit(1)
    }

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
    debug(scripts[this.ptype].lua)

    this.redis.defineCommand('qwork', {
      ...scripts[this.ptype],
      numberOfKeys: 0
    })
  }

  setupScripts (program, engine = 'bull') {
    const path = join(__dirname, './lua', `${engine}.lua`)
    if (!fs.existsSync(path)) {
      console.error(`lua script not found: ${path}`)
      process.exit(1)
    }

    let lua = fs.readFileSync(path).toString('utf8')
    for (const key in program) {
      const needle = `PROGRAM_${key.toUpperCase()}`
      lua = lua.replace(new RegExp(needle, 'g'), program[key])
    }

    return { [engine]: { lua } }
  }
}

module.exports = Caron
