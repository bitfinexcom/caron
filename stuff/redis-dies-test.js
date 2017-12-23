'use strict'

// process.env.DEBUG = 'ioredis:redis'
process.env.DEBUG = '*'

// a brand new redis should not kill caron
// kill redis-server two times and see if
// caron stays happy :)

const { spawn } = require('child_process')
const fs = require('fs')
const path = require('path')
const assert = require('assert')
const os = require('os')

const Caron = require('../')

const restarts = 2
let killCount = 0

let victims = []
function spawnVictim () {
  const tmpdir = fs.mkdtempSync(os.tmpdir())
  const conf = `dir ${tmpdir}`
  const confP = path.join(tmpdir, 'r.conf')
  fs.writeFileSync(confP, conf)

  let opts = [ confP ]
  const v = spawn('redis-server', opts, { cwd: tmpdir })

  v.stdout.on('data', (d) => {
    const out = d.toString()
    console.log(out)

    if (/Ready to accept connections/.test(out)) {
      console.log('redis ok, kills so far:', killCount)
      setTimeout(() => {
        victimReady(killCount !== restarts)
      }, 4000)
    }
  })

  victims.push(v)
}

let caron
function victimReady (kill) {
  if (!caron) {
    startCaron()
  }

  if (!kill) {
    console.log('----------------------------------')
    console.log('starting bull and filling queue...')
    test()
    return
  }

  killCount++
  killVictim()
  setTimeout(spawnVictim, 4000)
}

function killVictim () {
  victims.pop().kill()
}

function startCaron () {
  const redisOpts = {
    port: '6379',
    host: '127.0.0.1'
  }
  caron = new Caron({
    type: 'bull',
    list: 'bull_test',
    redis: redisOpts,
    freq: 25,
    batch: 100,
    q_prefix: '',
    q_lifo: false,
    def_queue: 'default',
    def_worker: 'BaseJob',
    def_attempts: 1,
    // exit: false,
    debug: true
  })

  caron.start()
}

function test () {
  const Queue = require('bull')
  const testQueue = new Queue('default')
  const Redis = require('ioredis')
  const redis = new Redis()

  setTimeout(() => {
    throw new Error('queue timeout :(')
  }, 5000)

  testQueue.process((job, cb) => {
    console.log('assert', job.data.foo, '===', 'bar')
    assert.equal(job.data.foo, 'bar')

    cb(null)
    testQueue.close().then(() => {
      caron.stop(() => {
        caron.redis.disconnect()
        killVictim()
        console.log('success, exiting')
        process.exit(0)
      })
    })
  })

  const payload = JSON.stringify({ foo: 'bar', queue: 'default' })
  redis.lpush('bull_test', payload)
}

spawnVictim()
