/* eslint-env mocha */

'use strict'

const assert = require('assert')

const Caron = require('../')
const Redis = require('ioredis')
const redis = new Redis()

const Queue = require('bull')
const testQueue = new Queue('default')

let caron

beforeEach(() => {
  caron = new Caron({
    type: 'bull',
    list: 'bull_test',
    redis: 'redis://127.0.0.1:6379',
    freq: 25,
    batch: 100,
    q_prefix: '',
    q_lifo: false,
    def_queue: 'default',
    def_worker: 'BaseJob',
    def_attempts: 5,
    exit: false,
    debug: false
  })
})

after(() => {
  redis.disconnect()
})

describe('integration', () => {
  it('pulls stuff from redis', (done) => {
    testQueue.process((job, cb) => {
      assert.strictEqual(job.data.foo, 'bar')

      cb(null)
      testQueue.close().then(() => {
        caron.stop(() => {
          caron.redis.disconnect()
          done()
        })
      })
    })

    caron.start()

    const payload = JSON.stringify({ foo: 'bar', queue: 'default' })
    redis.lpush('bull_test', payload)
  })
})

describe('bull params', () => {
  it('uses queue from payload instead of def_queue', (done) => {
    const testQueue = new Queue('default-test')

    testQueue.process((job, cb) => {
      assert.strictEqual(job.data.foo, 'bar')
      assert.strictEqual(job.queue.name, 'default-test')

      cb(null)
      testQueue.close().then(() => {
        caron.stop(() => {
          caron.redis.disconnect()
          done()
        })
      })
    })

    caron.start()

    const payload = JSON.stringify({ foo: 'bar', '$queue': 'default-test' })
    redis.lpush('bull_test', payload)
  })

  it('uses jobId from payload instead of default', (done) => {
    const testQueue = new Queue('default')

    testQueue.process((job, cb) => {
      assert.strictEqual(job.id, 'firstJob')
      assert.strictEqual(job.data.foo, 'bar')

      cb(null)
      testQueue.close().then(() => {
        caron.stop(() => {
          caron.redis.disconnect()
          done()
        })
      })
    })

    caron.start()

    const payload = JSON.stringify({ foo: 'bar', '$jobId': 'firstJob' })
    redis.lpush('bull_test', payload)
  })

  it('uses delay from payload instead of default', (done) => {
    const testQueue = new Queue('default')

    testQueue.process((job, cb) => {
      assert.strictEqual(job.opts.delay, 1000)
      assert.strictEqual(job.data.foo, 'bar')

      cb(null)
      testQueue.close().then(() => {
        caron.stop(() => {
          caron.redis.disconnect()
          done()
        })
      })
    })

    caron.start()

    const payload = JSON.stringify({ foo: 'bar', '$delay': 1000 })
    redis.lpush('bull_test', payload)
    setTimeout(() => {
      redis.exists("bull:default:delayed").then((res) => {
        assert.strictEqual(res, 1)
      })
    }, 500)
  })

  it('uses attempts from payload instead of default', (done) => {
    const testQueue = new Queue('default')

    testQueue.process((job, cb) => {
      assert.strictEqual(job.opts.attempts, 10)
      assert.strictEqual(job.data.foo, 'bar')

      cb(null)
      testQueue.close().then(() => {
        caron.stop(() => {
          caron.redis.disconnect()
          done()
        })
      })
    })

    caron.start()

    const payload = JSON.stringify({ foo: 'bar', '$attempts': 10, '$removeOnComplete': false })
    redis.lpush('bull_test', payload)
  })

  it('uses attempts from config', (done) => {
    const testQueue = new Queue('default')

    testQueue.process((job, cb) => {
      assert.strictEqual(job.opts.attempts, 5)
      assert.strictEqual(job.data.foo, 'bar')

      cb(null)
      testQueue.close().then(() => {
        caron.stop(() => {
          caron.redis.disconnect()
          done()
        })
      })
    })

    caron.start()

    const payload = JSON.stringify({ foo: 'bar' })
    redis.lpush('bull_test', payload)
  })

  it('uses removeOnComplete from payload instead of default', (done) => {
    const testQueue = new Queue('default')

    testQueue.process((job, cb) => {
      assert.strictEqual(job.opts.removeOnComplete, false)
      assert.strictEqual(job.data.foo, 'bar')

      cb(null)
      testQueue.close().then(() => {
        caron.stop(() => {
          caron.redis.disconnect()
          done()
        })
      })
    })

    caron.start()

    const payload = JSON.stringify({ foo: 'bar', '$removeOnComplete': false })
    redis.lpush('bull_test', payload)
  })

  it('uses removeOnComplete from payload instead of default', (done) => {
    const testQueue = new Queue('default')

    testQueue.process((job, cb) => {
      assert.strictEqual(job.opts.removeOnComplete, false)
      assert.strictEqual(job.data.foo, 'bar')

      cb(null)
      testQueue.close().then(() => {
        caron.stop(() => {
          caron.redis.disconnect()
          done()
        })
      })
    })

    caron.start()

    const payload = JSON.stringify({ foo: 'bar', '$removeOnComplete': false })
    redis.lpush('bull_test', payload)
  })
})

describe('100k items', () => {
  it('process 100k items', (done) => {
    const testQueue = new Queue('default')

    testQueue.process((job, cb) => {
      assert.strictEqual(job.data.foo, 'bar')

      cb(null)
      testQueue.close().then(() => {
        caron.stop(() => {
          caron.redis.disconnect()
          done()
        })
      })
    })

    caron.start()

    for (let i = 0; i < 100000; i++) {
      const payload = JSON.stringify({ test: i, foo: 'bar', queue: 'default' })
      redis.lpush('bull_test', payload)
    }
  }).timeout(5000)
})
