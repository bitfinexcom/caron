/* eslint-env mocha */

'use strict'

const assert = require('assert')

const Caron = require('../../')
const Redis = require('ioredis')
const redis = new Redis()

const Queue = require('bull')
const testQueue = new Queue('default')
const defList = 'bull_test'

let caron

beforeEach(() => {
  caron = new Caron({
    type: 'bull',
    list: defList,
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

before(() => {
  redis.del(defList)
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
    redis.lpush(defList, payload)
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

    const payload = JSON.stringify({ foo: 'bar', $queue: 'default-test' })
    redis.lpush(defList, payload)
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

    const payload = JSON.stringify({ foo: 'bar', $jobId: 'firstJob' })
    redis.lpush(defList, payload)
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

    const payload = JSON.stringify({ foo: 'bar', $delay: 1000 })
    redis.lpush(defList, payload)
    setTimeout(() => {
      redis.exists('bull:default:delayed').then((res) => {
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

    const payload = JSON.stringify({ foo: 'bar', $attempts: 10, $removeOnComplete: false })
    redis.lpush(defList, payload)
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
    redis.lpush(defList, payload)
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

    const payload = JSON.stringify({ foo: 'bar', $removeOnComplete: false })
    redis.lpush(defList, payload)
  })
})

describe('10k items', () => {
  it('process 10k items in less than 10 secs', (done) => {
    const testQueue = new Queue('default')
    const now = new Date()

    testQueue.process((job, cb) => {
      assert.strictEqual(job.data.foo, 'bar')

      cb(null)
      if (job.data.test === 9999) {
        testQueue.close().then(() => {
          caron.stop(() => {
            const finished = new Date()
            assert.strictEqual(finished.getTime() - now.getTime() < 10 * 1000, true)
            caron.redis.disconnect()
            done()
          })
        })
      }
    })

    caron.start()

    for (let i = 0; i < 10000; i++) {
      const payload = JSON.stringify({ test: i, foo: 'bar', queue: 'default' })
      redis.lpush(defList, payload)
    }
  }).timeout(10000)
})
