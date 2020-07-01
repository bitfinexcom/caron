/* eslint-env mocha */

'use strict'

const assert = require('assert')

const Caron = require('../../')
const Redis = require('ioredis')
const redis = new Redis()

const Queue = require('bull')
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

describe('many items', () => {
  const tests = [{
    name: '25k',
    max: 24999,
    thres: 15
  }, {
    name: '50k',
    max: 49999,
    thres: 30
  }]

  tests.forEach(test => {
    it(`process  ${test.name} items in less than ${test.thres} secs`, (done) => {
      const testQueue = new Queue('default')
      const now = new Date()

      testQueue.process((job, cb) => {
        assert.strictEqual(job.data.foo, 'bar')

        cb(null)
        if (job.data.test === test.max) {
          testQueue.close().then(() => {
            caron.stop(() => {
              const finished = new Date()
              const delta = finished.getTime() - now.getTime()
              console.info(`Processed ${test.max + 1} in ${delta}ms`)

              assert.strictEqual(delta < test.thres * 1000, true)
              caron.redis.disconnect()
              done()
            })
          })
        }
      })

      caron.start()

      for (let i = 0; i <= test.max; i++) {
        const payload = JSON.stringify({ test: i, foo: 'bar' })
        redis.lpush(defList, payload)
      }
    }).timeout(test.max * 1000)
  })
})
