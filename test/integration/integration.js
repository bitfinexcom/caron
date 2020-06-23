/* eslint-env mocha */

'use strict'

const assert = require('assert')

const Caron = require('../../')
const Redis = require('ioredis')
const redis = new Redis()

const Queue = require('bull')
const testQueue = new Queue('default')
const defList = 'bull_test'

before(() => {
  redis.del(defList)
})

after(() => {
  redis.disconnect()
})

describe('integration', () => {
  it('pulls stuff from redis', (done) => {
    const caron = new Caron({
      type: 'bull',
      list: defList,
      redis: 'redis://127.0.0.1:6379',
      freq: 25,
      batch: 100,
      q_prefix: '',
      q_lifo: false,
      def_queue: 'default',
      def_worker: 'BaseJob',
      def_attempts: 1,
      exit: false,
      debug: false
    })

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
