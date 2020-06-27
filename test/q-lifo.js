/* eslint-env mocha */

'use strict'

const assert = require('assert')

const Caron = require('../')
const Redis = require('ioredis')

const Queue = require('bull')

let caron
let redis
describe('q lifo', () => {
  beforeEach(() => {
    redis = new Redis()
  })
  afterEach(() => {
    redis.disconnect()
  })

  it('enabled lifo', async () => {
    const testQueue = new Queue('default_r')

    caron = new Caron({
      type: 'bull',
      list: 'bull_test_l',
      redis: 'redis://127.0.0.1:6379',
      freq: 25,
      batch: 100,
      q_prefix: '',
      q_lifo: true, // true == R
      def_queue: 'default_r',
      def_worker: 'BaseJob',
      def_attempts: 1,
      exit: false,
      debug: false
    })

    const payload = JSON.stringify({ num: 1, queue: 'default_r' })
    redis.rpush('bull_test_l', payload)
    const payload2 = JSON.stringify({ num: 2, queue: 'default_r' })
    redis.rpush('bull_test_l', payload2)
    const payload3 = JSON.stringify({ num: 3, queue: 'default_r' })
    redis.rpush('bull_test_l', payload3)
    const payload4 = JSON.stringify({ num: 4, queue: 'default_r' })
    redis.rpush('bull_test_l', payload4)

    const list = await redis.lrange('bull_test_l', 0, 99)
    assert.deepEqual(
      list,
      ['{"num":1,"queue":"default_r"}',
        '{"num":2,"queue":"default_r"}',
        '{"num":3,"queue":"default_r"}',
        '{"num":4,"queue":"default_r"}']
    )

    caron.start()

    const expectedOrder = [1, 2, 3, 4]
    return new Promise((resolve) => {
      testQueue.process((job, cb) => {
        const expected = expectedOrder.shift()
        assert.equal(job.data.num, expected)
        cb(null)

        if (expectedOrder.length !== 0) return

        testQueue.close().then(() => {
          caron.stop(() => {
            caron.redis.disconnect()
            resolve()
          })
        })
      })
    })
  })

  it('disabled lifo', async () => {
    const testQueue = new Queue('default_l')

    caron = new Caron({
      type: 'bull',
      list: 'bull_test_r',
      redis: 'redis://127.0.0.1:6379',
      freq: 25,
      batch: 100,
      q_prefix: '',
      q_lifo: false, // true == R
      def_queue: 'default_l',
      def_worker: 'BaseJob',
      def_attempts: 1,
      exit: false,
      debug: false
    })

    const payload = JSON.stringify({ num: 1, queue: 'default_l' })
    redis.lpush('bull_test_r', payload)
    const payload2 = JSON.stringify({ num: 2, queue: 'default_l' })
    redis.lpush('bull_test_r', payload2)
    const payload3 = JSON.stringify({ num: 3, queue: 'default_l' })
    redis.lpush('bull_test_r', payload3)

    const list = await redis.lrange('bull_test_r', 0, 99)
    assert.deepEqual(
      list,
      ['{"num":3,"queue":"default_l"}',
        '{"num":2,"queue":"default_l"}',
        '{"num":1,"queue":"default_l"}']
    )

    caron.start()
    const expectedOrder = [1, 2, 3]
    return new Promise((resolve) => {
      testQueue.process((job, cb) => {
        const expected = expectedOrder.shift()
        assert.equal(job.data.num, expected)

        cb(null)

        if (expectedOrder.length !== 0) return

        testQueue.close().then(() => {
          caron.stop(() => {
            caron.redis.disconnect()
            resolve()
          })
        })
      })
    })
  })
})
