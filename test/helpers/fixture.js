'use strict'

const { randomUUID } = require('crypto')
const Caron = require('../..')
const Redis = require('ioredis')
const Queue = require('bull')

module.exports = function fixture (t, opts = {}) {
  const redisUrl = process.env.TEST_REDIS_URL || 'redis://127.0.0.1:6379'
  const id = randomUUID()
  const list = `caron-test-${id}`
  const queueName = `caron-test-${id}-default`
  const redis = new Redis(redisUrl)
  const queues = []
  const caron = new Caron({
    type: 'bull',
    list,
    redis: redisUrl,
    freq: 25,
    batch: 100,
    q_prefix: '',
    q_lifo: false,
    def_queue: queueName,
    def_worker: 'BaseJob',
    def_attempts: 5,
    exit: false,
    ...opts
  })

  t.teardown(async () => {
    try {
      await new Promise((resolve) => caron.stop(resolve))
      for (const queue of queues) {
        try {
          await queue.obliterate({ force: true })
        } finally {
          await queue.close()
        }
      }
      await redis.del(list)
    } finally {
      caron.redis.disconnect()
      redis.disconnect()
    }
  }, { force: true })

  function queue (name = queueName) {
    const result = new Queue(name, redisUrl)
    queues.push(result)
    return result
  }

  function push (payload, method = 'lpush') {
    return redis[method](list, JSON.stringify(payload))
  }

  return { caron, redis, list, queueName, queue, push }
}

module.exports.processJobs = function processJobs (queue, handler, count = 1, expectedFailure) {
  return new Promise((resolve, reject) => {
    let processed = 0
    const event = expectedFailure ? 'failed' : 'completed'
    queue.on('error', reject)
    queue.on('failed', (job, err) => {
      if (err !== expectedFailure) reject(err)
    })
    queue.on(event, () => {
      if (++processed === count) resolve()
    })
    queue.process(async (job) => {
      try {
        handler(job)
      } catch (err) {
        reject(err)
        throw err
      }
      if (expectedFailure) throw expectedFailure
    }).catch(reject)
  })
}
