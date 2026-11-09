'use strict'

const test = require('./helpers/test')
const assert = require('node:assert/strict')
const { setTimeout: delay } = require('node:timers/promises')
const fixture = require('./helpers/fixture')
const { processJobs } = fixture

test('integration: pulls stuff from redis', { timeout: 2000 }, async (t) => {
  const f = fixture(t)
  const finished = processJobs(f.queue(), (job) => {
    assert.equal(job.data.foo, 'bar')
  })
  f.caron.start()
  await Promise.all([finished, f.push({ foo: 'bar', queue: f.queueName })])
  t.pass('processed the payload')
})

test('bull params: uses queue from payload instead of def_queue', { timeout: 2000 }, async (t) => {
  const f = fixture(t)
  const name = `${f.queueName}-override`
  const finished = processJobs(f.queue(name), (job) => {
    assert.equal(job.data.foo, 'bar')
    assert.equal(job.queue.name, name)
  })
  f.caron.start()
  await Promise.all([finished, f.push({ foo: 'bar', $queue: name })])
  t.pass('used the payload queue')
})

test('bull params: uses jobId from payload instead of default', { timeout: 2000 }, async (t) => {
  const f = fixture(t)
  const finished = processJobs(f.queue(), (job) => {
    assert.equal(job.id, 'firstJob')
    assert.equal(job.data.foo, 'bar')
  })
  f.caron.start()
  await Promise.all([finished, f.push({ foo: 'bar', $jobId: 'firstJob' })])
  t.pass('used the payload job ID')
})

test('bull params: uses delay from payload instead of default', { timeout: 2000 }, async (t) => {
  const f = fixture(t)
  const queue = f.queue()
  const finished = processJobs(queue, (job) => {
    assert.equal(job.opts.delay, 1000)
    assert.equal(job.data.foo, 'bar')
  })
  const controller = new AbortController()
  t.teardown(() => controller.abort(), { force: true })
  f.caron.start()
  await Promise.all([
    finished,
    f.push({ foo: 'bar', $delay: 1000 }),
    delay(500, undefined, { signal: controller.signal }).then(async () => {
      assert.equal(await f.redis.exists(queue.toKey('delayed')), 1)
    })
  ])
  t.pass('observed the delayed job before processing')
})

test('bull params: uses attempts from payload instead of default', { timeout: 2000 }, async (t) => {
  const f = fixture(t)
  const finished = processJobs(f.queue(), (job) => {
    assert.equal(job.opts.attempts, 10)
    assert.equal(job.data.foo, 'bar')
  })
  f.caron.start()
  await Promise.all([finished, f.push({ foo: 'bar', $attempts: 10, $removeOnComplete: false })])
  t.pass('used the payload attempt count')
})

test('bull params: uses attempts from config', { timeout: 2000 }, async (t) => {
  const f = fixture(t)
  const finished = processJobs(f.queue(), (job) => {
    assert.equal(job.opts.attempts, 5)
    assert.equal(job.data.foo, 'bar')
  })
  f.caron.start()
  await Promise.all([finished, f.push({ foo: 'bar' })])
  t.pass('used the configured attempt count')
})

for (const type of ['fixed', 'exponential']) {
  test(`bull params: uses ${type} backoff method with given delay`, { timeout: 2000 }, async (t) => {
    const f = fixture(t)
    const delay = 300
    const finished = processJobs(f.queue(), (job) => {
      assert.equal(job.opts.backoff.type, type)
      assert.equal(job.opts.backoff.delay, delay)
      assert.equal(job.data.foo, 'bar')
      assert.equal(job.data.$backoff, undefined)
    })
    f.caron.start()
    await Promise.all([finished, f.push({ foo: 'bar', $backoff: { type, delay } })])
    t.pass('preserved backoff options and removed metadata from the payload')
  })

  test(`bull params: uses ${type} backoff for failing task`, { timeout: 2000 }, async (t) => {
    const f = fixture(t)
    const delay = 300
    const attempts = type === 'fixed' ? 5 : 3
    let currentAttempt = 0
    let minimumElapsed = 0
    const start = Date.now()
    const finished = processJobs(f.queue(), () => {
      assert.ok(Date.now() - start >= minimumElapsed)
      currentAttempt++
      minimumElapsed += type === 'fixed' ? delay : (2 ** currentAttempt - 1) * delay
    }, attempts, new Error('expected job failure'))
    f.caron.start()
    await Promise.all([finished, f.push({ foo: 'bar', $attempts: attempts, $backoff: { type, delay } })])
    t.is(currentAttempt, attempts, 'processed every retry')
  })
}

test('bull params: uses removeOnComplete from payload instead of default', { timeout: 2000 }, async (t) => {
  const f = fixture(t)
  const finished = processJobs(f.queue(), (job) => {
    assert.equal(job.opts.removeOnComplete, false)
    assert.equal(job.data.foo, 'bar')
  })
  f.caron.start()
  await Promise.all([finished, f.push({ foo: 'bar', $removeOnComplete: false })])
  t.pass('used the payload completion option')
})

test('10k items: process 10k items in less than 10 secs', { timeout: 10000 }, async (t) => {
  const f = fixture(t)
  const start = Date.now()
  const finished = processJobs(f.queue(), (job) => {
    assert.equal(job.data.foo, 'bar')
  }, 10000)
  f.caron.start()
  const pushes = []
  for (let i = 0; i < 10000; i++) {
    pushes.push(f.push({ test: i, foo: 'bar', queue: f.queueName }))
  }
  await Promise.all([finished, ...pushes])
  t.ok(Date.now() - start < 10000, 'processed all 10000 jobs within 10 seconds')
})
