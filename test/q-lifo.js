'use strict'

const test = require('./helpers/test')
const assert = require('node:assert/strict')
const fixture = require('./helpers/fixture')
const { processJobs } = fixture

for (const lifo of [true, false]) {
  test(`q lifo: ${lifo ? 'enabled' : 'disabled'} lifo`, { timeout: 2000 }, async (t) => {
    const f = fixture(t, { q_lifo: lifo, def_attempts: 1 })
    const queue = f.queue()
    const numbers = lifo ? [1, 2, 3, 4] : [1, 2, 3]
    const payloads = numbers.map(num => ({ num, queue: f.queueName }))
    for (const payload of payloads) {
      await f.push(payload, lifo ? 'rpush' : 'lpush')
    }
    const expectedList = payloads.map(payload => JSON.stringify(payload))
    if (!lifo) expectedList.reverse()
    t.alike(await f.redis.lrange(f.list, 0, 99), expectedList)

    const expectedOrder = [...numbers]
    const finished = processJobs(queue, (job) => {
      assert.equal(job.data.num, expectedOrder.shift())
    }, numbers.length)
    f.caron.start()
    await finished
    t.is(expectedOrder.length, 0, 'processed every job in order')
  })
}
