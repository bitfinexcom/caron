'use strict'

const test = require('brittle')
const { setTimeout: delay } = require('node:timers/promises')

module.exports = function integrationTest (name, { timeout }, run) {
  // Reject test work before Brittle's timeout so forced teardown can run.
  return test(name, { timeout: timeout + 5000 }, async (t) => {
    const controller = new AbortController()
    const deadline = delay(timeout, undefined, { signal: controller.signal }).then(() => {
      throw new Error(`Test "${name}" timed out after ${timeout} ms`)
    })
    try {
      await Promise.race([deadline, Promise.resolve().then(() => run(t))])
    } finally {
      controller.abort()
    }
  })
}
