'use strict'

const test = require('brittle')

module.exports = function integrationTest (name, { timeout }, run) {
  // Reject test work before Brittle's timeout so forced teardown can run.
  return test(name, { timeout: timeout + 5000 }, async (t) => {
    let timer
    const deadline = new Promise((resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`Test "${name}" timed out after ${timeout} ms`)), timeout)
    })
    try {
      await Promise.race([deadline, run(t)])
    } finally {
      clearTimeout(timer)
    }
  })
}
