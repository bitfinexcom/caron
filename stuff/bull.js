'use strict'

// caron -t bull -l bull_test

const Queue = require('bull')
const testQueue = new Queue('default')

testQueue.process((job, done) => {
  console.log('Job done by worker', job.jobId)
  done()
  // done(new Error('error transcoding'))
  // done(null, { foo: 'apple' })
})
