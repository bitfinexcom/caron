'use strict'

const Redis = require('ioredis')

const redis = new Redis()

setInterval(() => {
  redis.lpush('bull_test', JSON.stringify({ foo: 'bar', queue: 'default' }))
}, 1)
