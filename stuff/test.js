'use strict'

const Redis = require('ioredis')

var redis = new Redis()

setInterval(() => {
  redis.lpush('bull_test', JSON.stringify({ foo: 'bar', queue: 'default' }))
}, 1)
