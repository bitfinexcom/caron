# caron

Atomic Job enqueuer for common Job Queues (Sidekiq, Bull, ...)

**Caron** pops messages from a redis list and atomically creates a Job for the specified Job Queue.

Uses `lua` scripting internally to provide atomicity (http://redis.io/commands/EVAL)

### Support

* [Sidekiq-4.1.2](https://github.com/mperham/sidekiq)
* [Bull-1.0.0](https://github.com/OptimalBits/bull)

### Install
```
npm install -g caron
```

### Usage
```
$ caron --help

  Usage: caron [options]

  Options:

    -h, --help               output usage information
    -V, --version            output the version number
    -t, --type <val>         queue type [sidekiq | bull]
    -l, --list <val>         source redis list (i.e: global_jobs)
    -r, --redis <val>        redis url (i.e: redis://127.0.0.1:6379)
    -f, --freq <n>           poll frequency (in milliseconds) - default: 10
    -b, --batch <n>          max number of jobs created per batch - default: 1000
    --def_queue <val>        default dest queue - default: default
    --def_sk_worker <val>    default Job Queue worker - default: BaseJob
    --def_bl_attempts <val>  default Bull Job attempts - default: 1
    --def_bl_lifo            Bull LIFO mode
    --debug                  debug    
    
```

```
caron --type sidekiq --list sidekiq_jobs --redis "redis://127.0.0.1:6379" --freq 25
```

### Examples

##### Push from redis-cli
```
// Sidekiq job enqueue
redis-cli > lpush "sidekiq_jobs" "{\"$queue\":\"critical\",\"$class\":\"BackendJob\",\"foo\":\"bar\",\"my\":\"stuff\",\"other\":\"stuff\",\"other\":{\"f\":5}}"

// Bull job enqueue
redis-cli > lpush "bull_jobs" "{\"$queue\":\"critical\",\"$attempts\":4,\"foo\":\"bar\",\"my\":\"stuff\",\"other\":{\"f\":5}}"
```

##### Push from Node.js

```
'use strict'

const Redis = require('ioredis')

var redis = new Redis()

setInterval(() => {
  redis.lpush('sidekiq_test', JSON.stringify({ foo: 'bar', '$queue': 'critical', '$class': 'MyCriticalJob', other: { a: 1, b: 2 } }))
  redis.lpush('bull_test', JSON.stringify({ foo: 'bar', '$queue': 'priority', '$attempts': 5, other: { a: 1, b: 2 } }))
  redis.lpush('bull_test', JSON.stringify({ foo: 'bar', '$queue': 'lazy', '$attempts': 1, '$delay': 5000, other: { a: 1, b: 2 } }))
}, 1)
```

##### Push from Ruby

```
require 'redis'
require 'json'

rcli = Redis.new

rcli.lpush('sidekiq_test', JSON.dump({ 'foo' => 'bar', '$queue' => 'critical', '$class' => 'MyCriticalJob', 'other' => { 'a' => 1, 'b' => 2 } }))
rcli.lpush('bull_test', JSON.dump({ 'foo' => 'bar', '$queue' => 'priority', '$attempts' => 5, 'other' => { 'a' => 1, 'b' => 2 } }))
rcli.lpush('bull_test', JSON.dump({ 'foo' => 'bar', '$queue' => 'lazy', '$attempts' => 1, '$delay' => 5000, other => { 'a' => 1, 'b' => 2 } }))
```
