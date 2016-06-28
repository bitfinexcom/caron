# caron

Atomic Job enqueuer for common Job Queues (Sidekiq, Bull, ...)

**Caron** pops messages from a redis list and atomically creates a Job for the specified Job Queue.

Uses `lua` scripting internally to provide atomicity (http://redis.io/commands/EVAL)

### Support

* [Bull-1.0.0](https://github.com/OptimalBits/bull)
* [Sidekiq-4.1.2](https://github.com/mperham/sidekiq)

### Install
```
npm install -g caron
```

### Usage
```
$ caron --help

Usage: caron [options]

Options:

  -h, --help         output usage information
  -V, --version      output the version number
  -t, --type <val>   queue type [sidekiq | bull]
  -l, --list <val>   source redis list (i.e: global_jobs)
  -r, --redis <val>  redis url (i.e: redis://127.0.0.1:6379)
  -f, --freq <n>     poll frequency (in milliseconds) - default: 10
  -b, --batch <n>    max number of jobs created per batch - default: 1000
  --debug            debug
```

```
caron --type sidekiq --list sidekiq_jobs --redis "redis://127.0.0.1:6379" --freq 25
```


### Examples

```
// Sidekiq job enqueue
redis-cli > lpush "sidekiq_jobs" "{\"queue\":\"critical\",\"class\":\"BackendJob\",\"foo\":\"bar\",\"my\":\"stuff\",\"other\":\"stuff\"}"
```

```
// Bull job enqueue
redis-cli > lpush "bull_jobs" "{\"queue\":\"critical\",\"foo\":\"bar\",\"my\":\"stuff\"}"
```
