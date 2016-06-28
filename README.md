# caron

Atomic Job enqueuer for common Job Queues (Sidekiq, Bull, ...)

**Caron** pops messages from a redis list and atomically creates a Job for the specified Job Queue.

Sidekiq(Ruby) and Bull(Node.js) are supported.

### Support

* Bull-1.0.0
* Sidekiq-4.1.2

### Install
```
npm install -g caron
```

### Usage
```
caron --help 
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
