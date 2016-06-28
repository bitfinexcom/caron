# caron

Atomic job enqueuer for common job queues (Sidekiq, Bull, ...)

**Caron** reads from a redis list and atomically creates a job for Sidekiq(Ruby) and Bull(Node.js)

### Install
```
npm install -g caron
```

### Usage
```
caron --help 
```

```
caron --type sidekiq --list sidekiq_jobs --redis "redis://127.0.0.1:6379" 
```
