# caron

Atomic job enqueuer for Bull and Sidekiq

*caron* reads from a redis list and atomically creates a job for Bull or Sidekiq

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
