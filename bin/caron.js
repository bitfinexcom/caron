#!/usr/bin/env node

'use strict'

const Caron = require('../caron.js')

const program = require('yargs')
  .option('t', {
    describe: 'queue type',
    alias: 'type',
    choices: ['sidekiq', 'bull'],
    demand: true
  })
  .option('l', {
    describe: 'source redis list (e.g: `my_job_queue`)',
    alias: 'list',
    demand: true,
    type: 'string'
  })
  .option('r', {
    describe: 'redis url',
    alias: 'redis',
    default: 'redis://127.0.0.1:6379',
    type: 'string'
  })
  .option('f', {
    describe: 'poll frequency (milliseconds)',
    alias: 'freq',
    default: 25,
    type: 'number'
  })
  .option('b', {
    describe: 'max number of jobs processed per batch',
    alias: 'batch',
    default: 100,
    type: 'number'
  })
  .option('q_prefix', {
    describe: 'redis queue prefix (e.g: "production:")',
    default: '',
    type: 'string'
  })
  .option('q_lifo', {
    describe: 'Bull LIFO mode',
    default: false,
    type: 'boolean'
  })
  .option('def_queue', {
    describe: 'default destination queue',
    default: 'default',
    type: 'string'
  })
  .option('def_worker', {
    describe: 'default job queue worker',
    default: 'BaseJob',
    type: 'string'
  })
  .option('def_attempts', {
    describe: 'default job attempts',
    default: 1,
    type: 'number'
  })
  .boolean('debug')
  .help('help')
  .version()
  .usage('Usage: $0 -t <val> -l <val> -r <val>')
  .argv

console.log('caron(' + program.redis + '/' + program.list + '/' + program.type + ')')

const caron = new Caron(program).start()
process.on('SIGINT', () => {
  caron.stop(() => {
    process.exit()
  })
})
