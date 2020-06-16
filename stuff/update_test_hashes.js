'use strict'

const fs = require('fs')
const Redis = require('ioredis-mock')
const proxyquire = require('proxyquire')
const crypto = require('crypto')
//const debug = require('debug')('caron:udpate_test_hashes')
const redis = new Redis({})
const path = require('path')

const hashJson = path.join(__dirname, '../', 'test/conf', 'hash.json')

const Caron = proxyquire('../caron', {
  'ioredis': {
    createClient: () => redis
  }
})

const hashes = {
  bull: {},
  sidekiq: {}
}

const optsVar = {
  list: ['test', 'test2'],
  batch: [100, 500],
  q_prefix: ['', 'l'],
  q_lifo: [false, true],
  def_queue: ['default', 'non'],
  def_worker: ['BaseJob', 'RootJob'],
  def_attempts: [1, 2]
}

for (let key in optsVar) {
  ['bull', 'sidekiq'].forEach(type => {
    hashes[type][key] = {}

    const opts1 = {
      type,
      list: optsVar['list'][0],
      batch: optsVar['batch'][0],
      q_prefix: optsVar['q_prefix'][0],
      q_lifo: optsVar['q_lifo'][0],
      def_queue: optsVar['def_queue'][0],
      def_worker: optsVar['def_worker'][0],
      def_attempts: optsVar['def_attempts'][0],
      [key]: optsVar[key][0]
    }

    const opts2 = {
      type,
      list: optsVar['list'][0],
      batch: optsVar['batch'][0],
      q_prefix: optsVar['q_prefix'][0],
      q_lifo: optsVar['q_lifo'][0],
      def_queue: optsVar['def_queue'][0],
      def_worker: optsVar['def_worker'][0],
      def_attempts: optsVar['def_attempts'][0],
      [key]: optsVar[key][1]
    }

    const caron = new Caron(opts1)

    let scripts = caron.setupScripts(opts1)
    const md51 = crypto.createHash('md5').update(scripts[type].lua).digest('hex')
    scripts = caron.setupScripts(opts2)
    const md52 = crypto.createHash('md5').update(scripts[type].lua).digest('hex')
    hashes[type][key]['orig'] = md51
    hashes[type][key]['orig_val'] = opts1[key]
    hashes[type][key]['ctrl'] = md52
    hashes[type][key]['ctrl_val'] = opts2[key]
  })
}

fs.writeFileSync(hashJson, JSON.stringify(hashes))
