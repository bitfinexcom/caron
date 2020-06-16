/* eslint-env mocha */

'use strict'

const { join } = require('path')
const proxyquire = require('proxyquire')
const Redis = require('ioredis-mock')
const assert = require('assert')
const crypto = require('crypto')
const redis = new Redis({})
const fs = require('fs')

const Caron = proxyquire('../caron', {
  'ioredis': {
    createClient: () => redis
  }
})

let caron, hashes

const optsBase = {
  redis: 'redis://127.0.0.1:6379',
  type: 'bull',
  freq: 25,
  exit: false,
  debug: false
}

const hashesContent = fs.readFileSync(join(__dirname, './conf', 'hash.json')).toString('utf8')
try {
  hashes = JSON.parse(hashesContent)
} catch (e) {
  console.error(`Error happened reading hashes: ${e.message}`)
  process.exit()
}
beforeEach(() => {
})

describe('caron tests', () => {
  it('sets processing false on redis error', (done) => {
    caron = new Caron(optsBase)
    caron.redis.emit('error', new Error('fake'))
    assert.strictEqual(caron.status.processing, false)
    done()
  })

  describe('setupScripts', () => {
    ['bull', 'sidekiq'].forEach(type => {
      describe(`${type} lua scripts`, () => {
        for (let key in hashes[type]) {
          it(`creates valid lua script for ${key}`, (done) => {
            const opts = Object.keys(hashes[type]).reduce((acc, cv) => {
              acc[cv] = hashes[type][cv]['orig_val']
              return acc
            }, optsBase)

            caron = new Caron(opts)
            opts['type'] = type
            const curKey = hashes[type][key]
            opts[key] = curKey['ctrl_val']
            let scripts = caron.setupScripts(opts)
            let hash = crypto.createHash('md5').update(scripts[type].lua).digest('hex')

            assert.strictEqual(hash, curKey['ctrl'])
            done()
          })
        }
      })
    })
  })
})
