import assert from 'node:assert/strict';

import {
  mmDbStateDbConfigGet,
  mmDbStateTableConfigGet
} from './mmDbState.mjs'

const isDeepEqual = (a, b, isEqual = false) => {
  try {
    isEqual = (assert.deepStrictEqual(a, b), true)
  } catch(e) {
    isEqual = false
  }

  return isEqual
}

const mmResChangeTypeADD = 'add'
const mmResChangeTypeREMOVE = 'remove'
const mmResChangeTypeCHANGE = 'change'
const mmResChangeTypeINITIAL = 'initial'
const mmResChangeTypeUNINITIAL = 'uninitial'
const mmResChangeTypeSTATE = 'state'

const generated_keys = 'generated_keys'
const deleted = 'deleted'
const inserted = 'inserted'
const replaced = 'replaced'
const unchanged = 'unchanged'
const skipped = 'skipped'
const changes = 'changes'
const errors = 'errors'

// rethinkdb response values are never 'undefined'
// remove 'undefined' definitions from object
const mmResFilterUndefined = obj => Object.keys(obj)
  .reduce((filtered, key) => (
    typeof obj[key] !== 'undefined'
      ? { [key]: obj[key], ...filtered }
      : filtered
  ), {})

const mmResChangesSpecFinal = (obj, opts) => {
  if (Array.isArray(obj.changes)) {
    obj.changes = opts && opts.returnChanges === true
      ? obj.changes
      : undefined
  }

  return mmResFilterUndefined(obj)
}

const mmResChangesFieldCreate = opts => mmResFilterUndefined({
  [deleted]: 0,
  [errors]: 0,
  [inserted]: 0,
  [replaced]: 0,
  [skipped]: 0,
  [unchanged]: 0,
  ...opts
})

const mmResChangesSpecPush = (spec, pushspec) => {
  const { new_val, old_val, generated_key } = pushspec
  
  spec[changes] = Array.isArray(spec[changes])
    ? spec[changes]
    : []

  if (generated_key) {
    spec[generated_keys] = (Array.isArray(spec[generated_keys])
      ? spec[generated_keys]
      : []).concat([generated_key])
  }
  
  if (new_val && old_val) {
    if (isDeepEqual(new_val, old_val)) {
      spec[unchanged] += 1
    } else {
      spec[replaced] += 1
      spec[changes].push({ new_val, old_val })
    }
  } else if (new_val && old_val === null) {
    spec[inserted] += 1
    spec[changes].push({ new_val, old_val })
  } else if (old_val && new_val === null) {
    spec[deleted] += 1
    spec[changes].push({ new_val, old_val })
  } else {
    spec[skipped] += 1
  }

  return spec
}

const mmResTableStatus = opts => mmResFilterUndefined({
  db: opts.db || null,
  id: opts.id || null,
  name: opts.name ||'tablename',
  raft_leader: opts.raft_leader || 'devdb_rethinkdb_multicluster',
  shards: opts.shards || [{
    primary_replica: 'replicaName',
    replicas: ['replicaName']
  }],
  status: opts.status || {
    all_replicas_ready: true,
    ready_for_outdated_reads: true,
    ready_for_reads: true,
    ready_for_writes: true
  }
})

const mmResTableInfo = (dbst, dbName, tableName) => {
  const tableConfig = mmDbStateTableConfigGet(dbst, dbName, tableName)
  const dbConfig = mmDbStateDbConfigGet(dbst, dbName)

  return mmResFilterUndefined({
    db: {
      ...dbConfig,
      type: 'DB'
    },
    doc_count_estimates: [0],
    id: tableConfig.id,
    indexes: [],
    name: tableConfig.name,
    primary_key: tableConfig.primary_key,
    type: 'TABLE'
  })
}

export {
  mmResFilterUndefined,
  mmResChangeTypeADD,
  mmResChangeTypeREMOVE,
  mmResChangeTypeCHANGE,
  mmResChangeTypeINITIAL,
  mmResChangeTypeUNINITIAL,
  mmResChangeTypeSTATE,

  mmResChangesSpecFinal,
  mmResChangesSpecPush,
  mmResChangesFieldCreate,
  mmResTableStatus,
  mmResTableInfo
}
