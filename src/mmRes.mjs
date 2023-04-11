import {
  mmDbConfigGet,
  mmDbTableConfigGet
} from './mmDb.mjs';

const mmResChangeTypeADD = 'add';
const mmResChangeTypeREMOVE = 'remove';
const mmResChangeTypeCHANGE = 'change';
const mmResChangeTypeINITIAL = 'initial';
const mmResChangeTypeUNINITIAL = 'uninitial';
const mmResChangeTypeSTATE = 'state';

// rethinkdb response values are never 'undefined'
// remove 'undefined' definitions from object
const mockdbFilterUndefined = obj => Object.keys(obj)
  .reduce((filtered, key) => (
    typeof obj[key] !== 'undefined'
      ? { [key]: obj[key], ...filtered }
      : filtered
  ), {});

const mmResChangesFieldCreate = opts => mockdbFilterUndefined({
  deleted: 0,
  errors: 0,
  inserted: 0,
  replaced: 0,
  skipped: 0,
  unchanged: 0,
  ...opts
});

// Some operations, such as replace, might apply multiple operations
// such as 'deleted', 'inserted' and 'replaced' depending upon how they
// are called. This tries to create values for these fields programatically
// when given the 'changes' object
const mmResChangesCreate = (changes, opts) => mmResChangesFieldCreate(
  changes.reduce((prev, change) => {
    if (change.new_val && change.old_val)
      prev.replaced += 1;
    else if (!change.new_val)
      prev.deleted += 1;
    else if (!change.old_val)
      prev.inserted += 1;

    return prev;
  }, {
    deleted: 0,
    inserted: 0,
    replaced: 0,
    ...opts
  })
);

const mmResTableStatus = opts => mockdbFilterUndefined({
  db: opts.db || null,
  id: opts.id || null,
  name: opts.name ||'tablename',
  raft_leader: opts.raft_leader || 'devdb_rethinkdb_multicluster',
  shards: opts.shards || [ {
    primary_replica: 'replicaName',
    replicas: [ 'replicaName' ]
  } ],
  status: opts.status || {
    all_replicas_ready: true,
    ready_for_outdated_reads: true,
    ready_for_reads: true,
    ready_for_writes: true
  }
});

const mmResTableInfo = (dbst, dbName, tableName) => {
  const tableConfig = mmDbTableConfigGet(dbst, dbName, tableName);
  const dbConfig = mmDbConfigGet(dbst, dbName);

  return mockdbFilterUndefined({
    db: {
      ...dbConfig,
      type: 'DB'
    },
    doc_count_estimates: [ 0 ],
    id: tableConfig.id,
    indexes: [],
    name: tableConfig.name,
    primary_key: tableConfig.primary_key,
    type: 'TABLE'
  });
};

export {
  mmResChangeTypeADD,
  mmResChangeTypeREMOVE,
  mmResChangeTypeCHANGE,
  mmResChangeTypeINITIAL,
  mmResChangeTypeUNINITIAL,
  mmResChangeTypeSTATE,

  mmResChangesFieldCreate,
  mmResChangesCreate,
  mmResTableStatus,
  mmResTableInfo
};
