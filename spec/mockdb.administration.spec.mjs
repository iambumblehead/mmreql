import test from 'ava'
import rethinkdbMocked from '../src/mmReql.mjs'

const isUUIDre = /^(?:[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}|00000000-0000-0000-0000-000000000000)$/i
const uuidValidate = str => typeof str === 'string' && isUUIDre.test(str)

test('db `config` should work (default)', async t => {
  const { r } = rethinkdbMocked([['Rooms']])
    
  const dbConfig = await r
    .db('default')
    .config()
    .run()

  t.true(uuidValidate(dbConfig.id))
  t.is(dbConfig.name, 'default')
})

test('db `config` should work (not default)', async t => {
  const { r } = rethinkdbMocked([{ db: 'cmdb' }, ['Rooms']])
    
  const dbConfig = await r
    .db('cmdb')
    .config()
    .run()

  t.true(uuidValidate(dbConfig.id))
  t.is(dbConfig.name, 'cmdb')
})

test('tables `config` should work', async t => {
  const { r } = rethinkdbMocked([{ db: 'cmdb' }, ['Rooms']])

  const tableConfig = await r
    .db('cmdb')
    .table('Rooms')
    .config()
    .run()

  t.true(uuidValidate(tableConfig.id))
  t.like(tableConfig, {
    db: 'cmdb',
    durability: 'hard',
    indexes: [],
    name: 'Rooms',
    primary_key: 'id',
    shards: [{
      primary_replica: 'replicaName',
      replicas: ['replicaName']
    }],
    write_acks: 'majority',
    write_hook: null
  })
})

test('tables `config` should work, incl secondary indexes', async t => {
  const { r } = rethinkdbMocked([{ db: 'cmdb' }, ['Rooms']])

  // secondary index and compound secondary index
  await r.table('Rooms').indexCreate('numeric_id').run()
  await r.table('Rooms').indexWait('numeric_id').run()
  await r.table('Rooms').indexCreate('name_numeric_cid', [
    r.row('name'), r.row('numeric_id')]).run()
  await r.table('Rooms').indexWait('name_numeric_cid').run()

  const tableConfig = await r
    .db('cmdb')
    .table('Rooms')
    .config()
    .run()

  t.true(uuidValidate(tableConfig.id))
  t.like(tableConfig, {
    db: 'cmdb',
    durability: 'hard',
    indexes: [
      'numeric_id',
      'name_numeric_cid'
    ],
    name: 'Rooms',
    primary_key: 'id',
    shards: [{
      primary_replica: 'replicaName',
      replicas: ['replicaName']
    }],
    write_acks: 'majority',
    write_hook: null
  })
})

test('`config` should throw if called with an argument', async t => {
  const { r } = rethinkdbMocked([{ db: 'cmdb' }])
  await t.throws(() => (
    r.db('cmdb').config('hello').run()
  ), {
    message: '`config` takes 0 arguments, 1 provided.'
  })
})

test('`status` should work', async t => {
  const { r, dbState } = rethinkdbMocked([{ db: 'cmdb' }, ['Rooms']])

  const result = await r
    .db('cmdb')
    .table('Rooms')
    .status()
    .run()

  t.like(result, {
    db: 'cmdb',
    id: dbState.dbConfig_cmdb_Rooms.id,
    name: 'Rooms',
    raft_leader: 'devdb_rethinkdb_multicluster',
    shards: [{
      primary_replica: 'replicaName',
      replicas: ['replicaName']
    }],
    status: {
      all_replicas_ready: true,
      ready_for_outdated_reads: true,
      ready_for_reads: true,
      ready_for_writes: true
    }
  })
})
