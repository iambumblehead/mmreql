import test from 'ava'
import rethinkdbMocked from '../src/mockdb.mjs'

test('`tableList` should return a cursor', async t => {
  const { r } = rethinkdbMocked([['Rooms']])

  const result = await r
    .db('default')
    .tableList()
    .run()

  t.true(Array.isArray(result))
  t.deepEqual(result, ['Rooms'])
})

test('`tableList` should show the table we created', async t => {
  const { r, dbState } = rethinkdbMocked([['Rooms']])

  const tableCreateRes = await r
    .db('default')
    .tableCreate('thenewtable')
    .run()

  t.deepEqual(tableCreateRes, {
    tables_created: 1,
    config_changes: [{
      new_val: {
        db: 'default',
        durability: 'hard',
        id: dbState.dbConfig_default_thenewtable.id,
        indexes: [],
        name: 'thenewtable',
        primary_key: 'id',
        shards: [{
          primary_replica: 'replicaName',
          replicas: [
            'replicaName'
          ]
        }],
        write_acks: 'majority',
        write_hook: null
      },
      old_val: null
    }]
  })
    
  const result2 = await r
    .db('default')
    .tableList()
    .run()

  t.true(Array.isArray(result2))
  t.true(result2.some(name => name === 'thenewtable'))
})

test('`tableCreate` should create a table -- primaryKey', async t => {
  const { r, dbState } = rethinkdbMocked([['Rooms']])
  const tableCreateRes = await r
    .db('default')
    .tableCreate('thenewtable', { primaryKey: 'foo' })
    .run()

  t.deepEqual(tableCreateRes, {
    tables_created: 1,
    config_changes: [{
      new_val: {
        db: 'default',
        durability: 'hard',
        id: dbState.dbConfig_default_thenewtable.id,
        indexes: [],
        name: 'thenewtable',
        primary_key: 'foo',
        shards: [{
          primary_replica: 'replicaName',
          replicas: [
            'replicaName'
          ]
        }],
        write_acks: 'majority',
        write_hook: null
      },
      old_val: null
    }]
  })

  const infoRes = await r
    .db('default')
    .table('thenewtable')
    .info()
    .run()

  t.deepEqual(infoRes, {
    db: {
      ...dbState.dbConfig_default,
      type: 'DB'
    },
    doc_count_estimates: [0],
    id: dbState.dbConfig_default_thenewtable.id,
    indexes: [],
    name: 'thenewtable',
    primary_key: 'foo',
    type: 'TABLE'
  })
})

test('`tableCreate` should throw if table exists', async t => {
  const { r } = rethinkdbMocked([['Rooms']])

  await t.throwsAsync(async () => (
    r.db('default').tableCreate('Rooms').run()
  ), {
    message: 'Table `default.Rooms` already exists.'
  })
})

test('`tableCreate` should throw -- non valid args', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => (
    r.db('default').tableCreate('thetablename', { nonValidArg: true }).run()
  ), {
    message: 'Unrecognized optional argument `nonValidArg`.'
  })
})

test('`tableCreate` should throw if no argument is given', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => (
    r.db('default').tableCreate().run()
  ), {
    message: '`r.tableCreate` takes at least 1 argument, 0 provided.'
  })
})

test('`tableCreate` should throw is the name contains special char', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => (
    r.db('default').tableCreate('^_-').run()
  ), {
    message: 'RethinkDBError [ReqlLogicError]: Table name `^_-` invalid (Use A-Z, a-z, 0-9, _ and - only)'
  })
})

test('`tableDrop` should drop a table', async t => {
  const { r, dbState } = rethinkdbMocked([['Rooms']])

  const tableCreateRes = await r
    .db('default')
    .tableCreate('thenewtable', { primaryKey: 'foo' })
    .run()

  t.is(tableCreateRes.tables_created, 1)

  const tableListRes = await r
    .db('default')
    .tableList()
    .run()

  t.deepEqual(tableListRes, ['Rooms', 'thenewtable'])

  const thenewtableid = dbState.dbConfig_default_thenewtable.id

  const tableDropRes = await r
    .db('default')
    .tableDrop('thenewtable')
    .run()

  t.deepEqual(tableDropRes, {
    tables_dropped: 1,
    config_changes: [{
      new_val: null,
      old_val: {
        db: 'default',
        durability: 'hard',
        id: thenewtableid,
        indexes: [],
        name: 'thenewtable',
        primary_key: 'foo',
        shards: [{
          primary_replica: 'replicaName',
          replicas: ['replicaName']
        }],
        write_acks: 'majority',
        write_hook: null
      }
    }]
  })

  const tableListRes2 = await r
    .db('default')
    .tableList()
    .run()

  t.deepEqual(tableListRes2, ['Rooms'])
})

test('`indexCreate` should work nested values', async t => {
  const { r } = rethinkdbMocked([['Applications', {
    name: 'testapp',
    tokens: [{
      created_at: new Date(),
      creator_id: 'creatorId-1234',
      value: 'ce1wzwq'
    }, {
      created_at: new Date(),
      creator_id: 'creatorId-5678',
      value: '7ljYP1v'
    }]
  }]])

  await r
    .table('Applications')
    .indexList()
    .contains('tokens')
    .or(r.table('Applications').indexCreate(
      'tokens', r.row('tokens').map(token => token('value')), { multi: true })
    ).run()

  await r.table('Applications').indexWait().run()

  const apps = await r
    .table('Applications')
    .getAll('7ljYP1v', { index: 'tokens' })
    .run()

  t.is(apps[0].name, 'testapp')
})

test('`indexCreate` should work with official doc example', async t => {
  const { r } = rethinkdbMocked([['friends', {
    id: 'fred',
    hobbies: ['cars', 'drawing'],
    sports: ['soccer', 'baseball']
  }]])

  await r.table('friends').indexCreate(
    'activities', row => row('hobbies').add(row('sports')), { multi: true }
  ).run()

  await r.table('friends').indexWait().run()
  const favorites = await r
    .table('friends')
    .getAll('baseball', { index: 'activities' })
    .run()

  t.deepEqual(favorites, [{
    id: 'fred',
    hobbies: ['cars', 'drawing'],
    sports: ['soccer', 'baseball']
  }])
})

test('`indexCreate` should work with basic index and multi ', async t => {
  const { r } = rethinkdbMocked([
    ['testtable',
      { foo: ['bar1', 'bar2'], buzz: 1 },
      { foo: ['bar1', 'bar3'], buzz: 2 }
    ]])

  const result = await r
    .db('default')
    .table('testtable')
    .indexCreate('foo', { multi: true })
    .run()
  t.deepEqual(result, { created: 1 })

  const result3 = await r
    .db('default')
    .table('testtable')
    .getAll('bar1', { index: 'foo' })
    .count()
    .run()
  t.is(result3, 2)

  const result6 = await r
    .db('default')
    .table('testtable')
    .getAll('bar2', { index: 'foo' })
    .count()
    .run()
  t.is(result6, 1)

  const result9 = await r
    .db('default')
    .table('testtable')
    .getAll('bar3', { index: 'foo' })
    .count()
    .run()
  t.is(result9, 1)
})

test('`indexCreate` should work with options', async t => {
  const { r } = rethinkdbMocked([
    ['testtable',
      { foo: ['bar1', 'bar2'], buzz: 1 },
      { foo: ['bar1', 'bar3'], buzz: 2 }
    ]])

  let result = await r
    .db('default')
    .table('testtable')
    .indexCreate('foo1', row => row('foo'), { multi: true })
    .run()
  t.deepEqual(result, { created: 1 })

  result = await r
    .db('default')
    .table('testtable')
    .indexCreate('foo2', doc => doc('foo'), { multi: true })
    .run()
  t.deepEqual(result, { created: 1 })

  const result4 = await r
    .db('default')
    .table('testtable')
    .getAll('bar1', { index: 'foo1' })
    .count()
    .run()
  t.is(result4, 2)
  const result5 = await r
    .db('default')
    .table('testtable')
    .getAll('bar1', { index: 'foo2' })
    .count()
    .run()
  t.is(result5, 2)

  const result7 = await r
    .db('default')
    .table('testtable')
    .getAll('bar2', { index: 'foo1' })
    .count()
    .run()
  t.is(result7, 1)
  const result8 = await r
    .db('default')
    .table('testtable')
    .getAll('bar2', { index: 'foo2' })
    .count()
    .run()
  t.is(result8, 1)

  const result10 = await r
    .db('default')
    .table('testtable')
    .getAll('bar3', { index: 'foo1' })
    .count()
    .run()
  t.is(result10, 1)
  const result11 = await r
    .db('default')
    .table('testtable')
    .getAll('bar3', { index: 'foo2' })
    .count()
    .run()
  t.is(result11, 1)
})

test('`indexCreate` should work with wrapped array', async t => {
  const { r } = rethinkdbMocked([
    ['testtable',
      { foo: ['bar1', 'bar2'], buzz: 1 },
      { foo: ['bar1', 'bar3'], buzz: 2 }
    ]])

  const result12 = await r
    .db('default')
    .table('testtable')
    .indexCreate('buzz', row => [row('buzz')])
    .run()
  t.deepEqual(result12, { created: 1 })

  await r
    .db('default')
    .table('testtable')
    .indexWait()
    .run()

  const result13 = await r
    .db('default')
    .table('testtable')
    .getAll([1], { index: 'buzz' })
    .count()
    .run()
  t.is(result13, 1)
})
