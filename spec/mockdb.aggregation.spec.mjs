import test from 'ava'
import rethinkdbMocked from '../src/mockdb.mjs'

test('`reduce` should work -- no base ', async t => {
  const { r } = rethinkdbMocked()
  const result1 = await r
    .expr([1, 2, 3])
    .reduce((left, right) => left.add(right))
    .run()

  t.is(result1, 6)
})

test('`reduce` should throw if no argument has been passed', async t => {
  const dbName = 'dbName'
  const tableName = 'tableName'
  const { r } = rethinkdbMocked([
    { db: dbName }, [tableName, { id : 'id' }]
  ])

  await t.throwsAsync(async () => r.db(dbName).table(tableName).reduce().run(), {
    message: '`reduce` takes 1 argument, 0 provided.'
  })
})

test('`reduce` should throw if empty stream', async t => {
  const { r } = rethinkdbMocked()
  await t.throwsAsync(async () => r.expr([]).reduce(l => l).run(), {
    message: 'Cannot reduce over an empty stream.'
  })
})

test('`reduce` should return lone atom if one element only', async t => {
  const { r } = rethinkdbMocked()
  
  t.is(await r.expr([5]).reduce(l => l.add(3)).run(), 5)
})

test('`reduce` should handle deeply nested math row query', async t => {
  const dbName = 'dbName'
  const tableName = 'tableName'
  const { r } = rethinkdbMocked([
    { db: dbName },
    [tableName,
      { id: 1, count: 1 },
      { id: 2, count: 2 },
      { id: 3, count: 3 }]
  ])

  const result1 = await r
    .db(dbName)
    .table(tableName)
    .reduce((left, right) => r.branch(
      right('count').ge(left('count')), right, left))
    .run()

  t.deepEqual(result1, { id: 3, count: 3 })
})

test('`fold` should work', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .expr([1, 2, 3])
    .fold(10, (left, right) => left.add(right))
    .run()

  t.is(result, 16)
})

test('`fold` should throw if no argument has been passed', async t => {
  const dbName = 'dbName'
  const tableName = 'tableName'
  const { r } = rethinkdbMocked([
    { db: dbName }, [tableName, { id : 'id' }]
  ])

  await t.throwsAsync(async () => r.db(dbName).table(tableName).fold().run(), {
    message: '`fold` takes 2 arguments, 0 provided.'
  })
})

test('`distinct` should work', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .expr([1, 2, 3, 1, 2, 1, 3, 2, 2, 1, 4])
    .distinct()
    .orderBy(row => row)
    .run()

  t.deepEqual(result, [1, 2, 3, 4])
})

test('`r.distinct` should work', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .distinct([1, 2, 3, 1, 2, 1, 3, 2, 2, 1, 4])
    .orderBy(row => row)
    .run()

  t.deepEqual(result, [1, 2, 3, 4])
})

test('`distinct` should work with an index', async t => {
  const { r } = rethinkdbMocked([
    { db: 'jobrunner' },
    ['JobEvents', {
      jobId : 1,
      name : 'job1'
    },{
      jobId : 2,
      name : 'job2'
    },{
      jobId : 1,
      name : 'job1-log'
    }]
  ])

  await r.db('jobrunner').table('JobEvents').indexCreate('jobId').run()
    
  const result = await r
    .db('jobrunner')
    .table('JobEvents')
    .distinct({ index: 'jobId' })
    .run()

  t.deepEqual(result, [1, 2])
})
