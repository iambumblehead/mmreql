import test from 'ava'
import rethinkdbMocked from '../src/mmReql.mjs'

test('`orderBy` should work on array -- string', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
    .orderBy('a')
    .run()

  t.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }])
})

test('`orderBy` should work on array -- row => row', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
    .orderBy(row => row('a'))
    .run()

  t.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }])
})

test('`orderBy` should work on a table -- pk', async t => {
  const { r } = rethinkdbMocked([
    ['marvel',
      { id: 'Iron Man', victories: 214 },
      { id: 'Jubilee', victories: 49 },
      { id: 'Slava', victories: 5 }]
  ])

  const result = await r
    .db('default')
    .table('marvel')
    .orderBy({ index: 'id' })
    .run()

  for (let i = 0; i < result.length - 1; i++) {
    t.true(result[i].id < result[i + 1].id)
  }
})

test('`orderBy` should work on a table -- secondary', async t => {
  const { r } = rethinkdbMocked([
    ['marvel',
      { name: 'Iron Man', victories: 214 },
      { name: 'Jubilee', victories: 49 },
      { name: 'Slava', victories: 5 }]
  ])

  await r.table('marvel').indexCreate('name').run()
  await r.table('marvel').indexWait('name').run()

  const result = await r
    .db('default')
    .table('marvel')
    .orderBy({ index: 'name' })
    .run()

  for (let i = 0; i < result.length - 1; i++) {
    t.true(result[i].name < result[i + 1].name)
  }
})

test('`orderBy` should work on a two fields', async t => {
  const { r } = rethinkdbMocked()
  const numDocs = 98

  await r.tableCreate('numbers').run()
  await r.table('numbers').insert(
    Array(numDocs).fill(0).map(() => ({ a: Math.random() }))
  ).run()

  const res = await r.table('numbers').orderBy('id', 'a').run()
  t.true(Array.isArray(res))
  t.true(res[0].id < res[1].id)
})

test('`orderBy` should throw if no argument has been passed', async t => {
  const { r } = rethinkdbMocked([
    ['marvel',
      { name: 'Iron Man', victories: 214 },
      { name: 'Jubilee', victories: 49 },
      { name: 'Slava', victories: 5 }]
  ])

  await t.throws(() => (
    r.table('marvel').orderBy().run()
  ), {
    message: '`orderBy` takes at least 1 argument, 0 provided.'
  })
})

test('`orderBy` should not wrap on r.asc', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
    .orderBy(r.asc(row => row('a')))
    .run()

  t.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }])
})

test('`orderBy` should not wrap on r.desc', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
    .orderBy(r.desc(row => row('a')))
    .run()

  t.deepEqual(result, [{ a: 100 }, { a: 23 }, { a: 10 }, { a: 0 }])
})

test('r.desc should work', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
    .orderBy(r.desc('a'))
    .run()
  t.deepEqual(result, [{ a: 100 }, { a: 23 }, { a: 10 }, { a: 0 }])
})

test('r.asc should work', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .expr([{ a: 23 }, { a: 10 }, { a: 0 }, { a: 100 }])
    .orderBy(r.asc('a'))
    .run()

  t.deepEqual(result, [{ a: 0 }, { a: 10 }, { a: 23 }, { a: 100 }])
})

test('`desc` is not defined after a term', async t => {
  const { r } = rethinkdbMocked()

  await t.throws(() => (
    r.expr(1).desc('foo').run()
  ), {
    message: '.desc is not a function'
  })
})

test('`asc` is not defined after a term', async t => {
  const { r } = rethinkdbMocked()

  await t.throws(() => (
    r.expr(1).asc('foo').run()
  ), {
    message: '.asc is not a function'
  })
})

