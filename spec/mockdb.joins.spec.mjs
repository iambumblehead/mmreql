import test from 'ava'
import rethinkdbMocked from '../src/mockdb.mjs'

const insertTestRooms = async r => r
  .db('default')
  .table('Rooms')
  .insert([{ val: 1 }, { val: 2 }, { val: 3 }])
  .run()

test('`eqJoin` should return -- pk -- array-stream - function', async t => {
  const { r } = rethinkdbMocked([['Rooms']])

  const roomInsert = await insertTestRooms(r)    
  const pks = roomInsert.generated_keys

  const result = await r
    .expr(pks)
    .eqJoin(
      elem => elem, r.db('default').table('Rooms'))
    .run()

  t.is(result.length, 3)
  t.truthy(result[0].left)
  t.truthy(result[0].right)
  t.truthy(result[1].left)
  t.truthy(result[1].right)
  t.truthy(result[2].left)
  t.truthy(result[2].right)
})

test('`eqJoin` should return -- pk -- array-stream - row => row', async t => {
  const { r } = rethinkdbMocked([['Rooms']])

  const roomInsert = await insertTestRooms(r)    
  const pks = roomInsert.generated_keys

  const result = await r
    .expr(pks)
    .eqJoin(row => row, r.db('default').table('Rooms'))
    .run()

  t.is(result.length, 3)
  t.truthy(result[0].left)
  t.truthy(result[0].right)
  t.truthy(result[1].left)
  t.truthy(result[1].right)
  t.truthy(result[2].left)
  t.truthy(result[2].right)
})

test('`eqJoin` should return -- pk -- secondary index -- array-stream - row => row', async t => {
  const { r } = rethinkdbMocked([['Rooms']])

  await insertTestRooms(r)

  // verified with live test...
  const result = await r
    .expr([1, 2, 3])
    .eqJoin(row => row, r.db('default').table('Rooms'), { index: 'val' })
    .run()

  t.is(result.length, 3)
  t.truthy(result[0].left)
  t.truthy(result[0].right)
  t.truthy(result[1].left)
  t.truthy(result[1].right)
  t.truthy(result[2].left)
  t.truthy(result[2].right)
})

test('`eqJoin` should throw if no argument', async t => {
  const { r } = rethinkdbMocked([['Rooms']])

  await t.throws(() => (r
    .expr([1, 2, 3])
    .eqJoin()
    .run()
  ), {
    message: '`eqJoin` takes at least 2 arguments, 0 provided.'
  })
})

test('`eqJoin` should throw with a non valid key', async t => {
  const { r } = rethinkdbMocked([['Rooms']])

  await t.throws(() => (r
    .expr([1, 2, 3])
    .eqJoin(row => row, r.db('default').table('Rooms'), {
      nonValidKey: 'val'
    })
    .run()
  ), {
    message: 'Unrecognized optional argument `nonValidKey`.'
  })
})

test('`zip` should throw with a non valid key', async t => {
  const { r } = rethinkdbMocked([['Rooms']])

  const roomInsert = await insertTestRooms(r)
  const pks = roomInsert.generated_keys

  const result = await r
    .expr(pks)
    .eqJoin(doc => doc, r.db('default').table('Rooms'))
    .zip()
    .run()

  t.is(result.length, 3)
  t.is(result[0].left, undefined)
})
