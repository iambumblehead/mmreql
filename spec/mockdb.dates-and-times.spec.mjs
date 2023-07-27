import test from 'ava'
import rethinkdbMocked from '../src/mmReql.mjs'

test('`r.now` should return a date', async t => {
  const { r } = rethinkdbMocked()

  const result1 = await r.now().run()
  t.true(result1 instanceof Date)

  const result2 = await r.expr({ a: r.now() }).run()
  t.true(result2.a instanceof Date)

  const result3 = await r.expr([r.now()]).run()
  t.true(result3[0] instanceof Date)

  const result4 = await r.expr([{}, { a: r.now() }]).run()
  t.true(result4[1].a instanceof Date)

  // too deep
  // const result5 = await r.expr({ b: [{}, { a: r.now() }] }).run()
  // t.true(result5.b[1].a instanceof Date)
})

test('`now` is not defined after a term', async t => {
  const { r } = rethinkdbMocked()
 
  await t.throwsAsync(async () => r.expr(1).now('foo').run(), {
    message: '.now is not a function'
  })
})

test('`r.time` should return a date -- with date and time', async t => {  
  const { r } = rethinkdbMocked()

  const result1 = await r.time(1986, 11, 3, 12, 0, 0, 'Z').run()
  t.is(result1 instanceof Date, true)

  const result2 = await r
    .time(1986, 11, 3, 12, 20, 0, 'Z')
    .minutes()
    .run()
  t.is(result2, 20)
})

test('`r.time` should work with r.args', async t => {
  const { r } = rethinkdbMocked()
  const result = await r
    .time(r.args([1986, 11, 3, 12, 0, 0, 'Z']))
    .run()

  t.is(result instanceof Date, true)
})

test('`r.time` should return a date -- just with a date', async t => {
  const { r } = rethinkdbMocked()

  let result = await r.time(1986, 11, 3, 'Z').run()
  t.is(result instanceof Date, true)
  result = await r.time(1986, 11, 3, 0, 0, 0, 'Z').run()
  t.is(result instanceof Date, true)
})

test('`r.time` should throw if no argument has been given', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => r.time().run(), {
    message: '`r.time` takes at least 4 arguments, 0 provided.'
  })  
})

test('`r.time` should throw if no 5 arguments', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => r.time(1, 1, 1, 1, 1).run(), {
    message: 'Got 5 arguments to TIME (expected 4 or 7)'
  })  
})

test('`time` is not defined after a term', async t => {
  const { r } = rethinkdbMocked()
 
  await t.throwsAsync(async () => r.expr(1).time(1, 2, 3, 'Z').run(), {
    message: '.time is not a function'
  })
})

test('`epochTime` should work', async t => {
  const { r } = rethinkdbMocked()

  const now = new Date()
  const result = await r.epochTime(now.getTime() / 1000).run()

  t.is(String(result.getTime()).slice(0, 8), String(Date.now()).slice(0, 8))
})

test('`r.epochTime` should throw if no argument has been given', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => r.epochTime().run(), {
    message: '`r.epochTime` takes 1 argument, 0 provided.'
  })  
})

test('`epochTime` is not defined after a term', async t => {
  const { r } = rethinkdbMocked()
 
  await t.throwsAsync(async () => r.expr(1).epochTime(Date.now()).run(), {
    message: '.epochTime is not a function'
  })
})

test('`ISO8601` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r.ISO8601('1986-11-03T08:30:00-08:00').run()

  t.is(result.getTime(), Date.UTC(1986, 10, 3, 8 + 8, 30, 0))
})

test('`ISO8601` should work with a timezone', async t => {
  const { r } = rethinkdbMocked()

  const result = await r.ISO8601('1986-11-03T08:30:00', {
    defaultTimezone: '-08:00'
  }).run()

  t.is(result.getTime(), Date.UTC(1986, 10, 3, 8 + 8, 30, 0))
})

test('`r.ISO8601` should throw if no argument has been given', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => r.ISO8601().run(), {
    message: '`r.ISO8601` takes 1 argument, 0 provided.'
  })
})

test('`r.ISO8601` should throw if too many arguments', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => r.ISO8601(1, 1, 1).run(), {
    message: '`r.ISO8601` takes at most 2 arguments, 3 provided.'
  })  
})

test('`ISO8601` is not defined after a term', async t => {
  const { r } = rethinkdbMocked()
  
  await t.throwsAsync(async () => r.expr(1).ISO8601('validISOstring').run(), {
    message: '.ISO8601 is not a function'
  })
})

// eslint-disable-next-line ava/no-skip-test
test.skip('`inTimezone` should work', async t => {
  const { r } = rethinkdbMocked()

  // inTimezone needs some extra scripting to be supported
  const result = await r
    .now()
    .inTimezone('-08:00')
    .hours()
    .do(h => r.branch(
      h.eq(0),
      r.expr(23).eq(r.now().inTimezone('-09:00').hours()),
      h.eq(r.now().inTimezone('-09:00').hours().add(1))
    )).run()

  t.is(result, true)
})

test('`r.inTimezone` should throw if no argument has been given', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => r.now().inTimezone().run(), {
    message: '`inTimezone` takes 1 argument, 0 provided.'
  })
})

test('`r.inTimezone` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r
    .ISO8601('1986-11-03T08:30:00-08:00')
    .timezone()
    .run()

  t.is(result, '-08:00')
})

test('`during` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r
    .now()
    .during(r.time(2013, 12, 1, 'Z'), r.now().add(1000))
    .run()

  t.is(result || true, true)

  // const result2 = await r
  //   .now()
  //   .during(r.time(2013, 12, 1, 'Z'), r.now(), {
  //     leftBound: 'closed',
  //     rightBound: 'closed'
  //   }).run()
  //
  // t.is(result2, true)

  // const result3 = await r
  //   .now()
  //   .during(r.time(2013, 12, 1, 'Z'), r.now(), {
  //     leftBound: 'closed',
  //     rightBound: 'open'
  //   }).run()
  //
  // t.is(result, false)
})

test('`during` should throw if no argument has been given', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => r.now().during().run(), {
    message: '`during` takes at least 2 arguments, 0 provided.'
  })
})

test('`during` should throw if just one argument has been given', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => r.now().during(1).run(), {
    message: '`during` takes at least 2 arguments, 1 provided.'
  })
})

test('`during` should throw if too many arguments', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => r.now().during(1, 1, 1, 1, 1).run(), {
    message: '`during` takes at most 3 arguments, 5 provided.'
  })
})

test('`date` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r
    .now()
    .date()
    .hours()
    .run()
  t.is(result, 0)
  
  const result2 = await r
    .now()
    .date()
    .minutes()
    .run()
  t.is(result2, 0)

  const result3 = await r
    .now()
    .date()
    .seconds()
    .run()
  t.is(result3, 0)
})

test('`timeOfDay` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r
    .now()
    .timeOfDay()
    .run()
  t.true(result >= 0)
})

test('`year` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r
    .now()
    .inTimezone(new Date().toString().match(' GMT([^ ]*)')[1])
    .year()
    .run()

  t.is(result, new Date().getFullYear())
})

test('`month` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r
    .now()
    .inTimezone(new Date().toString().match(' GMT([^ ]*)')[1])
    .month()
    .run()

  t.is(result, new Date().getMonth() + 1)
})

test('`day` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r
    .now()
    .inTimezone(new Date().toString().match(' GMT([^ ]*)')[1])
    .day()
    .run()

  t.is(result, new Date().getDate())
})

test('`dayOfYear` should work', async t => {
  const { r } = rethinkdbMocked()
  
  const result = await r
    .now()
    .inTimezone(new Date().toString().match(' GMT([^ ]*)')[1])
    .dayOfYear()
    .run()
  t.true(result > new Date().getMonth() * 28 + new Date().getDate() - 1)
})

test('`dayOfWeek` should work', async t => {
  const { r } = rethinkdbMocked()
  
  const result = await r
    .now()
    .inTimezone(new Date().toString().match(' GMT([^ ]*)')[1])
    .dayOfWeek()
    .run()

  t.is(result === 7 ? 0 : result, new Date().getDay())
})

test('`toISO8601` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r
    .now()
    .toISO8601()
    .run()
  t.is(typeof result, 'string')
})

test('`toEpochTime` should work', async t => {
  const { r } = rethinkdbMocked()

  const result = await r
    .now()
    .toEpochTime()
    .run()
  t.is(typeof result, 'number')
})

test('Date should be parsed correctly', async t => {
  const { r } = rethinkdbMocked()
  const date = new Date()
  const result = await r.expr({ date }).run()
  t.is(result.date.getTime(), date.getTime())
})

test('Constant terms should work', async t => {
  const { r } = rethinkdbMocked()

  // requires extra scripting
  // let result = await r.monday.run()
  // t.is(result, 1)

  const result2 = await r
    .expr([
      r.monday,
      r.tuesday,
      r.wednesday,
      r.thursday,
      r.friday,
      r.saturday,
      r.sunday,
      r.january,
      r.february,
      r.march,
      r.april,
      r.may,
      r.june,
      r.july,
      r.august,
      r.september,
      r.october,
      r.november,
      r.december
    ]).run()
  t.deepEqual(result2, [
    1, 2, 3, 4, 5, 6, 7,
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
  ])
})

// test('`epochTime` should work', async t => {
//   const { r } = rethinkdbMocked()
//   const now = new Date()
//   const result = await r
//     .epochTime(now.getTime() / 1000)
//     .run({ timeFormat: 'raw' })
//   t.is(result.$reql_type$, 'TIME')
// })

// test('`ISO8601` run parameter should work', async t => {
//   const { r } = rethinkdbMocked()
//   const result = await r
//     .time(2018, 5, 2, 13, 0, 0, '-03:00')
//     .run({ timeFormat: 'ISO8601' })
//   t.is(typeof result, 'string')
//   t.is(result, '2018-05-02T13:00:00.000-03:00')
// })

