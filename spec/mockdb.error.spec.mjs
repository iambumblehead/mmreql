import test from 'ava'
import rethinkdbMocked from '../src/mmReql.mjs'

test('ReqlUserError', async t => {
  const { r } = rethinkdbMocked()

  await t.throwsAsync(async () => (
    r.branch(r.error('a'), 1, 2).run()
  ), {
    message: 'a'
  })
})
