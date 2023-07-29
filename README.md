<h3 align="center"><img src="https://i.imgur.com/yG2T2o4.jpg" alt="logo" height="200px"></h3>
<p align="center"><code>mmreql</code> provides a mock rethinkdb-ts for tests and offline development</p>

<p align="center">
<a href="https://www.npmjs.com/package/mmreql"><img src="https://img.shields.io/npm/v/mmreql"></a>
<a href="https://github.com/iambumblehead/mmreql/workflows"><img src="https://github.com/iambumblehead/mmreql/workflows/test/badge.svg"></a>
<a href="./LICENSE.md"><img src="https://img.shields.io/badge/license-ISC-blue.svg"></a>
</p>


```javascript
import mmreql from 'mmreql'

const { r } = mmreql([
  ['Users',
    { id: 'userId-1234', name: 'fred' },
    { id: 'userId-5678', name: 'jane' }],
  ['Rooms', [{ primaryKey: 'room_id' }],
    { room_id: 'roomId-1234', name: 'the room' }],
  ['Memberships', {
    user_id: 'userId-1234',
    room_membership_type: 'INVITE',
    user_sender_id: 'userId-5678',
    room_id: 'roomId-1234'
  }]
])

await r.table('Memberships').indexCreate('user_id').run()

console.log(await r
 .table('Memberships')
 .getAll('userId-1234', { index: 'user_id' })
 .eqJoin('room_id', r.table('Rooms'))('right')
 .run())
// { room_id: 'roomId-1234', name: 'the room' }
```

A mock-[rethinkdb-ts][3] database package for javascript environments
 * support for complex and nested queries,
 * support for creating, updating, deleting and specifying many databases,
 * support for [changefeeds,][1]
 * optionally initialized with plain object-literal data,
 * copies and re-uses tests [from rethinkdb-ts,][2]
 * supports atomic row update and replace queries,
 * zero dependencies

This mock database was developed around various services to cover a wide range of use-cases. See [unit-tests][4] for more detailed examples. Feel free to open an issue or send an email for any other questions.

[0]: ./spec/template-js-rethinkdb-mocked-thinky.spec.js
[1]: https://rethinkdb.com/docs/changefeeds/javascript/
[2]: https://github.com/rethinkdb/rethinkdb-ts/blob/main/test/manipulating-tables.ts
[3]: https://github.com/rethinkdb/rethinkdb-ts
[4]: ./spec/
