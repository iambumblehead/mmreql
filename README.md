# mmreql

<h3 align="center"><img src="https://i.imgur.com/yG2T2o4.jpg" alt="logo" height="100px"></h3>
<p align="center"><code>mmreql</code> provides a mock rethinkdb-ts for tests and offline development</p>

<p align="center">
<a href="https://www.npmjs.com/package/mmreql"><img src="https://img.shields.io/npm/v/mmreql"></a>
<a href="https://github.com/iambumblehead/mmreql/workflows"><img src="https://github.com/iambumblehead/mmreql/workflows/test/badge.svg"></a>
<a href="./LICENSE.md"><img src="https://img.shields.io/badge/license-ISC-blue.svg"></a>
</p>


```javascript
import rethinkMocked from 'template-js-rethinkdb-mocked';

const { r } = rethinkdbMocked([
  ['Applications', {
    id: 'appid-1234',
    name: 'app name',
    description: 'app description'
  }],
  ['Users', {
    id: 'userid-fred-1234',
    name: 'fred'
  }, {
    id: 'userid-jane-1234',
    name: 'jane'
  }]
]);

const appDoc = await r.table( 'Applications' ).get( 'appid-1234' ).run();

console.log(appDoc);
// {
//   id: 'appid-1234',
//   name: 'app name',
//   description: 'app description'
// }
```

**v0.1.2** provides near-drop-in replacement for 'thinkyMock' and 'rethinkMock' files found in venom-api and the template application.

**v0.2.0** removes boilerplate required by v0.1.2 and adds unit-tests and support for more queries.

**v0.4.0** uses one logic for all query chains and removes duplicated branches of logic. adds support and unit tests for all query patterns. removes unused, over-complicated Thinky ORM code.

**v0.4.3** adds initial support for [changefeed cursors.][1]

**v0.6.0** adds support for multiple databases. adds table and database configuration queries. Copies and re-uses tests [from rethinkdb-ts.][2]

**v0.8.0** adds support for complex insert queries. documents can be inserted, replaced and modified at the same time through a single atomic query. improves support for tables with custom primary_keys. ex, 'user_id' as in the document `{ user_id: 'cyclingHal4482', presence: 'ONLINE' }`
 
**v0.9.5** adds support for advanced `getAll()` queries, `getAll().getField('name')`, `getAll(r.expr(...)).)`, support for deeply nested row query `r.row('name').eq('xavier').or(r.row('membership').eq('joined'))`, and completed support for configurable primaryKeys.

**v1.0.0** adds completed stream, changefeed and cursor support. adds basic support for 'multi' index. adds support for initializing mulitple databases.

**v1.1.0** adds more error messages that match those from rethinkdb, more support for attribute short hand `r.row('user')('nameattr')`, improved support for sub queries used with `eqJoin` and added more complex `eqJoin` tests. Prevent `merge( ... )` queries from mutating documents stored in table.


[0]: ./spec/template-js-rethinkdb-mocked-thinky.spec.js
[1]: https://rethinkdb.com/docs/changefeeds/javascript/
[2]: https://github.com/rethinkdb/rethinkdb-ts/blob/main/test/manipulating-tables.ts
