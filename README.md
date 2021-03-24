# template-js-rethinkdb-mocked

![rethinkdb](rethinkdb-1500x500.jpg)

[![pipeline status](https://code.venom360.com/platform/template-js-rethinkdb-mocked/badges/master/pipeline.svg)](https://code.venom360.com/platform/template-js-rethinkdb-mocked/commits/master)
[![coverage report](https://code.venom360.com/platform/template-js-rethinkdb-mocked/badges/master/coverage.svg)](https://code.venom360.com/platform/template-js-rethinkdb-mocked/commits/master)

```javascript
import rethinkMocked from 'template-js-rethinkdb-mocked';

const { r } = rethinkdbMocked([
    [ 'Applications', {
        id: 'appid-1234',
        name: 'app name',
        description: 'app description'
    } ],
    [ 'Users', {
        id: 'userid-fred-1234',
        name: 'fred'
    }, {
        id: 'userid-jane-1234',
        name: 'jane'
    } ]
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
 


[0]: ./spec/template-js-rethinkdb-mocked-thinky.spec.js
[1]: https://rethinkdb.com/docs/changefeeds/javascript/
[2]: https://github.com/rethinkdb/rethinkdb-ts/blob/main/test/manipulating-tables.ts
