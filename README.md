# template-js-rethinkdb-mocked

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

**v0.4.0** uses one logic for all query chains and removes duplicated branches of logic. adds support and unit tests for all query patterns. removes unused, over-complicated Thinky ORM code. this version is for you.




[0]: ./spec/template-js-rethinkdb-mocked-thinky.spec.js
