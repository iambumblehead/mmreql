# template-js-rethinkdb-mocked

Database mocking used by template projects. Using this package [requires extensive boilerplate.][0]

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

**Version 0.1.2** provides drop-in replacement for 'thinkyMock' and 'rethinkMock' files found in venom-api and the template application.

**Version 0.2.0** removes boilerplate required by 0.1.2 and adds unit-tests and support for more queries than previous versions incl `nth`, `add`, `append`, `epochTime` and more. Changes here are used by the social-profile application.


------------------
If you are updating this package, please try to add one or two tests and remove a few boilerplate requirements :).


[0]: ./spec/template-js-rethinkdb-mocked-thinky.spec.js
