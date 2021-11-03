import test from 'ava';
import rethinkdbMocked from '../src/mockdb.mjs';

test( '`expr` should work', async t => {
  const { r } = rethinkdbMocked();
    
  const result = await r.expr( 1 ).run();
  t.is( result, 1 );
});

test( '`dbList` should return a cursor', async t => {
  const { r } = rethinkdbMocked();
  const result = await r.dbList().run();

  t.true( Array.isArray( result ) );
});

test( '`dbCreate` should create a database', async t => {
  const { r } = rethinkdbMocked();
  const dbName = 'dbName';

  const result = await r.dbCreate( dbName ).run();
  t.is( result.dbs_created, 1 );
});

test( '`dbCreate` should throw if no argument is given', async t => {
  const { r } = rethinkdbMocked();    

  await t.throws( () => (
    r.dbCreate().run()
  ), {
    message: '`r.dbCreate` takes 1 argument, 0 provided.'
  });
});

// test( '`dbCreate` is not defined after a term', async t => {
//     const { r } = rethinkdbMocked();    
//     await t.throws( () => (
//         r.expr( 1 ).dbCreate().run()
//     ), {
//         messageendsWith: '.db is not a function'
//     });
// });

test( '`db` should throw is the name contains special char', async t => {
  const { r } = rethinkdbMocked();    

  await t.throws( () => (
    r.db( '*_*' ).run()
  ), {
    message: 'Database name `*_*` invalid (Use A-Z, a-z, 0-9, _ and - only)'
  });
});

test( '`dbList` should show the database we created ("default" db always created)', async t => {
  const { r } = rethinkdbMocked();
  const dbName = 'dbName'; // export to the global scope

  const result1 = await r.dbCreate( dbName ).run();
  t.is( result1.dbs_created, 1 );

  const result2 = await r.dbList().run();

  t.deepEqual( result2, [ 'default', dbName ]);
});

test( '`dbDrop` should drop a table', async t => {
  const { r } = rethinkdbMocked();
  const dbName = 'dbName';

  let result = await r.dbCreate( dbName ).run();
  t.is( result.dbs_created, 1 );

  result = await r.dbDrop( dbName ).run();
  t.is( result.dbs_dropped, 1 );
});

test( '`dbDrop` should throw if given too many arguments', async t => {
  const { r } = rethinkdbMocked();    

  await t.throws( () => (
    r.dbDrop( 'foo', 'bar', 'ette' ).run()
  ), {
    message: '`r.dbDrop` takes 1 argument, 3 provided.'
  });
});

test( '`dbDrop` should throw if no argument is given', async t => {
  const { r } = rethinkdbMocked();

  await t.throws( () => (
    r.dbDrop().run()
  ), {
    message: '`r.dbDrop` takes 1 argument, 0 provided.'
  });
});

// test( '`dbDrop` is not defined after a term', async t => {
//     const { r } = rethinkdbMocked();    
//     await t.throws( () => (
//         r.expr( 1 ).dbCreate().run()
//     ), {
//         messageendsWith: '.dbDrop is not a function'
//     });
// })

//  test( '`dbList` is not defined after a term', async t => {
//     const { r } = rethinkdbMocked();    
//     await t.throws( () => (
//         r.expr( 'foo' ).dbList.dbCreate().run()
//     ), {
//         messageendsWith: '.dbList is not a function'
//     });
//
//  });

test( '`dbList` should contain dropped databases', async t => {
  const { r } = rethinkdbMocked();
  const dbName = 'dbName'; // export to the global scope

  const result1 = await r.dbCreate( dbName ).run();
  t.is( result1.dbs_created, 1 );

  const result2 = await r.dbDrop( dbName ).run();
  t.is( result2.dbs_dropped, 1 );

  const result3 = await r.dbList().run();
  t.deepEqual( result3, [ 'default' ]);
});
