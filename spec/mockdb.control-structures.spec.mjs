import test from 'ava';
import rethinkdbMocked from '../src/mockdb.mjs';

test( '`do` should work', async t => {
  const { r } = rethinkdbMocked();

  const result = await r
    .expr({ a: 1 })
    .do( doc => doc( 'a' ) )
    .run();

  t.is( result, 1 );
});

test( '`r.do` should work', async t => {
  const { r } = rethinkdbMocked();

  const result1 = await r
    .do( 1, 2, a => a )
    .run();

  t.is( result1, 1 );

  const result2 = await r
    .do( 1, 2, ( a, b ) => b )
    .run();

  t.is( result2, 2 );

  const result3 = await r.do( 3 ).run();
  t.is( result3 , 3 );

  const result4 = await r
    .expr( 4 )
    .do()
    .run();
  t.is( result4, 4 );

  const result5 = await r.do( 1, 2 ).run();
  t.is( result5, 2 );

  const result6 = await r
    .do( r.args([ r.expr( 3 ), r.expr( 4 ) ]) )
    .run();

  t.is( result6, 3 );
});

test( '`forEach` should work', async t => {
  const { r } = rethinkdbMocked();
  const dbName = 'testdb';
  const tableName = 'testtable';
  let result;

  result = await r.dbCreate( dbName ).run();
  t.is( result.dbs_created, 1 );

  result = await r
    .db( dbName )
    .tableCreate( tableName )
    .run();

  t.is( result.tables_created, 1 );

  result = await r
    .expr([ { foo: 'bar' }, { foo: 'foo' } ])
    .forEach( doc => r.db( dbName ).table( tableName ).insert( doc ) )
    .run();

  t.is( result.inserted, 2 );
});

test( '`forEach` should throw if not given a function', async t => {
  const { r } = rethinkdbMocked([ { db: 'cmdb' } ]);
  await t.throws( () => (
    r.expr([ { foo: 'bar' }, { foo: 'foo' } ]).forEach().run()
  ), {
    message: '`forEach` takes 1 argument, 0 provided.'
  });
});
