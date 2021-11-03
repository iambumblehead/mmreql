import test from 'ava';
import rethinkdbMocked from '../src/mockdb.mjs';

test( '`without` should work', async t => {
  const { r } = rethinkdbMocked();
  const result1 = await r
    .expr({ a: 0, b: 1, c: 2 })
    .without( 'c' )
    .run();

  t.deepEqual( result1, { a: 0, b: 1 });

  const result2 = await r
    .expr([ { a: 0, b: 1, c: 2 }, { a: 0, b: 10, c: 20 } ])
    .without( 'a', 'c' )
    .run();

  t.deepEqual( result2, [ { b: 1 }, { b: 10 } ]);
});

test( '`without` should throw if no argument has been passed', async t => {
  const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);
    
  await t.throws( () => ( r        
    .table( 'Rooms' )
    .without()
    .run()
  ), {
    message: '`without` takes 1 argument, 0 provided.'
  });
});

test( '`prepend` should work', async t => {
  const { r } = rethinkdbMocked();

  const result = await r
    .expr([ 1, 2, 3 ])
    .prepend( 4 )
    .run();

  t.deepEqual( result, [ 4, 1, 2, 3 ]);
});

test( '`prepend` should throw if now argument has been passed', async t => {
  const { r } = rethinkdbMocked();

  await t.throws( () => ( r
    .expr([ 1, 2, 3 ])
    .prepend()
    .run()
  ), {
    message: '`prepend` takes 1 argument, 0 provided.'
  });
});

test( '`difference` should work', async t => {
  const { r } = rethinkdbMocked();

  const result = await r
    .expr([ 1, 2, 3 ])
    .difference([ 2 ])
    .run();

  t.deepEqual( result, [ 1, 3 ]);
});

test( '`difference` should throw if now argument has been passed', async t => {
  const { r } = rethinkdbMocked();

  await t.throws( () => ( r
    .expr([ 1, 2, 3 ])
    .difference()
    .run()
  ), {
    message: '`difference` takes 1 argument, 0 provided.'
  });
});

test( '`difference` should work with table names', async t => {
  const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

  const result = await r([ 'Rooms', 'Users' ])
    .difference( r.db( 'default' ).tableList() )
    .run();

  t.deepEqual( result, [ 'Users' ]);
});

test( '`merge` should work', async t => {
  const { r } = rethinkdbMocked();
  let result;

  result = await r
    .expr({ a: 0 })
    .merge({ b: 1 })
    .run();

  t.deepEqual( result, { a: 0, b: 1 });

  result = await r
    .expr([ { a: 0 }, { a: 1 }, { a: 2 } ])
    .merge({ b: 1 })
    .run();

  t.deepEqual( result, [ { a: 0, b: 1 }, { a: 1, b: 1 }, { a: 2, b: 1 } ]);

  result = await r
    .expr({ a: 0, c: { l: 'tt' } })
    .merge({ b: { c: { d: { e: 'fff' } }, k: 'pp' } })
    .run();

  t.deepEqual( result, {
    a: 0,
    b: { c: { d: { e: 'fff' } }, k: 'pp' },
    c: { l: 'tt' }
  });

  result = await r
    .expr({ a: 1 })
    .merge({ date: r.now() })
    .run();

  t.is( result.a, 1 );
  t.true( result.date instanceof Date );

  result = await r
    .expr({ a: 1 })
    .merge( row => ({ nested: row }), { b: 2 })
    .run();
  t.deepEqual( result, { a: 1, nested: { a: 1 }, b: 2 });
});

test( '`merge` should take an anonymous function', async t => {
  const { r } = rethinkdbMocked();
    
  const result = await r
    .expr({ a: 0 })
    .merge( doc => ({ b: doc( 'a' ).add( 1 ) }) )
    .run();

  t.deepEqual( result, { a: 0, b: 1 });
});

test( '`merge` should map an anonymous function against a list', async t => {
  const { r } = rethinkdbMocked();
    
  const result = await r
    .expr([ { a: 0 }, { a: 1 }, { a: 2 } ])
    .merge( doc => ({ b: doc( 'a' ).add( 1 ) }) )
    .run();

  t.deepEqual( result, [ { a: 0, b: 1 }, { a: 1, b: 2 }, { a: 2, b: 3 } ]);
});

test( '`merge` should throw if no argument has been passed', async t => {
  const { r } = rethinkdbMocked();
  await t.throws( () => ( r
    .expr([])
    .merge()
    .run()
  ), {
    message: '`merge` takes at least 1 argument, 0 provided.'
  });
});

