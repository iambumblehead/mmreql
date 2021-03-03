import test from 'ava';
import rethinkdbMocked from '../src/mockdb.js';

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
