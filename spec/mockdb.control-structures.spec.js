import test from 'ava';
import rethinkdbMocked from '../src/mockdb.js';

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
