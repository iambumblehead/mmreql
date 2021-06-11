import test from 'ava';
import { Readable } from 'stream';
import { v4 as uuidv4 } from 'uuid';
import rethinkdbMocked from '../src/mockdb.mjs';

test( '`delete` should work`', async t => {
    const { r } = rethinkdbMocked([
        [ 'Rooms', [ { primaryKey: 'numeric_id' } ], {
            id: 'roomAId-1234',
            numeric_id: 755090,
            foo: 'baz'
        }, {
            id: 'roomBId-1234',
            numeric_id: 123321,
            foo: 'baz'
        } ]
    ]);

    const result1 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete()
        .run();

    t.is( result1.deleted, 2 );

    const result2 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete()
        .run();

    t.is( result2.deleted, 0 );
});

test( '`delete` should work -- soft durability`', async t => {
    const { r } = rethinkdbMocked([
        [ 'Rooms', [ { primaryKey: 'numeric_id' } ], {
            id: 'roomAId-1234',
            numeric_id: 755090,
            foo: 'baz'
        }, {
            id: 'roomBId-1234',
            numeric_id: 123321,
            foo: 'baz'
        } ]
    ]);

    const result1 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete()
        .run();
    t.truthy( result1 );

    const result2 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({})
        .run();

    t.truthy( result2 );
    const result3 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete({ durability: 'soft' })
        .run();

    t.is( result3.deleted, 1 );

    const result4 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({})
        .run();

    t.truthy( result4 );

    const result5 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete()
        .run();

    t.is( result5.deleted, 1 );
});

test( '`delete` should work -- hard durability`', async t => {
    const { r } = rethinkdbMocked([
        [ 'Rooms', [ { primaryKey: 'numeric_id' } ], {
            id: 'roomAId-1234',
            numeric_id: 755090,
            foo: 'baz'
        }, {
            id: 'roomBId-1234',
            numeric_id: 123321,
            foo: 'baz'
        } ]
    ]);

    const result1 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete()
        .run();

    t.truthy( result1 );

    const result2 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({})
        .run();

    t.truthy( result2 );

    const result3 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete({ durability: 'hard' })
        .run();

    t.is( result3.deleted, 1 );

    const result4 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({})
        .run();

    t.truthy( result4 );

    const result5 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete()
        .run();

    t.is( result5.deleted, 1 );
});

test( '`delete` should throw if non valid option', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    await t.throws( () => ( r
        .db( 'default' )
        .table( 'Rooms' )
        .delete({ nonValidKey: true })
        .run()
    ), {
        message: 'Unrecognized optional argument `nonValidKey`.'
    });
});

test( '`update` should work - point update`', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    let result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete()
        .run();

    t.truthy( result );

    result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({ id: 1 })
        .run();
    t.truthy( result );

    result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 1 )
        .update({ foo: 'bar' })
        .run();

    t.is( result.replaced, 1 );

    result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 1 )
        .run();

    t.deepEqual( result, { id: 1, foo: 'bar' });
});

test( '`update` should work - range update`', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    let result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .delete()
        .run();
    t.truthy( result );

    result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert([ { id: 1 }, { id: 2 } ])
        .run();

    t.truthy( result );

    result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .update({ foo: 'bar' })
        .run();

    t.is( result.replaced, 2 );

    result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 1 )
        .run();

    t.deepEqual( result, { id: 1, foo: 'bar' });

    result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 2 )
        .run();

    t.deepEqual( result, { id: 2, foo: 'bar' });
});

test( '`insert` should work - single insert`', async t => {
    const { r } = rethinkdbMocked([
        [ 'Rooms', {
            id: 'roomAId-1234',
            numeric_id: 755090,
            foo: 'baz'
        }, {
            id: 'roomBId-1234',
            numeric_id: 123321,
            foo: 'baz'
        } ]
    ]);

    let result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({})
        .run();
    t.is( result.inserted, 1 );

    result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert( Array( 100 ).fill({ }) )
        .run();

    t.is( result.inserted, 100 );
});

test( '`insert` should work - batch insert 1`', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert([ {}, {} ])
        .run();

    t.is( result.inserted, 2 );
});

test( '`insert` should work - batch insert 2`', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert( Array( 100 ).fill({}) )
        .run();

    t.is( result.inserted, 100 );
});

test( '`insert` should work - with returnChanges true`', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({}, { returnChanges: true })
        .run();

    t.is( result.inserted, 1 );
    t.truthy( result.changes[0].new_val );
    t.is( result.changes[0].old_val, null );
});

test( '`insert` should work - with returnChanges false`', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({}, { returnChanges: false })
        .run();
    t.is( result.inserted, 1 );
    t.is( result.changes, undefined );
});

test( '`insert` should work - with durability soft`', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({}, { durability: 'soft' })
        .run();
    t.is( result.inserted, 1 );
});

test( '`insert` should work - with durability hard`', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({}, { durability: 'hard' })
        .run();
    t.is( result.inserted, 1 );
});

test( '`insert` should work - testing conflict` (custom id)', async t => {
    const { r } = rethinkdbMocked();

    await r
        .db( 'default' )
        .tableCreate( 'Rooms', { primaryKey: 'cid' })
        .run();

    const result1 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({}, { conflict: 'update' })
        .run();
    t.is( result1.inserted, 1 );

    const [ pk ] = result1.generated_keys;

    const result2 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({ cid: pk, val: 1 }, { conflict: 'update' })
        .run();

    t.is( result2.replaced, 1 );

    const result3 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({ cid: pk, val: 2 }, { conflict: 'replace' })
        .run();

    t.is( result3.replaced, 1 );

    const result4 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({ cid: pk, val: 3 }, { conflict: 'error' })
        .run();
    t.is( result4.errors, 1 );
});

test( '`insert` should throw if options param `undefined` is passed', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);
    await t.throws( () => ( r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({ val: 3 }, undefined )
        .run()
    ), {
        message: 'Second argument of `insert` must be an object.'
    });
});

test( '`insert` should throw if no argument is given', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    await t.throws( () => ( r
        .db( 'default' )
        .table( 'Rooms' )
        .insert()
        .run()
    ), {
        message: '`insert` takes 1 argument, 0 provided.'
    });
});

test( '`insert` work with dates - 1', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);
    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({ name: 'Michel', age: 27, birthdate: new Date() })
        .run();

    t.is( result.inserted, 1 );
});

test( '`insert` work with dates - 2', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);
    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert([ {
            name: 'Michel',
            age: 27,
            birthdate: new Date()
        }, {
            name: 'Sophie',
            age: 23
        } ]).run();

    t.is( result.inserted, 2 );
});

test( '`insert` work with dates - 3', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);
    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({
            field: 'test',
            field2: { nested: 'test' },
            date: new Date()
        }).run();
    t.is( result.inserted, 1 );
});

test( '`insert` work with dates - 4', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);
    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({
            field: 'test',
            field2: { nested: 'test' },
            date: r.now()
        }).run();

    t.is( result.inserted, 1 );
});

test( '`insert` should throw if non valid option', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    await t.throws( () => ( r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({}, { nonValidKey: true })
        .run()
    ), {
        message: 'Unrecognized optional argument `nonValidKey`.'
    });
});

test( '`insert` with a conflict method', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);
    const result1 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({
            count: 7
        }).run();

    const [ savedId ] = result1.generated_keys;

    const result2 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .insert({
            id: savedId,
            count: 10
        }, {
            conflict: ( id, oldDoc, newDoc ) => newDoc.merge({
                count: newDoc( 'count' ).add( oldDoc( 'count' ) )
            })
        }).run();

    t.is( result2.replaced, 1 );

    const result3 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( savedId )
        .run();

    t.deepEqual( result3, {
        id: savedId,
        count: 17
    });
});

test( '`delete` should work', async t => {
    const { r } = rethinkdbMocked([
        [ 'Rooms', {
            id: 'roomAId-1234',
            numeric_id: 755090
        }, {
            id: 'roomBId-1234',
            numeric_id: 123321
        } ]
    ]);

    const result = await r
        .db( 'cmdb' )
        .table( 'Rooms' )
        .delete()
        .run();

    t.is( result.deleted, 2 );

    const result2 = await r
        .db( 'cmdb' )
        .table( 'Rooms' )
        .delete()
        .run();

    t.is( result2.deleted, 0 );
});

test( '`update` should work - point update', async t => {
    const { r } = rethinkdbMocked([
        [ 'Rooms', {
            id: 'roomAId-1234',
            numeric_id: 755090,
            foo: 'baz'
        }, {
            id: 'roomBId-1234',
            numeric_id: 123321,
            foo: 'baz'
        } ]
    ]);

    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 'roomAId-1234' )
        .update({ foo: 'bar' })
        .run();

    t.is( result.replaced, 1 );

    const result2 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 'roomAId-1234' )
        .run();

    t.deepEqual( result2, {
        id: 'roomAId-1234',
        numeric_id: 755090,
        foo: 'bar'
    });

    t.is( await r.db( 'default' ).table( 'Rooms' ).count().run(), 2 );
});

test( '`update` should work - soft durability', async t => {
    const { r } = rethinkdbMocked([
        [ 'Rooms', {
            id: 'roomAId-1234',
            numeric_id: 755090,
            foo: 'baz'
        }, {
            id: 'roomBId-1234',
            numeric_id: 123321,
            foo: 'baz'
        } ]
    ]);

    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 'roomAId-1234' )
        .update({ foo: 'bar' }, { durability: 'soft' })
        .run();

    t.is( result.replaced, 1 );

    const result2 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 'roomAId-1234' )
        .run();

    t.deepEqual( result2, {
        id: 'roomAId-1234',
        numeric_id: 755090,
        foo: 'bar'
    });

    t.is( await r.db( 'default' ).table( 'Rooms' ).count().run(), 2 );
});

test( '`update` should work - returnChanges true', async t => {
    const { r } = rethinkdbMocked([
        [ 'Rooms', {
            id: 'roomAId-1234',
            numeric_id: 755090,
            foo: 'baz'
        }, {
            id: 'roomBId-1234',
            numeric_id: 123321,
            foo: 'baz'
        } ]
    ]);

    const result = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 'roomAId-1234' )
        .update({ foo: 'bar' }, { returnChanges: true })
        .run();

    t.is( result.replaced, 1 );
    t.deepEqual( result.changes[0].new_val, {
        id: 'roomAId-1234',
        numeric_id: 755090,
        foo: 'bar'
    });
    t.deepEqual( result.changes[0].old_val, {
        id: 'roomAId-1234',
        numeric_id: 755090,
        foo: 'baz'
    });

    const result2 = await r
        .db( 'default' )
        .table( 'Rooms' )
        .get( 'roomAId-1234' )
        .run();

    t.deepEqual( result2, {
        id: 'roomAId-1234',
        numeric_id: 755090,
        foo: 'bar'
    });

    t.is( await r.db( 'default' ).table( 'Rooms' ).count().run(), 2 );
});
