import test from 'ava';
import { Readable } from 'stream';
import { v4 as uuidv4 } from 'uuid';
import rethinkdbMocked from '../src/mockdb.js';

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
