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
