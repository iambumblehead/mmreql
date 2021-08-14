import test from 'ava';
import rethinkdbMocked from '../src/mockdb.mjs';
import { v4 as uuidv4 } from 'uuid';

test( '`close` should work on feed', async t => {
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);
    
    const feed = await r.table( 'Presence' ).changes().run();
    await feed.close();

    t.pass();
});

test( '`close` should work on feed with events', async t => {
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);
    
    const feed = await r.table( 'Presence' ).changes().run();
    
    const promise = new Promise( ( resolve, reject ) => {
        feed.on( 'error', reject );
        feed.on( 'data', () => null ).on( 'end', resolve );
    });

    await feed.close();
    await promise;

    t.pass();
});

test( '`on` should work on feed', async t => {
    const smallNumDocs = 2;
    const { r } = rethinkdbMocked([
        [ 'Presence', [ { primaryKey: 'user_id' } ], {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        }, {
            user_id: 'userId-5678',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);

    const feed = await r.table( 'Presence' ).changes().run();

    const promise = new Promise( ( resolve, reject ) => {
        let i = 0;

        feed.on( 'data', () => {
            i = i + 1;
            if ( i === smallNumDocs ) {
                // eslint-disable-next-line promise/prefer-await-to-then
                feed.close().then( resolve ).catch( reject );
            }
        });
        feed.on( 'error', reject );
    });

    await r.table( 'Presence' ).update({
        state: 'ONLINE',
        time_last_seen: new Date()
    }).run();

    await promise;
    t.pass();
});

test( '`on` should work on cursor - a `end` event shoul be eventually emitted on a cursor', async t => {
    const { r } = rethinkdbMocked([
        [ 'Presence',  {
            id: 'userId-1234',
            foo: new Date( Date.now() - 1000 )
        } ]
    ]);

    const cursor = await r
        .table( 'Presence' )
        .getCursor();

    const promise = new Promise( ( resolve, reject ) => {
        cursor.on( 'data', () => null ).on( 'end', resolve );
        cursor.on( 'error', reject );
    });

    await r.table( 'Presence' ).update({ foo: r.now() }).run();
    await promise;
    t.pass();
});

test( '`next` should work on an atom feed', async t => {
    const idValue = uuidv4();

    const { r } = rethinkdbMocked([
        [ 'LatestNews', {
            id: 'latestId-1234',
            message: 'news'
        } ]
    ]);

    const feed = await r
        .table( 'LatestNews' )
        .get( idValue )
        .changes({ includeInitial: true })
        .run();

    /* eslint-disable promise/prefer-await-to-then */
    const promise = new Promise( ( resolve, reject ) => {
        feed.next()
            .then( res => t.deepEqual( res, { new_val: null }) )
            .then( () => feed.next() )
            .then( res => t.deepEqual( res, { new_val: { id: idValue }, old_val: null }) )
            .then( resolve )
            .catch( reject );
    });
    /* eslint-enable promise/prefer-await-to-then */

    await r.table( 'LatestNews' ).insert({ id: idValue }).run();
    await promise;
    await feed.close();
    t.pass();
});

test( '`next` should work -- testing common pattern', async t => {
    const smallNumDocs = 2;
    const { r } = rethinkdbMocked([
        [ 'Presence', [ { primaryKey: 'user_id' } ], {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        }, {
            user_id: 'userId-5678',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);

    const cursor = await r
        .table( 'Presence' )
        .getCursor();

    let i = 0;

    await t.throwsAsync( async () => {
        while ( true ) {
            const result = await cursor.next();
            t.truthy( result );
            i = i + 1;
        }
    }, {
        message: 'No more rows in the cursor.'
    });

    t.is( smallNumDocs, i );
});

test( '`cursor.close` should return a promise', async t => {
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);
    
    const cursor1 = await r.table( 'Presence' ).getCursor();

    await cursor1.close();

    t.pass();
});

test( '`cursor.close` should still return a promise if the cursor was closed', async t => {
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);

    const state = {};
    const cursor = await r.table( 'Presence' ).changes().run();

    await cursor.close();
    const result = cursor.close();
    try {
        // eslint-disable-next-line
        result.then( () => undefined ); // Promise's contract is to have a `then` method
    } catch ( e ) {
        t.fail( 'failed' );
    }

    t.pass();
});

test( '`next` should return a document', async t => {
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);

    const cursor = await r.table( 'Presence' ).getCursor();
    const result = await cursor.next();

    t.is( result.user_id, 'userId-1234' );
});

test( '`each` should work', async t => {
    const numDocs = 2;
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        }, {
            user_id: 'userId-5678',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);

    const cursor = await r.table( 'Presence' ).getCursor();

    await new Promise( ( resolve, reject ) => {
        let count = 0;
        cursor.each( err => {
            if ( err ) {
                reject( err );
            }
            count = count + 1;
            if ( count === numDocs ) {
                resolve();
            }
        });
    });

    t.pass();
});

test( '`each` should work - onFinish - reach end', async t => {
    const numDocs = 2;
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        }, {
            user_id: 'userId-5678',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);

    const cursor = await r.table( 'Presence' ).getCursor();

    await new Promise( ( resolve, reject ) => {
        let count = 0;
        cursor.each(
            err => {
                if ( err ) {
                    reject( err );
                }
                count = count + 1;
            },
            () => {
                if ( count !== numDocs ) {
                    reject(
                        new Error(
                            `expected count (${count}) to equal numDocs (${numDocs})`
                        )
                    );
                }
                t.is( count, numDocs );
                resolve();
            }
        );
    });

    t.pass();
});

test( '`next` should error when hitting an error -- not on the first batch', async t => {
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            user_id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);
    
    const connection = await r.connect({
        host: 'localhost',
        port: 8080,
        user: 'user',
        password: 'pass'
    });

    const cursor = await r
        .table( 'Presence' )
        .orderBy({ index: 'id' })
        .map( row => row( 'val' ).add( 1 ) )
        .getCursor( connection, { maxBatchRows: 10 });

    let i = 0;

    await t.throwsAsync( async () => {
        while ( true ) {
            const result = await cursor.next();

            i = i + 1;
        }
    }, {
        message: 'No attribute `val` in object'
    });

    await connection.close();
    t.false( connection.open );
});

test( '`changes` with `includeTypes` should work', async t => {
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        }, {
            id: 'userId-5678',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);

    const feed = await r
        .table( 'Presence' )
        .orderBy({ index: 'id' })
        .limit( 2 )
        .changes({
            includeTypes: true,
            includeInitial: true
        }).run();

    let counter = 0;

    // eslint-disable-next-line promise/prefer-await-to-callbacks
    const promise = new Promise( ( resolve, reject ) => {
        // eslint-disable-next-line promise/prefer-await-to-callbacks
        feed.each( ( error, change ) => {
            if ( error ) {
                reject( error );
            }
            t.is( typeof change.type, 'string' );

            if ( counter > 0 ) {
                // eslint-disable-next-line
                feed.close().then( resolve ).catch( reject );
            }
            counter = counter + 1;
        });
    });

    await r.table( 'Presence' ).insert({ id: 0 }).run();

    await promise;

    t.pass();
});

test( '`asyncIterator` should work', async t => {
    const { r } = rethinkdbMocked([
        [ 'Presence', {
            id: 'userId-1234',
            state: 'OFFLINE',
            time_last_seen: new Date()
        }, {
            id: 'userId-5678',
            state: 'OFFLINE',
            time_last_seen: new Date()
        } ]
    ]);

    const feed = await r.table( 'Presence' ).changes().run();
    const value = 1;

    const promise = ( async () => {
        let res;
        for await ( const row of feed ) {
            res = row;
            feed.close();
        }
        return res;
    })();

    await r.table( 'Presence' ).insert({ foo: value }).run();
    const result = await promise;
    t.is( result.new_val.foo, value );
});
