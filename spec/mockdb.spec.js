import test from 'ava';
import rethinkdbMocked from '../src/mockdb.js';
import mockedReql from '../src/mockdbReql.js';
import { mockdbResErrorDuplicatePrimaryKey } from '../src/mockdbRes.js';

// use when order not important and sorting helps verify a list
const compare = ( a, b, prop ) => {
    if ( a[prop] < b[prop]) return -1;
    if ( a[prop] > b[prop]) return 1;
    return 0;
};

test( 'supports add()', async t => {
    const r = mockedReql();
    const start = Date.now();

    t.is( await r.expr( 2 ).add( 2 ).run(), 4 );
    t.is( await r.expr( 'foo' ).add( 'bar', 'baz' ).run(), 'foobarbaz' );
    t.is( await r.add( r.args([ 'bar', 'baz' ]) ).run(), 'barbaz' );
    t.true( ( new Date( await r.now().add( 365 ).run() ) ).getTime() <= start + 365 );
});

test( 'supports r.args()', async t => {
    const r = mockedReql();

    t.is( await r.add( r.args([ 'bar', 'baz' ]) ).run(), 'barbaz' );
});

test( 'provides a faux connectPool function', async t => {
    const { r } = rethinkdbMocked([
        [ 'table', { id: 'id-document-1234' } ]
    ]);

    await r.connectPool({
        db: 'cmdb',
        host: 'host',
        port: 8000,
        password: ''
    });

    t.pass();
});

test( 'provides primaryIndex (id) lookups', async t => {
    const { r } = rethinkdbMocked([
        [ 'AppUserDevices', {
            id: 'id-document-1234',
            device_id: 'device-1234',
            app_user_id: 'appuser-1234',
            application_id: 'application-1234'
        } ]
    ]);

    const AppUserDevice = await r
        .table( 'AppUserDevices' )
        .getAll( 'id-document-1234' )
        .filter({ device_id: 'device-1234' })
        .limit( 1 )
        .run();

    t.is( AppUserDevice[0].id, 'id-document-1234' );
});

test( 'provides secondary index methods and lookups', async t => {
    const { r } = rethinkdbMocked([
        [ 'AppUserDevices', {
            id: 'id-document-1234',
            device_id: 'device-1234',
            app_user_id: 'appuser-1234',
            application_id: 'application-1234'
        } ]
    ]);

    const indexList = await r.table( 'AppUserDevices' ).indexList().run();

    if ( !indexList.includes( 'app_user_id' ) ) {
        await r.table( 'AppUserDevices' ).indexCreate( 'app_user_id' ).run();
        await r.table( 'AppUserDevices' ).indexWait( 'app_user_id' ).run();
    }

    const AppUserDevice = await r
        .table( 'AppUserDevices' )
        .getAll( 'appuser-1234', { index: 'app_user_id' })
        .filter({ device_id: 'device-1234' })
        .limit( 1 )
        .run();

    t.is( AppUserDevice[0].id, 'id-document-1234' );
});

test( 'provides secondary index methods and lookups, numeric', async t => {
    const { r } = rethinkdbMocked([
        [ 'Rooms', {
            id: 'roomAId-1234',
            numeric_id: 755090
        }, {
            id: 'roomBId-1234',
            numeric_id: 123321
        } ]
    ]);

    await r.table( 'Rooms' ).indexCreate( 'numeric_id' ).run();
    await r.table( 'Rooms' ).indexWait( 'numeric_id' ).run();
    const room = await r
        .table( 'Rooms' )
        .getAll( 755090, { index: 'numeric_id' })
        .nth( 0 )
        .run();

    t.is( room.id, 'roomAId-1234' );
});

test( 'provides compound index methods and lookups', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 5848,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-5678',
            numeric_id: 9457,
            name_screenname: 'screenname'
        } ]
    ]);

    const indexList = await r.table( 'UserSocial' ).indexList().run();

    if ( !indexList.includes( 'app_user_id' ) ) {
        await r.table( 'UserSocial' ).indexCreate( 'screenname_numeric_cid', [
            r.row( 'name_screenname' ),
            r.row( 'numeric_id' )
        ]).run();
        await r.table( 'UserSocial' ).indexWait( 'screenname_numeric_cid' ).run();
    }

    const userSocialDocs = await r
        .table( 'UserSocial' )
        .getAll([ 'screenname', 5848 ], { index: 'screenname_numeric_cid' })
        .run();

    t.is( userSocialDocs.length, 1 );
});

test( 'returns an app document', async t => {
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

    t.is( appDoc.id, 'appid-1234' );

    const usersDoc = await r.table( 'Users' ).run();

    t.is( usersDoc.length, 2 );
});

test( 'supports .get()', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 5848,
            name_screenname: 'screenname'
        } ]
    ]);

    const res = await r.table( 'UserSocial' )
        .get( 'userSocialId-1234' )
        .default({ defaultValue: true })
        .run();

    t.deepEqual( res, {
        id: 'userSocialId-1234',
        numeric_id: 5848,
        name_screenname: 'screenname'
    });
});

test( 'supports .get(), returns null if document not found', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 5848,
            name_screenname: 'screenname'
        } ]
    ]);

    const resNoDoc = await r.table( 'UserSocial' )
        .get( 'userSocialId-7575' )
        .run();

    t.is( resNoDoc, null );
});

test( 'supports .count()', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 3333,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-5678',
            numeric_id: 2222,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-9012',
            numeric_id: 5555,
            name_screenname: 'screenname'
        } ]
    ]);

    const res = await r
        .table( 'UserSocial' )
        .filter( hero => hero( 'numeric_id' ).lt( 4000 ) )
        .count()
        .run();

    t.is( res, 2 );
});

test( 'supports .max()', async t => {
    const { r } = rethinkdbMocked([
        [ 'games',
            { id: 2, player: 'Bob', points: 15, type: 'ranked' },
            { id: 5, player: 'Alice', points: 7, type: 'free' },
            { id: 11, player: 'Bob', points: 10, type: 'free' },
            { id: 12, player: 'Alice', points: 2, type: 'free' } ]
    ]);

    const res = await r.table( 'games' ).max( 'points' ).run();

    t.deepEqual( res, {
        id: 2, player: 'Bob', points: 15, type: 'ranked'
    });
});

test( 'supports .min()', async t => {
    const { r } = rethinkdbMocked([
        [ 'games',
            { id: 2, player: 'Bob', points: 15, type: 'ranked' },
            { id: 5, player: 'Alice', points: 7, type: 'free' },
            { id: 11, player: 'Bob', points: 10, type: 'free' },
            { id: 12, player: 'Alice', points: 2, type: 'free' } ]
    ]);

    const res = await r.table( 'games' ).min( 'points' ).run();

    t.deepEqual( res, {
        id: 12, player: 'Alice', points: 2, type: 'free'
    });
});

test( 'supports .pluck() from document', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 3333,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-5678',
            numeric_id: 2222,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-9012',
            numeric_id: 5555,
            name_screenname: 'screenname'
        } ]
    ]);

    const res = await r
        .table( 'UserSocial' )
        .get( 'userSocialId-1234' )
        .pluck( 'numeric_id', 'id' )
        .run();

    t.deepEqual( res, {
        id: 'userSocialId-1234',
        numeric_id: 3333
    });
});

test( 'supports .pluck() (from list)', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 3333,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-5678',
            numeric_id: 2222,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-9012',
            numeric_id: 5555,
            name_screenname: 'screenname'
        } ]
    ]);

    const res = await r
        .table( 'UserSocial' )
        .pluck( 'numeric_id', 'id' )
        .run();

    t.deepEqual( res, [ {
        id: 'userSocialId-1234',
        numeric_id: 3333
    }, {
        id: 'userSocialId-5678',
        numeric_id: 2222
    }, {
        id: 'userSocialId-9012',
        numeric_id: 5555
    } ]);
});

test( 'supports .slice(1,2)', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 3333,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-5678',
            numeric_id: 2222,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-9012',
            numeric_id: 5555,
            name_screenname: 'screenname'
        } ]
    ]);

    const res = await r
        .table( 'UserSocial' )
        .slice( 1, 2 )
        .run();

    t.deepEqual( res, [ {
        id: 'userSocialId-5678',
        numeric_id: 2222,
        name_screenname: 'screenname'
    } ]);
});

test( 'supports .skip(1)', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 3333,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-5678',
            numeric_id: 2222,
            name_screenname: 'screenname'
        }, {
            id: 'userSocialId-9012',
            numeric_id: 5555,
            name_screenname: 'screenname'
        } ]
    ]);

    const res = await r
        .table( 'UserSocial' )
        .skip( 1 )
        .run();

    t.deepEqual( res, [ {
        id: 'userSocialId-5678',
        numeric_id: 2222,
        name_screenname: 'screenname'
    }, {
        id: 'userSocialId-9012',
        numeric_id: 5555,
        name_screenname: 'screenname'
    } ]);
});

test( 'supports .group() (basic)', async t => {
    const { r } = rethinkdbMocked([
        [ 'games',
            { id: 2, player: 'Bob', points: 15, type: 'ranked' },
            { id: 5, player: 'Alice', points: 7, type: 'free' },
            { id: 11, player: 'Bob', points: 10, type: 'free' },
            { id: 12, player: 'Alice', points: 2, type: 'free' } ]
    ]);

    const res = await r.table( 'games' ).group( 'player' ).run();

    t.deepEqual( res.sort( ( a, b ) => compare( a, b, 'group' ) ), [ {
        group: 'Alice',
        reduction: [
            { id: 5, player: 'Alice', points: 7, type: 'free' },
            { id: 12, player: 'Alice', points: 2, type: 'free' }
        ]
    }, {
        group: 'Bob',
        reduction: [
            { id: 2, player: 'Bob', points: 15, type: 'ranked' },
            { id: 11, player: 'Bob', points: 10, type: 'free' }
        ]
    } ]);
});

test( 'supports .group().max() query', async t => {
    const { r } = rethinkdbMocked([
        [ 'games',
            { id: 2, player: 'Bob', points: 15, type: 'ranked' },
            { id: 5, player: 'Alice', points: 7, type: 'free' },
            { id: 11, player: 'Bob', points: 10, type: 'free' },
            { id: 12, player: 'Alice', points: 2, type: 'free' } ]
    ]);

    const res = await r.table( 'games' ).group( 'player' ).max( 'points' ).run();

    t.deepEqual( res.sort( ( a, b ) => compare( a, b, 'group' ) ), [ {
        group: 'Alice',
        reduction: { id: 5, player: 'Alice', points: 7, type: 'free' }
    }, {
        group: 'Bob',
        reduction: { id: 2, player: 'Bob', points: 15, type: 'ranked' }
    } ]);
});

test( 'supports .group().min() query', async t => {
    const { r } = rethinkdbMocked([
        [ 'games',
            { id: 2, player: 'Bob', points: 15, type: 'ranked' },
            { id: 5, player: 'Alice', points: 7, type: 'free' },
            { id: 11, player: 'Bob', points: 10, type: 'free' },
            { id: 12, player: 'Alice', points: 2, type: 'free' } ]
    ]);

    const res = await r.table( 'games' ).group( 'player' ).min( 'points' ).run();

    t.deepEqual( res.sort( ( a, b ) => compare( a, b, 'group' ) ), [ {
        group: 'Alice',
        reduction: { id: 12, player: 'Alice', points: 2, type: 'free' }
    }, {
        group: 'Bob',
        reduction: { id: 11, player: 'Bob', points: 10, type: 'free' }
    } ]);
});

test( 'supports .ungroup() query', async t => {
    const { r } = rethinkdbMocked([
        [ 'games',
            { id: 2, player: 'Bob', points: 15, type: 'ranked' },
            { id: 5, player: 'Alice', points: 7, type: 'free' },
            { id: 11, player: 'Bob', points: 10, type: 'free' },
            { id: 12, player: 'Alice', points: 2, type: 'free' } ]
    ]);

    const res = await r
        .table( 'games' )
        .group( 'player' )
        .ungroup()
        .run();

    t.deepEqual( res.sort( ( a, b ) => compare( a, b, 'group' ) ), [ {
        group: 'Alice',
        reduction: [
            { id: 5, player: 'Alice', points: 7, type: 'free' },
            { id: 12, player: 'Alice', points: 2, type: 'free' }
        ]
    }, {
        group: 'Bob',
        reduction: [
            { id: 2, player: 'Bob', points: 15, type: 'ranked' },
            { id: 11, player: 'Bob', points: 10, type: 'free' }
        ]
    } ]);
});

// eslint-disable-next-line ava/no-skip-test
test.skip( 'supports .ungroup() complex query', async t => {
    const { r } = rethinkdbMocked([
        [ 'games',
            { id: 2, player: 'Bob', points: 15, type: 'ranked' },
            { id: 5, player: 'Alice', points: 7, type: 'free' },
            { id: 11, player: 'Bob', points: 10, type: 'free' },
            { id: 12, player: 'Alice', points: 2, type: 'free' } ]
    ]);

    const res = await r
        .table( 'games' )
        .group( 'player' ).max( 'points' )( 'points' )
        .ungroup().orderBy( r.desc( 'reduction' ) )
        .run();

    t.deepEqual( res, [ {
        group: 'Bob',
        reduction: 15
    }, {
        group: 'Alice',
        reduction: 7
    } ]);
});

test( 'supports .eqJoin()', async t => {
    const { r } = rethinkdbMocked([
        [ 'players',
            { id: 1, player: 'George', gameId: 1 },
            { id: 2, player: 'Agatha', gameId: 3 },
            { id: 3, player: 'Fred', gameId: 2 },
            { id: 4, player: 'Marie', gameId: 2 },
            { id: 5, player: 'Earnest', gameId: 1 },
            { id: 6, player: 'Beth', gameId: 3 } ],
        [ 'games',
            { id: 1, field: 'Little Delving' },
            { id: 2, field: 'Rushock Bog' },
            { id: 3, field: 'Bucklebury' } ]
    ]);

    const res = await r
        .table( 'players' )
        .eqJoin( 'gameId', r.table( 'games' ) )
        .run();

    t.deepEqual( res, [ {
        left: { id: 1, player: 'George', gameId: 1 },
        right: { id: 1, field: 'Little Delving' }
    }, {
        left: { id: 2, player: 'Agatha', gameId: 3 },
        right: { id: 3, field: 'Bucklebury' }
    }, {
        left: { id: 3, player: 'Fred', gameId: 2 },
        right: { id: 2, field: 'Rushock Bog' }
    }, {
        left: { id: 4, player: 'Marie', gameId: 2 },
        right: { id: 2, field: 'Rushock Bog' }
    }, {
        left: { id: 5, player: 'Earnest', gameId: 1 },
        right: { id: 1, field: 'Little Delving' }
    }, {
        left: { id: 6, player: 'Beth', gameId: 3 },
        right: { id: 3, field: 'Bucklebury' }
    } ]);
});

test( 'supports .innerJoin', async t => {
    const { r } = rethinkdbMocked([
        [ 'streetfighter',
            { id: 1, name: 'ryu', strength: 6 },
            { id: 2, name: 'balrog', strength: 5 },
            { id: 3, name: 'chun-li', strength: 7 } ],
        [ 'pokemon',
            { id: 1, name: 'squirtle', strength: 3 },
            { id: 2, name: 'charmander', strength: 8 },
            { id: 3, name: 'fiery', strength: 5 } ]
    ]);

    const res = await r
        .table( 'streetfighter' )
        .innerJoin( r.table( 'pokemon' ), ( sfRow, pokeRow ) => (
            sfRow( 'strength' ).lt( pokeRow( 'strength' ) )
        ) ).run();

    t.deepEqual( res, [ {
        left: { id: 1, name: 'ryu', strength: 6 },
        right: { id: 2, name: 'charmander', strength: 8 }
    }, {
        left: { id: 2, name: 'balrog', strength: 5 },
        right: { id: 2, name: 'charmander', strength: 8 }
    }, {
        left: { id: 3, name: 'chun-li', strength: 7 },
        right: { id: 2, name: 'charmander', strength: 8 }
    } ]);
});

test( 'supports .merge()', async t => {
    const { r } = rethinkdbMocked([
        [ 'marvel',
            { id: 'wolverine', name: 'wolverine' },
            { id: 'thor', name: 'thor' },
            { id: 'xavier', name: 'xavier' } ],
        [ 'equipment',
            { id: 'hammer', type: 'air' },
            { id: 'sickle', type: 'ground' },
            { id: 'pimento_sandwich', type: 'dream' } ]
    ]);

    const res = await r.table( 'marvel' ).get( 'thor' ).merge(
        await r.table( 'equipment' ).get( 'hammer' ),
        await r.table( 'equipment' ).get( 'pimento_sandwich' )
    ).run();

    t.deepEqual( res, { id: 'pimento_sandwich', name: 'thor', type: 'dream' });
});

test( 'supports .orderBy()', async t => {
    const { r } = rethinkdbMocked([
        [ 'streetfighter',
            { id: 1, name: 'ryu', strength: 6 },
            { id: 2, name: 'balrog', strength: 5 },
            { id: 3, name: 'chun-li', strength: 7 } ]
    ]);
    await r.table( 'streetfighter' ).indexCreate( 'strength' ).run();
    await r.table( 'streetfighter' ).indexWait( 'strength' ).run();

    const res = await r
        .table( 'streetfighter' )
        .orderBy({ index: 'strength' })
        .run();

    t.deepEqual( res, [
        { id: 2, name: 'balrog', strength: 5 },
        { id: 1, name: 'ryu', strength: 6 },
        { id: 3, name: 'chun-li', strength: 7 }
    ]);
});

test( 'supports .orderBy(), desc date', async t => {
    const now = Date.now();
    const earliest = new Date( now - 80000 );
    const latest = new Date( now + 50 );

    const { r } = rethinkdbMocked([
        [ 'streetfighter',
            { id: 1, name: 'ryu', date: now },
            { id: 2, name: 'balrog', date: earliest },
            { id: 3, name: 'chun-li', date: latest } ]
    ]);
    await r.table( 'streetfighter' ).indexCreate( 'date' ).run();
    await r.table( 'streetfighter' ).indexWait( 'date' ).run();

    const res = await r
        .table( 'streetfighter' )
        .orderBy({ index: r.desc( 'date' ) })
        .run();

    t.deepEqual( res, [
        { id: 3, name: 'chun-li', date: latest },
        { id: 1, name: 'ryu', date: now },
        { id: 2, name: 'balrog', date: earliest }
    ]);
});

test( 'supports .orderBy(), asc date', async t => {
    const now = Date.now();
    const earliest = new Date( now - 80000 );
    const latest = new Date( now + 50 );

    const { r } = rethinkdbMocked([
        [ 'streetfighter',
            { id: 1, name: 'ryu', date: now },
            { id: 2, name: 'balrog', date: earliest },
            { id: 3, name: 'chun-li', date: latest } ]
    ]);
    await r.table( 'streetfighter' ).indexCreate( 'date' ).run();
    await r.table( 'streetfighter' ).indexWait( 'date' ).run();

    const res = await r
        .table( 'streetfighter' )
        .orderBy({ index: r.asc( 'date' ) })
        .run();

    t.deepEqual( res, [
        { id: 2, name: 'balrog', date: earliest },
        { id: 1, name: 'ryu', date: now },
        { id: 3, name: 'chun-li', date: latest }
    ]);
});

test( 'supports .orderBy(), dynamic function', async t => {
    const { r } = rethinkdbMocked([
        [ 'streetfighter',
            { id: 1, name: 'ryu', upvotes: 6, downvotes: 30 },
            { id: 2, name: 'balrog', upvotes: 5, downvotes: 0 },
            { id: 3, name: 'chun-li', upvotes: 7, downvotes: 3 } ]
    ]);

    const res = await r
        .table( 'streetfighter' )
        // sub document query-chains not yet supported
        // .orderBy( doc => doc( 'upvotes' ).sub( doc( 'downvotes' ) ) )
        .orderBy( doc => doc( 'upvotes' ) )
        .run();

    t.deepEqual( res, [
        { id: 2, name: 'balrog', upvotes: 5, downvotes: 0 },
        { id: 1, name: 'ryu', upvotes: 6, downvotes: 30 },
        { id: 3, name: 'chun-li', upvotes: 7, downvotes: 3 }
    ]);
});

test( 'supports .gt(), applied to row', async t => {
    const { r } = rethinkdbMocked([
        [ 'player', {
            id: 'playerId-1234',
            score: 10
        }, {
            id: 'playerId-5678',
            score: 6
        } ]
    ]);

    const res = await r
        .table( 'player' )
        .filter( row => row( 'score' ).gt( 8 ) )
        .run();

    t.deepEqual( res, [ {
        id: 'playerId-1234',
        score: 10
    } ]);
});

test( 'supports .lt(), applied to row', async t => {
    const { r } = rethinkdbMocked([
        [ 'player', {
            id: 'playerId-1234',
            score: 10
        }, {
            id: 'playerId-5678',
            score: 6
        } ]
    ]);

    const res = await r
        .table( 'player' )
        .filter( row => row( 'score' ).lt( 8 ) )
        .run();

    t.deepEqual( res, [ {
        id: 'playerId-5678',
        score: 6
    } ]);
});

test( 'supports .eq(), applied to row', async t => {
    const { r } = rethinkdbMocked([
        [ 'player', {
            id: 'playerId-1234',
            score: 10
        }, {
            id: 'playerId-5678',
            score: 6
        } ]
    ]);

    const res = await r
        .table( 'player' )
        .filter( row => row( 'score' ).eq( 6 ) )
        .run();

    t.deepEqual( res, [ {
        id: 'playerId-5678',
        score: 6
    } ]);
});

test( 'supports .ne(), applied to row', async t => {
    const { r } = rethinkdbMocked([
        [ 'player', {
            id: 'playerId-1234',
            score: 10
        }, {
            id: 'playerId-5678',
            score: 6
        } ]
    ]);

    const res = await r
        .table( 'player' )
        .filter( row => row( 'score' ).ne( 6 ) )
        .run();

    // r function not yet supported
    // t.is( await r( true ).not().run(), false );
    t.deepEqual( res, [ {
        id: 'playerId-1234',
        score: 10
    } ]);
});

test( 'supports .not(), applied to row', async t => {
    const { r } = rethinkdbMocked([
        [ 'player', {
            id: 'playerId-1234',
            score: 10
        }, {
            id: 'playerId-5678',
            score: 6
        } ]
    ]);

    const res = await r
        .table( 'player' )
        .filter( row => row( 'score' ).eq( 6 ).not() )
        .run();

    t.deepEqual( res, [ {
        id: 'playerId-1234',
        score: 10
    } ]);
});

test( 'supports .nth()', async t => {
    const { r } = rethinkdbMocked([
        [ 'marvel', {
            id: 'IronMan',
            equipment: []
        } ]
    ]);

    const ironman = await r.table( 'marvel' ).nth( 0 ).run();

    t.is( ironman.id, 'IronMan' );
});

test( 'supports .nth(), non-trivial guery', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 5848,
            name_screenname: 'screenname'
        } ]
    ]);

    await r.table( 'UserSocial' ).indexCreate( 'screenname_numeric_cid', [
        r.row( 'name_screenname' ),
        r.row( 'numeric_id' )
    ]).run();
    await r.table( 'UserSocial' ).indexWait( 'screenname_numeric_cid' ).run();

    await t.throwsAsync( () => (
        r.table( 'UserSocial' )
            .getAll([ 'notfound', 7575 ], { index: 'screenname_numeric_cid' })
            .limit( 1 )
            .nth( 0 )
            .run()
    ), {
        message: 'ReqlNonExistanceError: Index out of bounds: 0'
    });
});

test( 'supports .default()', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 5848,
            name_screenname: 'screenname'
        } ]
    ]);

    const res = await r.table( 'UserSocial' )
        .get( 'userSocialId-7575' )
        .default({ defaultValue: true })
        .run();

    t.true( res.defaultValue );
});

test( 'supports .default(), non-trivial guery', async t => {
    const { r } = rethinkdbMocked([
        [ 'UserSocial', {
            id: 'userSocialId-1234',
            numeric_id: 5848,
            name_screenname: 'screenname'
        } ]
    ]);

    await r.table( 'UserSocial' ).indexCreate( 'screenname_numeric_cid', [
        r.row( 'name_screenname' ),
        r.row( 'numeric_id' )
    ]).run();
    await r.table( 'UserSocial' ).indexWait( 'screenname_numeric_cid' ).run();

    const result = (
        await r.table( 'UserSocial' )
            .getAll([ 'notfound', 7575 ], { index: 'screenname_numeric_cid' })
            .limit( 1 )
            .nth( 0 )
            .default( null )
            .run()
    );

    t.is( result, null );
});

test( 'supports .epochTime()', async t => {
    const { r } = rethinkdbMocked([
        [ 'user', {
            id: 'John',
            birthdate: new Date()
        } ]
    ]);

    await r.table( 'user' ).insert({
        id: 'Jane',
        birthdate: r.epochTime( 531360000 )
    }).run();

    const janeDoc = await r.table( 'user' ).get( 'Jane' ).run();

    t.is( janeDoc.birthdate.getTime(), 531360000000 );
});

test( 'supports .update()', async t => {
    const { r } = rethinkdbMocked([
        [ 'user', {
            id: 'John',
            birthdate: new Date()
        } ]
    ]);

    const result = await r.table( 'user' ).get( 'John' ).update({
        birthdate: r.epochTime( 531360000 )
    }).run();

    t.deepEqual( result, {
        unchanged: 0,
        skipped: 0,
        replaced: 1,
        inserted: 0,
        errors: 0,
        deleted: 0
    });
});

test( 'supports .during()', async t => {
    const now = Date.now();
    const { r } = rethinkdbMocked([
        [ 'RoomCodes', {
            id: 'expired',
            time_expire: new Date( now - ( 1000 * 60 * 60 * 24 ) )
        } ]
    ]);

    const expiredDocs = await r
        .table( 'RoomCodes' )
        .filter(
            r.row( 'time_expire' ).during(
                r.epochTime( 0 ),
                r.epochTime( now / 1000 )
            ) )
        .run();

    t.is( expiredDocs[0].id, 'expired' );
});

test( 'supports .during(), correctly handles empty results', async t => {
    const now = Date.now();
    const timeExpire = new Date( now - ( 1000 * 60 * 60 * 24 ) );
    const { r } = rethinkdbMocked([
        [ 'RoomCodes', {
            id: 'expired',
            time_expire: timeExpire
        } ]
    ]);

    // results in runtime error :(
    const expiredDocs = await r
        .table( 'RoomCodes' )
        .filter(
            r.row( 'time_expire' ).during(
                r.epochTime( 0 ),
                r.epochTime( now / 1000 )
            ) )
        .run();

    t.deepEqual( expiredDocs, [ {
        id: 'expired',
        time_expire: timeExpire
    } ]);
});

test( 'supports .getField()', async t => {
    const { r } = rethinkdbMocked([
        [ 'marvel', {
            id: 'IronMan',
            equipment: [ 'boots' ]
        } ]
    ]);

    const ironManEquipment = await r
        .table( 'marvel' )
        .get( 'IronMan' )
        .getField( 'equipment' )
        .run();

    t.is( ironManEquipment[0], 'boots' );
});

test( 'supports brackets lookup ()', async t => {
    const { r } = rethinkdbMocked([
        [ 'marvel', {
            id: 'IronMan',
            equipment: [ 'boots' ]
        } ]
    ]);

    const res = await r
        .table( 'marvel' )
        .get( 'IronMan' )( 'equipment' )
        .upcase().run();

    t.is( res, 'BOOTS' );
});

/*
test( 'supports .append()', async t => {
    const { r } = rethinkdbMocked([
        [ 'marvel', {
            id: 'IronMan',
            equipment: []
        } ]
    ]);

    const ironmand = await r.table( 'marvel' ).get( 'IronMan' )( 'equipment' );
    // const ironmand = await r.table( 'marvel' ).get( 'IronMan' ).toString();
    // await r.table( 'marvel' ).get( 'IronMan' )( 'equipment' ).append( 'newBoots' ).run();
    // await r.table( 'marvel' ).get( 'IronMan' )( 'equipment' ).append( 'newBoots' ).run();
    // const whatis = await r.table( 'marvel' ).get( 'IronMan' )('equipment');
    // const ironman = await r.table( 'marvel' ).get( 'IronMan' ).run();
    // t.deepEqual( ironman.equipment, [ 'newBoots' ]);
    t.true( true );
});
*/
test( 'supports .insert(, {})', async t => {
    const { r } = rethinkdbMocked([
        [ 'posts', {
            id: 'post-1234',
            title: 'post title',
            content: 'post content'
        } ]
    ]);

    const insertRes = await r.table( 'posts' ).insert({
        title: 'Lorem ipsum',
        content: 'Dolor sit amet'
    }).run();

    const [ generatedKey ] = insertRes.generated_keys;

    t.deepEqual( insertRes, {
        deleted: 0,
        errors: 0,
        generated_keys: [ generatedKey ],
        inserted: 1,
        replaced: 0,
        skipped: 0,
        unchanged: 0
    });
});

test( 'supports .insert(, { returnChanges: true })', async t => {
    const { r } = rethinkdbMocked([
        [ 'posts', {
            id: 'post-1234',
            title: 'post title',
            content: 'post content'
        } ]
    ]);

    const insertRes = await r.table( 'posts' ).insert({
        title: 'Lorem ipsum',
        content: 'Dolor sit amet'
    }, {
        returnChanges: true
    }).run();

    const [ generatedKey ] = insertRes.generated_keys;

    t.deepEqual( insertRes, {
        deleted: 0,
        errors: 0,
        generated_keys: [ generatedKey ],
        inserted: 1,
        replaced: 0,
        skipped: 0,
        unchanged: 0,
        changes: [ {
            old_val: null,
            new_val: {
                id: generatedKey,
                title: 'Lorem ipsum',
                content: 'Dolor sit amet'
            }
        } ]
    });
});

test( '.insert(, {}) returns error if inserted document is found', async t => {
    const existingDoc = {
        id: 'post-1234',
        title: 'post title',
        content: 'post content'
    };

    const conflictDoc = {
        id: 'post-1234',
        title: 'Conflict Lorem ipsum',
        content: 'Conflict Dolor sit amet'
    };

    const { r } = rethinkdbMocked([
        [ 'posts', existingDoc ]
    ]);

    const insertRes = await r.table( 'posts' ).insert( conflictDoc ).run();

    t.deepEqual( insertRes, {
        unchanged: 0,
        skipped: 0,
        replaced: 0,
        inserted: 0,
        errors: 1,
        deleted: 0,
        firstError: mockdbResErrorDuplicatePrimaryKey( existingDoc, conflictDoc )
    });
});

test( '.update(, { prop: val }) should update a document', async t => {
    const { r } = rethinkdbMocked([
        [ 'AppUserTest', {
            id: 'appuser-1234',
            name: 'appusername-1234'
        }, {
            id: 'appuser-5678',
            name: 'appusername-5678'
        } ]
    ]);

    const updateRes = await r
        .table( 'AppUserTest' )
        .get( 'appuser-1234' )
        .update({ user_social_id: 'userSocial-1234' })
        .run();

    t.deepEqual( updateRes, {
        deleted: 0,
        errors: 0,
        inserted: 0,
        replaced: 1,
        skipped: 0,
        unchanged: 0
    });

    const queriedAppUser = await r
        .table( 'AppUserTest' )
        .get( 'appuser-1234' )
        .run();

    t.is( queriedAppUser.user_social_id, 'userSocial-1234' );
});

test( '.delete() should delete a document', async t => {
    const { r } = rethinkdbMocked([
        [ 'AppUserTest', {
            id: 'appuser-1234',
            name: 'appusername-1234'
        }, {
            id: 'appuser-5678',
            name: 'appusername-5678'
        } ]
    ]);

    const updateRes = await r
        .table( 'AppUserTest' )
        .get( 'appuser-1234' )
        .delete()
        .run();

    t.deepEqual( updateRes, {
        deleted: 1,
        errors: 0,
        inserted: 0,
        replaced: 0,
        skipped: 0,
        unchanged: 0
    });
});

test( '.delete() should delete all documents', async t => {
    const { r } = rethinkdbMocked([
        [ 'AppUserTest', {
            id: 'appuser-1234',
            name: 'appusername-1234'
        }, {
            id: 'appuser-5678',
            name: 'appusername-5678'
        } ]
    ]);

    const updateRes = await r
        .table( 'AppUserTest' )
        .delete()
        .run();

    t.deepEqual( updateRes, {
        deleted: 2,
        errors: 0,
        inserted: 0,
        replaced: 0,
        skipped: 0,
        unchanged: 0
    });
});

test( '.delete() should delete filtered documents', async t => {
    const { r } = rethinkdbMocked([
        [ 'AppUserTest', {
            id: 'appuser-1234',
            name: 'appusername-1234'
        }, {
            id: 'appuser-5678',
            name: 'appusername-5678'
        } ]
    ]);

    const updateRes = await r
        .table( 'AppUserTest' )
        .filter({ name: 'appusername-5678' })
        .delete()
        .run();

    t.deepEqual( updateRes, {
        deleted: 1,
        errors: 0,
        inserted: 0,
        replaced: 0,
        skipped: 0,
        unchanged: 0
    });
});

test( '.contains() should return containing documents', async t => {
    const { r } = rethinkdbMocked([
        [ 'marvel', {
            id: 'appuser-1234',
            name: 'Siren',
            city: 'Los Angeles'
        }, {
            id: 'appuser-5678',
            name: 'Xavier',
            city: 'Detroit'
        }, {
            id: 'appuser-9012',
            name: 'Wolverine',
            city: 'Chicago'
        } ]
    ]);

    const cities = [ 'Detroit', 'Chicago', 'Hoboken' ];
    const updateRes = await r
        .table( 'marvel' )
        .filter( hero => r.expr( cities ).contains( hero( 'city' ) ) )
        .run();

    t.deepEqual( updateRes, [ {
        id: 'appuser-5678',
        name: 'Xavier',
        city: 'Detroit'
    }, {
        id: 'appuser-9012',
        name: 'Wolverine',
        city: 'Chicago'
    } ]);
});
