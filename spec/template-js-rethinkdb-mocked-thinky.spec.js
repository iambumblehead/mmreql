import test from 'ava';
import rethinkdbMocked from '../src/template-js-rethinkdb-mocked.js';

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

    // update not yet supported
    // await r.table( 'user' ).get( 'John' ).update({
    //     birthdate: r.epochTime( 531360000 )
    // }).run();
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

// test.only( 'supports .during(), correctly handles empty results', async t => {
//     const mockedDB = thinkyMockedDB();
//     const now = Date.now();
//
//     const tables = {
//         RoomCodes: thinkyMockedDBDocGen( mockedDB,
//             thinkyMockedDBObject( 'RoomCodes', () => ({
//                 id: 'expired',
//                 time_expire: new Date( now - ( 1000 * 60 * 60 * 24 ) )
//             }) )
//         )
//     };
//
//     const { r } = createThinkyMock( tables );
//
//     // results in runtime error :(
//     const expiredDocs = await r
//         .table( 'RoomCodes' )
//         .filter(
//             r.row( 'time_expire' ).during(
//                 r.epochTime( 0 ),
//                 r.epochTime( now / 1000 )
//             ) )
//         .delete()
//         .run();
//
//     t.is( true, true );
// });

test( 'supports .append()', async t => {
    const { r } = rethinkdbMocked([
        [ 'marvel', {
            id: 'IronMan',
            equipment: []
        } ]
    ]);

    // 'get' does not return a function
    // await r.table( 'marvel' ).get( 'IronMan' )( 'equipment' ).append( 'newBoots' ).run();

    const ironman = await r.table( 'marvel' ).get( 'IronMan' ).run();

    t.true( Array.isArray( ironman.equipment ) );
});
