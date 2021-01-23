import test from 'ava';
import rethinkdbMocked from '../src/mockdb.js';
import mockedReql from '../src/mockdbReql.js';
import { mockdbResErrorDuplicatePrimaryKey } from '../src/mockdbRes.js';

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
