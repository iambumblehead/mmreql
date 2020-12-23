import test from 'ava';
import createThinkyMock, {
    thinkyMockedDB,
    thinkyMockedDBObject,
    thinkyMockedDBDocGen
} from '../src/template-js-rethinkdb-mocked-thinky.js';

test( 'returns an app document', async t => {
    const mockedDB = thinkyMockedDB();

    const tables = {
        Applications: thinkyMockedDBDocGen( mockedDB,
            thinkyMockedDBObject( 'Applications', () => ({
                id: 'appid-1234',
                name: 'app name',
                description: 'app description'
            }) )
        )
    };

    const { r } = createThinkyMock( tables );

    const appDoc = await r.table( 'Applications' ).get( 'appid-1234' ).run();

    t.is( appDoc.id, 'appid-1234' );
});
