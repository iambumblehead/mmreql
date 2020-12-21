import test from 'ava';
import createThinkyMock from '../src/template-js-rethinkdb-mocked-thinky.js';

const model = ( table, data, singularName ) => {
    const constructor = ( options = {}) => Object.assign( data(), options );
    constructor.table = table;
    constructor.singularName = singularName || `${table[0].toLowerCase()}${table.slice( 1, -1 )}`;
    // modelConstructors.push( constructor );
    return constructor;
};

function gen ( generator, count = 1, options = {}) {
    return Array( Math.max( count, 0 ) || 0 ).fill().map( () => generator( options ) );
}

test( 'returns an app document', async t => {
    const modelConstructors = [];

    const baseOptions = modelConstructors.reduce( ( obj, { table, singularName }) => Object.assign( obj, {
        [table]: null,
        [`add${table}`]: 0,
        [`${singularName}Options`]: {}
    }), {});

    let options = {};
    options = Object.assign( baseOptions, {
        adminOptions: {}
    }, options );

    const genOne = constructor => options[constructor.table] || [
        constructor( options[`${constructor.singularName}Options`]),
        ...gen( constructor, options[`add${constructor.table}`])
    ];

    const tables = {
        Applications: genOne(
            model( 'Applications', () => ({
                id: 'appid-1234',
                name: 'app name',
                description: 'app description'
            }) )
        )
    };

    const { r } = createThinkyMock({
        tables,
        ...tables
    });

    const appDoc = await r.table( 'Applications' ).get( 'appid-1234' ).run();

    t.is( appDoc.id, 'appid-1234' );
});
