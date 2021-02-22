import test from 'ava';
import rethinkdbMocked from '../src/mockdb.js';

test( '`tableList` should return a cursor', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    const result = await r
        .db( 'cmdb' )
        .tableList()
        .run();

    t.true( Array.isArray( result ) );
    t.deepEqual( result, [ 'Rooms' ]);
});

test( '`tableList` should show the table we created', async t => {
    const { r, dbState } = rethinkdbMocked([ [ 'Rooms' ] ]);

    const tableCreateRes = await r
        .db( 'default' )
        .tableCreate( 'thenewtable' )
        .run();

    t.deepEqual( tableCreateRes, {
        tables_created: 1,
        config_changes: [ {
            new_val: {
                db: 'default',
                durability: 'hard',
                id: dbState.dbConfig_default_thenewtable.id,
                indexes: [],
                name: 'thenewtable',
                primary_key: 'id',
                shards: [ {
                    primary_replica: 'replicaName',
                    replicas: [
                        'replicaName'
                    ]
                } ],
                write_acks: 'majority',
                write_hook: null
            },
            old_val: null
        } ]
    });
    
    const result2 = await r
        .db( 'cmdb' )
        .tableList()
        .run();

    t.true( Array.isArray( result2 ) );
    t.true( result2.some( name => name === 'thenewtable' ) );
});

test( '`tableCreate` should create a table -- primaryKey', async t => {
    const { r, dbState } = rethinkdbMocked([ [ 'Rooms' ] ]);
    const tableCreateRes = await r
        .db( 'default' )
        .tableCreate( 'thenewtable', { primaryKey: 'foo' })
        .run();

    t.deepEqual( tableCreateRes, {
        tables_created: 1,
        config_changes: [ {
            new_val: {
                db: 'default',
                durability: 'hard',
                id: dbState.dbConfig_default_thenewtable.id,
                indexes: [],
                name: 'thenewtable',
                primary_key: 'foo',
                shards: [ {
                    primary_replica: 'replicaName',
                    replicas: [
                        'replicaName'
                    ]
                } ],
                write_acks: 'majority',
                write_hook: null
            },
            old_val: null
        } ]
    });

    const infoRes = await r
        .db( 'default' )
        .table( 'thenewtable' )
        .info()
        .run();

    t.deepEqual( infoRes, {
        db: {
            ...dbState.dbConfig_default,
            type: 'DB'
        },
        doc_count_estimates: [ 0 ],
        id: dbState.dbConfig_default_thenewtable.id,
        indexes: [],
        name: 'thenewtable',
        primary_key: 'foo',
        type: 'TABLE'
    });
});

test( '`tableCreate` should throw if table exists', async t => {
    const { r } = rethinkdbMocked([ [ 'Rooms' ] ]);

    await t.throws( () => ( r
        .db( 'default' )
        .tableCreate( 'Rooms' )
        .run()
    ), {
        message: 'Table `default.Rooms` already exists.'
    });
});

test( '`tableCreate` should throw -- non valid args', async t => {
    const { r } = rethinkdbMocked();

    await t.throws( () => ( r
        .db( 'default' )
        .tableCreate( 'thetablename', { nonValidArg: true })
        .run()
    ), {
        message: '[ReqlUnknownError]: Unrecognized optional argument `nonValidArg` in:'
    });
});

test( '`tableCreate` should throw if no argument is given', async t => {
    const { r } = rethinkdbMocked();

    await t.throws( () => ( r
        .db( 'default' )
        .tableCreate()
        .run()
    ), {
        message: 'RethinkDBError [ReqlDriverError]: `r.tableCreate` takes at least 1 argument, 0 provided.'
    });
});

test( '`tableCreate` should throw is the name contains special char', async t => {
    const { r } = rethinkdbMocked();

    await t.throws( () => ( r
        .db( 'default' )
        .tableCreate( '^_-' )
        .run()
    ), {
        message: 'RethinkDBError [ReqlLogicError]: Table name `^_-` invalid (Use A-Z, a-z, 0-9, _ and - only)'
    });
});

test( '`tableDrop` should drop a table', async t => {
    const { r, dbState } = rethinkdbMocked([ [ 'Rooms' ] ]);

    const tableCreateRes = await r
        .db( 'default' )
        .tableCreate( 'thenewtable', { primaryKey: 'foo' })
        .run();

    t.is( tableCreateRes.tables_created, 1 );

    const tableListRes = await r
        .db( 'default' )
        .tableList()
        .run();

    t.deepEqual( tableListRes, [ 'Rooms', 'thenewtable' ]);

    const thenewtableid = dbState.dbConfig_default_thenewtable.id;

    const tableDropRes = await r
        .db( 'default' )
        .tableDrop( 'thenewtable' )
        .run();

    t.deepEqual( tableDropRes, {
        tables_dropped: 1,
        config_changes: [ {
            new_val: null,
            old_val: {
                db: 'default',
                durability: 'hard',
                id: thenewtableid,
                indexes: [],
                name: 'thenewtable',
                primary_key: 'foo',
                shards: [ {
                    primary_replica: 'replicaName',
                    replicas: [ 'replicaName' ]
                } ],
                write_acks: 'majority',
                write_hook: null
            }
        } ]
    });

    const tableListRes2 = await r
        .db( 'default' )
        .tableList()
        .run();

    t.deepEqual( tableListRes2, [ 'Rooms' ]);
});
