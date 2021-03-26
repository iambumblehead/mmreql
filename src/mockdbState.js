import castas from 'castas';
import { v4 as uuidv4 } from 'uuid';

const mockdbStateTableCreateIndexTuple = ( name, fields = [], config = {}) => (
    [ name, fields, config ]);

const mockdbStateSelectedDb = ( dbState, dbIndex ) => (
    dbState.db[dbState.dbSelected]);

const mockdbStateDbTableConfigKeyGet = ( dbName, tableName ) => (
    `dbConfig_${dbName}_${tableName}` );

const mockdbStateDbCursorsKeyGet = dbName => (
    `dbConfig_${dbName}_cursor` );

const mockdbStateDbConfigKeyGet = dbName => (
    `dbConfig_${dbName}` );

const mockdbStateDbCreate = ( state, dbName ) => {
    state.dbSelected = dbName;
    state[mockdbStateDbConfigKeyGet( dbName )] = {
        name: dbName,
        id: uuidv4()
    };

    state[mockdbStateDbCursorsKeyGet( dbName )] = {};
    state.db[dbName] = {};

    return state;
};

const mockdbStateDbDrop = ( state, dbName ) => {
    delete state[mockdbStateDbConfigKeyGet( dbName )];
    delete state[mockdbStateDbCursorsKeyGet( dbName )];
    delete state.db[dbName];

    if ( state.dbSelected === dbName )
        [ state.dbSelected ] = Object.keys( state.db );

    return state;
};

const mockdbStateCreate = opts => {
    const dbConfigList = castas.arr( opts.dbs, [ {
        db: opts.db || 'default'
    } ]);

    return dbConfigList.reduce( ( state, s ) => {
        state = mockdbStateDbCreate( state, s.db );

        return state;
    }, {
        dbConnections: castas.arr( opts.connections, []),
        db: {}
    });
};

const mockdbStateTableConfigGet = ( dbState, tableName ) => {
    const dbName = dbState.dbSelected;
    const tableKey = mockdbStateDbTableConfigKeyGet( dbName, tableName );

    return dbState[tableKey];
};

const mockdbStateDbConfigGet = ( dbState, dbName ) => {
    dbName = dbName || dbState.dbSelected;

    return dbState[mockdbStateDbConfigKeyGet( dbName )];
};

const mockdbStateDbCursorConfigGet = dbState => {
    const dbName = dbState.dbSelected;

    return dbState[mockdbStateDbCursorsKeyGet( dbName )];
};

const mockdbStateTableSet = ( dbState, tableName, table ) => {
    const dbName = dbState.dbSelected;

    dbState.db[dbName][tableName] = table;

    return dbState;
};

const mockdbStateTableRm = ( dbState, tableName ) => {
    const dbName = dbState.dbSelected;

    delete dbState.db[dbName][tableName];

    return dbState;
};

const mockdbStateTableGet = ( dbState, tableName ) => {
    const dbName = dbState.dbSelected;

    return dbState.db[dbName][tableName];
};

const mockdbStateTableConfigSet = ( dbState, tableName, tableConfig ) => {
    const dbName = dbState.dbSelected;
    const tableKey = mockdbStateDbTableConfigKeyGet( dbName, tableName );

    dbState[tableKey] = tableConfig;

    return dbState;
};

const mockdbStateTableConfigRm = ( dbState, tableName ) => {
    const dbName = dbState.dbSelected;
    const tableKey = mockdbStateDbTableConfigKeyGet( dbName, tableName );

    delete dbState[tableKey];

    return dbState;
};

const mockdbStateTableCreate = ( dbState, tableName, config ) => {
    const dbName = dbState.dbSelected;

    dbState = mockdbStateTableConfigSet( dbState, tableName, {
        db: dbState.dbSelected,
        id: uuidv4(),
        durability: 'hard',
        indexes: [],
        name: tableName,
        primary_key: ( config && config.primaryKey ) || 'id',
        shards: [ {
            primary_replica: 'replicaName',
            replicas: [ 'replicaName' ]
        } ],
        write_acks: 'majority',
        write_hook: null
    });

    dbState = mockdbStateTableSet( dbState, tableName, []);

    return dbState;
};

const mockdbStateTableDrop = ( dbState, tableName, config ) => {
    dbState = mockdbStateTableConfigRm( dbState, tableName );
    dbState = mockdbStateTableRm( dbState, tableName );

    return dbState;
};

const mockdbStateTableGetIndexNames = ( dbState, tableName ) => {
    const tableConfig = mockdbStateTableConfigGet( dbState, tableName );
    
    return tableConfig ? tableConfig.indexes.map( i => i[0]) : [];
};

const mockdbStateTableGetPrimaryKey = ( dbState, tableName ) => {
    const tableConfig = mockdbStateTableConfigGet( dbState, tableName );

    return ( tableConfig && tableConfig.primary_key ) || 'id';
};

const mockdbStateTableIndexExists = ( db, tableName, indexName ) => {
    const indexNames = mockdbStateTableGetIndexNames( db, tableName );

    return indexNames.includes( indexName );
};

const mockdbStateTableGetOrCreate = ( dbState, tableName ) => {
    const table = mockdbStateTableGet( dbState, tableName );
    
    if ( !table )
        dbState = mockdbStateTableCreate( dbState, tableName );

    return mockdbStateTableGet( dbState, tableName );
};

const mockdbStateTableIndexAdd = ( dbState, tableName, indexName, fields, config ) => {
    mockdbStateTableGetOrCreate( dbState, tableName );

    const tableConfig = mockdbStateTableConfigGet( dbState, tableName );

    tableConfig.indexes.push(
        mockdbStateTableCreateIndexTuple( indexName, fields, config ) );

    return tableConfig;
};

// by default, a tuple for primaryKey 'id' is returned,
// this should be changed. ech table config should provide a primary key
// using 'id' as the defautl for each one.
const mockdbStateTableGetIndexTuple = ( dbState, tableName, indexName ) => {
    const db = mockdbStateSelectedDb( dbState );
    const tableConfig = mockdbStateTableConfigGet( dbState, tableName );    
    const indexTuple = ( tableConfig && tableConfig.indexes )
        && tableConfig.indexes.find( i => i[0] === indexName );

    if ( !indexTuple && indexName !== 'id' && indexName !== tableConfig.primary_key ) {
        console.warn( `table index not found. ${tableName}, ${indexName}` );
    }

    return indexTuple || mockdbStateTableCreateIndexTuple( indexName );
};

const mockdbStateTableCursorSplice = ( dbState, tableName, cursorIndex ) => {
    const db = mockdbStateSelectedDb( dbState );
    const cursorConfig = mockdbStateDbCursorConfigGet( dbState, db );
    const tableCursors = cursorConfig[tableName];

    tableCursors.splice( cursorIndex, 1 );

    return dbState;
};

const mockdbStateTableDocCursorSplice = ( dbState, tableName, doc, cursorIndex ) => {
    const db = mockdbStateSelectedDb( dbState );
    const cursorConfig = mockdbStateDbCursorConfigGet( dbState, db );
    const tableDocId = [ tableName, doc.id ].join( '-' );
    const tableCursors = cursorConfig[tableDocId];

    tableCursors.splice( cursorIndex, 1 );

    return dbState;
};

const mockdbStateTableCursorSet = ( dbState, tableName, cursor ) => {
    const cursors = mockdbStateDbCursorConfigGet( dbState );
    const tableCursors = cursors[tableName];

    tableCursors.push( cursor );

    return dbState;
};

const mockdbStateTableDocCursorSet = ( dbState, tableName, doc, cursor ) => {
    const db = mockdbStateSelectedDb( dbState );
    const cursorConfig = mockdbStateDbCursorConfigGet( dbState, db );
    const tableDocId = [ tableName, doc.id ].join( '-' );

    cursorConfig[tableDocId] = cursorConfig[tableDocId] || [];
    cursorConfig[tableDocId].push( cursor );

    return dbState;
};

const mockdbStateTableCursorsPushChanges = ( dbState, tableName, changes ) => {
    const db = mockdbStateSelectedDb( dbState );
    const cursorConfig = mockdbStateDbCursorConfigGet( dbState, db );
    const cursors = cursorConfig[tableName] || [];

    cursors.forEach( c => {
        changes.forEach( d => c.push( d ) );
    });

    return dbState;
};

const mockdbStateTableCursorsGetOrCreate = ( dbState, tableName ) => {
    const cursors = mockdbStateDbCursorConfigGet( dbState );

    cursors[tableName] = cursors[tableName] || [];

    return cursors;
};

const mockdbStateTableDocCursorsGetOrCreate = ( dbState, tableName, doc ) => {
    const db = mockdbStateSelectedDb( dbState );
    const tableDocId = [ tableName, doc.id ].join( '-' );
    const cursorConfig = mockdbStateDbCursorConfigGet( dbState, db );

    cursorConfig[tableDocId] = cursorConfig[tableDocId] || [];

    return cursorConfig[tableDocId];
};

const mockdbStateAggregate = ( oldState, aggState ) => (
    Object.keys( aggState ).reduce( ( state, key ) => {
        if ( typeof aggState[key] === 'number' ) {
            if ( typeof state[key] === 'undefined' )
                state[key] = 0;
            
            state[key] += aggState[key];
        } else if ( Array.isArray( aggState[key]) ) {
            if ( !Array.isArray( state[key]) )
                state[key] = [];

            state[key].push( ...aggState[key]);
        }

        return state;
    }, oldState ) );

export {
    mockdbStateCreate,
    mockdbStateSelectedDb,
    mockdbStateAggregate,
    mockdbStateDbCreate,
    mockdbStateDbDrop,
    mockdbStateTableCreate,
    mockdbStateTableDrop,
    mockdbStateTableGet,
    mockdbStateTableSet,
    mockdbStateTableGetOrCreate,
    mockdbStateTableGetIndexNames,
    mockdbStateTableGetPrimaryKey,
    mockdbStateTableIndexAdd,
    mockdbStateTableIndexExists,
    mockdbStateTableGetIndexTuple,
    mockdbStateTableCursorSet,
    mockdbStateTableDocCursorSet,
    mockdbStateTableCursorSplice,
    mockdbStateTableDocCursorSplice,
    mockdbStateTableCursorsPushChanges,
    mockdbStateTableCursorsGetOrCreate,
    mockdbStateTableDocCursorsGetOrCreate,
    mockdbStateTableConfigGet,
    mockdbStateDbConfigGet,
    mockdbStateDbCursorConfigGet
};
