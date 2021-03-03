import {
    mockdbStateDbConfigGet,
    mockdbStateTableConfigGet
} from './mockdbState.js';

// rethinkdb response values are never 'undefined'
// remove 'undefined' definitions from object
const mockdbFilterUndefined = obj => Object.keys( obj )
    .reduce( ( filtered, key ) => (
        typeof obj[key] !== 'undefined'
            ? { [key]: obj[key], ...filtered }
            : filtered
    ), {});

const mockdbResChangesFieldCreate = opts => mockdbFilterUndefined({
    deleted: 0,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 0,
    unchanged: 0,
    ...opts
});

const mockdbResTableStatus = opts => mockdbFilterUndefined({
    db: opts.db || null,
    id: opts.id || null,
    name: opts.name ||'tablename',
    raft_leader: opts.raft_leader || 'devdb_rethinkdb_multicluster',
    shards: opts.shards || [ {
        primary_replica: 'replicaName',
        replicas: [ 'replicaName' ]
    } ],
    status: opts.status || {
        all_replicas_ready: true,
        ready_for_outdated_reads: true,
        ready_for_reads: true,
        ready_for_writes: true
    }
});

const mockdbResTableInfo = ( dbState, tableName ) => {
    const tableConfig = mockdbStateTableConfigGet( dbState, tableName );
    const dbConfig = mockdbStateDbConfigGet( dbState, tableConfig.db );

    return mockdbFilterUndefined({
        db: {
            ...dbConfig,
            type: 'DB'
        },
        doc_count_estimates: [ 0 ],
        id: tableConfig.id,
        indexes: [],
        name: tableConfig.name,
        primary_key: tableConfig.primary_key,
        type: 'TABLE'
    });
};

const mockdbResStringify = obj => JSON.stringify( obj, null, '\t' );

const mockdbResErrorDuplicatePrimaryKey = ( existingDoc, conflictDoc ) => (
    'Duplicate primary key `id`:\n :existingDoc\n:conflictDoc'
        .replace( /:existingDoc/, mockdbResStringify( existingDoc ) )
        .replace( /:conflictDoc/, mockdbResStringify( conflictDoc ) ) );

const mockdbResErrorArgumentsNumber = ( queryId, takesArgs = 0, givenArgs = 1, atLeast = false ) => (
    '`:queryId` takes :takesArgs :argument, :givenArgs provided.'
        .replace( /:queryId/, queryId )
        .replace( /:argument/, takesArgs === 1 ? 'argument' : 'arguments' )
        .replace( /:takesArgs/, atLeast ? `at least ${takesArgs}` : takesArgs )
        .replace( /:givenArgs/, givenArgs ) );

const mockdbResErrorIndexOutOfBounds = index => (
    `ReqlNonExistanceError: Index out of bounds: ${index}` );

const mockdbResErrorUnrecognizedOption = ( key, value ) => (
    '[ReqlUnknownError]: Unrecognized optional argument `:key` in:'
        .replace( /:key/, key ) );

const mockdbResErrorInvalidTableName = tableName => (
    'RethinkDBError [ReqlLogicError]: Table name `:tableName` invalid (Use A-Z, a-z, 0-9, _ and - only)'
        .replace( /:tableName/, tableName ) );

const mockdbResErrorInvalidDbName = dbName => (
    'Database name `:dbName` invalid (Use A-Z, a-z, 0-9, _ and - only)'
        .replace( /:dbName/, dbName ) );

const mockdbResErrorTableExists = ( dbName, tableName ) => (
    'Table `:tableName` already exists.'
        .replace( /:tableName/, [ dbName, tableName ].join( '.' ) ) );

const mockdbResErrorPrimaryKeyWrongType = primaryKey => (
    'Primary keys must be either a number, string, bool, pseudotype or array (got type :type)'
        .replace( /:type/, String( typeof primaryKey ).toUpperCase() ) );

export {
    mockdbResChangesFieldCreate,
    mockdbResStringify,
    mockdbResTableStatus,
    mockdbResTableInfo,
    mockdbResErrorDuplicatePrimaryKey,
    mockdbResErrorArgumentsNumber,
    mockdbResErrorIndexOutOfBounds,
    mockdbResErrorUnrecognizedOption,
    mockdbResErrorInvalidTableName,
    mockdbResErrorInvalidDbName,
    mockdbResErrorTableExists,
    mockdbResErrorPrimaryKeyWrongType
};
