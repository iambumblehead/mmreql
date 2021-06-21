import { v4 as uuidv4 } from 'uuid';
import { Readable } from 'stream';

import {
    mockdbStateSelectedDb,
    mockdbStateAggregate,
    mockdbStateDbCreate,
    mockdbStateDbDrop,
    mockdbStateTableIndexAdd,
    mockdbStateTableGetIndexNames,
    mockdbStateTableGetIndexTuple,
    mockdbStateTableGetPrimaryKey,
    mockdbStateTableCursorSet,
    mockdbStateTableDocCursorSet,
    mockdbStateTableCursorSplice,
    mockdbStateTableDocCursorSplice,
    mockdbStateTableCursorsPushChanges,
    mockdbStateTableCursorsGetOrCreate,
    mockdbStateTableDocCursorsGetOrCreate,
    mockdbStateTableConfigGet,
    mockdbStateTableCreate,
    mockdbStateTableDrop,
    mockdbStateDbConfigGet,
    mockdbStateTableGet
} from './mockdbState.mjs';

import {
    mockdbTableGetDocument,
    mockdbTableSetDocument,
    mockdbTableGetDocuments,
    mockdbTableSetDocuments,
    mockdbTableDocGetIndexValue,
    mockdbTableDocEnsurePrimaryKey,
    mockdbTableSet
} from './mockdbTable.mjs';

import {
    mockdbResChangesFieldCreate,
    mockdbResErrorArgumentsNumber,
    mockdbResErrorDuplicatePrimaryKey,
    mockdbResErrorIndexOutOfBounds,
    mockdbResErrorUnrecognizedOption,
    mockdbResErrorInvalidTableName,
    mockdbResErrorInvalidDbName,
    mockdbResErrorTableExists,
    mockdbResErrorSecondArgumentOfQueryMustBeObject,
    mockdbResErrorPrimaryKeyWrongType,
    mockdbResErrorNotATIMEpsudotype,
    mockdbResTableStatus,
    mockdbResTableInfo
} from './mockdbRes.mjs';

const isReqlObj = obj => Boolean(
    obj && /object|function/.test( typeof obj ) && obj.isReql );

const isConfigObj = ( obj, objType = typeof obj ) => obj
      && /object|function/.test( objType )
      && !isReqlObj( obj )
      && !Array.isArray( obj );

// return last query argument (optionally) provides query configurations
const queryArgsOptions = ( queryArgs, queryOptionsDefault = {}) => {
    const queryOptions = queryArgs.slice( -1 )[0] || {};

    return ( isConfigObj( queryOptions ) )
        ? queryOptions
        : queryOptionsDefault;
};

// use when order not important and sorting helps verify a list
const compare = ( a, b, prop ) => {
    if ( a[prop] < b[prop]) return -1;
    if ( a[prop] > b[prop]) return 1;
    return 0;
};

const isReqlArgs = value => (
    isReqlObj( value ) && value.queryName === 'args' );

export const spend = ( value, reqlChain, doc, type = typeof value, f = null ) => {
    if ( value === f ) {
        f = value;
    } else if ( isReqlObj( value ) ) {
        if ( value.record ) {
            f = value.run( doc );
        } else {
            f = value.run();
        }
    } else if ( /string|boolean|number|undefined/.test( type ) ) {
        f = value;
        if ( doc && !doc.playbackStub ) {
            f = doc[value];
        }
    } else if ( Array.isArray( value ) ) {
        // detach if value is has args
        if ( isReqlArgs( value.slice( -1 )[0]) ) {
            f = value.slice( -1 )[0].run();
        } else {
            f = value.map( v => spend( v, reqlChain ) );
        }
    } else if ( type === 'function' ) {
        if ( doc ) {
            f = value( reqlChain().expr( doc ) ).run();
        } else {
            f = value( doc ).run();
        }
    } else if ( value instanceof Date ) {
        f = value;
    } else {
        f = Object.keys( value ).reduce( ( prev, key ) => {
            prev[key] = spend( value[key], reqlChain );

            return prev;
        }, {});
    }

    return f;
};

const reql = {};

reql.connect = ( queryState, args, reqlChain, dbState ) => {
    const [ connection ] = args;
    const { db, host, port, user, password } = connection;

    if ( dbState.dbConnections.every( c => c.db !== connection.db ) ) {
        dbState.dbConnections.push( connection );
    }

    return {
        connectionOptions: {
            host,
            port
        },
        db,
        options: {
            host,
            port
        },
        clientPort: port,
        clientAddress: host,
        timeout: 20,
        pingInterval: -1,
        silent: false,
        socket: {
            runningQueries: {},
            isOpen: true,
            nextToken: 3,
            buffer: {
                type: 'Buffer',
                data: []
            },
            mode: 'response',
            connectionOptions: {
                host,
                port
            },
            user,
            password: {
                type: 'Buffer',
                data: [ 0 ]
            }
        },
        close: () => {}
    };
};


reql.connectPool = ( queryState, args, reqlChain, dbState ) => {
    const [ connection ] = args;
    const { db, host, port, user, password } = connection;

    if ( dbState.dbConnections.every( c => c.db !== connection.db ) ) {
        dbState.dbConnections.push( connection );
    }

    return {
        draining: false,
        healthy: true,
        discovery: false,
        connParam: {
            db,
            user,
            password: '',
            buffer: 1,
            max: 1,
            timeout: 20,
            pingInterval: -1,
            timeoutError: 1000,
            timeoutGb: 3600000,
            maxExponent: 6,
            silent: false
        },
        servers: [ {
            host,
            port
        } ],
        serverPools: [ {
            draining: false,
            healthy: true,
            connections: [ {
                connectionOptions: {
                    host,
                    port
                },
                db,
                options: {
                    host,
                    port
                },
                clientPort: port,
                clientAddress: host,
                timeout: 20,
                pingInterval: -1,
                silent: false,
                socket: {
                    runningQueries: {},
                    isOpen: true,
                    nextToken: 3,
                    buffer: {
                        type: 'Buffer',
                        data: []
                    },
                    mode: 'response',
                    connectionOptions: {
                        host,
                        port
                    },
                    user,
                    password: {
                        type: 'Buffer',
                        data: [ 0 ]
                    },
                    socket: {
                        connecting: false,
                        allowHalfOpen: false,
                        server: null
                    }
                }
            } ],
            timers: {},
            buffer: 1,
            max: 1,
            timeoutError: 1000,
            timeoutGb: 3600000,
            maxExponent: 6,
            silent: false,
            server: {
                host,
                port
            },
            connParam: {
                db,
                user,
                password,
                timeout: 20,
                pingInterval: -1,
                silent: false
            }
        } ]
    };
};

reql.getPoolMaster = ( queryState, args, reqlChain, dbState ) => {
    const [ connection ] = dbState.dbConnections;
    const {
        db, host, port, user, password
    } = connection || {
        db: 'default',
        host: 'localhost',
        port: 28015,
        user: '',
        password: ''
    };

    return {
        isHealthy: true,
        _events: {},
        _eventsCount: 0,
        _maxListeners: undefined,
        draining: false,
        healthy: true,
        discovery: false,
        connParam: {
            db,
            user,
            password,
            buffer: 1,
            max: 1,
            timeout: 20,
            pingInterval: -1,
            timeoutError: 1000,
            timeoutGb: 3600000,
            maxExponent: 6,
            silent: false,
            log: console.log
        },
        servers: [ { host, port } ],
        serverPools: [ {
            _events: {},
            _eventsCount: 4,
            _maxListeners: undefined,
            draining: false,
            healthy: true,
            connections: [],
            timers: {},
            buffer: 1,
            max: 1,
            timeoutError: 1000,
            timeoutGb: 3600000,
            maxExponent: 6,
            silent: false,
            log: console.log,
            server: {
                host,
                port
            },
            connParam: [ {
                db,
                user,
                password,
                timeout: 20,
                pingInterval: -1,
                silent: false
            } ]
        } ]
    };
};

// used for selecting/specifying db, not supported yet
reql.db = ( queryState, args ) => {
    const [ dbName ] = args;
    const isValidDbNameRe = /^[A-Za-z0-9_]*$/;

    if ( !args.length ) {
        queryState.error = mockdbResErrorArgumentsNumber(
            'r.dbCreate', 1, args.length );
        queryState.target = null;

        return queryState;
    }

    if ( !isValidDbNameRe.test( dbName ) ) {
        queryState.error = mockdbResErrorInvalidDbName( dbName );
        queryState.target = null;

        return queryState;
    }

    queryState.db = dbName;

    return queryState;
};

reql.dbList = ( queryState, args, reqlChain, dbState ) => {
    queryState.target = Object.keys( dbState.db );

    return queryState;
};

reql.dbCreate = ( queryState, args, reqlChain, dbState ) => {
    const [ dbName ] = args;

    if ( !args.length ) {
        queryState.error = mockdbResErrorArgumentsNumber(
            'r.dbCreate', 1, args.length );
        queryState.target = null;

        return queryState;
    }

    dbState = mockdbStateDbCreate( dbState, dbName );

    queryState.target = {
        config_changes: [ {
            new_val: mockdbStateDbConfigGet( dbName ),
            old_val: null
        } ],
        dbs_created: 1
    };

    return queryState;
};

reql.dbDrop = ( queryState, args, reqlChain, dbState ) => {
    const [ dbName ] = args;
    const dbConfig = mockdbStateDbConfigGet( dbState, dbName );
    const tables = mockdbStateSelectedDb( dbState, dbName );

    if ( args.length !== 1 ) {
        queryState.error = mockdbResErrorArgumentsNumber(
            'r.dbDrop', 1, args.length );
        queryState.target = null;

        return queryState;
    }

    dbState = mockdbStateDbDrop( dbState, dbName );

    queryState.target = {
        config_changes: [ {
            new_val: null,
            old_val: dbConfig
        } ],
        dbs_dropped: 1,
        tables_dropped: Object.keys( tables ).length
    };

    return queryState;
};

reql.config = ( queryState, args, reqlChain, dbState ) => {
    if ( args.length ) {
        queryState.error = mockdbResErrorArgumentsNumber(
            'config', 0, args.length );
        queryState.target = null;

        return queryState;
    }

    if ( queryState.tablename ) {
        queryState.target = mockdbStateTableConfigGet( dbState, queryState.tablename );
        queryState.target = { // remove indexes data added for internal use
            ...queryState.target,
            indexes: queryState.target.indexes.map( i => i[0])
        };
    } else {
        queryState.target = mockdbStateDbConfigGet( dbState, queryState.tableName );
    }

    return queryState;
};

reql.status = ( queryState, args, reqlChain, dbState ) => {
    const tableConfig = mockdbStateTableConfigGet( dbState, queryState.tablename );

    queryState.target = mockdbResTableStatus( tableConfig );

    return queryState;
};

reql.info = ( queryState, args, reqlChain, dbState ) => {
    queryState.target = mockdbResTableInfo( dbState, queryState.tablename );

    return queryState;
};

reql.tableList = ( queryState, args, reqlChain, dbState ) => {
    const tables = mockdbStateSelectedDb( dbState, queryState.db );

    queryState.target = Object.keys( tables );

    return queryState;
};

reql.tableCreate = ( queryState, args, reqlChain, dbState ) => {
    const [ tableName ] = args;
    const isValidConfigKeyRe = /^(primaryKey|durability)$/;
    const isValidTableNameRe = /^[A-Za-z0-9_]*$/;
    const config = queryArgsOptions( args );
    const invalidConfigKey = Object.keys( config ).find( k => !isValidConfigKeyRe.test( k ) );

    if ( invalidConfigKey ) {
        queryState.error = mockdbResErrorUnrecognizedOption(
            invalidConfigKey, config[invalidConfigKey]);
        queryState.target = null;

        return queryState;
    }

    if ( !tableName ) {
        queryState.error = mockdbResErrorArgumentsNumber(
            'r.tableCreate', 1, 0, true );
        queryState.target = null;

        return queryState;
    }

    if ( !isValidTableNameRe.test( tableName ) ) {
        queryState.error = mockdbResErrorInvalidTableName( tableName );
        queryState.target = null;

        return queryState;
    }

    const dbName = queryState.db || dbState.dbSelected;
    const tables = mockdbStateSelectedDb( dbState, dbName );
    if ( tableName in tables ) {
        queryState.error = mockdbResErrorTableExists( dbName, tableName );
        queryState.target = null;

        return queryState;
    }

    dbState = mockdbStateTableCreate( dbState, tableName, config );

    const tableConfig = mockdbStateTableConfigGet( dbState, tableName );

    queryState.target = {
        tables_created: 1,
        config_changes: [ {
            new_val: tableConfig,
            old_val: null
        } ]
    };

    return queryState;
};

reql.tableDrop = ( queryState, args, reqlChain, dbState ) => {
    const [ tableName ] = args;

    const tableConfig = mockdbStateTableConfigGet( dbState, tableName );

    dbState = mockdbStateTableDrop( dbState, tableName );
    
    queryState.target = {
        tables_dropped: 1,
        config_changes: [ {
            new_val: null,
            old_val: tableConfig
        } ]
    };

    return queryState;
};

reql.indexCreate = ( queryState, args, reqlChain, dbState ) => {
    const [ indexName ] = args;
    const fields = spend( Array.isArray( args[1]) ? args[1] : [], reqlChain );
    const config = queryArgsOptions( args );

    mockdbStateTableIndexAdd(
        dbState, queryState.tablename, indexName, fields, config );

    // should throw ReqlRuntimeError if index exits already
    queryState.target = { created: 1 };

    return queryState;
};

reql.indexWait = ( queryState, args, reqlChain, dbState ) => {
    const tableIndexList = mockdbStateTableGetIndexNames(
        dbState, queryState.tablename );

    queryState.target = tableIndexList.map( indexName => ({
        index: indexName,
        ready: true,
        function: 1234,
        multi: false,
        geo: false,
        outdated: false
    }) );

    return queryState;
};

reql.indexList = ( queryState, args, reqlChain, dbState ) => {
    const tableConfig = mockdbStateTableConfigGet( dbState, queryState.tablename );

    queryState.target = tableConfig.indexes.map( i => i[0]);

    return queryState;
};

reql.insert = ( queryState, args, reqlChain, dbState ) => {
    // both argument types (list or atom) resolved to a list here
    let documents = Array.isArray( args[0]) ? args[0] : args.slice( 0, 1 );
    let table = queryState.tablelist;
    const primaryKey = mockdbStateTableGetPrimaryKey( dbState, queryState.tablename );
    const options = args[1] || {};

    const isValidConfigKeyRe = /^(returnChanges|durability|conflict)$/;
    const invalidConfigKey = Object.keys( options )
        .find( k => !isValidConfigKeyRe.test( k ) );

    if ( args.length > 1 && ( !args[1] || typeof args[1] !== 'object' ) ) {
        queryState.error = mockdbResErrorSecondArgumentOfQueryMustBeObject(
            'insert' );
        queryState.target = null;

        return queryState;
    }

    if ( invalidConfigKey ) {
        queryState.error = mockdbResErrorUnrecognizedOption(
            invalidConfigKey, options[invalidConfigKey]);
        queryState.target = null;

        return queryState;
    }

    if ( documents.length === 0 ) {
        queryState.error = mockdbResErrorArgumentsNumber( 'insert', 1, 0 );
        queryState.target = null;

        return queryState;
    }

    documents = documents
        .map( doc => mockdbTableDocEnsurePrimaryKey( doc, primaryKey ) );

    const existingDocs = mockdbTableGetDocuments(
        queryState.tablelist, documents.map( doc => doc[primaryKey]), primaryKey );

    if ( existingDocs.length ) {
        // only processes single document now
        if ( typeof options.conflict === 'function' ) {
            const oldDoc = reqlChain().expr( existingDocs[0]);
            const newDoc = reqlChain().expr( documents[0]);

            const resDoc = options.conflict( documents[0].id, oldDoc, newDoc ).run();
            const changes = [ {
                old_val: existingDocs[0],
                new_val: resDoc
            } ];

            mockdbTableSetDocument( table, resDoc, primaryKey );
            
            queryState.target = mockdbResChangesFieldCreate({
                replaced: documents.length,
                changes: options.returnChanges === true ? changes : undefined
            });

            return queryState;
        } else if ( /^(update|replace)$/.test( options.conflict ) ) {
            const conflictIds = existingDocs.map( doc => doc[primaryKey]);
            // eslint-disable-next-line security/detect-non-literal-regexp
            const conflictIdRe = new RegExp( `^(${conflictIds.join( '|' )})$` );
            const conflictDocs = documents.filter( doc => conflictIdRe.test( doc[primaryKey]) );

            queryState = options.conflict === 'update'
                ? reql.update( queryState, conflictDocs, reqlChain, dbState )
                : reql.replace( queryState, conflictDocs, reqlChain, dbState );

            const conflictTarget = queryState.target;

            return queryState;
        } else {
            queryState.target = mockdbResChangesFieldCreate({
                errors: 1,
                firstError: mockdbResErrorDuplicatePrimaryKey(
                    existingDocs[0],
                    documents.find( doc => doc[primaryKey] === existingDocs[0][primaryKey])
                )
            });
        }
        
        return queryState;
    }

    [ table, documents ] = mockdbTableSetDocuments(
        table, documents.map( doc => spend( doc, reqlChain ) ), primaryKey );

    const changes = documents.map( doc => ({
        old_val: null,
        new_val: doc
    }) );

    dbState = mockdbStateTableCursorsPushChanges(
        dbState, queryState.tablename, changes );

    queryState.target = mockdbResChangesFieldCreate({
        inserted: documents.length,
        generated_keys: documents.map( doc => doc[primaryKey]),
        changes: options.returnChanges === true ? changes : undefined
    });

    return queryState;
};

reql.update = ( queryState, args, reqlChain, dbState ) => {
    const queryTarget = queryState.target;
    const queryTable = queryState.tablelist;
    const updateProps = spend( args[0], reqlChain );
    const primaryKey = mockdbStateTableGetPrimaryKey( dbState, queryState.tablename );
    const options = args[1] || {};

    const updateTarget = targetDoc => {
        const oldDoc = mockdbTableGetDocument( queryTable, targetDoc[primaryKey], primaryKey );
        let newDoc = oldDoc && Object.assign({}, oldDoc, updateProps || {});
        
        if ( oldDoc ) {
            [ , newDoc ] = mockdbTableSetDocument( queryTable, newDoc, primaryKey );
        }

        return [ newDoc, oldDoc ];
    };

    const changesDocs = (
        Array.isArray( queryTarget )
            ? queryTarget
            : [ queryTarget ]
    ).reduce( ( changes, targetDoc ) => {
        const [ newDoc, oldDoc ]= updateTarget( targetDoc );

        if ( newDoc ) {
            changes.push({
                new_val: newDoc,
                old_val: oldDoc
            });
        }

        return changes;
    }, []);

    queryState.target = mockdbResChangesFieldCreate({
        replaced: changesDocs.length,
        changes: options.returnChanges === true ? changesDocs : undefined
    });

    return queryState;
};

reql.get = ( queryState, args, reqlChain, dbState ) => {
    const primaryKeyValue = spend( args[0], reqlChain );
    const primaryKey = mockdbStateTableGetPrimaryKey( dbState, queryState.tablename );
    const tableDoc = mockdbTableGetDocument( queryState.target, primaryKeyValue, primaryKey );

    if ( args.length === 0 ) {
        queryState.error = mockdbResErrorArgumentsNumber( 'get', 1, 0 );
        queryState.target = null;

        return queryState;
    }

    if ( !/^(number|string|bool)$/.test( typeof primaryKeyValue )
        && !Array.isArray( primaryKeyValue ) ) {
        queryState.error = mockdbResErrorPrimaryKeyWrongType( primaryKeyValue );
        queryState.target = null;

        return queryState;
    }

    queryState.target = tableDoc || null;

    return queryState;
};

reql.get.fn = ( queryState, args, reqlChain ) => {
    queryState.target = reqlChain()
        .expr( queryState.target ).getField( args[0]).run();

    return queryState;
};

reql.getAll = ( queryState, args, reqlChain, dbState ) => {
    const queryOptions = queryArgsOptions( args );
    const primaryKeyValues = spend(
        ( queryOptions && queryOptions.index ) ? args.slice( 0, -1 ) : args, reqlChain
    );
    const { tablename } = queryState;
    const primaryKey = queryOptions.index || mockdbStateTableGetPrimaryKey( dbState, queryState.tablename );
    const tableIndexTuple = mockdbStateTableGetIndexTuple( dbState, tablename, primaryKey );

    if ( primaryKeyValues.length ) {
        // eslint-disable-next-line security/detect-non-literal-regexp
        const targetValueRe = new RegExp( `^(${primaryKeyValues.join( '|' )})$` );
        
        queryState.target = queryState.target.filter( doc => (
            targetValueRe.test( mockdbTableDocGetIndexValue( doc, tableIndexTuple, spend, reqlChain ) )
        ) );
    }

    queryState.target = queryState.target.slice().sort( () => 0.5 - Math.random() );
    // rethink output array is not in-order

    return queryState;
};

reql.replace = ( queryState, args, reqlChain, dbState ) => {
    let replaced = 0;
    const replacement = spend( args[0], reqlChain );
    const targetIndexName = mockdbStateTableGetPrimaryKey( dbState, queryState.tablename );

    const targetIndex = queryState.tablelist
        .findIndex( doc => doc[targetIndexName] === replacement[targetIndexName]);


    if ( targetIndex > -1 ) {
        replaced += 1;
        queryState.tablelist[targetIndex] = replacement;
    }

    queryState.target = mockdbResChangesFieldCreate({
        replaced
    });

    return queryState;
};

reql.prepend = ( queryState, args, reqlChain ) => {
    const prependValue = spend( args[0], reqlChain );

    if ( typeof prependValue === 'undefined' ) {
        queryState.error = mockdbResErrorArgumentsNumber(
            'prepend', 1, 0, false );
        queryState.target = null;

        return queryState;
    }

    queryState.target.unshift( prependValue );

    return queryState;
};

reql.difference = ( queryState, args, reqlChain ) => {
    const differenceValues = spend( args[0], reqlChain );

    if ( typeof differenceValues === 'undefined' ) {
        queryState.error = mockdbResErrorArgumentsNumber(
            'difference', 1, 0, false );
        queryState.target = null;

        return queryState;
    }

    queryState.target = queryState.target
        .filter( e => !differenceValues.some( a => e == a ) );

    return queryState;
};

reql.nth = ( queryState, args, reqlChain ) => {
    const nthIndex = spend( args[0], reqlChain );

    if ( nthIndex >= queryState.target.length ) {
        queryState.error = mockdbResErrorIndexOutOfBounds( nthIndex );
        queryState.target = null;
    } else {
        queryState.target = queryState.target[args[0]];
    }

    return queryState;
};

// r.row → value
reql.row = ( queryState, args, reqlChain ) => {
    const rowFieldName = spend( args[0], reqlChain );

    queryState.target = queryState.target
        ? queryState.target[rowFieldName]
        : rowFieldName;

    return queryState;
};

reql.default = ( queryState, args, reqlChain ) => {
    if ( queryState.target === null ) {
        queryState.error = null;
        queryState.target = spend( args[0], reqlChain );
    }

    return queryState;
};

// time.during(startTime, endTime[, {leftBound: "closed", rightBound: "open"}]) → bool
reql.during = ( queryState, args, reqlChain ) => {
    const [ start, end ] = args;
    const startTime = spend( start, reqlChain );
    const endTime = spend( end, reqlChain );

    queryState.target = (
        queryState.target.getTime() > startTime.getTime()
            && queryState.target.getTime() < endTime.getTime()
    );

    return queryState;
};

reql.append = ( queryState, args, reqlChain ) => {
    queryState.target = spend( args, reqlChain ).reduce( ( list, val ) => {
        list.push( val );

        return list;
    }, queryState.target );

    return queryState;
};

// NOTE rethinkdb uses re2 syntax
// re using re2-specific syntax will fail
reql.match = ( queryState, args, reqlChain ) => {
    let regexString = spend( args[0], reqlChain );

    let flags = '';
    if ( regexString.startsWith( '(?i)' ) ) {
        flags = 'i';
        regexString = regexString.slice( '(?i)'.length );
    }

    // eslint-disable-next-line security/detect-non-literal-regexp
    const regex = new RegExp( regexString, flags );

    queryState.target = regex.test( queryState.target );

    return queryState;
};

reql.delete = ( queryState, args, reqlChain, dbState ) => {
    const queryTarget = queryState.target;
    const queryTable = queryState.tablelist;
    const primaryKey = mockdbStateTableGetPrimaryKey( dbState, queryState.tablename );
    const tableIndexTuple = mockdbStateTableGetIndexTuple(
        dbState, queryState.tablename, primaryKey );
    const targetIds = ( Array.isArray( queryTarget ) ? queryTarget : [ queryTarget ])
        .map( doc => mockdbTableDocGetIndexValue( doc, tableIndexTuple, spend ) );
    // eslint-disable-next-line security/detect-non-literal-regexp
    const targetIdRe = new RegExp( `^(${targetIds.join( '|' )})$` );
    const tableFiltered = queryTable.filter( doc => !targetIdRe.test(
        mockdbTableDocGetIndexValue( doc, tableIndexTuple, spend ) ) );
    const deleted = queryTable.length - tableFiltered.length;
    const queryConfig = queryArgsOptions( args );
    const isValidConfigKeyRe = /^(durability|returnChanges|ignoreWriteHook)$/;
    const invalidConfigKey = Object.keys( queryConfig )
        .find( k => !isValidConfigKeyRe.test( k ) );

    if ( invalidConfigKey ) {
        queryState.error = mockdbResErrorUnrecognizedOption(
            invalidConfigKey, queryConfig[invalidConfigKey]);
        queryState.target = null;

        return queryState;
    }

    mockdbTableSet( queryTable, tableFiltered );

    queryState.target = mockdbResChangesFieldCreate({
        deleted
    });

    return queryState;
};

reql.contains = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;

    if ( !args.length ) {
        throw new Error( 'Rethink supports contains(0) but rethinkdbdash does not.' );
    }

    queryState.target = args.every( predicate => (
        queryTarget.includes( spend( predicate, reqlChain ) ) ) );

    return queryState;
};

// Get a single field from an object. If called on a sequence, gets that field
// from every object in the sequence, skipping objects that lack it.
//
// https://rethinkdb.com/api/javascript/get_field
reql.getField = ( queryState, args, reqlChain ) => {
    const [ fieldName ] = spend( args, reqlChain );

    queryState.target = Array.isArray( queryState.target )
        ? queryState.target.map( t => t[fieldName])
        : queryState.target[fieldName];

    return queryState;
};

reql.filter = ( queryState, args, reqlChain ) => {
    const [ predicate ] = args;

    queryState.target = queryState.target.filter( item => {
        const finitem = spend( predicate, reqlChain, item );

        if ( finitem && typeof finitem === 'object' ) {
            return Object
                .keys( finitem )
                .every( key => finitem[key] === item[key]);
        }

        return finitem;
    });

    return queryState;
};

reql.count = queryState => {
    queryState.target = queryState.target.length;

    return queryState;
};

reql.pluck = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const pluckObj = ( obj, props ) => props.reduce( ( plucked, prop ) => {
        plucked[prop] = obj[prop];
        return plucked;
    }, {});

    args = spend( args, reqlChain );

    queryState.target = Array.isArray( queryTarget )
        ? queryTarget.map( t => pluckObj( t, args ) )
        : pluckObj( queryTarget, args );

    return queryState;
};

reql.hasFields = ( queryState, args ) => {
    const queryTarget = queryState.target;

    queryState.target = queryTarget.filter( item => {
        if ( !item ) return false;
        return args.every( name => Object.prototype.hasOwnProperty.call( item, name ) );
    });

    return queryState;
};

reql.slice = ( queryState, args, reqlChain ) => {
    const [ begin, end ] = spend( args.slice( 0, 2 ), reqlChain );

    if ( queryState.isGrouped ) { // slice from each group
        queryState.target = queryState.target.map( targetGroup => {
            targetGroup.reduction = targetGroup.reduction.slice( begin, end );

            return targetGroup;
        });
    } else {
        queryState.target = queryState.target.slice( begin, end );
    }

    return queryState;
};

reql.skip = ( queryState, args, reqlChain ) => {
    const count = spend( args[0], reqlChain );

    queryState.target = queryState.target.slice( count );

    return queryState;
};

reql.limit = ( queryState, args ) => {
    queryState.target = queryState.target.slice( 0, args[0]);

    return queryState;
};

// Documents in the result set consist of pairs of left-hand and right-hand documents,
// matched when the field on the left-hand side exists and is non-null and an entry
// with that field’s value exists in the specified index on the right-hand side.
reql.eqJoin = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const isNonNull = v => v !== null && v !== undefined;
    const queryConfig = queryArgsOptions( args );
    const isValidConfigKeyRe = /^index$/;
    const rightFields = spend( args[1], reqlChain );
    const rightFieldConfig = args[1] && spend( args[1] && args[1].config(), reqlChain );
    const rightFieldKey = ( queryConfig.index )
        || ( rightFieldConfig && rightFieldConfig.primary_key );
    const invalidConfigKey = Object.keys( queryConfig )
        .find( k => !isValidConfigKeyRe.test( k ) );

    if ( invalidConfigKey ) {
        queryState.error = mockdbResErrorUnrecognizedOption(
            invalidConfigKey, queryConfig[invalidConfigKey]);
        queryState.target = null;

        return queryState;
    }
    
    if ( args.length === 0 ) {
        queryState.error = mockdbResErrorArgumentsNumber(
            'eqJoin', 2, 0, true );
        queryState.target = null;

        return queryState;
    }

    queryState.target = queryTarget.reduce( ( joins, item ) => {
        const leftFieldSpend = spend( args[0], reqlChain, item );
        const leftFieldValue = queryState.tablelist
            ? item // if value comes from table use full document
            : leftFieldSpend;
        const leftFieldValueType = typeof leftFieldValue;

        if ( isNonNull( leftFieldValue ) ) {
            const rightFieldValue = rightFields
                .find( rf => rf[rightFieldKey] === leftFieldSpend );

            if ( isNonNull( rightFieldValue ) ) {
                joins.push({
                    left: leftFieldValue,
                    right: rightFieldValue
                });
            }
        }

        return joins;
    }, []);
    
    return queryState;
};

// Used to ‘zip’ up the result of a join by merging the ‘right’ fields into
// ‘left’ fields of each member of the sequence.
reql.zip = ( queryState, args, reqlChain ) => {
    queryState.target = queryState.target
        .map( t => ({ ...t.left, ...t.right }) );

    return queryState;
};

reql.innerJoin = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const [ otherSequence, joinFunc ] = args;
    const otherTable = spend( otherSequence, reqlChain );

    queryState.target = queryTarget.map( item => otherTable.map( otherItem => {
        const isJoined = spend( first => {
            const second = reqlChain().expr( otherItem );

            return joinFunc( first, second );
        }, reqlChain, item );

        return {
            left: item,
            right: isJoined ? otherItem : null
        };
    }) ).flat().filter( ({ right }) => right );

    return queryState;
};

reql.now = queryState => {
    queryState.target = new Date();

    return queryState;
};

reql.toEpochTime = queryState => {
    queryState.target = ( new Date( queryState.target ) ).getTime() / 1000;

    return queryState;
};

reql.epochTime = ( queryState, args ) => {
    queryState.target = new Date( args[0] * 1000 );

    return queryState;
};

reql.not = queryState => {
    const queryTarget = queryState.target;

    if ( typeof queryTarget !== 'boolean' )
        throw new Error( 'Cannot call not() on non-boolean value.' );

    queryState.target = !queryTarget;

    return queryState;
};

reql.gt = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target > argTarget;

    return queryState;
};

reql.ge = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target >= argTarget;

    return queryState;
};

reql.lt = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    if ( argTarget instanceof Date ) {
        if ( !( queryState.target instanceof Date ) ) {
            queryState.error = mockdbResErrorNotATIMEpsudotype(
                'forEach', 1, args.length );
            queryState.target = null;

            return queryState;
        }
    }

    if ( typeof queryState.target === typeof queryState.target ) {
        queryState.target = queryState.target < argTarget;
    }

    return queryState;
};

reql.le = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target <= argTarget;

    return queryState;
};

reql.eq = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target === argTarget;

    return queryState;
};

reql.ne = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target !== argTarget;

    return queryState;
};

reql.max = ( queryState, args ) => {
    const targetList = queryState.target;
    const getListMax = ( list, prop ) => list.reduce( ( maxDoc, doc ) => (
        maxDoc[prop] > doc[prop] ? maxDoc : doc
    ), targetList );

    const getListMaxGroups = ( groups, prop ) => (
        groups.reduce( ( prev, target ) => {
            prev.push({
                ...target,
                reduction: getListMax( target.reduction, prop )
            });

            return prev;
        }, [])
    );

    queryState.target = queryState.isGrouped
        ? getListMaxGroups( targetList, args[0])
        : getListMax( targetList, args[0]);

    return queryState;
};

reql.max.fn = ( queryState, args, reqlChain ) => {
    const field = spend( args[0], reqlChain );

    if ( queryState.isGrouped ) {
        queryState.target = queryState.target.map( targetGroup => {
            targetGroup.reduction = targetGroup.reduction[field];

            return targetGroup;
        });
    } else {
        queryState.target = queryState.target[field];
    }

    return queryState;
};

reql.min = ( queryState, args ) => {
    const targetList = queryState.target;
    const getListMin = ( list, prop ) => list.reduce( ( maxDoc, doc ) => (
        maxDoc[prop] < doc[prop] ? maxDoc : doc
    ), targetList );

    const getListMinGroups = ( groups, prop ) => (
        groups.reduce( ( prev, target ) => {
            prev.push({
                ...target,
                reduction: getListMin( target.reduction, prop )
            });

            return prev;
        }, [])
    );

    queryState.target = queryState.isGrouped
        ? getListMinGroups( targetList, args[0])
        : getListMin( targetList, args[0]);

    return queryState;
};

reql.merge = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const merges = args.map( arg => spend( arg, reqlChain ) );

    queryState.target = merges
        .reduce( ( p, next ) => Object.assign( p, next ), queryTarget );

    return queryState;
};

reql.concatMap = ( queryState, args, reqlChain ) => {
    const [ func ] = args;

    queryState.target = queryState
        .target.map( t => spend( func, reqlChain, t ) ).flat();

    return queryState;
};

reql.isEmpty = queryState => {
    queryState.target = queryState.target.length === 0;

    return queryState;
};

reql.add = ( queryState, args, reqlChain ) => {
    const { target } = queryState;
    const values = spend( args, reqlChain );

    let result;

    if ( typeof target === 'undefined' ) {
        if ( Array.isArray( values ) ) {
            result = values.slice( 1 ).reduce( ( prev, val ) => prev + val, values[0]);
        } else {
            result = values;
        }
    } else if ( /number|string/.test( typeof target ) ) {
        result = values.reduce( ( prev, val ) => prev + val, target );
    } else if ( Array.isArray( target ) ) {
        result = [ ...target, ...values ];
    }

    queryState.target = result;

    return queryState;
};

reql.group = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const [ arg ] = args;
    const groupedData = queryTarget.reduce( ( group, item ) => {
        const key = spend( arg, reqlChain );
        const groupKey = item[key];

        group[groupKey] = group[groupKey] || [];
        group[groupKey].push( item );

        return group;
    }, {});
    const rethinkFormat = Object.entries( groupedData )
        .map( ([ group, reduction ]) => ({ group, reduction }) );

    queryState.isGrouped = true;
    queryState.target = rethinkFormat;

    return queryState;
};

// array.sample(number) → array
reql.sample = ( queryState, args ) => {
    queryState.target = queryState.target
        .sort( () => 0.5 - Math.random() )
        .slice( 0, args );

    return queryState;
};

reql.ungroup = queryState => {
    queryState.isGrouped = false;

    return queryState;
};

reql.orderBy = ( queryState, args, reqlChain, dbState ) => {
    const queryTarget = queryState.target;
    const queryOptions = typeof args[0] === 'function'
        ? args[0]
        : queryArgsOptions( args );

    const queryOptionsIndex = spend( queryOptions.index, reqlChain );
    const indexSortBy = typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortBy;
    const indexSortDirection = ( typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortDirection ) || 'asc';
    const indexString = typeof queryOptionsIndex === 'string' && queryOptionsIndex;
    const argsSortPropValue = typeof args[0] === 'string' && args[0];
    const indexName = indexSortBy || indexString || 'id';
    const tableIndexTuple = mockdbStateTableGetIndexTuple( dbState, queryState.tablename, indexName );
    const sortDirection = isAscending => (
        isAscending * ( indexSortDirection === 'asc' ? 1 : -1 ) );

    const getSortFieldValue = doc => {
        let value;

        if ( typeof queryOptions === 'function' ) {
            value = spend( queryOptions, reqlChain, doc );
        } else if ( argsSortPropValue ) {
            value = doc[argsSortPropValue];
        } else {
            value = mockdbTableDocGetIndexValue( doc, tableIndexTuple, spend );
        }

        return value;
    };

    queryState.target = queryTarget.sort( ( doca, docb ) => {
        const docaField = getSortFieldValue( doca, tableIndexTuple );
        const docbField = getSortFieldValue( docb, tableIndexTuple );

        return sortDirection( docaField < docbField ? -1 : 1 );
    });

    return queryState;
};

// Return the hour in a time object as a number between 0 and 23.
reql.hours = queryState => {
    queryState.target = new Date( queryState.target ).getHours();

    return queryState;
};

reql.minutes = queryState => {
    queryState.target = new Date( queryState.target ).getMinutes();

    return queryState;
};

reql.uuid = queryState => {
    queryState.target = uuidv4();

    return queryState;
};

reql.expr = ( queryState, args ) => {
    const [ value ] = args;
    const resolved = spend( value );

    queryState.targetOriginal = resolved;
    queryState.target = resolved;

    return queryState;
};

reql.expr.fn = ( queryState, args ) => {
    queryState.target = queryState.target[args[0]];

    return queryState;
};

reql.coerceTo = ( queryState, args, reqlChain ) => {
    const [ coerceType ] = args;
    let resolved = spend( queryState.target, reqlChain );

    if ( coerceType === 'string' )
        resolved = String( resolved );

    queryState.target = resolved;

    return queryState;
};

reql.upcase = queryState => {
    queryState.target = String( queryState.target ).toUpperCase();

    return queryState;
};

reql.downcase = queryState => {
    queryState.target = String( queryState.target ).toLowerCase();

    return queryState;
};

reql.map = ( queryState, args, reqlChain ) => {
    const [ func ] = args;

    queryState.target = queryState
        .target.map( t => spend( func, reqlChain, t ) );

    return queryState;
};

reql.without = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;

    args = spend( args, reqlChain );

    queryState.target = args.reduce( arg => {
        delete queryTarget[arg];

        return queryTarget;
    }, queryTarget );

    return queryState;
};

// Call an anonymous function using return values from other
// ReQL commands or queries as arguments.
reql.do = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const [ doFn ] = args.slice( -1 );
    const spentArgs = !isReqlObj( doFn ) && typeof doFn === 'function'
        ? spend( args.slice( 0, -1 ), reqlChain )
        : spend( args, reqlChain );

    if ( !isReqlObj( doFn ) && typeof doFn === 'function' ) {
        queryState.target = queryTarget
            ? spend( doFn, reqlChain, queryTarget )
            : doFn( ...spentArgs );
    } else if ( isReqlObj( args[0]) ) {
        [ queryState.target ] = spentArgs;
    } else if ( typeof doFn !== 'undefined' ) {
        queryState.target = doFn;
    }

    return queryState;
};

reql.or = ( queryState, args, reqlChain ) => {
    queryState.target = args.reduce( ( current, value ) => Boolean(
        current || spend( value, reqlChain )
    ), queryState.target );

    return queryState;
};

reql.and = ( queryState, args, reqlChain ) => {
    queryState.target = args.reduce( ( current, value ) => Boolean(
        current && spend( value, reqlChain )
    ), queryState.target );

    return queryState;
};

// if the conditionals return any value but false or null (i.e., “truthy” values),
// reql.branch = {};
reql.branch = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const isResultTruthy = result => (
        result !== false && result !== null );

    const nextCondition = ( condition, branches ) => {
        const conditionResult = spend( condition, reqlChain, queryState.target );

        if ( branches.length === 0 )
            return conditionResult;

        if ( isResultTruthy( conditionResult ) ) {
            return spend( branches[0], reqlChain, queryTarget );
        }

        return nextCondition( branches[1], branches.slice( 2 ) );
    };

    queryState.target = nextCondition( args[0], args.slice( 1 ) );

    return queryState;
};

// Rethink has its own alg for finding distinct,
// but unique by ID should be sufficient here.
reql.distinct = ( queryState, args, reqlChain, dbState ) => {
    const queryOptions = queryArgsOptions( args );

    if ( Array.isArray( queryState.target )
        && queryState.tablename

        // skip if target is filtered, concatenated or manipulated in some way
        && !/string|boolean|number/.test( typeof queryState.target[0]) ) {
        const primaryKey = queryOptions.index
            || mockdbStateTableGetPrimaryKey( dbState, queryState.tablename );

        const keys = {};
        queryState.target = queryState.target.reduce( ( disti, row ) => {
            const value = row[primaryKey];

            if ( !keys[value]) {
                keys[value] = true;
                disti.push( value );
            }

            return disti;
        }, []);
    } else if ( Array.isArray( queryState.target ) ) {
        queryState.target = queryState.target.filter(
            ( item, pos, self ) => self.indexOf( item ) === pos );
    } else if ( Array.isArray( args[0]) ) {
        queryState.target = args[0].filter(
            ( item, pos, self ) => self.indexOf( item ) === pos );
    }

    return queryState;
};

reql.union = ( queryState, args, reqlChain ) => {
    const queryOptions = queryArgsOptions( args );

    if ( queryOptions )
        args.splice( -1, 1 );

    let res = args.reduce( ( argData, value ) => {
        value = spend( value, reqlChain );
        return argData.concat( value );
    }, queryState.target );

    if ( queryOptions && queryOptions.interleave ) {
        res = res.sort(
            ( a, b ) => compare( a, b, queryOptions.interleave )
        );
    }

    queryState.target = res;

    return queryState;
};

reql.table = ( queryState, args, reqlChain, dbState ) => {
    const [ tablename ] = args;
    const db = mockdbStateSelectedDb( dbState );
    const table = db[tablename];

    queryState.tablename = tablename;
    queryState.tablelist = table;
    queryState.target = table.slice();

    return queryState;
};

// r.args(array) → special
reql.args = ( queryState, args, reqlChain ) => {
    args = spend( args[0], reqlChain );

    if ( !Array.isArray( args ) ) {
        throw new Error( 'args must be an array' );
    }

    queryState.target = args;

    return queryState;
};

reql.desc = ( queryState, args, reqlChain ) => {
    queryState.target = {
        sortBy: spend( args[0], reqlChain ),
        sortDirection: 'desc'
    };

    return queryState;
};

reql.asc = ( queryState, args, reqlChain ) => {
    queryState.target = {
        sortBy: spend( args[0], reqlChain ),
        sortDirection: 'asc'
    };

    return queryState;
};

reql.run = queryState => {
    if ( queryState.error ) {
        throw new Error( queryState.error );
    }

    return queryState.target;
};

reql.serialize = queryState => JSON.stringify( queryState.chain );

reql.changes = ( queryState, args ) => {
    const options = queryArgsOptions( args ) || {};

    queryState.isChanges = true;
    queryState.includeInitial = Boolean( options.includeInitial );

    return queryState;
};

reql.forEach =  ( queryState, args, reqlChain ) => {
    const [ forEachFn ] = args;

    if ( args.length !== 1 ) {
        queryState.error = mockdbResErrorArgumentsNumber(
            'forEach', 1, args.length );
        queryState.target = null;

        return queryState;
    }

    queryState.target = queryState.target.reduce( ( st, arg ) => {
        const result = spend( forEachFn, reqlChain, arg );

        return mockdbStateAggregate( st, result );
    }, {});

    return queryState;
};

// cursor may target a table or a document
reql.getCursor = ( queryState, args, reqlChain, dbState, tables ) => {
    const queryTarget = queryState.target;
    const queryOptions = queryArgsOptions( args );
    const tableName = queryState.tablename;
    const cursorTargetType = tableName
        ? ( Array.isArray( queryTarget ) ? 'table' : 'doc' ) : 'expr';

    let cursors = null;
    if ( cursorTargetType === 'doc' ) {
        cursors = mockdbStateTableDocCursorsGetOrCreate(
            dbState, tableName, queryTarget );
    } else if ( cursorTargetType === 'table' ) {
        cursors = mockdbStateTableCursorsGetOrCreate(
            dbState, tableName );
    }

    const cursorIndex = cursors ? cursors.length : null;

    const cursor = new Readable({
        objectMode: true,
        read ( size ) {
            const item = Array.isArray( queryTarget )
                ? queryTarget.pop()
                : queryTarget;

            if ( !item ) {
                this.push( null );
                return;
            }
            this.push( item );
        }
    });

    cursor.close = () => {
        cursor.destroy();

        if ( cursorTargetType === 'table' )
            dbState = mockdbStateTableCursorSplice( dbState, tableName, cursorIndex );
        else if ( cursorTargetType === 'doc' )
            dbState = mockdbStateTableDocCursorSplice( dbState, tableName, queryTarget, cursorIndex );
    };

    if ( cursorTargetType === 'table' ) {
        dbState = mockdbStateTableCursorSet( dbState, tableName, cursor );
    } else if ( cursorTargetType === 'doc' ) {
        dbState = mockdbStateTableDocCursorSet( dbState, tableName, queryTarget, cursor );
    }

    if ( !queryState.isChanges || queryOptions.includeInitial === true ) {
        if ( cursorTargetType === 'table' ) {
            const changes = queryTarget.map( doc => ({
                old_val: null,
                new_val: doc
            }) );

            dbState = mockdbStateTableCursorsPushChanges(
                dbState, tableName, changes );
        }
    }

    return cursor;
};

reql.isReql = true;

export default reql;
