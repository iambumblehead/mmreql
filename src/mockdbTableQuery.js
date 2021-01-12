import {
    mockdbStateTableIndexAdd,
    mockdbStateTableIndexList
} from './mockdbState.js';

const indexCreate = ( mockdb, tableName, args ) => {
    mockdbStateTableIndexAdd( mockdb, tableName, args[0]);

    // should throw ReqlRuntimeError if index exits already
    return {
        data: [ { created: 1 } ],
        isSingle: true,
        wrap: false
    };
};

const indexWait = ( mockdb, tableName ) => {
    const tableIndexList = mockdbStateTableIndexList( mockdb, tableName );

    return {
        data: tableIndexList.map( indexName => ({
            index: indexName,
            ready: true,
            function: 1234,
            multi: false,
            geo: false,
            outdated: false
        }) ),
        isSingle: true,
        wrap: false
    };
};

const indexList = ( mockdb, tableName ) => {
    const tableIndexList = mockdbStateTableIndexList( mockdb, tableName );

    return {
        data: [ tableIndexList ],
        isSingle: true,
        wrap: false
    };
};

export {
    indexCreate,
    indexWait,
    indexList
};
