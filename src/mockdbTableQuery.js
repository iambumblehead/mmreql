import {
    mockdbStateTableIndexAdd,
    mockdbStateTableIndexList
} from './mockdbState.js';

import {
    mockdbTableGetDocument,
    mockdbTableSetDocument,
    mockdbTableGetDocuments,
    mockdbTableSetDocuments
} from './mockdbTable.js';

import {
    unwrapObject
} from './mockdbReql.js';

import {
    mockdbResChangesFieldCreate,
    mockdbResErrorDuplicatePrimaryKey
} from './mockdbRes.js';

const queryValueAsList = value => (
    Array.isArray( value ) ? value : [ value ]);

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

const insert = ( mockdb, tableName, args, table, documents ) => {
    documents = queryValueAsList( args[0]);
    const options = args[1] || {};
    const [ existingDoc ] = mockdbTableGetDocuments(
        table, documents.map( doc => doc.id ) );

    if ( existingDoc ) {
        return {
            isSingle: true,
            wrap: false,
            data: [
                mockdbResChangesFieldCreate({
                    errors: 1,
                    firstError: mockdbResErrorDuplicatePrimaryKey(
                        existingDoc,
                        documents.find( doc => doc.id === existingDoc.id )
                    )
                })
            ]
        };
    }

    [ table, documents ] = mockdbTableSetDocuments(
        table, documents.map( doc => unwrapObject( doc ) ) );

    return {
        data: [
            mockdbResChangesFieldCreate({
                inserted: documents.length,
                generated_keys: documents.map( doc => doc.id ),
                changes: options.returnChanges === true
                    ? documents.map( doc => ({
                        old_val: null,
                        new_val: doc
                    }) )
                    : undefined
            })
        ],
        isSingle: true,
        wrap: false
    };
};

const update = ( mockdb, tableName, targetDocuments, table, args ) => {
    const updateProps = args[0];
    const updatedDocuments = targetDocuments.reduce( ( updated, targetDoc ) => {
        let tableDoc = mockdbTableGetDocument( table, targetDoc.id );

        if ( tableDoc ) {
            Object.assign( tableDoc, updateProps || {});
            [ table, tableDoc ] = mockdbTableSetDocument( table, tableDoc );

            updated.push( tableDoc );
        }

        return updated;
    }, []);

    return {
        isSingle: true,
        wrap: false,
        data: [
            mockdbResChangesFieldCreate({
                replaced: updatedDocuments.length
            })
        ]
    };
};

export {
    indexCreate,
    indexWait,
    indexList,
    insert,
    update
};
