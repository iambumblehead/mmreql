import {
    isPlainObject,
    isMatch,
    flatten,
    pick
} from 'lodash/fp.js';

import {
    mockdbStateTableIndexAdd,
    mockdbStateTableIndexList,
    mockdbStateTableGetIndexTuple
} from './mockdbState.js';

import {
    mockdbTableGetDocument,
    mockdbTableSetDocument,
    mockdbTableGetDocuments,
    mockdbTableSetDocuments,
    mockdbTableDocGetIndexValue,
    mockdbTableSet
} from './mockdbTable.js';

import {
    PseudoQuery,
    unwrapObject,
    unwrap
} from './mockdbReql.js';

import {
    mockdbResChangesFieldCreate,
    mockdbResErrorDuplicatePrimaryKey
} from './mockdbRes.js';

const queryValueAsList = value => (
    Array.isArray( value ) ? value : [ value ]);

// return last query argument (optionally) provides query configurations
const queryArgsOptions = ( queryArgs, queryOptionsDefault = {}) => {
    const queryOptions = queryArgs.slice( -1 )[0] || {};
    const queryOptionsType = typeof queryOptions;

    return ( queryOptions && /object|function/.test( queryOptionsType ) )
        ? queryOptions
        : queryOptionsDefault;
};

const reql = {};

// r.table('comments')
//    .indexCreate('postAndDate', [r.row("postId"), r.row("date")]).run()
//
reql.indexCreate = ( mockdb, tableName, targetDocuments, table, args ) => {
    const fields = Array.isArray( args[1]) ? args[1] : [];
    const config = queryArgsOptions( args );

    mockdbStateTableIndexAdd( mockdb, tableName, args[0], fields, config );

    // should throw ReqlRuntimeError if index exits already
    return {
        data: [ { created: 1 } ],
        isSingle: true,
        wrap: false
    };
};

reql.indexWait = ( mockdb, tableName ) => {
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

reql.indexList = ( mockdb, tableName ) => {
    const tableIndexList = mockdbStateTableIndexList( mockdb, tableName );

    return {
        data: [ tableIndexList ],
        isSingle: true,
        wrap: false
    };
};

reql.insert = ( mockdb, tableName, targetDocuments, table, args ) => {
    let documents = queryValueAsList( args[0]);
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

reql.update = ( mockdb, tableName, targetDocuments, table, args ) => {
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

reql.get = ( mockdb, tableName, targetDocuments, table, args ) => {
    const primaryKeyValue = unwrapObject( args[0]);
    const tableDoc = mockdbTableGetDocument( table, primaryKeyValue );
    // if ( !tableDoc ) {
    //     return {
    //         error: 'DocumentNotFound'
    //     };
    // }
    return {
        data: [ tableDoc ],
        isSingle: true
    };
};

reql.getAll = ( mockdb, tableName, targetDocuments, table, args ) => {
    const queryOptions = queryArgsOptions( args );
    const indexName = queryOptions.index || 'id';
    const indexTargetValue = Array.isArray( args[0]) ? args[0].join() : args[0];
    const tableIndexTuple = mockdbStateTableGetIndexTuple( mockdb, tableName, indexName );

    table = table.filter( doc => (
        mockdbTableDocGetIndexValue( doc, tableIndexTuple ) === indexTargetValue
    ) );

    // rethink output array is not in-order
    return {
        isSingle: false,
        wrap: true,
        data: table.sort( () => 0.5 - Math.random() )
    };
};

reql.nth = ( mockdb, tableName, targetDocuments, table, args ) => {
    if ( args[0] >= targetDocuments )
        return {
            error: `ReqlNonExistanceError: Index out of bounds: ${args[0]}`
        };

    return {
        data: [ targetDocuments[args[0]] ],
        isSingle: true,
        wrap: false
    };
};

reql.default = ( mockdb, tableName, targetDocuments, table, args ) => ({
    data: [
        args.reduce( ( current, value ) => (
            ( typeof current === 'undefined' ? unwrapObject( value ) : current )
        ), targetDocuments[0])
    ],
    isSingle: true
});

reql.append = () => {
    throw new Error( 'not yet implemented' );
};

reql.delete = ( mockdb, tableName, targetDocuments, table ) => {
    const indexName = 'id';
    const tableIndexTuple = mockdbStateTableGetIndexTuple( mockdb, tableName, indexName );
    const targetIds = targetDocuments
        .map( doc => mockdbTableDocGetIndexValue( doc, tableIndexTuple ) );
    // eslint-disable-next-line security/detect-non-literal-regexp
    const targetIdRe = new RegExp( `^(${targetIds.join( '|' )})$` );
    const tableFiltered = table.filter( doc => !targetIdRe.test(
        mockdbTableDocGetIndexValue( doc, tableIndexTuple ) ) );
    const deleted = table.length - tableFiltered.length;

    mockdbTableSet( table, tableFiltered );

    return {
        data: [
            mockdbResChangesFieldCreate({
                deleted
            })
        ],
        isSingle: true,
        wrap: false
    };
};

reql.contains = ( mockdb, tableName, targetDocuments, table, args ) => {
    if ( !args.length ) {
        throw new Error( 'Rethink supports contains(0) but rethinkdbdash does not.' );
    }

    return {
        data: [ args.every( predicate => {
            // A PseudoQuery argument is treated as a scalar argument,
            // and a direct function argument is treated as a predicate
            if ( typeof predicate === 'function' && !predicate.toFunction ) {
                return targetDocuments.some( item => unwrap( predicate, item ) );
            }
            return targetDocuments.includes( unwrap( predicate ) );
        }) ],
        isSingle: true,
        wrap: false
    };
};

reql.getField = ( mockdb, tableName, targetDocuments, table, args ) => ({
    data: [ targetDocuments[0][args[0]] ],
    isSingle: true,
    wrap: false
});

reql.filter = ( mockdb, tableName, targetDocuments, table, args ) => {
    const [ predicate ] = args;
    return targetDocuments.filter( item => {
        const itemPredicate = unwrap( predicate, item );
        if ( isPlainObject( itemPredicate ) )
            return isMatch( itemPredicate, item );
        return itemPredicate;
    });
};

reql.count = ( mockdb, tableName, targetDocuments ) => ({
    isSingle: true,
    wrap: false,
    data: [ targetDocuments.length ]
});

reql.pluck = ( mockdb, tableName, targetDocuments, table, args ) => {
    const props = flatten( args.map( unwrap ).map( arg => arg.args || arg ) );
    return targetDocuments.map( pick( props ) );
};

reql.slice = ( mockdb, tableName, targetDocuments, table, args ) => {
    let [ begin, end ] = args;
    begin = unwrap( begin );
    end = unwrap( end );
    return targetDocuments.slice( begin, end ); // Don't support changing bounds yet
};

reql.skip = ( mockdb, tableName, targetDocuments, table, args ) => {
    const count = unwrap( args[0]);
    return targetDocuments.slice( count );
};

reql.eqJoin = ( mockdb, tableName, targetDocuments, table, args ) => {
    let [ leftKey, otherSequence ] = args; // eslint-disable-line prefer-const
    leftKey = unwrap( leftKey );

    // eslint-disable-next-line no-underscore-dangle
    const otherData = otherSequence._getResults({ wrap: false });
    return targetDocuments.map( item => ({
        left: item,
        right: otherData.find( other => other.id === item[leftKey])
    }) ).filter( ({ right }) => right );
};

reql.innerJoin = ( mockdb, tableName, targetDocuments, table, args ) => {
    const [ otherSequence, joinFunc ] = args;
    const otherItems = unwrap( otherSequence );

    return flatten( targetDocuments.map( item => otherItems.map( otherItem => {
        const resultFunc = joinFunc( new PseudoQuery( item ), new PseudoQuery( otherItem ) );
        const match = resultFunc.toFunction()();
        return match ? {
            left: item,
            right: otherItem
        } : null;
    }) ) ).filter( x => x !== null );
};

reql.merge = ( mockdb, tableName, targetDocuments, table, args ) => (
    targetDocuments.map( item => {
        const merges = args.map( arg => unwrap( arg, item ) );

        return Object.assign({}, item, ...merges );
    })
);

reql.orderBy = ( mockdb, tableName, targetDocuments, table, args ) => {
    const queryOptions = queryArgsOptions( args );
    const queryOptionsIndex = queryOptions.index;
    const indexSortBy = typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortBy;
    const indexSortDirection = ( typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortDirection ) || 'asc';
    const indexString = typeof queryOptionsIndex === 'string' && queryOptionsIndex;
    const indexName = indexSortBy || indexString || 'id';
    const tableIndexTuple = mockdbStateTableGetIndexTuple( mockdb, tableName, indexName );
    const sortDirection = isAscending => (
        isAscending * ( indexSortDirection === 'asc' ? 1 : -1 ) );

    return targetDocuments.sort( ( doca, docb ) => {
        const sortFieldValue = doc => (
            typeof queryOptions === 'function'
                ? unwrap( queryOptions, doc )
                : mockdbTableDocGetIndexValue( doc, tableIndexTuple )
        );

        const docaField = sortFieldValue( doca, tableIndexTuple );
        const docbField = sortFieldValue( docb, tableIndexTuple );

        return sortDirection( docaField < docbField ? -1 : 1 );
    });
};

export default reql;
