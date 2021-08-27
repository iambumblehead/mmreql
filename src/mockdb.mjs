import queryReql from './mockdbReql.mjs';
import mockdbChain from './mockdbChain.mjs';

import {
    mockdbStateCreate,
    mockdbStateDbCreate,
    mockdbStateTableSet,
    mockdbStateTableCreate
} from './mockdbState.mjs';

// obj not instances of mockdbReql fn...
const objlookup = ( nsstr, obj ) => String( nsstr )
    .split( '.' )
    .reduce( ( a, b ) => ( b in a ? a[b] : null ), obj );

const queryChainResolve = ( chain, dbState, startState ) => {
    let queryState = { target: startState, chain };

    const chainNext = chainRest => {
        if ( chainRest.length ) {
            if ( queryState.error
                && ( chainRest[0].queryName !== 'default' && chainRest.length > 1 ) ) {
                return chainNext( chainRest.slice( 1 ) );
            }

            queryState = objlookup( chainRest[0].queryName, queryReql )(
                queryState,
                chainRest[0].queryArgs,
                () => mockdbChain( dbState, queryChainResolve, queryState.target ),
                dbState
            );

            return chainNext( chainRest.slice( 1 ) );
        }

        return queryState;
    };

    return chainNext( chain );
};

const buildChain = ( dbState = {}) => {
    const r = mockdbChain( dbState, queryChainResolve );

    // make, for example, r.add callable through r.row.add
    Object.assign( r.row, r );

    return {
        r: Object.assign( ( ...args ) => r.expr( ... args ), r ),
        dbState
    };
};

const buildDb = ( tables, config ) => {
    const dbConfig = config || mockdbStateCreate(
        ( tables[0] && tables[0].db ) ? tables[0] : {});
    const dbConfigTables = ( tables[0] && tables[0].db )
        ? tables.slice( 1 )
        : tables;

    return dbConfigTables.reduce( ( dbState, tablelist, i, arr ) => {
        const tableConfig = Array.isArray( tablelist[1]) && tablelist[1];

        if ( !Array.isArray( tablelist ) ) {
            dbState = mockdbStateDbCreate( dbState, tablelist.db );
            dbState = buildDb( arr.slice( i + 1 ), dbState );
            arr.splice( 1 );
            return dbState;
        }

        dbState = mockdbStateTableCreate( dbState, tablelist[0], tableConfig[0]);
        dbState = mockdbStateTableSet(
            dbState, tablelist[0], tablelist.slice( tableConfig ? 2 : 1 ) );

        return dbState;
    }, dbConfig );
};

export default configList => buildChain(
    buildDb( configList || []) );
