import queryReql from './mockdbReql.js';
import mockdbChain from './mockdbChain.js';

import {
    mockdbStateCreate,
    mockdbStateTableSet,
    mockdbStateTableCreate
} from './mockdbState.js';

// obj not instances of mockdbReql fn...
const objlookup = ( nsstr, obj ) => String( nsstr )
    .split( '.' )
    .reduce( ( a, b ) => ( b in a ? a[b] : null ), obj );

const queryChainResolve = ( chain, dbState, startState ) => {
    let queryState = { target: startState, chain };

    const chainNext = chainRest => {
        if ( chainRest.length ) {
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
        r,
        dbState
    };
};

export default configList => {
    const tables = ( configList || []);
    const dbConfig = mockdbStateCreate(
        ( tables[0] && tables[0].db ) ? tables[0] : {});
    const dbConfigTables = ( tables[0] && tables[0].db )
        ? tables.slice( 1 )
        : tables;

    const mockdb = dbConfigTables.reduce( ( dbState, tablelist ) => {
        dbState = mockdbStateTableCreate( dbState, tablelist[0]);
        dbState = mockdbStateTableSet( dbState, tablelist[0], tablelist.slice( 1 ) );

        return dbState;
    }, dbConfig );

    return buildChain( mockdb );
};
