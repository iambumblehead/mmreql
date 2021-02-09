import queryReql from './mockdbReql.js';
import mockdbChain from './mockdbChain.js';

import {
    mockdbStateTableCreate
} from './mockdbState.js';

// obj not instances of mockdbReql fn...
const objlookup = ( nsstr, obj ) => String( nsstr )
    .split( '.' )
    .reduce( ( a, b ) => ( b in a ? a[b] : null ), obj );

const queryChainResolve = ( chain, dbState, tables, startState ) => {
    let queryState = { target: startState, chain };

    const chainNext = chainRest => {
        if ( chainRest.length ) {
            queryState = objlookup( chainRest[0].queryName, queryReql )(
                queryState,
                chainRest[0].queryArgs,
                () => mockdbChain( dbState, tables, queryChainResolve, queryState.target ),
                dbState,
                tables
            );

            return chainNext( chainRest.slice( 1 ) );
        }

        return queryState;
    };

    return chainNext( chain );
};

const buildChain = ( tables = {}, dbState = {}) => {
    const r = mockdbChain( dbState, tables, queryChainResolve );

    // make, for example, r.add callable through r.row.add
    Object.assign( r.row, r );

    return {
        r,
        tables,
        dbState
    };
};

export default tables => {
    const { tableMap, dbState } = ( tables || []).reduce( ( map, tablelist ) => {
        map.tableMap[tablelist[0]] = tablelist.slice( 1 );
        map.dbState = mockdbStateTableCreate( map.dbState, tablelist[0]);

        return map;
    }, {
        tableMap: {},
        dbState: {}
    });

    return buildChain( tableMap, dbState );
};
