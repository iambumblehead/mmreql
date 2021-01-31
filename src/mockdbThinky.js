import queryReql, { spend } from './mockdbTableQuery.js';

const addQueryEvent = ( o, queryfn, queryable ) => {
    queryable = queryable || Object.keys( queryReql ).reduce( ( prev, queryName ) => {
        prev[queryName] = ( ...args ) => queryfn( queryName, args, o );
        return prev;
    }, {});

    return Object.assign( o, queryable );
};

// only loops query methods once and the function can be updated over time
// for each new query
const queryChainCreate = ( queryfn = () => {}) => {
    queryfn = () => {};

    const queryable = Object.keys( queryReql ).reduce( ( prev, queryName ) => {
        prev[queryName] = ( ...args ) => queryfn( queryName, args );
        return prev;
    }, {});

    return ( o, fn ) => {
        queryfn = fn;
        return addQueryEvent( o, queryfn, queryable );
    };
};

const recordingPlay = ( queryState, record ) => {
    if ( record.length ) {
        return recordingPlay(
            queryState[record[0].queryName]( ...record[0].queryArgs ),
            record.slice( 1 ) );
    }

    return queryState.run();
};

const recordingChain = reqlChain => {
    const chain = queryChainCreate();
    const state = {
        record: []
    };

    return chain( state, ( queryName, queryArgs ) => {
        const play = queryState => {
            if ( queryState ) {
                // mutable queryState could be problematic
                return recordingPlay( reqlChain().expr( spend( queryState ) ), state.record );
            }

            return state.record[0].queryArgs[0];
        };

        if ( queryName === 'run' ) {
            return play({ playbackStub: true });
        }

        state.record.push({
            queryName,
            queryArgs
        });

        return Object.assign( ( ...args ) => {
            throw new Error( 'not implemented yet', args );
        }, state, { play });
    });
};

export default function thinkyMock ( tables = {}, dbState = {}) {
    const chainQuery = ( queryState = {}, chain = queryChainCreate(), id = 0 ) => (
        chain( queryState, ( queryName, queryArgs ) => {
            queryState.queryName = queryName;

            if ( queryReql[`${queryName}Playback`]) {
                chain = recordingChain( () => chainQuery({}) );

                return chain[`${queryName}Playback`]( ...queryArgs );
            }

            if ( queryName === 'run' ) {
                if ( queryState.error ) {
                    throw new Error( queryState.error );
                } else {
                    return queryReql[queryName]( queryState );
                }
            }

            if ( !queryReql[queryName]) {
                throw new Error( `not a reql method: ${queryName}` );
            }

            const newqState = queryReql[queryName](
                queryState,
                queryArgs,
                () => chainQuery({}),
                dbState,
                tables
            );

            if ( typeof queryReql[queryName].fn === 'function' ) {
                const doc = ( ...args ) => queryReql[queryName].fn(
                    queryState,
                    args,
                    () => chainQuery({}),
                    dbState,
                    tables
                );

                Object.assign( doc, queryState );

                return chainQuery( doc, chain, id + 1 );
            }

            return chainQuery( newqState, chain, id + 1 );
        })
    );

    const r = addQueryEvent({}, ( queryName, queryArgs ) => {
        const statefulQuery = chainQuery({});

        return statefulQuery[queryName]( ...queryArgs );
    });

    return {
        r,
        dbState
    };
}
