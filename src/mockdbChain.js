import queryReql from './mockdbReql.js';

const resolvingQueries = [
    'serialize',
    'run',
    'getCursor',
    'connect',
    'connectPool',
    'getPoolMaster'
];

// eslint-disable-next-line security/detect-non-literal-regexp
const isResolvingQueryRe = new RegExp( `^(${resolvingQueries.join( '|' )})$` );

const staleChains = Object.keys( queryReql ).reduce( ( prev, queryName ) => {
    prev[queryName] = function ( ...args ) {
        this.record.push({
            queryName,
            queryArgs: args
        });

        if ( isResolvingQueryRe.test( queryName ) ) {
            const res = this.queryChainResolve( this.record, args[0]);

            this.record.pop();

            return res;
        }

        return Object.assign( ( ...fnargs ) => {
            const record = this.record.slice();
            record.push({
                queryName: `${queryName}.fn`,
                queryArgs: fnargs
            });

            return { ...this, record, ...staleChains };
        }, this, staleChains );
    };

    return prev;
}, {});

const chains = Object.keys( queryReql ).reduce( ( prev, queryName ) => {
    prev[queryName] = function ( ...args ) {
        return staleChains[queryName].apply({
            state: this.state,
            tables: this.tables,
            queryChainResolve: this.queryChainResolve,

            // queryName reference helps to resolve 'args' list result
            queryName,
            record: []
        }, args );
    };

    return prev;
}, {});

// this complex flow is an optimization.
// query record calls are looped and defined once only.
// record calls are mapped to functions 'applied' to unique chain state
const mockdbChain = ( state, queryChainResolve ) => ({
    state,
    queryChainResolve: ( record, startState ) => (
        queryChainResolve( record, state, startState ) ),
    ...chains
});

export default mockdbChain;
