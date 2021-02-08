import queryReql from './mockdbReql.js';

const staleChains = Object.keys( queryReql ).reduce( ( prev, queryName ) => {
    prev[queryName] = function ( ...args ) {
        this.record.push({
            queryName,
            queryArgs: args
        });

        if ( /^(serialize|run|getCursor|connect|connectPool)$/.test( queryName ) ) {
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
// eslint-disable-next-line prefer-object-spread
const mockdbChain = ( state, tables, queryChainResolve ) => Object.assign({
    state,
    tables,
    queryChainResolve: ( record, startState ) => (
        queryChainResolve( record, state, tables, startState ) )
}, chains );

export default mockdbChain;
