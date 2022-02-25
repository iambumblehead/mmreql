import queryReql from './mockdbReql.mjs';

const resolvingQueries = [
  'serialize',
  'run',
  'getCursor',
  'connect',
  'connectPool',
  'getPoolMaster'
];

const firstTermQueries = [
  'desc',
  'asc'
];

// eslint-disable-next-line security/detect-non-literal-regexp
const isResolvingQueryRe = new RegExp( `^(${resolvingQueries.join( '|' )})$` );
// eslint-disable-next-line security/detect-non-literal-regexp
const isFirstTermQueryRe = new RegExp( `^(${firstTermQueries.join( '|' )})$` );

const staleChains = Object.keys( queryReql ).reduce( ( prev, queryName ) => {
  prev[queryName] = function ( ...args ) {
    // must not follow another term, ex: r.expr( ... ).desc( 'foo' )
    if ( this.record.length && isFirstTermQueryRe.test( queryName ) ) {
      throw new Error( `.${queryName} is not a function` );
    }

    this.record.push({
      queryName,
      queryArgs: args
    });

    if ( isResolvingQueryRe.test( queryName ) ) {
      let res;

      if ( queryName === 'getCursor' ) {
        try {
          res = this.queryChainResolve( this.record, args[0]);
        } catch ( e ) {
          res = { next: () => new Promise( ( resolve, reject ) => reject( e ) ) };
        }
      } else {
        res = this.queryChainResolve( this.record, args[0]);
      }

      this.record.pop();

      return res;
    }

    return Object.assign( ( ...fnargs ) => {
      const record = this.record.slice();
      record.push({
        queryName: `${queryName}.fn`,
        queryArgs: fnargs
      });

      // this is called when using row attrbute look ex,
      // .filter( row => row( 'field' )( 'attribute' ).eq( 'OFFLINE' ) )
      function attributeFn ( ...attributeFnArgs ) {
        record.push({
          queryName: 'getField',
          queryArgs: attributeFnArgs
        });

        return { ...attributeFn, record, ...staleChains };
      }

      Object.assign( attributeFn, { ...this, record, ...staleChains });

      return attributeFn;
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
