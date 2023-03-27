import {
  mmRecChainCreate,
  mmRecChainSubCreate,
  mmRecChainCreateNext,
  mmRecChainNext,
  mmRecChainClear
} from './mmRecChain.mjs';

import queryReql, {
  spend,
  spendCursor
} from './mockdbReql.mjs';

import {
  mockdbSpecFromRawArg
} from './mockdbSpec.mjs';

const resolvingQueries = [
  'serialize',
  'run',
  'drain',
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
const isResolvingQueryRe = new RegExp(`^(${resolvingQueries.join('|')})$`);
// eslint-disable-next-line security/detect-non-literal-regexp
const isFirstTermQueryRe = new RegExp(`^(${firstTermQueries.join('|')})$`);

const staleChains = Object.keys(queryReql).reduce((prev, queryName) => {
  prev[queryName] = function (...args) {
    const isResolvingQuery = isResolvingQueryRe.test(queryName);
    const chain = mmRecChainNext(this, {
      queryName,
      queryArgs: isResolvingQuery
        ? args : mockdbSpecFromRawArg(args, mockdbChain)
    });

    // must not follow another term, ex: r.expr( ... ).desc( 'foo' )
    if (chain.recs.length > 1 && isFirstTermQueryRe.test(queryName)) {
      throw new Error(`.${queryName} is not a function`);
    }

    if (isResolvingQuery) {
      let res;
      if (queryName === 'serialize') {
        res = JSON.stringify(chain.recs);
      } else {
        res = (queryName === 'getCursor' ? spendCursor : spend)(this.state, {
          recId: 'start'
        }, mmRecChainSubCreate(chain));
      }

      mmRecChainClear(chain);

      return res;
    }

    return Object.assign((...fnargs) => {
      // this.recIndex = chain.recIndex;
      // const chainNext = mmRecChainCreateNext(this, {
      const chainNext = mmRecChainCreateNext(chain, {
        queryName: `${queryName}.fn`,
        queryArgs: mockdbSpecFromRawArg(fnargs, mockdbChain)
      });
        
      // this is called when using row attrbute look ex,
      // .filter( row => row( 'field' )( 'attribute' ).eq( 'OFFLINE' ) )
      function attributeFn (...attributeFnArgs) {
        const chainNextAttr = mmRecChainCreateNext(chainNext, {
          queryName: `getField`,
          queryArgs: mockdbSpecFromRawArg(attributeFnArgs, mockdbChain)
        });

        return Object.assign(attributeFn, chainNextAttr, staleChains);
      }
      return Object.assign(attributeFn, chainNext, staleChains);
    }, chain, staleChains);
  };

  return prev;
}, {});

const chains = Object.keys(queryReql).reduce((prev, queryName) => {
  prev[queryName] = function (...args) {
    return staleChains[queryName].apply(
      mmRecChainCreate({
        state: this.state
      }), args);
  };

  return prev;
}, {});

// this complex flow is an optimization.
// query record calls are looped and defined once only.
// record calls are mapped to functions 'applied' to unique chain state
const mockdbChain = state => ({
  state,
  ...chains
});

export default mockdbChain;
