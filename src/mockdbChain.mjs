import queryReql, {
  spend,
  spendCursor,
  reqlARGSSUSPEND
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

const reqlCHAIN = 'reqlCHAIN';

// eslint-disable-next-line security/detect-non-literal-regexp
const isResolvingQueryRe = new RegExp(`^(${resolvingQueries.join('|')})$`);
// eslint-disable-next-line security/detect-non-literal-regexp
const isFirstTermQueryRe = new RegExp(`^(${firstTermQueries.join('|')})$`);

const recIndexGet = that => that.recHist.length - 1;
const recListFromIndex = (that, index) => {
  return that.recHist[index].slice();
};

const recClear = (that, recs) => {
  recs.splice(0, recs.length);

  that.recHist = [ [] ];
};

const recPush = (that, recs, atom) => {
  recs.push(atom);

  that.recHist.push(recs.slice());
};

const staleChains = Object.keys(queryReql).reduce((prev, queryName) => {
  prev[queryName] = function (...args) {
    // when a query is used to compose multiple, longer query chains
    // recordindex is a unique point at the chain used to recover
    // the record list from that time, rather than using recs
    // added from external chains that might have added queries
    // to this base chain
    const isResolvingQuery = isResolvingQueryRe.test(queryName);

    this.recs = recListFromIndex(this, this.recIndex);
    // must not follow another term, ex: r.expr( ... ).desc( 'foo' )
    if (this.recs.length && isFirstTermQueryRe.test(queryName)) {
      throw new Error(`.${queryName} is not a function`);
    }

    recPush(this, this.recs, {
      queryName,
      queryArgs: isResolvingQuery
        ? args : mockdbSpecFromRawArg(args, mockdbChain)
    });

    if (isResolvingQuery) {
      let res;

      if (queryName === 'serialize') {
        res = JSON.stringify(this.recs);
      } else {
        res = (queryName === 'getCursor' ? spendCursor : spend)(this.state, {
          recId: 'start'
        }, {
          type: reqlARGSSUSPEND,
          toString: () => reqlARGSSUSPEND,
          recs: this.recs.slice(),
          recIndex: this.recIndex || 0,
          recHist: this.recHist || [ [] ]
        });
      }

      recClear(this, this.recs);

      return res;
    }

    return Object.assign((...fnargs) => {
      const recs = this.recs.slice();
      const { recHist } = this;

      recPush(this, recs, {
        queryName: `${queryName}.fn`,
        queryArgs: mockdbSpecFromRawArg(fnargs, mockdbChain)
      });

      // this is called when using row attrbute look ex,
      // .filter( row => row( 'field' )( 'attribute' ).eq( 'OFFLINE' ) )
      function attributeFn (...attributeFnArgs) {
        recPush({ recHist }, recs, {
          queryName: 'row',
          queryArgs: mockdbSpecFromRawArg(attributeFnArgs, mockdbChain)
        });
        
        return { ...attributeFn, recs, recIndex: recIndexGet({ recHist }), ...staleChains };
      }

      Object.assign(attributeFn, {
        ...this,
        recs,
        recIndex: recIndexGet(this),
        ...staleChains
      });

      return attributeFn;
    }, this, staleChains, { recIndex: recIndexGet(this) });
  };

  return prev;
}, {});

const chains = Object.keys(queryReql).reduce((prev, queryName) => {
  prev[queryName] = function (...args) {
    return staleChains[queryName].apply({
      state: this.state,
      tables: this.tables,
      recs: [],
      recIndex: 0,
      recHist: [ [] ],
      toString: () => reqlCHAIN
    }, args);
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
