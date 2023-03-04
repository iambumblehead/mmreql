import queryReql from './mockdbReql.mjs';

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

const msgClearQueryLevelDescription = `
By default, the query chain is cleared to reduce memory.

Use { clearQueryLevelNum: 0 } to disable query chain cleanup behaviour and
use r.getPool().drain() later to clear the query chain from r instance.

  2 is the default and most aggressive clearQueryLevelNum. Query behaviour is
    accurate and if the query cannot be computed, this error message is shown,
  1 uses a small amount of memory which could be ignored in most cases, but
    query behaviour is less accurate when complex queries reuse one chain
    multiple times,
  0 uses the most memory and query chains should be cleared manually

ex,

const { r } = rethinkdbMocked({ clearQueryLevelNum: 0 })

const doc = r.expr({ id: 1 })
console.log(await doc.hasFields('name').not().run())
console.log(await doc.merge({ name: 'fred' }).run())
r.getPool().drain()`;


const errReusingQueryRequiresMemoryManagement = queryName => new Error([
  `[!!!] error: could not reuse the query chain to attach query "${queryName}".`,
  'This happens because, by default, the query chain is cleared to reduce memory.',
  `\n\n${msgClearQueryLevelDescription}`
].join(' '));

const staleChains = Object.keys(queryReql).reduce((prev, queryName) => {
  prev[queryName] = function (...args) {
    // when a query is used to compose multiple, longer query chains
    // recordindex is a unique point at the chain used to recover
    // the record list from that time, rather than using records
    // added from external chains that might have added queries
    // to this base chain
    const { clearQueryLevelNum } = this.state;
    const recIndexGet = () => this.recHist.length - 1;
    const recListFromIndex = index => {
      // level 1 uses a single record list and does not store or
      // recall other query chains history
      if (clearQueryLevelNum === 1)
        return this.record;

      if (!Array.isArray(this.recHist[index]))
        throw errReusingQueryRequiresMemoryManagement(queryName);

      return this.recHist[index].slice();
    };
    const recClear = records => {
      if (clearQueryLevelNum !== 1 || queryName === 'drain')
        records.splice(0,records.length);

      if (clearQueryLevelNum > 0 || queryName === 'drain') {
        this.recHist = [ [] ];
      }
    };
    const recPush = (records, atom) => {
      records.push(atom);

      if (clearQueryLevelNum === 1)
        return records.length;

      return this.recHist.push(records.slice());
    };

    this.record = recListFromIndex(this.recIndex);
    // must not follow another term, ex: r.expr( ... ).desc( 'foo' )
    if (this.record.length && isFirstTermQueryRe.test(queryName)) {
      throw new Error(`.${queryName} is not a function`);
    }
    
    recPush(this.record, {
      queryName,
      queryArgs: args
    });

    if (isResolvingQueryRe.test(queryName)) {
      let res;

      if (queryName === 'getCursor') {
        try {
          res = this.queryChainResolve(this.record, args[0]);
        } catch (e) {
          res = { next: () => new Promise((resolve, reject) => reject(e)) };
        }
      } else {
        res = this.queryChainResolve(this.record, args[0]);
      }

      this.record.pop();
      recClear(this.record);

      return res;
    }

    return Object.assign((...fnargs) => {
      const record = this.record.slice();

      recPush(record, {
        queryName: `${queryName}.fn`,
        queryArgs: fnargs
      });

      // this is called when using row attrbute look ex,
      // .filter( row => row( 'field' )( 'attribute' ).eq( 'OFFLINE' ) )
      function attributeFn (...attributeFnArgs) {
        recPush(record, {
          queryName: 'getField',
          queryArgs: attributeFnArgs
        });
        
        return { ...attributeFn, record, recIndex: recIndexGet(), ...staleChains };
      }

      Object.assign(attributeFn, { ...this, record, recIndex: recIndexGet(), ...staleChains });

      return attributeFn;
    }, this, staleChains, { recIndex: recIndexGet() });
  };

  return prev;
}, {});

const chains = Object.keys(queryReql).reduce((prev, queryName) => {
  prev[queryName] = function (...args) {
    return staleChains[queryName].apply({
      state: this.state,
      tables: this.tables,
      queryChainResolve: this.queryChainResolve,

      // queryName reference helps to resolve 'args' list result
      queryName,
      record: [],
      recIndex: 0,
      recHist: [ [] ]
    }, args);
  };

  return prev;
}, {});

// this complex flow is an optimization.
// query record calls are looped and defined once only.
// record calls are mapped to functions 'applied' to unique chain state
const mockdbChain = (state, queryChainResolve) => ({
  state,
  queryChainResolve: (record, startState) => (
    queryChainResolve(record, state, startState)),
  ...chains
});

export default mockdbChain;
