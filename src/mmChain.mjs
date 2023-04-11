import {
  mmChainRecNext,
  mmChainRecFnCreate
} from './mmChainRec.mjs';

import mmQuery, {
  spend
} from './mmQuery.mjs';

import mmChainRawArg from './mmChainRawArg.mjs';

import {
  mmEnumQueryNameIsRESOLVINGRe,
  mmEnumQueryNameIsFIRSTTERMRe
} from './mmEnum.mjs';

const chainFnCreate = (chains, queryName) => function (...args) {
  const chain = mmChainRecNext(this, {
    queryName,
    queryArgs: mmChainRawArg(args, mmChain)
  });

  // must not follow another term, ex: r.expr( ... ).desc( 'foo' )
  if (chain.recs.length > 1 && mmEnumQueryNameIsFIRSTTERMRe.test(queryName)) {
    throw new Error(`.${queryName} is not a function`);
  }

  if (mmEnumQueryNameIsRESOLVINGRe.test(queryName)) {
    return queryName === 'serialize'
      ? JSON.stringify(chain.recs)
      : spend(chain.state, { recId: chain.recId }, mmChainRecNext(chain));
  }

  return mmChainRecFnCreate(chains, chain, (...fnargs) => {
    // eg: row => row('field')
    const chainNext = mmChainRecNext(chain, {
      queryName: `${queryName}.fn`,
      queryArgs: mmChainRawArg(fnargs, mmChain)
    });

    return mmChainRecFnCreate(chains, chainNext, (...attributeFnArgs) => (
      // eg: row => row('field')('attribute')
      mmChainRecFnCreate(chains, mmChainRecNext(chainNext, {
        queryName: `getField`,
        queryArgs: mmChainRawArg(attributeFnArgs, mmChain)
      }), chainNext)
    ));
  });
};

const chain = (() => {
  const chainPart = Object.keys(mmQuery).reduce((prev, queryName) => {
    prev[queryName] = chainFnCreate(prev, queryName);

    return prev;
  }, {});

  Object.assign(chainPart.row, chainPart);

  return chainPart;
})()

// this complex flow is an optimization.
// query record calls are looped and defined once only.
// record calls are mapped to functions 'applied' to unique chain state
const mmChain = state => Object
  .assign({ state }, chain);

export default mmChain;
