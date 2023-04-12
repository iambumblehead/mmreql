import mmChainRawArg from './mmChainRawArg.mjs'
import mmChainRecNext from './mmChainRec.mjs'
import mmQuery from './mmQuery.mjs'

import {
  mmEnumQueryNameIsRESOLVINGRe,
  mmEnumQueryNameIsFIRSTTERMRe,
  mmEnumQueryArgTypeCHAIN
} from './mmEnum.mjs'

const chainFnCreate = (chains, queryName) => function (...args) {
  const chainCreate = mmChain // eslint-disable-line no-use-before-define
  const chain = mmChainRecNext(this, [
    queryName, mmChainRawArg(args, this.recId, chainCreate)])

  // must not follow another term, ex: r.expr( ... ).desc( 'foo' )
  if (chain.recs.length > 1 && mmEnumQueryNameIsFIRSTTERMRe.test(queryName)) {
    throw new Error(`.${queryName} is not a function`)
  }

  if (mmEnumQueryNameIsRESOLVINGRe.test(queryName)) {
    return queryName === 'serialize'
      ? JSON.stringify(chain.recs)
      : mmQuery(chain.state, { recId: chain.recId }, mmChainRecNext(chain))
  }

  return Object.assign((...fnargs) => {
    // eg: row => row('field')
    const chainNext = mmChainRecNext(chain, [
      `${queryName}.fn`, mmChainRawArg(fnargs, chain.recId, chainCreate)])

    return Object.assign((...attributeFnArgs) => (
      // eg: row => row('field')('attribute')
      Object.assign(chainNext, mmChainRecNext(chainNext, [
        'getField', mmChainRawArg(attributeFnArgs, chainNext.recId, chainCreate)
      ]), chains)
    ), chainNext, chains)
  }, chain, chains)
}

const chain = (() => {
  const chainPart = Object.keys(mmQuery).reduce((prev, queryName) => {
    prev[queryName] = typeof mmQuery[queryName] === 'function'
      ? chainFnCreate(prev, queryName)
      : mmQuery[queryName]

    return prev
  }, {
    toString: () => mmEnumQueryArgTypeCHAIN,
    type: mmEnumQueryArgTypeCHAIN
  })

  Object.assign(chainPart.row, chainPart)

  return chainPart
})()

// this complex flow is an optimization.
// query record calls are looped and defined once only.
// record calls are mapped to functions 'applied' to unique chain state
const mmChain = state => Object
  .assign({ state }, chain)

export default mmChain
