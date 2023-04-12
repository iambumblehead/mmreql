import {
  mmRecChainCreate,
  mmRecChainSubCreate,
  mmRecChainFnCreate
} from './mmRecChain.mjs'

import queryReql, {
  spend,
  spendCursor
} from './mockdbReql.mjs'

import {
  mockdbSpecFromRawArg
} from './mockdbSpec.mjs'

import {
  mmEnumQueryNameIsRESOLVINGRe,
  mmEnumQueryNameIsFIRSTTERMRe
} from './mmEnum.mjs'

const staleChains = Object.keys(queryReql).reduce((prev, queryName) => {
  prev[queryName] = function (...args) {
    const chain = mmRecChainCreate(this, {
      queryName,
      queryArgs: mockdbSpecFromRawArg(args, mockdbChain)
    })

    // must not follow another term, ex: r.expr( ... ).desc( 'foo' )
    if (chain.recs.length > 1 && mmEnumQueryNameIsFIRSTTERMRe.test(queryName)) {
      throw new Error(`.${queryName} is not a function`)
    }

    if (mmEnumQueryNameIsRESOLVINGRe.test(queryName)) {
      return queryName === 'serialize'
        ? JSON.stringify(chain.recs)

        : (queryName === 'getCursor' ? spendCursor : spend)(
          chain.state, { recId: 'start' }, mmRecChainSubCreate(chain))
      // spendCursor pending removal
      // : spend(chain.state, { recId: 'start' }, mmRecChainSubCreate(chain));
    }

    return mmRecChainFnCreate(staleChains, chain, (...fnargs) => {
      // eg: row => row('field')
      const chainNext = mmRecChainCreate(chain, {
        queryName: `${queryName}.fn`,
        queryArgs: mockdbSpecFromRawArg(fnargs, mockdbChain)
      })
        
      return mmRecChainFnCreate(staleChains, chainNext, (...attributeFnArgs) => (
        // eg: row => row('field')('attribute')
        mmRecChainFnCreate(staleChains, mmRecChainCreate(chainNext, {
          queryName: `getField`,
          queryArgs: mockdbSpecFromRawArg(attributeFnArgs, mockdbChain)
        }), chainNext)
      ))
    })
  }

  return prev
}, {})

const chains = Object.keys(queryReql).reduce((prev, queryName) => {
  prev[queryName] = function (...args) {
    return staleChains[queryName].apply(
      mmRecChainCreate({
        state: this.state
      }), args)
  }

  return prev
}, {})

// this complex flow is an optimization.
// query record calls are looped and defined once only.
// record calls are mapped to functions 'applied' to unique chain state
const mockdbChain = state => ({
  state,
  ...chains
})

export default mockdbChain
