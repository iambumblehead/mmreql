import {
  mmEnumRecTypeCHAIN,
  mmEnumRecTypeROW,
  mmEnumQueryArgTypeROWFN,
  mmEnumQueryArgTypeROW
} from './mmEnum.mjs'

// chain originates from nested row or sub query as a reqlCHAIN or pojo. ex,
//  * `map(hero => hero('name'))`
//  * `map(hero => ({ heroName: hero('name') }))`
const mmRecChainRowFnCreate = (chain, recId) => ({
  toString: () => mmEnumQueryArgTypeROWFN,
  type: mmEnumQueryArgTypeROWFN,
  recs: chain.recs,
  recId: recId || ('orphan' + Date.now())
})

const mmRecChainRowCreate = (chain, recId) => ({
  toString: () => mmEnumQueryArgTypeROW,
  type: mmEnumQueryArgTypeROW,
  recs: chain.recs,
  recId: recId || ('orphan' + Date.now())
})


const mmRecChainSubCreate = chain => ({
  toString: () => mmEnumRecTypeROW,
  type: mmEnumRecTypeROW,
  recs: chain.recs.slice()
})

const mmRecChainCreate = (chain, rec, recs = (chain.recs || []).slice()) => ({
  toString: () => mmEnumRecTypeCHAIN,
  state: chain.state || {},
  recs: rec
    ? recs.push(rec) && recs
    : recs
})

const mmRecChainFnCreate = (queryFns, chain, fn) => (
  Object.assign(fn, chain, queryFns))

export {
  mmRecChainFnCreate,
  mmRecChainRowFnCreate,
  mmRecChainRowCreate,
  mmRecChainCreate,
  mmRecChainSubCreate
}
