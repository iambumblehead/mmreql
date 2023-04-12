import {
  mmEnumQueryArgTypeROWFN,
  mmEnumQueryArgTypeROW
} from './mmEnum.mjs'

// chain originates from nested row or sub query as a reqlCHAIN or pojo. ex,
//  * `map(hero => hero('name'))`
//  * `map(hero => ({ heroName: hero('name') }))`
const mmChainRecRowFnCreate = (chain, recId) => ({
  toString: () => mmEnumQueryArgTypeROWFN,
  type: mmEnumQueryArgTypeROWFN,
  recs: chain.recs,
  recId: recId || ('orphan' + Date.now())
})

// creates chain from previous chain
const mmChainRecNext = (chain, rec, recs = (chain.recs || []).slice()) => ({
  toString: () => mmEnumQueryArgTypeROW,
  type: mmEnumQueryArgTypeROW,
  state: chain.state || {},
  recs: rec
    ? recs.push(rec) && recs
    : recs,
  recId: 'orphan' + Date.now()
})

const mmChainRecFnCreate = (queryFns, chain, fn) => (
  Object.assign(fn, chain, queryFns))

export {
  mmChainRecFnCreate,
  mmChainRecRowFnCreate,
  mmChainRecNext
}
