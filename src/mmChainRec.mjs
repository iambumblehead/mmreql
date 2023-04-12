import {
  mmEnumQueryArgTypeCHAINFN,
  mmEnumQueryArgTypeCHAIN
} from './mmEnum.mjs'

// chain originates from nested row or sub query as a reqlCHAIN or pojo. ex,
//  * `map(hero => hero('name'))`
//  * `map(hero => ({ heroName: hero('name') }))`
const mmChainRecRowFnCreate = (chain, recId) => ({
  toString: () => mmEnumQueryArgTypeCHAINFN,
  type: mmEnumQueryArgTypeCHAINFN,
  recs: chain.recs,
  recId: recId || ('orphan' + Date.now())
})

// creates chain from previous chain
const mmChainRecNext = (chain, rec, recs = (chain.recs || []).slice()) => ({
  toString: () => mmEnumQueryArgTypeCHAIN,
  type: mmEnumQueryArgTypeCHAIN,
  state: chain.state || {},
  recs: rec
    ? recs.push(rec) && recs
    : recs,
  recId: (chain.recId || '') +
    (rec ? '/' + rec[0] : '')
})

const mmChainRecFnCreate = (queryFns, chain, fn) => (
  Object.assign(fn, chain, queryFns))

export {
  mmChainRecFnCreate,
  mmChainRecRowFnCreate,
  mmChainRecNext
}
