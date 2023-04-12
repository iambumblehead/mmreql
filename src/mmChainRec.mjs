import {
  mmEnumQueryArgTypeCHAIN
} from './mmEnum.mjs'

// creates chain from previous chain, incl nested row and sub query. ex,
//  * `map(hero => hero('name'))`
//  * `map(hero => ({ heroName: hero('name') }))`
const mmChainRecNext = (chain, rec, id, recs = (chain.recs || []).slice()) => ({
  toString: () => mmEnumQueryArgTypeCHAIN,
  type: mmEnumQueryArgTypeCHAIN,
  state: chain.state || {},
  recId: id || ((chain.recId || '') + (rec ? '.' + rec[0] : '')),
  recs: rec ? recs.push(rec) && recs : recs
})

const mmChainRecFnCreate = (queryFns, chain, fn) => (
  Object.assign(fn, chain, queryFns))

export {
  mmChainRecFnCreate,
  mmChainRecNext
}
