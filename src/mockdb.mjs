import mmChain from './mmChain.mjs'

import {
  mockdbStateCreate,
  mockdbStateDbCreate,
  mockdbStateTableSet,
  mockdbStateTableCreate
} from './mockdbState.mjs'

const buildChain = (dbState = {}) => {
  const r = mmChain(dbState)

  // make, for example, r.add callable through r.row.add
  // Object.assign(r.row, r)
  return {
    r: Object.assign((...args) => r.expr(... args), r),
    dbState
  }
}

const buildDb = (tables, config) => {
  const dbConfig = config || mockdbStateCreate(
    (tables[0] && tables[0].db) ? tables[0] : {})
  const dbConfigTables = (tables[0] && tables[0].db)
    ? tables.slice(1)
    : tables

  return dbConfigTables.reduce((dbState, tablelist, i, arr) => {
    const tableConfig = Array.isArray(tablelist[1]) && tablelist[1]

    if (!Array.isArray(tablelist)) {
      dbState = mockdbStateDbCreate(dbState, tablelist.db)
      dbState = buildDb(arr.slice(i + 1), dbState)
      arr.splice(1)
      return dbState
    }

    dbState = mockdbStateTableCreate(dbState, dbState.dbSelected, tablelist[0], tableConfig[0])
    dbState = mockdbStateTableSet(
      dbState, dbState.dbSelected, tablelist[0], tablelist.slice(tableConfig ? 2 : 1))

    return dbState
  }, dbConfig)
}

// opts can be optionally passed. ex,
//
//   rethinkdbMocked([ ...db ])
//
export default (opts, configList) => buildChain(
  buildDb(Array.isArray(opts) ? opts : configList || []), opts)
