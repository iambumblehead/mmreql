import mmChain from './mmChain.mjs'

import {
  mmDbStateCreate,
  mmDbStateDbCreate,
  mmDbStateTableSet,
  mmDbStateTableCreate
} from './mmDbState.mjs'

const buildChain = (dbState = {}) => {
  const r = mmChain(dbState)

  // make, for example, r.add callable through r.row.add
  return {
    r: Object.assign((...args) => r.expr(... args), r),
    dbState
  }
}

const buildDb = (tables, config) => {
  const dbConfig = config || mmDbStateCreate(
    (tables[0] && tables[0].db) ? tables[0] : {})
  const dbConfigTables = (tables[0] && tables[0].db)
    ? tables.slice(1)
    : tables

  return dbConfigTables.reduce((dbState, tablelist, i, arr) => {
    const tableConfig = Array.isArray(tablelist[1]) && tablelist[1]

    if (!Array.isArray(tablelist)) {
      dbState = mmDbStateDbCreate(dbState, tablelist.db)
      dbState = buildDb(arr.slice(i + 1), dbState)
      arr.splice(1)
      return dbState
    }

    dbState = mmDbStateTableCreate(dbState, dbState.dbSelected, tablelist[0], tableConfig[0])
    dbState = mmDbStateTableSet(
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
