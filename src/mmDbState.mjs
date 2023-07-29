import { randomUUID } from 'crypto'

const mmDbStateTableCreateIndexTuple = (name, fields = [], config = {}) => (
  [name, fields, config])

const mmDbStateDbGet = (dbState, dbName) => (
  dbState.db[dbName])

const mmDbStateDbTableConfigKeyGet = (dbName, tableName) => (
  `dbConfig_${dbName}_${tableName}`)

const mmDbStateDbCursorsKeyGet = dbName => (
  `dbConfig_${dbName}_cursor`)

const mmDbStateDbConfigKeyGet = dbName => (
  `dbConfig_${dbName}`)

const mmDbStateDbCreate = (state, dbName) => {
  state.dbSelected = dbName
  state[mmDbStateDbConfigKeyGet(dbName)] = {
    name: dbName,
    id: randomUUID()
  }

  state[mmDbStateDbCursorsKeyGet(dbName)] = {}
  state.db[dbName] = {}

  return state
}

const mmDbStateDbDrop = (state, dbName) => {
  delete state[mmDbStateDbConfigKeyGet(dbName)]
  delete state[mmDbStateDbCursorsKeyGet(dbName)]
  delete state.db[dbName]

  if (state.dbSelected === dbName)
    [state.dbSelected] = Object.keys(state.db)

  return state
}

const mmDbStateCreate = opts => {
  const dbConfigList = Array.isArray(opts.dbs) ? opts.dbs : [{
    db: opts.db || 'default'
  }]

  return dbConfigList.reduce((state, s) => {
    state = mmDbStateDbCreate(state, s.db)

    return state
  }, {
    dbConnections: Array.isArray(opts.connections)
      ? opts.connections : [],
    db: {}
  })
}

const mmDbStateTableConfigGet = (dbState, dbName, tableName) => {
  const tableKey = mmDbStateDbTableConfigKeyGet(dbName, tableName)

  return dbState[tableKey]
}

const mmDbStateDbConfigGet = (dbState, dbName) => (
  dbState[mmDbStateDbConfigKeyGet(dbName) ])

const mmDbStateDbCursorConfigGet = (dbState, dbName) => (
  dbState[mmDbStateDbCursorsKeyGet(dbName)])

const mmDbStateTableSet = (dbState, dbName, tableName, table) => {
  dbState.db[dbName][tableName] = table

  return dbState
}

const mmDbStateTableRm = (dbState, dbName, tableName) => {
  delete dbState.db[dbName][tableName]

  return dbState
}

const mmDbStateTableGet = (dbState, dbName, tableName) => (
  dbState.db[dbName][tableName])

const mmDbStateTableConfigSet = (dbState, dbName, tableName, tableConfig) => {
  const tableKey = mmDbStateDbTableConfigKeyGet(dbName, tableName)

  dbState[tableKey] = tableConfig

  return dbState
}

const mmDbStateTableConfigRm = (dbState, dbName, tableName) => {
  const tableKey = mmDbStateDbTableConfigKeyGet(dbName, tableName)

  delete dbState[tableKey]

  return dbState
}

const mmDbStateTableCreate = (dbState, dbName, tableName, config) => {
  dbState = mmDbStateTableConfigSet(dbState, dbName, tableName, {
    db: dbState.dbSelected,
    id: randomUUID(),
    durability: 'hard',
    indexes: [],
    name: tableName,
    primary_key: (config && config.primaryKey) || 'id',
    shards: [{
      primary_replica: 'replicaName',
      replicas: ['replicaName']
    }],
    write_acks: 'majority',
    write_hook: null
  })

  dbState = mmDbStateTableSet(dbState, dbName, tableName, [])

  return dbState
}

const mmDbStateTableDrop = (dbState, dbName, tableName) => {
  dbState = mmDbStateTableConfigRm(dbState, dbName, tableName)
  dbState = mmDbStateTableRm(dbState, dbName, tableName)

  return dbState
}

const mmDbStateTableGetIndexNames = (dbState, dbName, tableName) => {
  const tableConfig = mmDbStateTableConfigGet(dbState, dbName, tableName)
    
  return tableConfig ? tableConfig.indexes.map(i => i[0]) : []
}

const mmDbStateTableGetPrimaryKey = (dbState, dbName, tableName) => {
  const tableConfig = mmDbStateTableConfigGet(dbState, dbName, tableName)

  return (tableConfig && tableConfig.primary_key) || 'id'
}

const mmDbStateTableIndexExists = (db, dbName, tableName, indexName) => {
  const indexNames = mmDbStateTableGetIndexNames(db, dbName, tableName)

  return indexNames.includes(indexName)
}

const mmDbStateTableGetOrCreate = (dbState, dbName, tableName) => {
  const table = mmDbStateTableGet(dbState, dbName, tableName)
    
  if (!table)
    dbState = mmDbStateTableCreate(dbState, dbName, tableName)

  return mmDbStateTableGet(dbState, dbName, tableName)
}

const mmDbStateTableIndexAdd = (dbState, dbName, tableName, indexName, fields, config) => {
  mmDbStateTableGetOrCreate(dbState, dbName, tableName)

  const tableConfig = mmDbStateTableConfigGet(dbState, dbName, tableName)

  tableConfig.indexes.push(
    mmDbStateTableCreateIndexTuple(indexName, fields, config))

  return tableConfig
}

// by default, a tuple for primaryKey 'id' is returned,
// this should be changed. ech table config should provide a primary key
// using 'id' as the defautl for each one.
const mmDbStateTableGetIndexTuple = (dbState, dbName, tableName, indexName) => {
  const tableConfig = mmDbStateTableConfigGet(dbState, dbName, tableName)
  const indexTuple = (tableConfig && tableConfig.indexes)
        && tableConfig.indexes.find(i => i[0] === indexName)

  if (!indexTuple && indexName !== 'id' && indexName !== tableConfig.primary_key) {
    console.warn(`table index not found. ${tableName}, ${indexName}`)
  }

  return indexTuple || mmDbStateTableCreateIndexTuple(indexName)
}

const mmDbStateTableCursorSplice = (dbState, dbName, tableName, cursorIndex) => {
  const cursorConfig = mmDbStateDbCursorConfigGet(dbState, dbName)
  const tableCursors = cursorConfig[tableName]

  tableCursors.splice(cursorIndex, 1)

  return dbState
}

const mmDbStateTableDocCursorSplice = (dbState, dbName, tableName, doc, cursorIndex) => {
  const cursorConfig = mmDbStateDbCursorConfigGet(dbState, dbName)
  const tablePrimaryKey = mmDbStateTableGetPrimaryKey(dbState, dbName, tableName)
  const tableDocId = [tableName, doc[tablePrimaryKey]].join('-')
  const tableCursors = cursorConfig[tableDocId]

  tableCursors.splice(cursorIndex, 1)

  return dbState
}

const mmDbStateTableDocCursorSet = (dbState, dbName, tableName, doc, cursor) => {
  const cursorConfig = mmDbStateDbCursorConfigGet(dbState, dbName)
  const tablePrimaryKey = mmDbStateTableGetPrimaryKey(dbState, dbName, tableName)
  const tableDocId = [tableName, doc[tablePrimaryKey]].join('-')

  cursorConfig[tableDocId] = cursorConfig[tableDocId] || []
  cursorConfig[tableDocId].push(cursor)

  return dbState
}

const mmDbStateTableCursorsPushChanges = (dbState, dbName, tableName, changes, changeType) => {
  const tablePrimaryKey = mmDbStateTableGetPrimaryKey(dbState, dbName, tableName)
  const cursorConfig = mmDbStateDbCursorConfigGet(dbState, dbName) || {}
  const cursors = cursorConfig[tableName] || []

  cursors.forEach(c => {
    changes.forEach(d => {
      const data = { ...d, type: changeType }

      if (typeof c.streamFilter === 'function') {
        if (c.streamFilter(data)) {
          c.push(data)
        }
      } else {
        c.push(data)
      }
    })
  })

  changes.forEach(c => {
    const doc = c.new_val || c.old_val
    const docKey = doc && [tableName, doc[tablePrimaryKey]].join('-')
    const docCursors = cursorConfig[docKey] || []

    docCursors.forEach(d => d.push({ ...c, type: changeType }))
  })

  return dbState
}

const mmDbStateTableCursorsGetOrCreate = (dbState, dbName, tableName) => {
  const cursors = mmDbStateDbCursorConfigGet(dbState, dbName)

  cursors[tableName] = cursors[tableName] || []

  return cursors
}

const mmDbStateTableDocCursorsGetOrCreate = (dbState, dbName, tableName, doc) => {
  const tablePrimaryKey = mmDbStateTableGetPrimaryKey(dbState, dbName, tableName)
  const tableDocId = [tableName, doc[tablePrimaryKey]].join('-')
  const cursorConfig = mmDbStateDbCursorConfigGet(dbState, dbName)

  cursorConfig[tableDocId] = cursorConfig[tableDocId] || []

  return cursorConfig[tableDocId]
}

const mmDbStateTableCursorSet = (dbState, dbName, tableName, cursor) => {
  const cursors = mmDbStateDbCursorConfigGet(dbState, dbName)
  const tableCursors = cursors[tableName]

  tableCursors.push(cursor)

  return dbState
}

const mmDbStateAggregate = (oldState, aggState) => (
  Object.keys(aggState).reduce((state, key) => {
    if (typeof aggState[key] === 'number') {
      if (typeof state[key] === 'undefined')
        state[key] = 0
            
      state[key] += aggState[key]
    } else if (Array.isArray(aggState[key])) {
      if (!Array.isArray(state[key]))
        state[key] = []

      state[key].push(...aggState[key])
    }

    return state
  }, oldState))

export {
  mmDbStateCreate,
  mmDbStateDbGet,
  mmDbStateAggregate,
  mmDbStateDbCreate,
  mmDbStateDbDrop,
  mmDbStateTableCreate,
  mmDbStateTableDrop,
  mmDbStateTableGet,
  mmDbStateTableSet,
  mmDbStateTableGetOrCreate,
  mmDbStateTableGetIndexNames,
  mmDbStateTableGetPrimaryKey,
  mmDbStateTableIndexAdd,
  mmDbStateTableIndexExists,
  mmDbStateTableGetIndexTuple,
  mmDbStateTableCursorSet,
  mmDbStateTableDocCursorSet,
  mmDbStateTableCursorSplice,
  mmDbStateTableDocCursorSplice,
  mmDbStateTableCursorsPushChanges,
  mmDbStateTableCursorsGetOrCreate,
  mmDbStateTableDocCursorsGetOrCreate,
  mmDbStateTableConfigGet,
  mmDbStateDbConfigGet,
  mmDbStateDbCursorConfigGet
}
