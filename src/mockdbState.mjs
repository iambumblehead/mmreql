import castas from 'castas';
import { randomUUID } from 'crypto';

const mockdbStateTableCreateIndexTuple = (name, fields = [], config = {}) => (
  [ name, fields, config ]);

const mockdbStateDbGet = (dbState, dbName) => (
  dbState.db[dbName]);

const mockdbStateDbTableConfigKeyGet = (dbName, tableName) => (
  `dbConfig_${dbName}_${tableName}`);

const mockdbStateDbCursorsKeyGet = dbName => (
  `dbConfig_${dbName}_cursor`);

const mockdbStateDbConfigKeyGet = dbName => (
  `dbConfig_${dbName}`);

const mockdbStateDbCreate = (state, dbName) => {
  state.dbSelected = dbName;
  state[mockdbStateDbConfigKeyGet(dbName)] = {
    name: dbName,
    id: randomUUID()
  };

  state[mockdbStateDbCursorsKeyGet(dbName)] = {};
  state.db[dbName] = {};

  return state;
};

const mockdbStateDbDrop = (state, dbName) => {
  delete state[mockdbStateDbConfigKeyGet(dbName)];
  delete state[mockdbStateDbCursorsKeyGet(dbName)];
  delete state.db[dbName];

  if (state.dbSelected === dbName)
    [ state.dbSelected ] = Object.keys(state.db);

  return state;
};

const mockdbStateCreate = opts => {
  const dbConfigList = castas.arr(opts.dbs, [ {
    db: opts.db || 'default'
  } ]);

  return dbConfigList.reduce((state, s) => {
    state = mockdbStateDbCreate(state, s.db);

    return state;
  }, {
    dbConnections: castas.arr(opts.connections, []),
    db: {}
  });
};

const mockdbStateTableConfigGet = (dbState, dbName, tableName) => {
  const tableKey = mockdbStateDbTableConfigKeyGet(dbName, tableName);

  return dbState[tableKey];
};

const mockdbStateDbConfigGet = (dbState, dbName) => (
  dbState[mockdbStateDbConfigKeyGet(dbName) ]);

const mockdbStateDbCursorConfigGet = (dbState, dbName) => (
  dbState[mockdbStateDbCursorsKeyGet(dbName)]);

const mockdbStateTableSet = (dbState, dbName, tableName, table) => {
  dbState.db[dbName][tableName] = table;

  return dbState;
};

const mockdbStateTableRm = (dbState, dbName, tableName) => {
  delete dbState.db[dbName][tableName];

  return dbState;
};

const mockdbStateTableGet = (dbState, dbName, tableName) => (
  dbState.db[dbName][tableName]);

const mockdbStateTableConfigSet = (dbState, dbName, tableName, tableConfig) => {
  const tableKey = mockdbStateDbTableConfigKeyGet(dbName, tableName);

  dbState[tableKey] = tableConfig;

  return dbState;
};

const mockdbStateTableConfigRm = (dbState, dbName, tableName) => {
  const tableKey = mockdbStateDbTableConfigKeyGet(dbName, tableName);

  delete dbState[tableKey];

  return dbState;
};

const mockdbStateTableCreate = (dbState, dbName, tableName, config) => {
  dbState = mockdbStateTableConfigSet(dbState, dbName, tableName, {
    db: dbState.dbSelected,
    id: randomUUID(),
    durability: 'hard',
    indexes: [],
    name: tableName,
    primary_key: (config && config.primaryKey) || 'id',
    shards: [ {
      primary_replica: 'replicaName',
      replicas: [ 'replicaName' ]
    } ],
    write_acks: 'majority',
    write_hook: null
  });

  dbState = mockdbStateTableSet(dbState, dbName, tableName, []);

  return dbState;
};

const mockdbStateTableDrop = (dbState, dbName, tableName) => {
  dbState = mockdbStateTableConfigRm(dbState, dbName, tableName);
  dbState = mockdbStateTableRm(dbState, dbName, tableName);

  return dbState;
};

const mockdbStateTableGetIndexNames = (dbState, dbName, tableName) => {
  const tableConfig = mockdbStateTableConfigGet(dbState, dbName, tableName);
    
  return tableConfig ? tableConfig.indexes.map(i => i[0]) : [];
};

const mockdbStateTableGetPrimaryKey = (dbState, dbName, tableName) => {
  const tableConfig = mockdbStateTableConfigGet(dbState, dbName, tableName);

  return (tableConfig && tableConfig.primary_key) || 'id';
};

const mockdbStateTableIndexExists = (db, dbName, tableName, indexName) => {
  const indexNames = mockdbStateTableGetIndexNames(db, dbName, tableName);

  return indexNames.includes(indexName);
};

const mockdbStateTableGetOrCreate = (dbState, dbName, tableName) => {
  const table = mockdbStateTableGet(dbState, dbName, tableName);
    
  if (!table)
    dbState = mockdbStateTableCreate(dbState, dbName, tableName);

  return mockdbStateTableGet(dbState, dbName, tableName);
};

const mockdbStateTableIndexAdd = (dbState, dbName, tableName, indexName, fields, config) => {
  mockdbStateTableGetOrCreate(dbState, dbName, tableName);

  const tableConfig = mockdbStateTableConfigGet(dbState, dbName, tableName);

  tableConfig.indexes.push(
    mockdbStateTableCreateIndexTuple(indexName, fields, config));

  return tableConfig;
};

// by default, a tuple for primaryKey 'id' is returned,
// this should be changed. ech table config should provide a primary key
// using 'id' as the defautl for each one.
const mockdbStateTableGetIndexTuple = (dbState, dbName, tableName, indexName) => {
  const tableConfig = mockdbStateTableConfigGet(dbState, dbName, tableName);
  const indexTuple = (tableConfig && tableConfig.indexes)
        && tableConfig.indexes.find(i => i[0] === indexName);

  if (!indexTuple && indexName !== 'id' && indexName !== tableConfig.primary_key) {
    console.warn(`table index not found. ${tableName}, ${indexName}`);
  }

  return indexTuple || mockdbStateTableCreateIndexTuple(indexName);
};

const mockdbStateTableCursorSplice = (dbState, dbName, tableName, cursorIndex) => {
  const cursorConfig = mockdbStateDbCursorConfigGet(dbState, dbName);
  const tableCursors = cursorConfig[tableName];

  tableCursors.splice(cursorIndex, 1);

  return dbState;
};

const mockdbStateTableDocCursorSplice = (dbState, dbName, tableName, doc, cursorIndex) => {
  const cursorConfig = mockdbStateDbCursorConfigGet(dbState, dbName);
  const tablePrimaryKey = mockdbStateTableGetPrimaryKey(dbState, dbName, tableName);
  const tableDocId = [ tableName, doc[tablePrimaryKey] ].join('-');
  const tableCursors = cursorConfig[tableDocId];

  tableCursors.splice(cursorIndex, 1);

  return dbState;
};

const mockdbStateTableDocCursorSet = (dbState, dbName, tableName, doc, cursor) => {
  const cursorConfig = mockdbStateDbCursorConfigGet(dbState, dbName);
  const tablePrimaryKey = mockdbStateTableGetPrimaryKey(dbState, dbName, tableName);
  const tableDocId = [ tableName, doc[tablePrimaryKey] ].join('-');

  cursorConfig[tableDocId] = cursorConfig[tableDocId] || [];
  cursorConfig[tableDocId].push(cursor);

  return dbState;
};

const mockdbStateTableCursorsPushChanges = (dbState, dbName, tableName, changes, changeType) => {
  const tablePrimaryKey = mockdbStateTableGetPrimaryKey(dbState, dbName, tableName);
  const cursorConfig = mockdbStateDbCursorConfigGet(dbState, dbName);
  const cursors = cursorConfig[tableName] || [];

  cursors.forEach(c => {
    changes.forEach(d => {
      const data = { ...d, type: changeType };

      if (typeof c.streamFilter === 'function') {
        if (c.streamFilter(data)) {
          c.push(data);
        }
      } else {
        c.push(data);
      }
    });
  });

  changes.forEach(c => {
    const doc = c.new_val || c.old_val;
    const docKey = doc && [ tableName, doc[tablePrimaryKey] ].join('-');
    const docCursors = cursorConfig[docKey] || [];

    docCursors.forEach(d => d.push({ ...c, type: changeType }));
  });

  return dbState;
};

const mockdbStateTableCursorsGetOrCreate = (dbState, dbName, tableName) => {
  const cursors = mockdbStateDbCursorConfigGet(dbState, dbName);

  cursors[tableName] = cursors[tableName] || [];

  return cursors;
};

const mockdbStateTableDocCursorsGetOrCreate = (dbState, dbName, tableName, doc) => {
  const tablePrimaryKey = mockdbStateTableGetPrimaryKey(dbState, dbName, tableName);
  const tableDocId = [ tableName, doc[tablePrimaryKey] ].join('-');
  const cursorConfig = mockdbStateDbCursorConfigGet(dbState, dbName);

  cursorConfig[tableDocId] = cursorConfig[tableDocId] || [];

  return cursorConfig[tableDocId];
};

const mockdbStateTableCursorSet = (dbState, dbName, tableName, cursor) => {
  const cursors = mockdbStateDbCursorConfigGet(dbState, dbName);
  const tableCursors = cursors[tableName];

  tableCursors.push(cursor);

  return dbState;
};

const mockdbStateAggregate = (oldState, aggState) => (
  Object.keys(aggState).reduce((state, key) => {
    if (typeof aggState[key] === 'number') {
      if (typeof state[key] === 'undefined')
        state[key] = 0;
            
      state[key] += aggState[key];
    } else if (Array.isArray(aggState[key])) {
      if (!Array.isArray(state[key]))
        state[key] = [];

      state[key].push(...aggState[key]);
    }

    return state;
  }, oldState));

export {
  mockdbStateCreate,
  mockdbStateDbGet,
  mockdbStateAggregate,
  mockdbStateDbCreate,
  mockdbStateDbDrop,
  mockdbStateTableCreate,
  mockdbStateTableDrop,
  mockdbStateTableGet,
  mockdbStateTableSet,
  mockdbStateTableGetOrCreate,
  mockdbStateTableGetIndexNames,
  mockdbStateTableGetPrimaryKey,
  mockdbStateTableIndexAdd,
  mockdbStateTableIndexExists,
  mockdbStateTableGetIndexTuple,
  mockdbStateTableCursorSet,
  mockdbStateTableDocCursorSet,
  mockdbStateTableCursorSplice,
  mockdbStateTableDocCursorSplice,
  mockdbStateTableCursorsPushChanges,
  mockdbStateTableCursorsGetOrCreate,
  mockdbStateTableDocCursorsGetOrCreate,
  mockdbStateTableConfigGet,
  mockdbStateDbConfigGet,
  mockdbStateDbCursorConfigGet
};
