import castas from 'castas';
import { randomUUID } from 'crypto';

const mmDbTableCreateIndexTuple = (name, fields = [], config = {}) => (
  [ name, fields, config ]);

const mmDbGet = (dbst, dbName) => (
  dbst.db[dbName]);

const mmDbTableConfigKeyGet = (dbName, tableName) => (
  `dbConfig_${dbName}_${tableName}`);

const mmDbCursorsKeyGet = dbName => (
  `dbConfig_${dbName}_cursor`);

const mmDbConfigKeyGet = dbName => (
  `dbConfig_${dbName}`);

const mmDbCreate = (state, dbName) => {
  state.dbSelected = dbName;
  state[mmDbConfigKeyGet(dbName)] = {
    name: dbName,
    id: randomUUID()
  };

  state[mmDbCursorsKeyGet(dbName)] = {};
  state.db[dbName] = {};

  return state;
};

const mmDbDrop = (state, dbName) => {
  delete state[mmDbConfigKeyGet(dbName)];
  delete state[mmDbCursorsKeyGet(dbName)];
  delete state.db[dbName];

  if (state.dbSelected === dbName)
    [ state.dbSelected ] = Object.keys(state.db);

  return state;
};

const mmDb = opts => {
  const dbConfigList = castas.arr(opts.dbs, [ {
    db: opts.db || 'default'
  } ]);

  return dbConfigList.reduce((state, s) => {
    state = mmDbCreate(state, s.db);

    return state;
  }, {
    dbConnections: castas.arr(opts.connections, []),
    db: {}
  });
};

const mmDbTableConfigGet = (db, dbName, tableName) => {
  const tableKey = mmDbTableConfigKeyGet(dbName, tableName);

  return db[tableKey];
};

const mmDbConfigGet = (dbst, dbName) => (
  dbst[mmDbConfigKeyGet(dbName) ]);

const mmDbCursorConfigGet = (dbst, dbName) => (
  dbst[mmDbCursorsKeyGet(dbName)]);

const mmDbTableSet = (dbst, dbName, tableName, table) => {
  dbst.db[dbName][tableName] = table;

  return dbst;
};

const mmDbTableRm = (dbst, dbName, tableName) => {
  delete dbst.db[dbName][tableName];

  return dbst;
};

const mmDbTableGet = (dbst, dbName, tableName) => (
  dbst.db[dbName][tableName]);

const mmDbTableConfigSet = (dbst, dbName, tableName, tableConfig) => {
  const tableKey = mmDbTableConfigKeyGet(dbName, tableName);

  dbst[tableKey] = tableConfig;

  return dbst;
};

const mmDbTableConfigRm = (dbst, dbName, tableName) => {
  const tableKey = mmDbTableConfigKeyGet(dbName, tableName);

  delete dbst[tableKey];

  return dbst;
};

const mmDbTableCreate = (dbst, dbName, tableName, config) => {
  dbst = mmDbTableConfigSet(dbst, dbName, tableName, {
    db: dbst.dbSelected,
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

  dbst = mmDbTableSet(dbst, dbName, tableName, []);

  return dbst;
};

const mmDbTableDrop = (dbst, dbName, tableName) => {
  dbst = mmDbTableConfigRm(dbst, dbName, tableName);
  dbst = mmDbTableRm(dbst, dbName, tableName);

  return dbst;
};

const mmDbTableGetIndexNames = (dbst, dbName, tableName) => {
  const tableConfig = mmDbTableConfigGet(dbst, dbName, tableName);
    
  return tableConfig ? tableConfig.indexes.map(i => i[0]) : [];
};

const mmDbTableGetPrimaryKey = (dbst, dbName, tableName) => {
  const tableConfig = mmDbTableConfigGet(dbst, dbName, tableName);

  return (tableConfig && tableConfig.primary_key) || 'id';
};

const mmDbTableIndexExists = (db, dbName, tableName, indexName) => {
  const indexNames = mmDbTableGetIndexNames(db, dbName, tableName);

  return indexNames.includes(indexName);
};

const mmDbTableGetOrCreate = (dbst, dbName, tableName) => {
  const table = mmDbTableGet(dbst, dbName, tableName);
    
  if (!table)
    dbst = mmDbTableCreate(dbst, dbName, tableName);

  return mmDbTableGet(dbst, dbName, tableName);
};

const mmDbTableIndexAdd = (dbst, dbName, tableName, indexName, fields, config) => {
  mmDbTableGetOrCreate(dbst, dbName, tableName);

  const tableConfig = mmDbTableConfigGet(dbst, dbName, tableName);

  tableConfig.indexes.push(
    mmDbTableCreateIndexTuple(indexName, fields, config));

  return tableConfig;
};

// by default, a tuple for primaryKey 'id' is returned,
// this should be changed. ech table config should provide a primary key
// using 'id' as the defautl for each one.
const mmDbTableGetIndexTuple = (dbst, dbName, tableName, indexName) => {
  const tableConfig = mmDbTableConfigGet(dbst, dbName, tableName);
  const indexTuple = (tableConfig && tableConfig.indexes)
        && tableConfig.indexes.find(i => i[0] === indexName);

  if (!indexTuple && indexName !== 'id' && indexName !== tableConfig.primary_key) {
    console.warn(`table index not found. ${tableName}, ${indexName}`);
  }

  return indexTuple || mmDbTableCreateIndexTuple(indexName);
};

const mmDbTableCursorSplice = (dbst, dbName, tableName, cursorIndex) => {
  const cursorConfig = mmDbCursorConfigGet(dbst, dbName);
  const tableCursors = cursorConfig[tableName];

  tableCursors.splice(cursorIndex, 1);

  return dbst;
};

const mmDbTableDocCursorSplice = (dbst, dbName, tableName, doc, cursorIndex) => {
  const cursorConfig = mmDbCursorConfigGet(dbst, dbName);
  const tablePrimaryKey = mmDbTableGetPrimaryKey(dbst, dbName, tableName);
  const tableDocId = [ tableName, doc[tablePrimaryKey] ].join('-');
  const tableCursors = cursorConfig[tableDocId];

  tableCursors.splice(cursorIndex, 1);

  return dbst;
};

const mmDbTableDocCursorSet = (dbst, dbName, tableName, doc, cursor) => {
  const cursorConfig = mmDbCursorConfigGet(dbst, dbName);
  const tablePrimaryKey = mmDbTableGetPrimaryKey(dbst, dbName, tableName);
  const tableDocId = [ tableName, doc[tablePrimaryKey] ].join('-');

  cursorConfig[tableDocId] = cursorConfig[tableDocId] || [];
  cursorConfig[tableDocId].push(cursor);

  return dbst;
};

const mmDbTableCursorsPushChanges = (dbst, dbName, tableName, changes, changeType) => {
  const tablePrimaryKey = mmDbTableGetPrimaryKey(dbst, dbName, tableName);
  const cursorConfig = mmDbCursorConfigGet(dbst, dbName) || {};
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

  return dbst;
};

const mmDbTableCursorsGetOrCreate = (dbst, dbName, tableName) => {
  const cursors = mmDbCursorConfigGet(dbst, dbName);

  cursors[tableName] = cursors[tableName] || [];

  return cursors;
};

const mmDbTableDocCursorsGetOrCreate = (dbst, dbName, tableName, doc) => {
  const tablePrimaryKey = mmDbTableGetPrimaryKey(dbst, dbName, tableName);
  const tableDocId = [ tableName, doc[tablePrimaryKey] ].join('-');
  const cursorConfig = mmDbCursorConfigGet(dbst, dbName);

  cursorConfig[tableDocId] = cursorConfig[tableDocId] || [];

  return cursorConfig[tableDocId];
};

const mmDbTableCursorSet = (dbst, dbName, tableName, cursor) => {
  const cursors = mmDbCursorConfigGet(dbst, dbName);
  const tableCursors = cursors[tableName];

  tableCursors.push(cursor);

  return dbst;
};

const mmDbAggregate = (oldState, aggState) => (
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
  mmDb,
  mmDbGet,
  mmDbAggregate,
  mmDbCreate,
  mmDbDrop,
  mmDbConfigGet,
  mmDbCursorConfigGet,
  mmDbTableCreate,
  mmDbTableDrop,
  mmDbTableGet,
  mmDbTableSet,
  mmDbTableGetOrCreate,
  mmDbTableGetIndexNames,
  mmDbTableGetPrimaryKey,
  mmDbTableIndexAdd,
  mmDbTableIndexExists,
  mmDbTableGetIndexTuple,
  mmDbTableCursorSet,
  mmDbTableDocCursorSet,
  mmDbTableCursorSplice,
  mmDbTableDocCursorSplice,
  mmDbTableCursorsPushChanges,
  mmDbTableCursorsGetOrCreate,
  mmDbTableDocCursorsGetOrCreate,
  mmDbTableConfigGet
};
