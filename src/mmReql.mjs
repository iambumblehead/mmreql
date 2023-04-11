import mmChain from './mmChain.mjs';

import {
  mmDb,
  mmDbCreate,
  mmDbTableSet,
  mmDbTableCreate
} from './mmDb.mjs';

const buildChain = (db = {}) => {
  const r = mmChain(db);

  return {
    r: Object.assign((...args) => r.expr(... args), r),
    db
  };
};

const buildDb = (tables, config) => {
  const dbConfig = config || mmDb((tables[0] && tables[0].db) ? tables[0] : {});
  const dbConfigTables = (tables[0] && tables[0].db)
    ? tables.slice(1)
    : tables;

  return dbConfigTables.reduce((db, tablelist, i, arr) => {
    const tableConfig = Array.isArray(tablelist[1]) && tablelist[1];

    if (!Array.isArray(tablelist)) {
      db = mmDbCreate(db, tablelist.db);
      db = buildDb(arr.slice(i + 1), db);
      arr.splice(1);
      return db;
    }

    db = mmDbTableCreate(db, db.dbSelected, tablelist[0], tableConfig[0]);
    db = mmDbTableSet(
      db, db.dbSelected, tablelist[0], tablelist.slice(tableConfig ? 2 : 1));

    return db;
  }, dbConfig);
};

// opts can be optionally passed. ex,
//
//   rethinkdbMocked([ ...db ])
//
export default (opts, configList) => buildChain(
  buildDb(Array.isArray(opts) ? opts : configList || []), opts);
