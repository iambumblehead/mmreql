import { randomUUID } from 'crypto';
import { Readable } from 'stream';
import mmConn from './mmConn.mjs';
import mmStream from './mmStream.mjs';

import {
  mmDbAggregate,
  mmDbConfigGet,
  mmDbCreate,
  mmDbDrop,
  mmDbGet,
  mmDbTableIndexAdd,
  mmDbTableGetIndexNames,
  mmDbTableGetIndexTuple,
  mmDbTableGetPrimaryKey,
  mmDbTableCursorSet,
  mmDbTableDocCursorSet,
  mmDbTableCursorSplice,
  mmDbTableDocCursorSplice,
  mmDbTableCursorsPushChanges,
  mmDbTableCursorsGetOrCreate,
  mmDbTableDocCursorsGetOrCreate,
  mmDbTableConfigGet,
  mmDbTableCreate,
  mmDbTableDrop
} from './mmDb.mjs';

import {
  mmTableDocsRm,
  mmTableDocsGet,
  mmTableDocsSet,
  mmTableDocGetIndexValue,
  mmTableDocEnsurePrimaryKey,
  mmTableDocHasIndexValueFn,
  mmTableSet
} from './mmTable.mjs';

import {
  mmResChangeTypeADD,
  mmResChangeTypeINITIAL,
  mmResChangesCreate,
  mmResChangesFieldCreate,
  mmResTableStatus,
  mmResTableInfo
} from './mmRes.mjs';

import {
  mmErrArgumentsNumber,
  mmErrDuplicatePrimaryKey,
  mmErrIndexOutOfBounds,
  mmErrUnrecognizedOption,
  mmErrInvalidTableName,
  mmErrInvalidDbName,
  mmErrTableExists,
  mmErrTableDoesNotExist,
  mmErrSecondArgumentOfQueryMustBeObject,
  mmErrPrimaryKeyWrongType,
  mmErrNotATIMEpsudotype,
  mmErrCannotUseNestedRow,
  mmErrNoAttributeInObject,
  mmErrExpectedTypeFOOButFoundBAR,
  mmErrCannotReduceOverEmptyStream,
  mmErrCannotCallFOOonBARTYPEvalue
} from './mmErr.mjs';

import {
  mmEnumTypeERROR,
  mmEnumQueryArgTypeARGS,
  mmEnumQueryArgTypeROW,
  mmEnumQueryArgTypeROWIsRe,
  mmEnumIsRowShallow,
  mmEnumQueryNameIsCURSORORDEFAULTRe,
  mmEnumIsQueryArgsResult,
  mmEnumIsRow
} from './mmEnum.mjs';

const isBoolNumStrRe = /boolean|number|string/;
const isBoolNumUndefRe = /boolean|number|undefined/;

const isLookObj = obj => obj
  && typeof obj === 'object'
  && !(obj instanceof Date);

const reqlArgsParse = obj => (
  obj[mmEnumQueryArgTypeARGS]);

const reqlArgsCreate = value => (
  { [mmEnumQueryArgTypeARGS]: value });

// created by 'asc' and 'desc' queries
const isSortObj = obj => isLookObj(obj)
  && 'sortBy' in obj;

const sortObjParse = o => isLookObj(o)
  ? (isSortObj(o) ? o : isSortObj(o.index) ? o.index : null)
  : null;

const isConfigObj = (obj, objType = typeof obj) => obj
  && /object|function/.test(objType)
  && !mmEnumIsRow(obj)
  && !Array.isArray(obj);

// return last query argument (optionally) provides query configurations
const queryArgsOptions = (queryArgs, queryOptionsDefault = {}) => {
  const queryOptions = queryArgs.slice(-1)[0] || {};

  return (isConfigObj(queryOptions))
    ? queryOptions
    : queryOptionsDefault;
};

// use when order not important and sorting helps verify a list
const compare = (a, b, prop) => {
  if (a[prop] < b[prop]) return -1;
  if (a[prop] > b[prop]) return 1;
  return 0;
};

const asList = value => Array.isArray(value) ? value : [ value ];

const query = {};

const mockdbSuspendArgSpend = (db, qst, reqlObj, rows) => {
  if (rows && rows.length) {
    qst.rowMap[reqlObj.recId] = rows.slice();
  }

  const val = reqlObj.recs.reduce((qstNext, rec, i) => {
    // avoid mutating original args w/ suspended values
    const queryArgs = rec.queryArgs.slice();

    if (qstNext.error && !mmEnumQueryNameIsCURSORORDEFAULTRe.test(rec.queryName))
      return qstNext;
    
    if (rec.queryName === 'row') {
      // following authentic rethinkdb, disallow most nested short-hand
      // row queries. legacy 'rethinkdb' driver is sometimes more permissive
      // than newer rethinkdb-ts: rethinkdb-ts behaviour preferred here
      //
      // ex, nested r.row('membership') elicits an error
      // ```
      // r.expr(list).filter( // throws error
      //   r.row('user_id').eq('xavier').or(r.row('membership').eq('join'))
      // ```
      if (qstNext.rowDepth >= 1 && i === 0 && (
        !mmEnumQueryArgTypeROWIsRe.test(rec.queryArgs[0]))) {
        throw mmErrCannotUseNestedRow();
      } else {
        qstNext.rowDepth += 1;
      }
    }

    if (reqlObj.type === mmEnumQueryArgTypeROW && rows && i === 0) {
      // assigns row from callee to this pattern target,
      //  * this pattern must represent the beginning of a chain
      //  * this pattern is not a 'function'; pattern will not resolve row
      //
      // ex, filter passes each item to the embedded 'row'
      // ```
      // r.expr(list).filter(
      //   r.row('time_expire').during(
      //     r.epochTime(0),
      //     r.epochTime(now / 1000)))
      // ```
      qstNext.target = rows[0];
    }

    try {
      qstNext = (/\.fn/.test(rec.queryName)
        ? query[rec.queryName.replace(/\.fn/, '')].fn
        : query[rec.queryName]
      )(db, qstNext, queryArgs, reqlObj);
    } catch (e) {
      // do not throw error if chain subsequently uses `.default(...)`
      // * if no future default query exists, tag error
      // * throw all tagged errors up to user
      qstNext.target = null;
      qstNext.error = e;

      if (reqlObj.recs.slice(-1)[0].queryName === 'getCursor')
        return qstNext;
 
      e[mmEnumTypeERROR] = typeof e[mmEnumTypeERROR] === 'boolean'
        ? e[mmEnumTypeERROR]
        : !reqlObj.recs.slice(i + 1).some(o => mmEnumQueryNameIsCURSORORDEFAULTRe.test(o.queryName));

      if (e[mmEnumTypeERROR])
        throw e;
    }

    return qstNext;
  }, {
    // if nested spec is not a function expression,
    // pass target value down from parent
    //
    // r.expr(...).map(
    //   r.branch(
    //     r.row('victories').gt(100),
    //     r.row('name').add(' is a hero'),
    //     r.row('name').add(' is very nice')))
    target: reqlObj.recs[0].queryName === 'row' ? qst.target : null,
    recId: reqlObj.recId,
    rowMap: qst.rowMap || {},
    rowDepth: qst.rowDepth || 0
  });

  return val.target;
};

const spend = (db, qst, qspec, rows, d = 0, type = typeof qspec, f = null) => {
  if (qspec === f
    || isBoolNumUndefRe.test(type)
    || qspec instanceof Date) {
    f = qspec;
  } else if (d === 0 && type === 'string') {
    // return field value from query like this,
    // ```
    // row('fieldname')
    // ```
    // seems okay now, may require adding an additonal
    // check later, such as !mmEnumIsRow(rows[0])
    f = rows && rows[0] ? rows[0][qspec] : qspec;
  } else if (mmEnumIsRow(qspec)) {
    // why re-use existing reql.rows, eg `spec.rows || rows`?
    // ```
    // r.expr([{ type: 'boot' }]).contains(row => r
    //  .expr([ 'cleat' ]).contains(row.getField('type')))
    //  .run();
    // ```
    // `{ type: 'boot' }` is correct, existing row at `row.getField('type')`,
    // but '.contains( ... )' would define incorrect row 'cleat'.
    //
    // current unit-tests pass, but logic could be wrong in some situations
    f = mockdbSuspendArgSpend(db, qst, qspec, rows);
  } else if (Array.isArray(qspec)) {
    // detach if spec is has args
    if (mmEnumIsQueryArgsResult(qspec.slice(-1)[0])) {
      f = qspec.slice(-1)[0].run();
    } else {
      f = qspec.map(v => spend(db, qst, v, rows, d + 1));
      f = mmEnumIsQueryArgsResult(f[0]) ? reqlArgsParse(f[0]) : f;
    }
    // render nested query objects, shallow. ex `row('id')`,
    // ```
    // r.expr([{ id: 1 }, { id: 2 }])
    //  .merge(row => ({ oldid: row('id'), id: 0 }))
    //  .run()
    // ```    
  } else if (mmEnumIsRowShallow(qspec)) {
    f = Object.keys(qspec).reduce((prev, key) => {
      prev[key] = spend(db, qst, qspec[key], rows, d + 1);

      return prev;
    }, {});
  } else {
    f = qspec;
  }

  return f;
};

const mockdbReqlQueryOrStateDbName = (qst, db) => (
  qst.db || db.dbSelected);

query.connect = (db, qst, args) => {
  const [ conn ] = args;
  const { host, port, user /*, password*/ } = conn;

  if (db.dbConnections.every(c => c.db !== conn.db)) {
    db.dbConnections.push(conn);
  }

  if (conn.db)
    db.dbSelected = conn.db;

  qst.target = new mmConn('connection', conn.db, host, port, user);

  return qst;
};


query.connectPool = (db, qst, args) => {
  const [ conn ] = args;
  const { host, port, user, password } = conn;

  if (db.dbConnections.every(c => c.db !== conn.db)) {
    db.dbConnections.push(conn);
  }

  if (conn.db)
    db.dbSelected = conn.db;

  qst.target = new mmConn(
    'connectionPool', conn.db, host, port, user, password);

  return qst;
};

query.getPoolMaster = (db, qst) => {
  const conn = db.dbConnections[0] || {
    db: 'default',
    host: 'localhost',
    port: 28015,
    user: '',
    password: ''
  };

  const { host, port, user, password } = conn;

  qst.target = new mmConn(
    'connectionPoolMaster', conn.db, host, port, user, password);

  return qst;
};

query.getPool = query.getPoolMaster;

// used for selecting/specifying db, not supported yet
query.db = (db, qst, args) => {
  const [ dbName ] = args;
  const isValidDbNameRe = /^[A-Za-z0-9_]*$/;

  if (!args.length) {
    throw mmErrArgumentsNumber('r.dbCreate', 1, args.length);
  }

  if (!isValidDbNameRe.test(dbName)) {
    throw mmErrInvalidDbName(dbName);
  }

  qst.db = dbName;

  return qst;
};

query.dbList = (db, qst) => {
  qst.target = Object.keys(db.db);

  return qst;
};

query.dbCreate = (db, qst, args) => {
  const dbName = spend(db, qst, args[0]);

  if (!args.length)
    throw mmErrArgumentsNumber('r.dbCreate', 1, args.length);

  mmDbCreate(db, dbName);

  qst.target = {
    config_changes: [ {
      new_val: mmDbConfigGet(dbName),
      old_val: null
    } ],
    dbs_created: 1
  };

  return qst;
};

query.dbDrop = (db, qst, args) => {
  const [ dbName ] = args;
  const dbConfig = mmDbConfigGet(db, dbName);
  const tables = mmDbGet(db, dbName);

  if (args.length !== 1)
    throw mmErrArgumentsNumber('r.dbDrop', 1, args.length);

  db = mmDbDrop(db, dbName);

  qst.target = {
    config_changes: [ {
      new_val: null,
      old_val: dbConfig
    } ],
    dbs_dropped: 1,
    tables_dropped: Object.keys(tables).length
  };

  return qst;
};

query.config = (db, qst, args) => {
  if (args.length)
    throw mmErrArgumentsNumber('config', 0, args.length);

  const dbName = mockdbReqlQueryOrStateDbName(qst, db);

  if (qst.tablename) {
    qst.target = mmDbTableConfigGet(db, dbName, qst.tablename);
    qst.target = { // remove indexes data added for internal use
      ...qst.target,
      indexes: qst.target.indexes.map(i => i[0])
    };
  } else {
    qst.target = mmDbConfigGet(db, dbName, qst.tableName);
  }

  return qst;
};

query.status = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const tableConfig = mmDbTableConfigGet(db, dbName, qst.tablename);

  qst.target = mmResTableStatus(tableConfig);

  return qst;
};

query.info = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  qst.target = mmResTableInfo(db, dbName, qst.tablename);

  return qst;
};

query.tableList = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const tables = mmDbGet(db, dbName);

  qst.target = Object.keys(tables);

  return qst;
};

query.tableCreate = (db, qst, args) => {
  const tableName = spend(db, qst, args[0]);
  const isValidConfigKeyRe = /^(primaryKey|durability)$/;
  const isValidTableNameRe = /^[A-Za-z0-9_]*$/;
  const config = queryArgsOptions(args);
  const invalidConfigKey = Object.keys(config)
    .find(k => !isValidConfigKeyRe.test(k));

  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(
      invalidConfigKey, config[invalidConfigKey]);
  }

  if (!tableName) {
    throw mmErrArgumentsNumber('r.tableCreate', 1, 0, true);
  }

  if (!isValidTableNameRe.test(tableName)) {
    throw mmErrInvalidTableName(tableName);
  }

  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const tables = mmDbGet(db, dbName);
  if (tableName in tables) {
    throw mmErrTableExists(dbName, tableName);
  }

  db = mmDbTableCreate(db, dbName, tableName, config);

  const tableConfig = mmDbTableConfigGet(db, dbName, tableName);

  qst.target = {
    tables_created: 1,
    config_changes: [ {
      new_val: tableConfig,
      old_val: null
    } ]
  };

  return qst;
};

query.tableDrop = (db, qst, args) => {
  const [ tableName ] = args;
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const tableConfig = mmDbTableConfigGet(db, dbName, tableName);

  db = mmDbTableDrop(db, dbName, tableName);
    
  qst.target = {
    tables_dropped: 1,
    config_changes: [ {
      new_val: null,
      old_val: tableConfig
    } ]
  };

  return qst;
};

// .indexCreate( 'foo' )
// .indexCreate( 'foo', { multi: true })
// .indexCreate( 'things', r.row( 'hobbies' ).add( r.row( 'sports' ) ), { multi: true })
// .indexCreate([ r.row('id'), r.row('numeric_id') ])
query.indexCreate = (db, qst, args) => {
  const [ indexName ] = args;
  const config = queryArgsOptions(args);
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);

  const fields = mmEnumIsRowShallow(args[1])
    ? args[1]
    : [ indexName ];

  mmDbTableIndexAdd(
    db, dbName, qst.tablename, indexName, fields, config);

  // should throw ReqlRuntimeError if index exits already
  qst.target = { created: 1 };

  return qst;
};

query.indexWait = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const tableIndexList = mmDbTableGetIndexNames(
    db, dbName, qst.tablename);

  qst.target = tableIndexList.map(indexName => ({
    index: indexName,
    ready: true,
    function: 1234,
    multi: false,
    geo: false,
    outdated: false
  }));

  return qst;
};

query.indexList = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const tableConfig = mmDbTableConfigGet(db, dbName, qst.tablename);

  qst.target = tableConfig.indexes.map(i => i[0]);

  return qst;
};
// pass query down to 'spend' and copy data
query.insert = (db, qst, args, reqlObj) => {
  // both argument types (list or atom) resolved to a list here
  let documents = Array.isArray(args[0]) ? args[0] : args.slice(0, 1);
  let table = qst.tablelist;
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const primaryKey = mmDbTableGetPrimaryKey(db, dbName, qst.tablename);
  const options = args[1] || {};

  const isValidConfigKeyRe = /^(returnChanges|durability|conflict)$/;
  const invalidConfigKey = Object.keys(options)
    .find(k => !isValidConfigKeyRe.test(k));

  if (args.length > 1 && (!args[1] || typeof args[1] !== 'object')) {
    throw mmErrSecondArgumentOfQueryMustBeObject('insert');
  }

  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(invalidConfigKey, options[invalidConfigKey]);
  }

  if (documents.length === 0) {
    throw mmErrArgumentsNumber('insert', 1, 0);
  }

  const documentIsPrimaryKeyPredefined = documents
    .some(d => primaryKey in d);

  documents = documents
    .map(doc => mmTableDocEnsurePrimaryKey(doc, primaryKey));

  const existingDocs = mmTableDocsGet(
    qst.tablelist, documents.map(doc => doc[primaryKey]), primaryKey);

  if (existingDocs.length) {
    if (mmEnumIsRow(options.conflict)) {
      const resDoc = spend(db, qst, options.conflict, [
        documents[0].id,
        existingDocs[0],
        documents[0]
      ]);

      const changes = [ {
        old_val: existingDocs[0],
        new_val: resDoc
      } ];

      mmTableDocsSet(table, [ resDoc ], primaryKey);

      qst.target = mmResChangesFieldCreate({
        replaced: documents.length,
        changes: options.returnChanges === true ? changes : undefined
      });

      return qst;
    } else if (/^(update|replace)$/.test(options.conflict)) {
      const conflictIds = existingDocs.map(doc => doc[primaryKey]);
      // eslint-disable-next-line security/detect-non-literal-regexp
      const conflictIdRe = new RegExp(`^(${conflictIds.join('|')})$`);
      const conflictDocs = documents.filter(doc => conflictIdRe.test(doc[primaryKey]));

      qst = options.conflict === 'update'
        ? query.update(db, qst, conflictDocs)
        : query.replace(db, qst, conflictDocs);

      return qst;
    } else {
      qst.target = mmResChangesFieldCreate({
        errors: 1,
        firstError: mmErrDuplicatePrimaryKey(
          existingDocs[0],
          documents.find(doc => doc[primaryKey] === existingDocs[0][primaryKey])
        ).message
      });
    }
        
    return qst;
  }

  [ table, documents ] = mmTableDocsSet(
    table, documents.map(doc => spend(db, qst, doc)), primaryKey);

  const changes = documents.map(doc => ({
    old_val: null,
    new_val: doc
  }));

  db = mmDbTableCursorsPushChanges(
    db, dbName, qst.tablename, changes, mmResChangeTypeADD);

  qst.target = mmResChangesFieldCreate({
    ...(documentIsPrimaryKeyPredefined || {
      generated_keys: documents.map(doc => doc[primaryKey])
    }),
    inserted: documents.length,
    changes: options.returnChanges === true ? changes : undefined
  });

  return qst;
};

query.update = (db, qst, args) => {
  const queryTarget = qst.target;
  const queryTable = qst.tablelist;
  const updateProps = spend(db, qst, args[0]);
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const primaryKey = mmDbTableGetPrimaryKey(db, dbName, qst.tablename);
  const options = args[1] || {};

  const updateTarget = targetDoc => {
    const [ oldDoc ] = mmTableDocsGet(queryTable, [ targetDoc[primaryKey] ], primaryKey);
    let newDoc = updateProps === null
      ? null
      : oldDoc && Object.assign({}, oldDoc, updateProps || {});

    if (oldDoc && newDoc) {
      mmTableDocsSet(queryTable, [ newDoc ], primaryKey);
      // [ , newDoc ] = mmTableSetDocument(queryTable, newDoc, primaryKey);
    }

    return [ newDoc, oldDoc ];
  };

  const targetList = asList(queryTarget);
  const changesDocs = targetList.reduce((changes, targetDoc) => {
    const [ newDoc, oldDoc ] = updateTarget(targetDoc);

    if (newDoc) {
      changes.push({
        new_val: newDoc,
        old_val: oldDoc
      });
    }

    return changes;
  }, []);

  db = mmDbTableCursorsPushChanges(
    db, dbName, qst.tablename, changesDocs);

  qst.target = mmResChangesCreate(changesDocs, {
    unchanged: targetList.length - changesDocs.length,
    changes: options.returnChanges === true ? changesDocs : undefined
  });

  return qst;
};

query.get = (db, qst, args) => {
  const primaryKeyValue = spend(db, qst, args[0]);
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const primaryKey = mmDbTableGetPrimaryKey(db, dbName, qst.tablename);
  const tableDoc = mmTableDocsGet(qst.target, [ primaryKeyValue ], primaryKey)[0];

  if (args.length === 0) {
    throw mmErrArgumentsNumber('get', 1, 0);
  }

  if (!isBoolNumStrRe.test(typeof primaryKeyValue)
    && !Array.isArray(primaryKeyValue)) {
    throw mmErrPrimaryKeyWrongType(primaryKeyValue);
  }

  // define primaryKeyValue on qst to use in subsequent change() query
  // for the case of change() request for document which does not exist (yet)
  qst.primaryKeyValue = primaryKeyValue;
  qst.target = tableDoc || null;

  return qst;
};

query.get.fn = (db, qst, args) => {
  qst.target = spend(db, qst, args[0], [ qst.target ]);
  
  return qst;
};

query.getAll = (db, qst, args) => {
  const queryOptions = queryArgsOptions(args);
  const primaryKeyValues = spend(
    db,
    qst,
    (queryOptions && queryOptions.index) ? args.slice(0, -1) : args
  );

  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const { tablename } = qst;
  const primaryKey = queryOptions.index
    || mmDbTableGetPrimaryKey(db, dbName, qst.tablename);
  const tableIndexTuple = mmDbTableGetIndexTuple(db, dbName, tablename, primaryKey);
  if (primaryKeyValues.length === 0) {
    qst.target = [];

    return qst;
  }

  const tableDocHasIndex = mmTableDocHasIndexValueFn(
    tableIndexTuple, primaryKeyValues, db);

  qst.target = qst.target
    .filter(doc => tableDocHasIndex(doc, spend, qst))
    .sort(() => 0.5 - Math.random());

  return qst;
};

// The replace command can be used to both insert and delete documents.
// If the “replaced” document has a primary key that doesn’t exist in the
// table, the document will be inserted; if an existing document is replaced
// with null, the document will be deleted. Since update and replace
// operations are performed atomically, this allows atomic inserts and
// deletes as well.
query.replace = (db, qst, args) => {
  const queryTarget = qst.target;
  const queryTable = qst.tablelist;
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const primaryKey = mmDbTableGetPrimaryKey(db, dbName, qst.tablename);
  const config = queryArgsOptions(args.slice(1));

  const isValidConfigKeyRe = /^(returnChanges|durability|nonAtomic|ignoreWriteHook)$/;
  const invalidConfigKey = Object.keys(config)
    .find(k => !isValidConfigKeyRe.test(k));
  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(
      invalidConfigKey, config[invalidConfigKey]);
  }

  if (!args.length) {
    throw mmErrArgumentsNumber('replace', 1, args.length, true);
  }

  const updateTarget = targetDoc => {
    const replacement = spend(db, qst, args[0], [ targetDoc ]);
    const [ oldDoc = null ] = (targetDoc &&
      mmTableDocsGet(queryTable, [ targetDoc[primaryKey] ], primaryKey)) || [];

    const newDoc = replacement === null
      ? null
      : replacement;
    if (newDoc)
      mmTableDocsSet(queryTable, [ newDoc ], primaryKey);

    if (oldDoc && newDoc === null)
      mmTableDocsRm(queryTable, [ oldDoc ], primaryKey);

    return [ newDoc, oldDoc ];
  };

  const targetList = asList(queryTarget);
  const changesDocs = targetList.reduce((changes, targetDoc) => {
    const [ newDoc, oldDoc ] = updateTarget(targetDoc);

    changes.push({
      new_val: newDoc,
      old_val: oldDoc
    });

    return changes;
  }, []);

  db = mmDbTableCursorsPushChanges(
    db, dbName, qst.tablename, changesDocs);

  qst.target = mmResChangesCreate(changesDocs, {
    changes: config.returnChanges === true
      ? changesDocs : undefined
  });

  return qst;
};

query.prepend = (db, qst, args) => {
  const prependValue = spend(db, qst, args[0]);

  if (typeof prependValue === 'undefined') {
    throw mmErrArgumentsNumber('prepend', 1, 0, false);
  }

  qst.target.unshift(prependValue);

  return qst;
};

query.difference = (db, qst, args) => {
  const differenceValues = spend(db, qst, args[0]);

  if (typeof differenceValues === 'undefined') {
    throw mmErrArgumentsNumber('difference', 1, 0, false);
  }

  qst.target = qst.target
    .filter(e => !differenceValues.some(a => e == a));

  return qst;
};

query.nth = (db, qst, args) => {
  const nthIndex = spend(db, qst, args[0]);

  if (nthIndex >= qst.target.length) {
    throw mmErrIndexOutOfBounds(nthIndex);
  }

  qst.target = qst.target[nthIndex];

  return qst;
};

// this calls row then row.fn
// r.row('age').gt(5)
// r.row → value
// row => row('name_screenname')
// r.row( 'hobbies' ).add( r.row( 'sports' )
//
// dynamic row sometimes passes value,
//   [ 'reqlARGSSUSPEND', 'reqlARGSIG.row', 0, 'row' ]
//
// target row will include value in qst.target
// question: should it be possible for qst.target to be defined here?
//           even when ...
//
query.row = (db, qst, args) => {
  if (args[0] === mmEnumQueryArgTypeROW && !(args[1] in qst.rowMap)) {
    // keep this for development
    // console.log(qst.target, mockdbSpecSignature(reqlObj), args, qst.rowMap);
    throw new Error('[!!!] error: missing ARGS from ROWMAP');
  }

  qst.target = args[0] === mmEnumQueryArgTypeROW
    ? qst.rowMap[args[1]][args[2]]
    : qst.target[args[0]];
  
  return qst;
};

query.row.fn = (db, qst, args, reqlObj) => {
  if (typeof args[0] === 'string' && !(args[0] in qst.target)) {
    throw mmErrNoAttributeInObject(args[0]);
  }

  return query.getField(db, qst, args, reqlObj);
};

query.default = (db, qst, args) => {
  if (qst.target === null) {
    qst.error = null;
    qst.target = spend(db, qst, args[0]);
  }

  return qst;
};

// time.during(startTime, endTime[, {leftBound: "closed", rightBound: "open"}]) → bool
query.during = (db, qst, args) => {
  const targetTime = qst.target.getTime();
  const startDate = spend(db, qst, args[0]);
  const endDate = spend(db, qst, args[1]);

  qst.target = (
    targetTime > startDate.getTime()
      && targetTime < endDate.getTime());

  return qst;
};

query.append = (db, qst, args) => {
  qst.target = spend(db, qst, args).reduce((list, val) => {
    list.push(val);

    return list;
  }, qst.target);

  return qst;
};

// NOTE rethinkdb uses re2 syntax
// re using re2-specific syntax will fail
query.match = (db, qst, args) => {
  let regexString = spend(db, qst, args[0]);

  let flags = '';
  if (regexString.startsWith('(?i)')) {
    flags = 'i';
    regexString = regexString.slice('(?i)'.length);
  }

  // eslint-disable-next-line security/detect-non-literal-regexp
  const regex = new RegExp(regexString, flags);

  if (typeof qst.target === 'number') {
    throw mmErrExpectedTypeFOOButFoundBAR('STRING', 'NUMBER');
  }

  qst.target = regex.test(qst.target);

  return qst;
};

query.delete = (db, qst, args) => {
  const queryTarget = qst.target;
  const queryTable = qst.tablelist;
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const primaryKey = mmDbTableGetPrimaryKey(db, dbName, qst.tablename);
  const tableIndexTuple = mmDbTableGetIndexTuple(
    db, dbName, qst.tablename, primaryKey);
  const targetList = asList(queryTarget);
  const targetIds = targetList
    .map(doc => mmTableDocGetIndexValue(doc, tableIndexTuple, spend, qst, db));
    // eslint-disable-next-line security/detect-non-literal-regexp
  const targetIdRe = new RegExp(`^(${targetIds.join('|')})$`);
  const options = queryArgsOptions(args);
  const tableFiltered = queryTable.filter(doc => !targetIdRe.test(
    mmTableDocGetIndexValue(doc, tableIndexTuple, spend, qst, db)));
  const queryConfig = queryArgsOptions(args);
  const isValidConfigKeyRe = /^(durability|returnChanges|ignoreWriteHook)$/;
  const invalidConfigKey = Object.keys(queryConfig)
    .find(k => !isValidConfigKeyRe.test(k));

  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(
      invalidConfigKey, queryConfig[invalidConfigKey]);
  }

  const changesDocs = targetList.reduce((changes, targetDoc) => {
    if (targetDoc) {
      changes.push({
        new_val: null,
        old_val: targetDoc
      });
    }

    return changes;
  }, []);

  mmTableSet(queryTable, tableFiltered);

  db = mmDbTableCursorsPushChanges(
    db, dbName, qst.tablename, changesDocs);

  qst.target = mmResChangesFieldCreate({
    deleted: changesDocs.length,
    changes: options.returnChanges === true ? changesDocs : undefined
  });

  return qst;
};

query.contains = (db, qst, args) => {
  const queryTarget = qst.target;

  if (!args.length) {
    throw new Error('Rethink supports contains(0) but rethinkdbdash does not.');
  }

  if (!Array.isArray(qst.target)) {
    throw mmErrExpectedTypeFOOButFoundBAR('SEQUENCE', 'SINGLE_SELECTION');
  }

  if (mmEnumIsRow(args[0])) {
    qst.target = queryTarget.some(target => {
      const res = spend(db, qst, args[0], [ target ]);

      return typeof res === 'boolean'
        ? res
        : queryTarget.includes(res);
    });
  } else {
    qst.target = args.every(predicate => (
      queryTarget.includes(spend(db, qst, predicate))));
  }

  return qst;
};

// Get a single field from an object. If called on a sequence, gets that field
// from every object in the sequence, skipping objects that lack it.
//
// https://rethinkdb.com/api/javascript/get_field
query.getField = (db, qst, args, reqlObj) => {
  const [ fieldName ] = spend(db, qst, args);

  if (args.length === 0) {
    throw mmErrArgumentsNumber('(...)', 1, args.length);
  }

  // if ( Array.isArray( qst.target ) ) {
  //  qst.error = 'Expected type DATUM but found SEQUENCE"';
  //  qst.target = null;
  //   return qst;
  // }
  
  qst.target = Array.isArray(qst.target)
    ? qst.target.map(t => t[fieldName])
    : qst.target[fieldName];

  return qst;
};

query.filter = (db, qst, args) => {
  if (qst.target instanceof Readable
    && 'changesTarget' in qst) {
    // eg, changes().filter( filterQuery )
    qst.target.streamFilter = item => (
      spend(db, qst, args[0], [ item ]));
  } else {
    qst.target = qst.target.filter(item => {
      // predicate.rows = [ item ];
      const finitem = spend(db, qst, args[0], [ item ]);

      if (finitem && typeof finitem === 'object') {
        return Object
          .keys(finitem)
          .every(key => finitem[key] === item[key]);
      }

      return finitem;
    });
  }

  return qst;
};

query.filter.fn = query.getField;

query.count = (db, qst) => {
  qst.target = qst.target.length;

  return qst;
};

query.pluck = (db, qst, args) => {
  const target = qst.target;
  const pluckObj = (obj, props) => props.reduce((plucked, prop) => {
    plucked[prop] = obj[prop];
    return plucked;
  }, {});

  args = spend(db, qst, args);

  qst.target = Array.isArray(target)
    ? target.map(t => pluckObj(t, args))
    : pluckObj(target, args);

  return qst;
};

query.hasFields = (db, qst, args) => {
  const queryTarget = qst.target;
  const itemHasFields = item => Boolean(item && args
    .every(name => Object.prototype.hasOwnProperty.call(item, name)));

  qst.target = Array.isArray(queryTarget)
    ? queryTarget.filter(itemHasFields)
    : itemHasFields(queryTarget);

  return qst;
};

query.slice = (db, qst, args) => {
  const [ begin, end ] = spend(db, qst, args.slice(0, 2));

  if (qst.isGrouped) { // slice from each group
    qst.target = qst.target.map(targetGroup => {
      targetGroup.reduction = targetGroup.reduction.slice(begin, end);

      return targetGroup;
    });
  } else {
    qst.target = qst.target.slice(begin, end);
  }

  return qst;
};

query.skip = (db, qst, args) => {
  const count = spend(db, qst, args[0]);

  qst.target = qst.target.slice(count);

  return qst;
};

query.limit = (db, qst, args) => {
  qst.target = qst.target.slice(0, args[0]);

  return qst;
};

// Documents in the result set consist of pairs of left-hand and right-hand documents,
// matched when the field on the left-hand side exists and is non-null and an entry
// with that field’s value exists in the specified index on the right-hand side.
query.eqJoin = (db, qst, args) => {
  const queryTarget = qst.target;
  const isNonNull = v => v !== null && v !== undefined;
  const queryConfig = queryArgsOptions(args);
  const isValidConfigKeyRe = /^index$/;
  const rightFields = spend(db, qst, args[1]);

  // should remove this... get table name from args[1] record
  // and call query.config() directly
  const rightFieldConfig = args[1] && spend(db, qst, {
    type: mmEnumQueryArgTypeROW,
    recs: [
      ...args[1].recs,
      { queryName: 'config', queryArgs: [] }
    ]
  });
  
  const rightFieldKey = (queryConfig.index)
        || (rightFieldConfig && rightFieldConfig.primary_key);
  const invalidConfigKey = Object.keys(queryConfig)
    .find(k => !isValidConfigKeyRe.test(k));

  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(
      invalidConfigKey, queryConfig[invalidConfigKey]);
  }
    
  if (args.length === 0) {
    throw mmErrArgumentsNumber('eqJoin', 2, 0, true);
  }

  qst.target = queryTarget.reduce((joins, item) => {
    const leftFieldSpend = spend(db, qst, args[0], [ item ]);
    
    const leftFieldValue = qst.tablelist
      ? item // if value comes from table use full document
      : leftFieldSpend;

    if (isNonNull(leftFieldValue)) {
      const rightFieldValue = rightFields
        .find(rf => rf[rightFieldKey] === leftFieldSpend);

      if (isNonNull(rightFieldValue)) {
        joins.push({
          left: leftFieldValue,
          right: rightFieldValue
        });
      }
    }

    return joins;
  }, []);

  qst.eqJoinBranch = true;
  return qst;
};

query.eqJoin.fn = query.getField;

// Used to ‘zip’ up the result of a join by merging the ‘right’ fields into
// ‘left’ fields of each member of the sequence.
query.zip = (db, qst) => {
  qst.target = qst.target
    .map(t => ({ ...t.left, ...t.right }));

  return qst;
};

query.innerJoin = (db, qst, args) => {
  const queryTarget = qst.target;
  const [ otherSequence, joinFunc ] = args;
  const otherTable = spend(db, qst, otherSequence);

  qst.target = queryTarget.map(item => (
    otherTable.map(otherItem => {
      // problem here is we don't know if item will be evaluated first
      const oinSPend = spend(db, qst, joinFunc, [ item, otherItem ]);

      return {
        left: item,
        right: oinSPend ? otherItem : null
      };
    })
  )).flat().filter(({ right }) => right);

  return qst;
};

query.now = (db, qst) => {
  qst.target = new Date();

  return qst;
};

query.toEpochTime = (db, qst) => {
  qst.target = (new Date(qst.target)).getTime() / 1000;

  return qst;
};

query.epochTime = (db, qst, args) => {
  qst.target = new Date(args[0] * 1000);

  return qst;
};

query.not = (db, qst) => {
  const queryTarget = qst.target;

  if (typeof queryTarget !== 'boolean')
    throw mmErrCannotCallFOOonBARTYPEvalue('not()', 'non-boolean');

  qst.target = !queryTarget;

  return qst;
};

query.gt = (db, qst, args) => {
  qst.target = qst.target > spend(db, qst, args[0]);

  return qst;
};

query.ge = (db, qst, args) => {
  qst.target = qst.target >= spend(db, qst, args[0]);

  return qst;
};

query.lt = (db, qst, args) => {
  const argTarget = spend(db, qst, args[0]);
  
  if (argTarget instanceof Date && !(qst.target instanceof Date)) {
    throw mmErrNotATIMEpsudotype('forEach', 1, args.length);
  }

  if (typeof qst.target === typeof qst.target) {
    qst.target = qst.target < argTarget;
  }

  return qst;
};

query.le = (db, qst, args) => {
  qst.target = qst.target <= spend(db, qst, args[0]);

  return qst;
};

query.eq = (db, qst, args) => {
  qst.target = qst.target === spend(db, qst, args[0]);

  return qst;
};

query.ne = (db, qst, args) => {
  qst.target = qst.target !== spend(db, qst, args[0]);

  return qst;
};

query.max = (db, qst, args) => {
  const targetList = qst.target;
  const getListMax = (list, prop) => list.reduce((maxDoc, doc) => (
    maxDoc[prop] > doc[prop] ? maxDoc : doc
  ), targetList);

  const getListMaxGroups = (groups, prop) => (
    groups.reduce((prev, target) => {
      prev.push({
        ...target,
        reduction: getListMax(target.reduction, prop)
      });

      return prev;
    }, [])
  );

  qst.target = qst.isGrouped
    ? getListMaxGroups(targetList, args[0])
    : getListMax(targetList, args[0]);

  return qst;
};

query.max.fn = (db, qst, args) => {
  const field = spend(db, qst, args[0]);

  if (qst.isGrouped) {
    qst.target = qst.target.map(targetGroup => {
      targetGroup.reduction = targetGroup.reduction[field];

      return targetGroup;
    });
  } else {
    qst.target = qst.target[field];
  }

  return qst;
};

query.min = (db, qst, args) => {
  const targetList = qst.target;
  const getListMin = (list, prop) => list.reduce((maxDoc, doc) => (
    maxDoc[prop] < doc[prop] ? maxDoc : doc
  ), targetList);

  const getListMinGroups = (groups, prop) => (
    groups.reduce((prev, target) => {
      prev.push({
        ...target,
        reduction: getListMin(target.reduction, prop)
      });

      return prev;
    }, [])
  );

  qst.target = qst.isGrouped
    ? getListMinGroups(targetList, args[0])
    : getListMin(targetList, args[0]);

  return qst;
};

query.merge = (db, qst, args, queryObj) => {
  if (args.length === 0) {
    throw mmErrArgumentsNumber('merge', 1, args.length, true);
  }
  
  // evaluate anonymous function given as merge definition
  const mergeTarget = (merges, target) => merges.reduce((p, next) => (
    Object.assign(p, spend(db, qst, next, [ target ]))
  ), { ...target });

  qst.target = Array.isArray(qst.target)
    ? qst.target.map(i => mergeTarget(args, i))
    : mergeTarget(args, qst.target);

  return qst;
};

query.concatMap = (db, qst, args) => {
  const [ func ] = args;

  qst.target = qst
    .target.map(t => spend(db, qst, func, [ t ])).flat();

  return qst;
};

query.isEmpty = (db, qst) => {
  qst.target = qst.target.length === 0;

  return qst;
};

query.add = (db, qst, args) => {
  const target = qst.target;
  const values = spend(db, qst, args);

  if (target === null) {
    qst.target = Array.isArray(values)
      ? values.slice(1).reduce((prev, val) => prev + val, values[0])
      : values;
  } else if (isBoolNumStrRe.test(typeof target)) {
    qst.target = values.reduce((prev, val) => prev + val, target);
  } else if (Array.isArray(target)) {
    qst.target = [ ...target, ...values ];
  }

  return qst;
};

query.sub = (db, qst, args) => {
  const target = qst.target;
  const values = spend(db, qst, args);

  if (typeof target === null) {
    qst.target = Array.isArray(values)
      ? values.slice(1).reduce((prev, val) => prev - val, values[0])
      : values;
  } else if (isBoolNumStrRe.test(typeof target)) {
    qst.target = values.reduce((prev, val) => prev - val, target);
  }

  return qst;
};

query.mul = (db, qst, args) => {
  const target = qst.target;
  const values = spend(db, qst, args);

  if (typeof target === null) {
    qst.target = Array.isArray(values)
      ? values.slice(1).reduce((prev, val) => prev * val, values[0])
      : values;
  } else if (isBoolNumStrRe.test(typeof target)) {
    qst.target = values.reduce((prev, val) => prev * val, target);
  }

  return qst;
};

// .group(field | function..., [{index: <indexname>, multi: false}]) → grouped_stream
// arg can be stringy field name, { index: 'indexname' }, { multi: true }
query.group = (db, qst, args) => {
  const queryTarget = qst.target;
  const [ arg ] = args;
  const groupedData = queryTarget.reduce((group, item) => {
    const key = (typeof arg === 'object' && arg && 'index' in arg)
      ? arg.index
      : spend(db, qst, arg);
    const groupKey = item[key];

    group[groupKey] = group[groupKey] || [];
    group[groupKey].push(item);

    return group;
  }, {});
  const rethinkFormat = Object.entries(groupedData)
    .map(([ group, reduction ]) => ({ group, reduction }));

  qst.isGrouped = true;
  qst.target = rethinkFormat;

  return qst;
};

// array.sample(number) → array
query.sample = (db, qst, args) => {
  qst.target = qst.target
    .sort(() => 0.5 - Math.random())
    .slice(0, args);

  return qst;
};

query.ungroup = (db, qst) => {
  qst.isGrouped = false;

  return qst;
};

query.orderBy = (db, qst, args) => {
  const queryTarget = qst.target;
  const queryOptions = mmEnumIsRow(args[0])
    ? args[0]
    : queryArgsOptions(args);
  const queryOptionsIndex = spend(db, qst, queryOptions.index);
  const indexSortBy = typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortBy;
  const indexSortDirection = (typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortDirection) || 'asc';
  const indexString = typeof queryOptionsIndex === 'string' && queryOptionsIndex;
  const argsSortPropValue = typeof args[0] === 'string' && args[0];
  const indexName = indexSortBy || indexString || 'id';
  let fieldSortDirection = '';
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const tableIndexTuple = mmDbTableGetIndexTuple(db, dbName, qst.tablename, indexName);
  const sortDirection = (isAscending, dir = fieldSortDirection || indexSortDirection) => (
    isAscending * (dir === 'asc' ? 1 : -1));

  const getSortFieldValue = doc => {
    let value;

    // ex, queryOptions,
    //  ({ index: r.desc('date') })
    //  doc => doc('upvotes')
    if (mmEnumIsRowShallow(queryOptions)) {
      value = spend(db, qst, queryOptions, [ doc ]);

      const sortObj = sortObjParse(value);
      if (sortObj) {
        if (sortObj.sortDirection)
          fieldSortDirection = sortObj.sortDirection;

        value = sortObj.sortBy;
      }
    } else if (argsSortPropValue) {
      value = doc[argsSortPropValue];
    } else {
      value = mmTableDocGetIndexValue(doc, tableIndexTuple, spend, qst, db);
    }

    return value;
  };

  if (!args.length) {
    throw mmErrArgumentsNumber('orderBy', 1, args.length, true);
  }

  qst.target = queryTarget.sort((doca, docb) => {
    const docaField = getSortFieldValue(doca, tableIndexTuple);
    const docbField = getSortFieldValue(docb, tableIndexTuple);

    return sortDirection(docaField < docbField ? -1 : 1);
  });

  return qst;
};

// Return the hour in a time object as a number between 0 and 23.
query.hours = (db, qst) => {
  qst.target = new Date(qst.target).getHours();

  return qst;
};

query.minutes = (db, qst) => {
  qst.target = new Date(qst.target).getMinutes();

  return qst;
};

query.uuid = (db, qst) => {
  qst.target = randomUUID();

  return qst;
};

query.expr = (db, qst, args) => {
  const [ argvalue ] = args;

  qst.target = spend(db, qst, argvalue, [ qst.target ]);

  return qst;
};

query.expr.fn = (db, qst, args) => {
  if (Array.isArray(qst.target)) {
    qst.target = qst.target.map(t => t[args[0]]);
  } else if (args[0] in qst.target) {
    qst.target = qst.target[args[0]];
  } else {
    throw mmErrNoAttributeInObject(args[0]);
  }

  return qst;
};

query.coerceTo = (db, qst, args) => {
  const [ coerceType ] = args;
  let resolved = spend(db, qst, qst.target);

  if (coerceType === 'string')
    resolved = String(resolved);

  qst.target = resolved;

  return qst;
};

query.upcase = (db, qst) => {
  qst.target = String(qst.target).toUpperCase();

  return qst;
};

query.downcase = (db, qst) => {
  qst.target = String(qst.target).toLowerCase();

  return qst;
};

query.map = (db, qst, args) => {
  qst.target = qst
    .target.map(t => spend(db, qst, args[0], [ t ]));

  return qst;
};

query.without = (db, qst, args) => {
  const queryTarget = qst.target;

  const withoutFromDoc = (doc, withoutlist) => withoutlist
    .reduce((prev, arg) => {
      delete prev[arg];

      return prev;
    }, doc);
  const withoutFromDocList = (doclist, withoutlist) => doclist
    .map(doc => withoutFromDoc(doc, withoutlist));

  if (args.length === 0) {
    throw mmErrArgumentsNumber('without', 1, args.length);
  }

  args = spend(db, qst, args);

  if (qst.eqJoinBranch) {
    const isleft = 'left' in args[0];
    const isright = 'right' in args[0];
    const leftArgs = isleft && asList(args[0].left);
    const rightArgs = isright && asList(args[0].right);

    if (isleft || isright) {
      qst.target = queryTarget.map(qt => {
        if (isright)
          qt.right = withoutFromDoc(qt.right, rightArgs);

        if (isleft)
          qt.left = withoutFromDoc(qt.left, leftArgs);

        return qt;
      });
    }
  } else {
    qst.target = Array.isArray(queryTarget)
      ? withoutFromDocList(queryTarget, args)
      : withoutFromDoc(queryTarget, args);
  }

  return qst;
};

// Call an anonymous function using return values from other
// ReQL commands or queries as arguments.
query.do = (db, qst, args) => {
  const [ doFn ] = args.slice(-1);

  if (mmEnumIsRow(doFn)) {
    qst.target = args.length === 1
      ? spend(db, qst, doFn, [ qst.target ])
      : spend(db, qst, doFn, args.slice(0, -1));

    if (mmEnumIsQueryArgsResult(qst.target))
      qst.target = reqlArgsParse(qst.target)[0];

  } else if (args.length) {
    qst.target = doFn;
  }

  return qst;
};

query.or = (db, qst, args) => {
  const rows = [ qst.target ];

  qst.target = args.reduce((current, arg) => (
    current || spend(db, qst, arg, rows)
  ), qst.target);

  return qst;
};

query.and = (db, qst, args) => {
  const rows = [ qst.target ];

  qst.target = args.reduce((current, arg) => (
    current && spend(db, qst, arg, rows)
  ), typeof qst.target === 'boolean' ? qst.target : true);
  
  return qst;
};

// if the conditionals return any value but false or null (i.e., “truthy” values),
query.branch = (db, qst, args) => {
  const isResultTruthy = result => (
    result !== false && result !== null);

  const nextCondition = (condition, branches) => {
    const conditionResult = spend(db, qst, condition);

    if (branches.length === 0)
      return conditionResult;

    if (isResultTruthy(conditionResult)) {
      return spend(db, qst, branches[0]);
    }

    return nextCondition(branches[1], branches.slice(2));
  };

  qst.target = nextCondition(args[0], args.slice(1));

  return qst;
};

// Rethink has its own alg for finding distinct,
// but unique by ID should be sufficient here.
query.distinct = (db, qst, args) => {
  const queryOptions = queryArgsOptions(args);
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);

  if (Array.isArray(qst.target)
        && qst.tablename

        // skip if target is filtered, concatenated or manipulated in some way
        && !/string|boolean|number/.test(typeof qst.target[0])) {
    const primaryKey = queryOptions.index
            || mmDbTableGetPrimaryKey(db, dbName, qst.tablename);

    const keys = {};
    qst.target = qst.target.reduce((disti, row) => {
      const value = row[primaryKey];

      if (!keys[value]) {
        keys[value] = true;
        disti.push(value);
      }

      return disti;
    }, []);
  } else if (Array.isArray(qst.target)) {
    qst.target = qst.target.filter(
      (item, pos, self) => self.indexOf(item) === pos);
  } else if (Array.isArray(args[0])) {
    qst.target = args[0].filter(
      (item, pos, self) => self.indexOf(item) === pos);
  }

  return qst;
};

query.union = (db, qst, args) => {
  const queryOptions = queryArgsOptions(args, null);

  if (queryOptions)
    args.splice(-1, 1);

  let res = args.reduce((acc, arg) => {
    return acc.concat(spend(db, qst, arg));
  }, qst.target || []);

  if (queryOptions && queryOptions.interleave) {
    res = res.sort(
      (a, b) => compare(a, b, queryOptions.interleave)
    );
  }

  qst.target = res;

  return qst;
};

query.table = (db, qst, args) => {
  const [ tablename ] = args;
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const dbst = mmDbGet(db, dbName);
  const table = dbst[tablename];

  if (!Array.isArray(dbst[tablename])) {
    throw mmErrTableDoesNotExist(dbName, tablename);
  }

  qst.tablename = tablename;
  qst.tablelist = table;
  qst.target = table.slice();

  return qst;
};

query.table.fn = query.getField;

// r.args(array) → special
query.args = (db, qst, args) => {
  const result = spend(db, qst, args[0]);
  if (!Array.isArray(result))
    throw new Error('args must be an array');

  qst.target = reqlArgsCreate(result);

  return qst;
};

query.desc = (db, qst, args) => {
  qst.target = {
    sortBy: spend(db, qst, args[0], [ qst.target ]),
    sortDirection: 'desc'
  };

  return qst;
};

query.asc = (db, qst, args) => {
  qst.target = {
    sortBy: spend(db, qst, args[0], [ qst.target ]),
    sortDirection: 'asc'
  };

  return qst;
};

query.run = (db, qst) => {
  if (qst.error) {
    throw new Error(qst.error);
  }

  // return qst.target;
  return qst;
};

query.drain = query.run;

query.serialize = (db, qst) => {
  qst.target = JSON.stringify(qst.chain);

  return qst;
};

query.changes = (db, qst, args) => {
  const tableName = qst.tablename;
  const queryTarget = qst.target;
  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const queryTargetFuture = qst.target || {
    [mmDbTableGetPrimaryKey(db, dbName, tableName)]: qst.primaryKeyValue
  };
  const queryOptions = queryArgsOptions(args) || {};
  const cursorTargetType = tableName
    ? (Array.isArray(queryTarget) ? 'table' : 'doc') : 'expr';

  qst.isChanges = true;
  qst.includeInitial = Boolean(queryOptions.includeInitial);
  qst.includeTypes = Boolean(queryOptions.includeTypes);

  let cursors = null;

  if (typeof queryOptions.maxBatchRows !== 'number') {
    queryOptions.maxBatchRows = Math.Infinite;
  }

  if (cursorTargetType === 'doc') {
    cursors = mmDbTableDocCursorsGetOrCreate(
      db, dbName, tableName, queryTargetFuture);
  } else if (cursorTargetType === 'table') {
    cursors = mmDbTableCursorsGetOrCreate(
      db, dbName, tableName);
  }

  const cursorIndex = cursors ? cursors.length : null;
  const initialDocs = [];

  if (!qst.isChanges || qst.includeInitial) {
    asList(queryTarget).map(item => {
      if (cursorTargetType === 'doc' || item || /string|number|boolean/.test(typeof item)) {
        if (queryOptions.includeInitial) {
          initialDocs.push({
            type: mmResChangeTypeINITIAL,
            new_val: item
          });
        } else {
          initialDocs.push({
            new_val: item
          });
        }
      }
    });
  }

  const cursor = mmStream(
    initialDocs, !qst.isChanges, true, qst.includeTypes);

  cursor.close = () => {
    cursor.emit('end');
    cursor.destroy();

    if (cursorTargetType === 'doc')
      db = mmDbTableDocCursorSplice(db, dbName, tableName, queryTargetFuture, cursorIndex);
    if (cursorTargetType === 'table')
      db = mmDbTableCursorSplice(db, dbName, tableName, cursorIndex);

    return new Promise((resolve /*, reject */) => resolve());
  };

  if (cursorTargetType === 'doc')
    db = mmDbTableDocCursorSet(db, dbName, tableName, queryTargetFuture, cursor);
  else if (cursorTargetType === 'table')
    db = mmDbTableCursorSet(db, dbName, tableName, cursor);

  if (!qst.isChanges) {
    if (cursorTargetType === 'table') {
      const changes = queryTarget.map(doc => ({
        new_val: doc
      }));

      db = mmDbTableCursorsPushChanges(
        db, dbName, tableName, changes);
    }
  }

  qst.changesTarget = qst.target;
  qst.target = cursor;

  return qst;
};

// The reduction function can be called on the results of two previous
// reductions because the reduce command is distributed and parallelized
// across shards and CPU cores. A common mistaken when using the reduce
// command is to suppose that the reduction is executed from left to right.
// Read the map-reduce in RethinkDB article to see an example.
//
// If the sequence is empty, the server will produce a ReqlRuntimeError
// that can be caught with default.
//
// NOTE: take care when shape of reduced value differs from shape of sequence values
//
// await r.expr([
//   { count: 3 }, { count: 0 },
//   { count: 6 }, { count: 7 }
// ]).reduce((left, right) => (
//   left('count').add(right('count').add(5))
// )).run()
//
// > 'Cannot perform bracket on a non-object non-sequence `8`.'
//
query.reduce = (db, qst, args) => {
  if (args.length === 0) {
    throw mmErrArgumentsNumber('reduce', 1, args.length);
  }

  // live rethinkdb inst returns sequence of 0 as error
  if (qst.target.length === 0) {
    throw mmErrCannotReduceOverEmptyStream();
  }

  // live rethinkdb inst returns sequence of 1 atom
  if (qst.target.length === 1) {
    [ qst.target ] = qst.target;

    return qst;
  }

  const seq = qst.target.sort(() => 0.5 - Math.random());

  qst.target = seq.slice(1)
    .reduce((st, arg) => spend(db, qst, args[0], [ st, arg ]), seq[0]);

  return qst;
};

// fold has the following differences from reduce:
//
//  * it is guaranteed to proceed through the sequence from
//    first element to last.
//  * it passes an initial base value to the function with the
//    first element in place of the previous reduction result.
//
query.fold = (db, qst, args) => {
  const [ startVal, reduceFn ] = args;

  if (args.length < 2)
    throw mmErrArgumentsNumber('fold', 2, args.length);

  qst.target = qst.target
    .reduce((st, arg) => spend(db, qst, reduceFn, [ st, arg ]), startVal);

  return qst;
};

query.forEach =  (db, qst, args) => {
  const [ forEachRow ] = args;

  if (args.length !== 1)
    throw mmErrArgumentsNumber('forEach', 1, args.length);

  qst.target = qst.target.reduce((st, arg) => {
    const result = spend(db, qst, forEachRow, [ arg ]);

    return mmDbAggregate(st, result);
  }, {});

  return qst;
};

query.getCursor = (db, qst, args) => {
  // returning the changes()-defined 'target' here causes node to hang un-predictably
  if (qst.target instanceof Readable
      && 'changesTarget' in qst) {

    qst.target.close();
    qst.target = qst.changesTarget;
  }

  const dbName = mockdbReqlQueryOrStateDbName(qst, db);
  const tableName = qst.tablename;
  const queryTarget = qst.target;
  const queryTargetFuture = qst.target || {
    [mmDbTableGetPrimaryKey(db, dbName, tableName)]: qst.primaryKeyValue
  };
  const queryOptions = queryArgsOptions(args);
  const cursorTargetType = tableName
    ? (Array.isArray(queryTarget) ? 'table' : 'doc') : 'expr';

  let cursors = null;

  if (typeof queryOptions.maxBatchRows !== 'number') {
    queryOptions.maxBatchRows = Math.Infinite;
  }

  if (cursorTargetType === 'doc') {
    cursors = mmDbTableDocCursorsGetOrCreate(
      db, dbName, tableName, queryTargetFuture);
  } else if (cursorTargetType === 'table') {
    cursors = mmDbTableCursorsGetOrCreate(
      db, dbName, tableName);
  }
  const cursorIndex = cursors ? cursors.length : null;
  const initialDocs = [];

  if (!qst.isChanges || qst.includeInitial) {
    asList(queryTarget).map(item => {
      if (cursorTargetType === 'doc' || item || isBoolNumStrRe.test(typeof item)) {
        initialDocs.push({
          new_val: item
        });
      }
    });
  }

  if (qst.error) {
    initialDocs.push({ error: qst.error })
  }

  const cursor = mmStream(initialDocs, !qst.isChanges);

  cursor.close = () => {
    cursor.destroy();

    if (cursorTargetType === 'table')
      db = mmDbTableCursorSplice(db, dbName, tableName, cursorIndex);
    if (cursorTargetType === 'doc')
      db = mmDbTableDocCursorSplice(db, dbName, tableName, queryTargetFuture, cursorIndex);

    return new Promise((resolve /*, reject*/) => resolve());
  };

  if (cursorTargetType === 'table') {
    db = mmDbTableCursorSet(db, dbName, tableName, cursor);
  } else if (cursorTargetType === 'doc') {
    db = mmDbTableDocCursorSet(db, dbName, tableName, queryTargetFuture, cursor);
  }

  qst.target = cursor;

  return qst;
};

query.close = (db, qst, args) => {
  if (qst.target && typeof qst.target.close === 'function')
    qst.target.close();

  return qst;
};

query.isReql = true;

export {
  query as default,
  spend
}
