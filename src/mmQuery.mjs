import { randomUUID } from 'crypto'
import { Readable } from 'stream'
import mmConn from './mmConn.mjs'
import mmStream from './mmStream.mjs'

import {
  mmDbStateAggregate,
  mmDbStateDbCreate,
  mmDbStateDbDrop,
  mmDbStateDbGet,
  mmDbStateTableIndexAdd,
  mmDbStateTableGetIndexNames,
  mmDbStateTableGetIndexTuple,
  mmDbStateTableGetPrimaryKey,
  mmDbStateTableCursorSet,
  mmDbStateTableDocCursorSet,
  mmDbStateTableCursorSplice,
  mmDbStateTableDocCursorSplice,
  mmDbStateTableCursorsPushChanges,
  mmDbStateTableCursorsGetOrCreate,
  mmDbStateTableDocCursorsGetOrCreate,
  mmDbStateTableConfigGet,
  mmDbStateTableCreate,
  mmDbStateTableDrop,
  mmDbStateDbConfigGet
} from './mmDbState.mjs'

import {
  mmTableDocRm,
  mmTableDocGet,
  mmTableDocsGet,
  mmTableDocsSet,
  mmTableDocGetIndexValue,
  mmTableDocEnsurePrimaryKey,
  mmTableDocHasIndexValueFn,
  mmTableSet
} from './mmTable.mjs'

import {
  mmResChangeTypeADD,
  mmResChangeTypeINITIAL,
  mmResChangesErrorPush,
  mmResChangesSpecFinal,
  mmResChangesFieldCreate,
  mmResChangesSpecPush,
  mmResTableStatus,
  mmResTableInfo
} from './mmRes.mjs'

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
  mmErrPrimaryKeyCannotBeChanged,
  mmErrNotATIMEpsudotype,
  mmErrCannotUseNestedRow,
  mmErrNoAttributeInObject,
  mmErrExpectedTypeFOOButFoundBAR,
  mmErrCannotReduceOverEmptyStream,
  mmErrCannotCallFOOonBARTYPEvalue
} from './mmErr.mjs'

import {
  mmEnumTypeERROR,
  mmEnumQueryArgTypeARGSIG,
  mmEnumQueryArgTypeARGS,
  mmEnumQueryArgTypeCHAIN,
  mmEnumQueryNameIsCURSORORDEFAULTRe,
  mmEnumIsQueryArgsResult,
  mmEnumIsChainShallow,
  mmEnumIsChain
} from './mmEnum.mjs'

const isBoolNumStrRe = /boolean|number|string/
const isBoolNumUndefRe = /boolean|number|undefined/

const isLookObj = obj => obj
  && typeof obj === 'object'
  && !(obj instanceof Date)

const reqlArgsParse = obj => (
  obj[mmEnumQueryArgTypeARGS])

const reqlArgsCreate = value => (
  { [mmEnumQueryArgTypeARGS]: value })

// created by 'asc' and 'desc' queries
const isSortObj = obj => isLookObj(obj)
  && 'sortBy' in obj

const sortObjParse = o => isLookObj(o)
  ? (isSortObj(o) ? o : isSortObj(o.index) ? o.index : null)
  : null

const isConfigObj = (obj, objType = typeof obj) => obj
  && /object|function/.test(objType)
  && !mmEnumIsChain(obj)
  && !Array.isArray(obj)

// return last query argument (optionally) provides query configurations
const queryArgsOptions = (queryArgs, queryOptionsDefault = {}) => {
  const queryOptions = queryArgs.slice(-1)[0] || {}

  return isConfigObj(queryOptions)
    ? queryOptions
    : queryOptionsDefault
}

// use when order not important and sorting helps verify a list
const compare = (a, b, prop) => {
  if (a[prop] < b[prop]) return -1
  if (a[prop] > b[prop]) return 1
  return 0
}

const asList = value => Array.isArray(value) ? value : [value]

const q = {}

const spendRecs = (db, qst, reqlObj, rows) => {
  if (rows && rows.length) {
    qst.rowMap[reqlObj.recId] = rows.slice()
  }
  
  const val = reqlObj.recs.reduce((qstNext, rec, i) => {
    // avoid mutating original args w/ suspended values
    const queryArgs = rec[1].slice()

    if (qstNext.error && !mmEnumQueryNameIsCURSORORDEFAULTRe.test(rec[0]))
      return qstNext
    
    if (rec[0] === 'row') {
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
        // existance of ARGSIG indicates row function was used
        mmEnumQueryArgTypeARGSIG !== rec[1][0])) {
        throw mmErrCannotUseNestedRow()
      } else {
        qstNext.rowDepth += 1
      }
    }

    if (i === 0 && rows && !/\(.*\)/.test(reqlObj.recId)) {
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
      qstNext.target = rows[0]
    }

    try {
      qstNext = (/\.fn/.test(rec[0])
        ? q[rec[0].replace(/\.fn/, '')].fn
        : q[rec[0]]
      )(db, qstNext, queryArgs, reqlObj)
    } catch (e) {
      // do not throw error if chain subsequently uses `.default(...)`
      // * if no future default query exists, tag error
      // * throw all tagged errors up to user
      qstNext.target = null
      qstNext.error = e

      if (reqlObj.recs.slice(-1)[0][0] === 'getCursor')
        return qstNext

      e[mmEnumTypeERROR] = typeof e[mmEnumTypeERROR] === 'boolean'
        ? e[mmEnumTypeERROR]
        : !reqlObj.recs.slice(i + 1).some(o => mmEnumQueryNameIsCURSORORDEFAULTRe.test(o[0]))

      if (e[mmEnumTypeERROR])
        throw e
    }

    return qstNext
  }, {
    // if nested spec is not a function expression,
    // pass target value down from parent
    //
    // r.expr(...).map(
    //   r.branch(
    //     r.row('victories').gt(100),
    //     r.row('name').add(' is a hero'),
    //     r.row('name').add(' is very nice')))
    target: reqlObj.recs[0][0] === 'row' ? qst.target : null,
    recId: reqlObj.recId,
    rowMap: qst.rowMap || {},
    rowDepth: qst.rowDepth || 0
  })

  return val.target
}

const spend = (db, qst, qspec, rows, d = 0, type = typeof qspec, f = null) => {
  if (qspec === f
    || isBoolNumUndefRe.test(type)
    || qspec instanceof Date) {
    f = qspec
  } else if (d === 0 && type === 'string') {
    // return field value from query like this,
    // ```
    // row('fieldname')
    // ```
    // seems okay now, may require deeper lookup later
    f = rows && rows[0] ? rows[0][qspec] : qspec
  } else if (mmEnumIsChain(qspec)) {
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
    f = spendRecs(db, qst, qspec, rows)
  } else if (Array.isArray(qspec)) {
    // detach if spec is has args
    if (mmEnumIsQueryArgsResult(qspec.slice(-1)[0])) {
      f = qspec.slice(-1)[0].run()
    } else {
      f = qspec.map(v => spend(db, qst, v, rows, d + 1))
      f = mmEnumIsQueryArgsResult(f[0]) ? reqlArgsParse(f[0]) : f
    }
    // render nested query objects, shallow. ex `row('id')`,
    // ```
    // r.expr([{ id: 1 }, { id: 2 }])
    //  .merge(row => ({ oldid: row('id'), id: 0 }))
    //  .run()
    // ```    
  } else if (mmEnumIsChainShallow(qspec)) {
    f = Object.keys(qspec).reduce((prev, key) => {
      prev[key] = spend(db, qst, qspec[key], rows, d + 1)

      return prev
    }, {})
  } else {
    f = qspec
  }

  return f
}

const mockdbReqlQueryOrStateDbName = (qst, db) => (
  qst.db || db.dbSelected)

q.connect = (db, qst, args) => {
  const [conn] = args
  const { host, port, user /*, password*/ } = conn

  if (db.dbConnections.every(c => c.db !== conn.db)) {
    db.dbConnections.push(conn)
  }

  if (conn.db)
    db.dbSelected = conn.db

  qst.target = new mmConn('connection', conn.db, host, port, user)

  return qst
}

q.connectPool = (db, qst, args) => {
  const [conn] = args
  const { host, port, user, password } = conn

  if (db.dbConnections.every(c => c.db !== conn.db)) {
    db.dbConnections.push(conn)
  }

  if (conn.db)
    db.dbSelected = conn.db

  qst.target = new mmConn(
    'connectionPool', conn.db, host, port, user, password)

  return qst
}

q.getPoolMaster = (db, qst) => {
  const conn = db.dbConnections[0] || {
    db: 'default',
    host: 'localhost',
    port: 28015,
    user: '',
    password: ''
  }

  const { host, port, user, password } = conn

  qst.target = new mmConn(
    'connectionPoolMaster', conn.db, host, port, user, password)

  return qst
}

q.getPool = q.getPoolMaster

// used for selecting/specifying db, not supported yet
q.db = (db, qst, args) => {
  const [dbName] = args
  const isValidDbNameRe = /^[A-Za-z0-9_]*$/

  if (!args.length) {
    throw mmErrArgumentsNumber('r.db', 1, args.length)
  }

  if (!isValidDbNameRe.test(dbName)) {
    throw mmErrInvalidDbName(dbName)
  }

  qst.db = dbName

  return qst
}

q.dbList = (db, qst) => {
  qst.target = Object.keys(db.db)

  return qst
}

q.dbCreate = (db, qst, args) => {
  const dbName = spend(db, qst, args[0])

  if (!args.length)
    throw mmErrArgumentsNumber('r.dbCreate', 1, args.length)

  mmDbStateDbCreate(db, dbName)

  qst.target = {
    config_changes: [{
      new_val: mmDbStateDbConfigGet(dbName),
      old_val: null
    }],
    dbs_created: 1
  }

  return qst
}

q.dbDrop = (db, qst, args) => {
  const [dbName] = args
  const dbConfig = mmDbStateDbConfigGet(db, dbName)
  const tables = mmDbStateDbGet(db, dbName)

  if (args.length !== 1) {
    throw mmErrArgumentsNumber('r.dbDrop', 1, args.length)
  }

  db = mmDbStateDbDrop(db, dbName)

  qst.target = {
    config_changes: [{
      new_val: null,
      old_val: dbConfig
    }],
    dbs_dropped: 1,
    tables_dropped: Object.keys(tables).length
  }

  return qst
}

q.config = (db, qst, args) => {
  if (args.length) {
    throw mmErrArgumentsNumber('config', 0, args.length)
  }

  const dbName = mockdbReqlQueryOrStateDbName(qst, db)

  if (qst.tablename) {
    qst.target = mmDbStateTableConfigGet(db, dbName, qst.tablename)
    qst.target = { // remove indexes data added for internal use
      ...qst.target,
      indexes: qst.target.indexes.map(i => i[0])
    }
  } else {
    qst.target = mmDbStateDbConfigGet(db, dbName, qst.tableName)
  }

  return qst
}

q.status = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const tableConfig = mmDbStateTableConfigGet(db, dbName, qst.tablename)

  qst.target = mmResTableStatus(tableConfig)

  return qst
}

q.info = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  qst.target = mmResTableInfo(db, dbName, qst.tablename)

  return qst
}

q.info.fn = (db, qst, args) => {
  return q.getField(db, qst, args)
}

q.tableList = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const tables = mmDbStateDbGet(db, dbName)

  qst.target = Object.keys(tables)

  return qst
}

q.tableCreate = (db, qst, args) => {
  const tableName = spend(db, qst, args[0])
  const isValidConfigKeyRe = /^(primaryKey|durability)$/
  const isValidTableNameRe = /^[A-Za-z0-9_]*$/
  const config = queryArgsOptions(args)
  const invalidConfigKey = Object.keys(config)
    .find(k => !isValidConfigKeyRe.test(k))

  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(
      invalidConfigKey, config[invalidConfigKey])
  }

  if (!tableName) {
    throw mmErrArgumentsNumber('r.tableCreate', 1, 0, true)
  }

  if (!isValidTableNameRe.test(tableName)) {
    throw mmErrInvalidTableName(tableName)
  }

  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const tables = mmDbStateDbGet(db, dbName)
  if (tableName in tables) {
    throw mmErrTableExists(dbName, tableName)
  }

  db = mmDbStateTableCreate(db, dbName, tableName, config)

  const tableConfig = mmDbStateTableConfigGet(db, dbName, tableName)

  qst.target = {
    tables_created: 1,
    config_changes: [{
      new_val: tableConfig,
      old_val: null
    }]
  }

  return qst
}

q.tableDrop = (db, qst, args) => {
  const [tableName] = args
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const tableConfig = mmDbStateTableConfigGet(db, dbName, tableName)

  db = mmDbStateTableDrop(db, dbName, tableName)
    
  qst.target = {
    tables_dropped: 1,
    config_changes: [{
      new_val: null,
      old_val: tableConfig
    }]
  }

  return qst
}

// .indexCreate( 'foo' )
// .indexCreate( 'foo', { multi: true })
// .indexCreate( 'things', r.row( 'hobbies' ).add( r.row( 'sports' ) ), { multi: true })
// .indexCreate([ r.row('id'), r.row('numeric_id') ])
q.indexCreate = (db, qst, args) => {
  const [indexName] = args
  const config = queryArgsOptions(args)
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)

  const fields = mmEnumIsChainShallow(args[1])
    ? args[1]
    : [indexName]

  mmDbStateTableIndexAdd(
    db, dbName, qst.tablename, indexName, fields, config)

  // should throw ReqlRuntimeError if index exits already
  qst.target = { created: 1 }

  return qst
}

q.indexWait = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const tableIndexList = mmDbStateTableGetIndexNames(
    db, dbName, qst.tablename)

  qst.target = tableIndexList.map(indexName => ({
    index: indexName,
    ready: true,
    function: 1234,
    multi: false,
    geo: false,
    outdated: false
  }))

  return qst
}

q.indexList = (db, qst) => {
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const tableConfig = mmDbStateTableConfigGet(db, dbName, qst.tablename)

  qst.target = tableConfig.indexes.map(i => i[0])

  return qst
}
// pass query down to 'spend' and copy data
q.insert = (db, qst, args) => {
  // both argument types (list or atom) resolved to a list here
  let documents = Array.isArray(args[0]) ? args[0] : args.slice(0, 1)
  let table = qst.tablelist
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const primaryKey = mmDbStateTableGetPrimaryKey(db, dbName, qst.tablename)
  const options = args[1] || {}

  const isValidConfigKeyRe = /^(returnChanges|durability|conflict)$/
  const invalidConfigKey = Object.keys(options)
    .find(k => !isValidConfigKeyRe.test(k))

  if (args.length > 1 && (!args[1] || typeof args[1] !== 'object')) {
    throw mmErrSecondArgumentOfQueryMustBeObject('insert')
  }

  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(
      invalidConfigKey, options[invalidConfigKey])
  }

  if (documents.length === 0) {
    throw mmErrArgumentsNumber('insert', 1, 0)
  }

  const documentIsPrimaryKeyPredefined = documents
    .some(d => primaryKey in d)

  documents = documents
    .map(doc => mmTableDocEnsurePrimaryKey(doc, primaryKey))

  const existingDocs = mmTableDocsGet(
    qst.tablelist, documents, primaryKey)

  if (existingDocs.length) {
    if (mmEnumIsChain(options.conflict)) {
      const resDoc = spend(db, qst, options.conflict, [
        documents[0].id,
        existingDocs[0],
        documents[0]
      ])

      const resSpec = mmResChangesSpecPush(
        mmResChangesFieldCreate({ changes: [] }), {
          old_val: existingDocs[0],
          new_val: resDoc,
          generated_key: documentIsPrimaryKeyPredefined
            ? null : resDoc[primaryKey]
        })

      mmTableDocsSet(table, [resDoc], primaryKey)

      qst.target = mmResChangesSpecFinal(resSpec, options)

      return qst
    } else if (/^(update|replace)$/.test(options.conflict)) {
      const conflictIds = existingDocs.map(doc => doc[primaryKey])
      // eslint-disable-next-line security/detect-non-literal-regexp
      const conflictIdRe = new RegExp(`^(${conflictIds.join('|')})$`)
      const conflictDocs = documents.filter(doc => conflictIdRe.test(doc[primaryKey]))

      qst = options.conflict === 'update'
        ? q.update(db, qst, conflictDocs)
        : q.replace(db, qst, conflictDocs)

      return qst
    } else {
      qst.target = mmResChangesErrorPush(
        mmResChangesFieldCreate(),
        mmErrDuplicatePrimaryKey(
          existingDocs[0],
          documents.find(doc => doc[primaryKey] === existingDocs[0][primaryKey])))
    }
        
    return qst
  }

  [table, documents] = mmTableDocsSet(
    table, documents.map(doc => spend(db, qst, doc)), primaryKey)

  const resSpec = documents.reduce((spec, doc) => {
    return mmResChangesSpecPush(spec, {
      new_val: doc,
      old_val: null,
      generated_key: documentIsPrimaryKeyPredefined
        ? null : doc[primaryKey]
    })    
  }, mmResChangesFieldCreate({ changes: [] }))

  db = mmDbStateTableCursorsPushChanges(
    db, dbName, qst.tablename, resSpec.changes, mmResChangeTypeADD)

  qst.target = mmResChangesSpecFinal(resSpec, options)

  return qst
}

q.update = (db, qst, args) => {
  const queryTarget = qst.target
  const queryTable = qst.tablelist
  const updateProps = spend(db, qst, args[0], [qst.target])
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const primaryKey = mmDbStateTableGetPrimaryKey(db, dbName, qst.tablename)
  const options = args[1] || {}
  const resSpec = asList(queryTarget).reduce((spec, targetDoc) => {
    const oldDoc = mmTableDocGet(queryTable, targetDoc, primaryKey)
    const newDoc = updateProps === null
      ? oldDoc
      : oldDoc && Object.assign({}, oldDoc, updateProps || {})

    if (oldDoc && newDoc) {
      mmTableDocsSet(queryTable, [newDoc], primaryKey)
    }

    return mmResChangesSpecPush(spec, {
      new_val: newDoc,
      old_val: oldDoc
    })
  }, mmResChangesFieldCreate({ changes: [] }))

  db = mmDbStateTableCursorsPushChanges(
    db, dbName, qst.tablename, resSpec.changes)

  qst.target = mmResChangesSpecFinal(resSpec, options)

  return qst
}

q.get = (db, qst, args) => {
  const primaryKeyValue = spend(db, qst, args[0])
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const primaryKey = mmDbStateTableGetPrimaryKey(db, dbName, qst.tablename)
  const tableDoc = mmTableDocGet(qst.target, primaryKeyValue, primaryKey)

  if (args.length === 0) {
    throw mmErrArgumentsNumber('get', 1, 0)
  }

  if (!isBoolNumStrRe.test(typeof primaryKeyValue)
    && !Array.isArray(primaryKeyValue)) {
    throw mmErrPrimaryKeyWrongType(primaryKeyValue)
  }

  // define primaryKeyValue on qst to use in subsequent change() query
  // for the case of change() request for document which does not exist (yet)
  qst.primaryKeyValue = primaryKeyValue
  qst.target = tableDoc || null

  return qst
}

q.get.fn = (db, qst, args) => {
  qst.target = spend(db, qst, args[0], [qst.target])
  
  return qst
}

q.getAll = (db, qst, args) => {
  const queryOptions = queryArgsOptions(args)
  const primaryKeyValues = spend(
    db,
    qst,
    (queryOptions && queryOptions.index) ? args.slice(0, -1) : args
  )

  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const { tablename } = qst
  const primaryKey = queryOptions.index
    || mmDbStateTableGetPrimaryKey(db, dbName, qst.tablename)
  const tableIndexTuple = mmDbStateTableGetIndexTuple(db, dbName, tablename, primaryKey)
  if (primaryKeyValues.length === 0) {
    qst.target = []

    return qst
  }

  const tableDocHasIndex = mmTableDocHasIndexValueFn(
    tableIndexTuple, primaryKeyValues, db)

  qst.target = qst.target
    .filter(doc => tableDocHasIndex(doc, spend, qst))
    .sort(() => 0.5 - Math.random())

  return qst
}

// The replace command can be used to both insert and delete documents.
// If the “replaced” document has a primary key that doesn’t exist in the
// table, the document will be inserted; if an existing document is replaced
// with null, the document will be deleted. Since update and replace
// operations are performed atomically, this allows atomic inserts and
// deletes as well.
q.replace = (db, qst, args) => {
  const queryTarget = qst.target
  const queryTable = qst.tablelist
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const primaryKey = mmDbStateTableGetPrimaryKey(db, dbName, qst.tablename)
  const config = queryArgsOptions(args.slice(1))

  const isValidConfigKeyRe = /^(returnChanges|durability|nonAtomic|ignoreWriteHook)$/
  const invalidConfigKey = Object.keys(config)
    .find(k => !isValidConfigKeyRe.test(k))
  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(
      invalidConfigKey, config[invalidConfigKey])
  }

  if (!args.length) {
    throw mmErrArgumentsNumber('replace', 1, args.length, true)
  }

  const resSpec = asList(queryTarget).reduce((spec, targetDoc) => {
    const replacement = spend(db, qst, args[0], [targetDoc])
    const oldDoc = mmTableDocGet(queryTable, targetDoc, primaryKey)
    const newDoc = replacement === null ? null : replacement

    if (oldDoc === null
      && newDoc && ('primaryKeyValue' in qst)
      && newDoc[primaryKey] !== qst.primaryKeyValue) {
      return mmResChangesErrorPush(spec, mmErrPrimaryKeyCannotBeChanged(primaryKey))
    }

    if (oldDoc && newDoc === null)
      mmTableDocRm(queryTable, oldDoc, primaryKey)
    else if (newDoc)
      mmTableDocsSet(queryTable, [newDoc], primaryKey)

    return mmResChangesSpecPush(spec, {
      new_val: newDoc,
      old_val: oldDoc
    })
  }, mmResChangesFieldCreate({ changes: [] }))

  db = mmDbStateTableCursorsPushChanges(
    db, dbName, qst.tablename, resSpec.changes)

  qst.target = mmResChangesSpecFinal(resSpec, config)

  return qst
}

q.prepend = (db, qst, args) => {
  const prependValue = spend(db, qst, args[0])

  if (typeof prependValue === 'undefined') {
    throw mmErrArgumentsNumber('prepend', 1, 0, false)
  }

  qst.target.unshift(prependValue)

  return qst
}

q.difference = (db, qst, args) => {
  const differenceValues = spend(db, qst, args[0])

  if (typeof differenceValues === 'undefined') {
    throw mmErrArgumentsNumber('difference', 1, 0, false)
  }

  qst.target = qst.target
    .filter(e => !differenceValues.some(a => e == a))

  return qst
}

q.nth = (db, qst, args) => {
  const nthIndex = spend(db, qst, args[0])

  if (nthIndex >= qst.target.length) {
    throw mmErrIndexOutOfBounds(nthIndex)
  }

  qst.target = qst.target[nthIndex]

  return qst
}

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
q.row = (db, qst, args) => {
  if (args[0] === mmEnumQueryArgTypeARGSIG && !(args[1] in qst.rowMap)) {
    // keep this for development
    // console.log(qst.target, mockdbSpecSignature(reqlObj), args, qst.rowMap);
    throw new Error('[!!!] error: missing ARGS from ROWMAP')
  }

  qst.target = args[0] === mmEnumQueryArgTypeARGSIG
    ? qst.rowMap[args[1]][args[2]]
    : qst.target[args[0]]
  
  return qst
}

q.row.fn = (db, qst, args) => {
  if (typeof args[0] === 'string' && !(args[0] in qst.target)) {
    throw mmErrNoAttributeInObject(args[0])
  }

  return q.getField(db, qst, args)
}

q.default = (db, qst, args) => {
  if (qst.target === null) {
    qst.error = null
    qst.target = spend(db, qst, args[0])
  }

  return qst
}

// time.during(startTime, endTime[, {leftBound: "closed", rightBound: "open"}]) → bool
q.during = (db, qst, args) => {
  const [start, end] = args
  const startTime = spend(db, qst, start)
  const endTime = spend(db, qst, end)

  qst.target = (
    qst.target.getTime() > startTime.getTime()
      && qst.target.getTime() < endTime.getTime()
  )

  return qst
}

q.append = (db, qst, args) => {
  qst.target = spend(db, qst, args).reduce((list, val) => {
    list.push(val)

    return list
  }, qst.target)

  return qst
}

// NOTE rethinkdb uses re2 syntax
// re using re2-specific syntax will fail
q.match = (db, qst, args) => {
  let regexString = spend(db, qst, args[0])

  let flags = ''
  if (regexString.startsWith('(?i)')) {
    flags = 'i'
    regexString = regexString.slice('(?i)'.length)
  }

  // eslint-disable-next-line security/detect-non-literal-regexp
  const regex = new RegExp(regexString, flags)

  if (typeof qst.target === 'number') {
    throw mmErrExpectedTypeFOOButFoundBAR('STRING', 'NUMBER')
  }

  qst.target = regex.test(qst.target)

  return qst
}

q.delete = (db, qst, args) => {
  const queryTarget = qst.target
  const queryTable = qst.tablelist
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const primaryKey = mmDbStateTableGetPrimaryKey(db, dbName, qst.tablename)
  const tableIndexTuple = mmDbStateTableGetIndexTuple(
    db, dbName, qst.tablename, primaryKey)
  const targetList = asList(queryTarget)
  const targetIds = targetList
    .map(doc => mmTableDocGetIndexValue(doc, tableIndexTuple, spend, qst, db))
    // eslint-disable-next-line security/detect-non-literal-regexp
  const targetIdRe = new RegExp(`^(${targetIds.join('|')})$`)
  const options = queryArgsOptions(args)
  const tableFiltered = queryTable.filter(doc => !targetIdRe.test(
    mmTableDocGetIndexValue(doc, tableIndexTuple, spend, qst, db)))
  const queryConfig = queryArgsOptions(args)
  const isValidConfigKeyRe = /^(durability|returnChanges|ignoreWriteHook)$/
  const invalidConfigKey = Object.keys(queryConfig)
    .find(k => !isValidConfigKeyRe.test(k))

  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(
      invalidConfigKey, queryConfig[invalidConfigKey])
  }

  const resSpec = targetList.reduce((spec, targetDoc) => {
    if (targetDoc) {
      spec = mmResChangesSpecPush(spec, {
        new_val: null,
        old_val: targetDoc        
      })
    }

    return spec
  }, mmResChangesFieldCreate({ changes: [] }))

  mmTableSet(queryTable, tableFiltered)

  db = mmDbStateTableCursorsPushChanges(
    db, dbName, qst.tablename, resSpec.changes)

  qst.target = mmResChangesSpecFinal(resSpec, options)

  return qst
}

q.contains = (db, qst, args) => {
  const queryTarget = qst.target

  if (!args.length) {
    throw new Error('Rethink supports contains(0) but rethinkdbdash does not.')
  }

  if (!Array.isArray(qst.target)) {
    throw mmErrExpectedTypeFOOButFoundBAR('SEQUENCE', 'SINGLE_SELECTION')
  }

  if (mmEnumIsChain(args[0])) {
    qst.target = queryTarget.some(target => {
      const res = spend(db, qst, args[0], [target])

      return typeof res === 'boolean'
        ? res
        : queryTarget.includes(res)
    })
  } else {
    qst.target = args.every(predicate => (
      queryTarget.includes(spend(db, qst, predicate))))
  }

  return qst
}

q.error = (db, qst, args) => {
  const [error] = spend(db, qst, args)

  throw new Error(error)
}

// Get a single field from an object. If called on a sequence, gets that field
// from every object in the sequence, skipping objects that lack it.
//
// https://rethinkdb.com/api/javascript/get_field
q.getField = (db, qst, args) => {
  const [fieldName] = spend(db, qst, args)

  if (args.length === 0) {
    throw mmErrArgumentsNumber('(...)', 1, args.length)
  }

  // if ( Array.isArray( qst.target ) ) {
  //  qst.error = 'Expected type DATUM but found SEQUENCE"';
  //  qst.target = null;
  //   return qst;
  // }
  
  qst.target = Array.isArray(qst.target)
    ? qst.target.map(t => t[fieldName])
    : qst.target[fieldName]

  return qst
}

q.object = (db, qst, args) => {
  const reducetuples = (accum, list) => {
    if (list.length < 2) return accum
    
    accum[list[0]] = list[1]

    return reducetuples(accum, list.slice(2))
  }

  qst.target = reducetuples({}, spend(db, qst, args))

  return qst
}

q.filter = (db, qst, args) => {
  if (qst.target instanceof Readable
    && 'changesTarget' in qst) {
    // eg, changes().filter( filterQuery )
    qst.target.streamFilter = item => (
      spend(db, qst, args[0], [item]))
  } else {
    qst.target = qst.target.filter(item => {
      // predicate.rows = [ item ];
      const finitem = spend(db, qst, args[0], [item])

      if (finitem && typeof finitem === 'object') {
        return Object
          .keys(finitem)
          .every(key => finitem[key] === item[key])
      }

      return finitem
    })
  }

  return qst
}

q.filter.fn = q.getField

q.count = (db, qst) => {
  qst.target = qst.target.length

  return qst
}

q.pluck = (db, qst, args) => {
  const queryTarget = qst.target
  const pluckObj = (obj, props) => props.reduce((plucked, prop) => {
    plucked[prop] = obj[prop]
    return plucked
  }, {})

  args = spend(db, qst, args)

  qst.target = Array.isArray(queryTarget)
    ? queryTarget.map(t => pluckObj(t, args))
    : pluckObj(queryTarget, args)

  return qst
}

q.hasFields = (db, qst, args) => {
  const queryTarget = qst.target
  const itemHasFields = item => Boolean(item && args
    .every(name => Object.prototype.hasOwnProperty.call(item, name)))

  qst.target = Array.isArray(queryTarget)
    ? queryTarget.filter(itemHasFields)
    : itemHasFields(queryTarget)

  return qst
}

q.slice = (db, qst, args) => {
  const [begin, end] = spend(db, qst, args.slice(0, 2))

  if (qst.isGrouped) { // slice from each group
    qst.target = qst.target.map(targetGroup => {
      targetGroup.reduction = targetGroup.reduction.slice(begin, end)

      return targetGroup
    })
  } else {
    qst.target = qst.target.slice(begin, end)
  }

  return qst
}

q.skip = (db, qst, args) => {
  const count = spend(db, qst, args[0])

  qst.target = qst.target.slice(count)

  return qst
}

q.limit = (db, qst, args) => {
  qst.target = qst.target.slice(0, args[0])

  return qst
}

// Documents in the result set consist of pairs of left-hand and right-hand documents,
// matched when the field on the left-hand side exists and is non-null and an entry
// with that field’s value exists in the specified index on the right-hand side.
q.eqJoin = (db, qst, args) => {
  const queryTarget = qst.target
  const isNonNull = v => v !== null && v !== undefined
  const queryConfig = queryArgsOptions(args)
  const isValidConfigKeyRe = /^index$/
  const rightFields = spend(db, qst, args[1])

  // should remove this... get table name from args[1] record
  // and call q.config() directly
  const rightFieldConfig = args[1] && spend(db, qst, {
    type: mmEnumQueryArgTypeCHAIN,
    recs: [
      ...args[1].recs,
      ['config', []]
    ]
  })
  
  const rightFieldKey = queryConfig.index
    || (rightFieldConfig && rightFieldConfig.primary_key)
  const invalidConfigKey = Object.keys(queryConfig)
    .find(k => !isValidConfigKeyRe.test(k))

  if (invalidConfigKey) {
    throw mmErrUnrecognizedOption(
      invalidConfigKey, queryConfig[invalidConfigKey])
  }
    
  if (args.length === 0) {
    throw mmErrArgumentsNumber('eqJoin', 2, 0, true)
  }

  qst.target = queryTarget.reduce((joins, item) => {
    const leftFieldSpend = spend(db, qst, args[0], [item])
    
    const leftFieldValue = qst.tablelist
      ? item // if value comes from table use full document
      : leftFieldSpend

    if (isNonNull(leftFieldValue)) {
      const rightFieldValue = rightFields
        .find(rf => rf[rightFieldKey] === leftFieldSpend)

      if (isNonNull(rightFieldValue)) {
        joins.push({
          left: leftFieldValue,
          right: rightFieldValue
        })
      }
    }

    return joins
  }, [])

  qst.eqJoinBranch = true
  return qst
}

q.eqJoin.fn = q.getField

// Used to ‘zip’ up the result of a join by merging the ‘right’ fields into
// ‘left’ fields of each member of the sequence.
q.zip = (db, qst) => {
  qst.target = qst.target
    .map(t => ({ ...t.left, ...t.right }))

  return qst
}

q.innerJoin = (db, qst, args) => {
  const queryTarget = qst.target
  const [otherSequence, joinFunc] = args
  const otherTable = spend(db, qst, otherSequence)

  qst.target = queryTarget.map(item => (
    otherTable.map(otherItem => {
      // problem here is we don't know if item will be evaluated first
      const oinSPend = spend(db, qst, joinFunc, [item, otherItem])

      return {
        left: item,
        right: oinSPend ? otherItem : null
      }
    })
  )).flat().filter(({ right }) => right)

  return qst
}

q.now = (db, qst) => {
  qst.target = new Date()

  return qst
}

q.toEpochTime = (db, qst) => {
  qst.target = (new Date(qst.target)).getTime() / 1000

  return qst
}

q.epochTime = (db, qst, args) => {
  qst.target = new Date(args[0] * 1000)

  return qst
}

q.not = (db, qst) => {
  const queryTarget = qst.target

  if (typeof queryTarget !== 'boolean')
    throw mmErrCannotCallFOOonBARTYPEvalue('not()', 'non-boolean')

  qst.target = !queryTarget

  return qst
}

q.gt = (db, qst, args) => {
  qst.target = qst.target > spend(db, qst, args[0])

  return qst
}

q.ge = (db, qst, args) => {
  qst.target = qst.target >= spend(db, qst, args[0])

  return qst
}

q.lt = (db, qst, args) => {
  const argTarget = spend(db, qst, args[0])
  
  if (argTarget instanceof Date && !(qst.target instanceof Date)) {
    throw mmErrNotATIMEpsudotype('forEach', 1, args.length)
  }

  if (typeof qst.target === typeof qst.target) {
    qst.target = qst.target < argTarget
  }

  return qst
}

q.le = (db, qst, args) => {
  qst.target = qst.target <= spend(db, qst, args[0])

  return qst
}

q.eq = (db, qst, args) => {
  qst.target = qst.target === spend(db, qst, args[0])

  return qst
}

q.ne = (db, qst, args) => {
  qst.target = qst.target !== spend(db, qst, args[0])

  return qst
}

q.max = (db, qst, args) => {
  const targetList = qst.target
  const getListMax = (list, prop) => list.reduce((maxDoc, doc) => (
    maxDoc[prop] > doc[prop] ? maxDoc : doc
  ), targetList)

  const getListMaxGroups = (groups, prop) => (
    groups.reduce((prev, target) => {
      prev.push({
        ...target,
        reduction: getListMax(target.reduction, prop)
      })

      return prev
    }, [])
  )

  qst.target = qst.isGrouped
    ? getListMaxGroups(targetList, args[0])
    : getListMax(targetList, args[0])

  return qst
}

q.max.fn = (db, qst, args) => {
  const field = spend(db, qst, args[0])

  if (qst.isGrouped) {
    qst.target = qst.target.map(targetGroup => {
      targetGroup.reduction = targetGroup.reduction[field]

      return targetGroup
    })
  } else {
    qst.target = qst.target[field]
  }

  return qst
}

q.min = (db, qst, args) => {
  const targetList = qst.target
  const getListMin = (list, prop) => list.reduce((maxDoc, doc) => (
    maxDoc[prop] < doc[prop] ? maxDoc : doc
  ), targetList)

  const getListMinGroups = (groups, prop) => (
    groups.reduce((prev, target) => {
      prev.push({
        ...target,
        reduction: getListMin(target.reduction, prop)
      })

      return prev
    }, [])
  )

  qst.target = qst.isGrouped
    ? getListMinGroups(targetList, args[0])
    : getListMin(targetList, args[0])

  return qst
}

q.merge = (db, qst, args) => {
  if (args.length === 0) {
    throw mmErrArgumentsNumber('merge', 1, args.length, true)
  }
  
  // evaluate anonymous function given as merge definition
  const mergeTarget = (merges, target) => merges.reduce((p, next) => (
    Object.assign(p, spend(db, qst, next, [target]))
  ), { ...target })

  qst.target = Array.isArray(qst.target)
    ? qst.target.map(i => mergeTarget(args, i))
    : mergeTarget(args, qst.target)

  return qst
}

q.concatMap = (db, qst, args) => {
  const [func] = args

  qst.target = qst
    .target.map(t => spend(db, qst, func, [t])).flat()

  return qst
}

q.isEmpty = (db, qst) => {
  qst.target = qst.target.length === 0

  return qst
}

q.add = (db, qst, args) => {
  const target = qst.target
  const values = spend(db, qst, args)

  if (target === null) {
    qst.target = Array.isArray(values)
      ? values.slice(1).reduce((prev, val) => prev + val, values[0])
      : values
  } else if (isBoolNumStrRe.test(typeof target)) {
    qst.target = values.reduce((prev, val) => prev + val, target)
  } else if (Array.isArray(target)) {
    qst.target = [...target, ...values]
  }

  return qst
}

q.sub = (db, qst, args) => {
  const target = qst.target
  const values = spend(db, qst, args)

  if (typeof target === null) {
    qst.target = Array.isArray(values)
      ? values.slice(1).reduce((prev, val) => prev - val, values[0])
      : values
  } else if (isBoolNumStrRe.test(typeof target)) {
    qst.target = values.reduce((prev, val) => prev - val, target)
  }

  return qst
}

q.mul = (db, qst, args) => {
  const target = qst.target
  const values = spend(db, qst, args)

  if (typeof target === null) {
    qst.target = Array.isArray(values)
      ? values.slice(1).reduce((prev, val) => prev * val, values[0])
      : values
  } else if (isBoolNumStrRe.test(typeof target)) {
    qst.target = values.reduce((prev, val) => prev * val, target)
  }

  return qst
}

// .group(field | function..., [{index: <indexname>, multi: false}]) → grouped_stream
// arg can be stringy field name, { index: 'indexname' }, { multi: true }
q.group = (db, qst, args) => {
  const queryTarget = qst.target
  const [arg] = args
  const groupedData = queryTarget.reduce((group, item) => {
    const key = (typeof arg === 'object' && arg && 'index' in arg)
      ? arg.index
      : spend(db, qst, arg)
    const groupKey = item[key]

    group[groupKey] = group[groupKey] || []
    group[groupKey].push(item)

    return group
  }, {})
  const rethinkFormat = Object.entries(groupedData)
    .map(([group, reduction]) => ({ group, reduction }))

  qst.isGrouped = true
  qst.target = rethinkFormat

  return qst
}

// array.sample(number) → array
q.sample = (db, qst, args) => {
  qst.target = qst.target
    .sort(() => 0.5 - Math.random())
    .slice(0, args)

  return qst
}

q.ungroup = (db, qst) => {
  qst.isGrouped = false

  return qst
}

q.orderBy = (db, qst, args) => {
  const queryTarget = qst.target
  const queryOptions = mmEnumIsChain(args[0])
    ? args[0]
    : queryArgsOptions(args)
  const queryOptionsIndex = spend(db, qst, queryOptions.index)
  const indexSortBy = typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortBy
  const indexSortDirection = (typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortDirection) || 'asc'
  const indexString = typeof queryOptionsIndex === 'string' && queryOptionsIndex
  const argsSortPropValue = typeof args[0] === 'string' && args[0]
  const indexName = indexSortBy || indexString || 'id'
  let fieldSortDirection = ''
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const tableIndexTuple = mmDbStateTableGetIndexTuple(db, dbName, qst.tablename, indexName)
  const sortDirection = (isAscending, dir = fieldSortDirection || indexSortDirection) => (
    isAscending * (dir === 'asc' ? 1 : -1))

  const getSortFieldValue = doc => {
    let value

    // ex, queryOptions,
    //  ({ index: r.desc('date') })
    //  doc => doc('upvotes')
    if (mmEnumIsChainShallow(queryOptions)) {
      value = spend(db, qst, queryOptions, [doc])

      const sortObj = sortObjParse(value)
      if (sortObj) {
        if (sortObj.sortDirection)
          fieldSortDirection = sortObj.sortDirection

        value = sortObj.sortBy
      }
    } else if (argsSortPropValue) {
      value = doc[argsSortPropValue]
    } else {
      value = mmTableDocGetIndexValue(doc, tableIndexTuple, spend, qst, db)
    }

    return value
  }

  if (!args.length) {
    throw mmErrArgumentsNumber('orderBy', 1, args.length, true)
  }

  qst.target = queryTarget.sort((doca, docb) => {
    const docaField = getSortFieldValue(doca, tableIndexTuple)
    const docbField = getSortFieldValue(docb, tableIndexTuple)

    return sortDirection(docaField < docbField ? -1 : 1)
  })

  return qst
}

// Return the hour in a time object as a number between 0 and 23.
q.hours = (db, qst) => {
  qst.target = new Date(qst.target).getHours()

  return qst
}

q.minutes = (db, qst) => {
  qst.target = new Date(qst.target).getMinutes()

  return qst
}

q.uuid = (db, qst) => {
  qst.target = randomUUID()

  return qst
}

q.expr = (db, qst, args) => {
  const [argvalue] = args

  qst.target = spend(db, qst, argvalue, [qst.target])

  return qst
}

q.expr.fn = (db, qst, args) => {
  if (Array.isArray(qst.target)) {
    qst.target = qst.target.map(t => t[args[0]])
  } else if (args[0] in qst.target) {
    qst.target = qst.target[args[0]]
  } else {
    throw mmErrNoAttributeInObject(args[0])
  }

  return qst
}

q.coerceTo = (db, qst, args) => {
  const [coerceType] = args
  let resolved = spend(db, qst, qst.target)

  if (coerceType === 'string')
    resolved = String(resolved)

  qst.target = resolved

  return qst
}

q.upcase = (db, qst) => {
  qst.target = String(qst.target).toUpperCase()

  return qst
}

q.downcase = (db, qst) => {
  qst.target = String(qst.target).toLowerCase()

  return qst
}

q.map = (db, qst, args) => {
  qst.target = qst
    .target.map(t => spend(db, qst, args[0], [t]))

  return qst
}

q.without = (db, qst, args) => {
  const queryTarget = qst.target

  const withoutFromDoc = (doc, withoutlist) => Object.keys(doc)
    .reduce((newdoc, key) => {
      if (!withoutlist.includes(key))
        newdoc[key] = doc[key]

      return newdoc
    }, {})

  const withoutFromDocList = (doclist, withoutlist) => doclist
    .map(doc => withoutFromDoc(doc, withoutlist))

  if (args.length === 0) {
    throw mmErrArgumentsNumber('without', 1, args.length)
  }

  args = spend(db, qst, args)

  if (qst.eqJoinBranch) {
    const isleft = 'left' in args[0]
    const isright = 'right' in args[0]
    const leftArgs = isleft && asList(args[0].left)
    const rightArgs = isright && asList(args[0].right)

    if (isleft || isright) {
      qst.target = queryTarget.map(qt => {
        if (isright)
          qt.right = withoutFromDoc(qt.right, rightArgs)

        if (isleft)
          qt.left = withoutFromDoc(qt.left, leftArgs)

        return qt
      })
    }
  } else {
    qst.target = Array.isArray(queryTarget)
      ? withoutFromDocList(queryTarget, args)
      : withoutFromDoc(queryTarget, args)
  }

  return qst
}

// Call an anonymous function using return values from other
// ReQL commands or queries as arguments.
q.do = (db, qst, args) => {
  const [doFn] = args.slice(-1)

  if (mmEnumIsChain(doFn)) {
    qst.target = args.length === 1
      ? spend(db, qst, doFn, [qst.target])
      : spend(db, qst, doFn, args.slice(0, -1))

    if (mmEnumIsQueryArgsResult(qst.target))
      qst.target = reqlArgsParse(qst.target)[0]

  } else if (args.length) {
    qst.target = doFn
  }

  return qst
}

q.or = (db, qst, args) => {
  const rows = [qst.target]

  qst.target = args.reduce((current, arg) => (
    current || spend(db, qst, arg, rows)
  ), qst.target)

  return qst
}

q.and = (db, qst, args) => {
  const rows = [qst.target]

  qst.target = args.reduce((current, arg) => (
    current && spend(db, qst, arg, rows)
  ), typeof qst.target === 'boolean' ? qst.target : true)
  
  return qst
}

// if the conditionals return any value but false or null (i.e., “truthy” values),
q.branch = (db, qst, args) => {
  const isResultTruthy = result => (
    result !== false && result !== null)

  const nextCondition = (condition, branches) => {
    const conditionResult = spend(db, qst, condition)

    if (branches.length === 0)
      return conditionResult

    if (isResultTruthy(conditionResult)) {
      return spend(db, qst, branches[0])
    }

    return nextCondition(branches[1], branches.slice(2))
  }

  qst.target = nextCondition(args[0], args.slice(1))

  return qst
}

// Rethink has its own alg for finding distinct,
// but unique by ID should be sufficient here.
q.distinct = (db, qst, args) => {
  const queryOptions = queryArgsOptions(args)
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)

  if (Array.isArray(qst.target)
    && qst.tablename
    // skip if target is filtered, concatenated or manipulated in some way
      && !isBoolNumStrRe.test(typeof qst.target[0])) {
    const primaryKey = queryOptions.index
      || mmDbStateTableGetPrimaryKey(db, dbName, qst.tablename)

    const keys = {}
    qst.target = qst.target.reduce((disti, row) => {
      const value = row[primaryKey]

      if (!keys[value]) {
        keys[value] = true
        disti.push(value)
      }

      return disti
    }, [])
  } else if (Array.isArray(qst.target)) {
    qst.target = qst.target.filter(
      (item, pos, self) => self.indexOf(item) === pos)
  } else if (Array.isArray(args[0])) {
    qst.target = args[0].filter(
      (item, pos, self) => self.indexOf(item) === pos)
  }

  return qst
}

q.union = (db, qst, args) => {
  const queryOptions = queryArgsOptions(args, null)

  if (queryOptions)
    args.splice(-1, 1)

  let res = args.reduce((acc, arg) => {
    return acc.concat(spend(db, qst, arg))
  }, qst.target || [])

  if (queryOptions && queryOptions.interleave) {
    res = res.sort(
      (a, b) => compare(a, b, queryOptions.interleave)
    )
  }

  qst.target = res

  return qst
}

q.table = (db, qst, args) => {
  const [tablename] = args
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const dbState = mmDbStateDbGet(db, dbName)
  const table = dbState[tablename]

  if (!Array.isArray(dbState[tablename])) {
    throw mmErrTableDoesNotExist(dbName, tablename)
  }

  qst.tablename = tablename
  qst.tablelist = table
  qst.target = table.slice()

  return qst
}

q.table.fn = q.getField

// r.args(array) → special
q.args = (db, qst, args) => {
  const result = spend(db, qst, args[0])
  if (!Array.isArray(result))
    throw new Error('args must be an array')

  qst.target = reqlArgsCreate(result)

  return qst
}

q.desc = (db, qst, args) => {
  qst.target = {
    sortBy: spend(db, qst, args[0], [qst.target]),
    sortDirection: 'desc'
  }

  return qst
}

q.asc = (db, qst, args) => {
  qst.target = {
    sortBy: spend(db, qst, args[0], [qst.target]),
    sortDirection: 'asc'
  }

  return qst
}

q.run = (db, qst) => {
  if (qst.error) {
    throw new Error(qst.error)
  }

  // return qst.target;
  return qst
}

q.drain = q.run

q.serialize = (db, qst) => {
  qst.target = JSON.stringify(qst.chain)

  return qst
}

q.changes = (db, qst, args) => {
  const tableName = qst.tablename
  const queryTarget = qst.target
  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const queryTargetFuture = qst.target || {
    [mmDbStateTableGetPrimaryKey(db, dbName, tableName)]: qst.primaryKeyValue
  }
  const queryOptions = queryArgsOptions(args) || {}
  const cursorTargetType = tableName
    ? (Array.isArray(queryTarget) ? 'table' : 'doc') : 'expr'

  qst.isChanges = true
  qst.includeInitial = Boolean(queryOptions.includeInitial)
  qst.includeTypes = Boolean(queryOptions.includeTypes)

  let cursors = null

  if (typeof queryOptions.maxBatchRows !== 'number') {
    queryOptions.maxBatchRows = Math.Infinite
  }

  if (cursorTargetType === 'doc') {
    cursors = mmDbStateTableDocCursorsGetOrCreate(
      db, dbName, tableName, queryTargetFuture)
  } else if (cursorTargetType === 'table') {
    cursors = mmDbStateTableCursorsGetOrCreate(
      db, dbName, tableName)
  }

  const cursorIndex = cursors ? cursors.length : null
  const initialDocs = []

  if (!qst.isChanges || qst.includeInitial) {
    asList(queryTarget).map(item => {
      if (cursorTargetType === 'doc' || item || /string|number|boolean/.test(typeof item)) {
        if (queryOptions.includeInitial) {
          initialDocs.push({
            type: mmResChangeTypeINITIAL,
            new_val: item
          })
        } else {
          initialDocs.push({
            new_val: item
          })
        }
      }
    })
  }

  const cursor = mmStream(
    initialDocs, !qst.isChanges, true, qst.includeTypes)

  cursor.close = () => {
    cursor.emit('end')
    cursor.destroy()

    if (cursorTargetType === 'doc')
      db = mmDbStateTableDocCursorSplice(db, dbName, tableName, queryTargetFuture, cursorIndex)
    if (cursorTargetType === 'table')
      db = mmDbStateTableCursorSplice(db, dbName, tableName, cursorIndex)

    return new Promise((resolve /*, reject */) => resolve())
  }

  if (cursorTargetType === 'doc')
    db = mmDbStateTableDocCursorSet(db, dbName, tableName, queryTargetFuture, cursor)
  else if (cursorTargetType === 'table')
    db = mmDbStateTableCursorSet(db, dbName, tableName, cursor)

  if (!qst.isChanges) {
    if (cursorTargetType === 'table') {
      const changes = queryTarget.map(doc => ({
        new_val: doc
      }))

      db = mmDbStateTableCursorsPushChanges(
        db, dbName, tableName, changes)
    }
  }

  qst.changesTarget = qst.target
  qst.target = cursor

  return qst
}

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
q.reduce = (db, qst, args) => {
  if (args.length === 0) {
    throw mmErrArgumentsNumber('reduce', 1, args.length)
  }

  // live rethinkdb inst returns sequence of 0 as error
  if (qst.target.length === 0) {
    throw mmErrCannotReduceOverEmptyStream()
  }

  // live rethinkdb inst returns sequence of 1 atom
  if (qst.target.length === 1) {
    [qst.target] = qst.target

    return qst
  }

  const seq = qst.target.sort(() => 0.5 - Math.random())

  qst.target = seq.slice(1)
    .reduce((st, arg) => spend(db, qst, args[0], [st, arg]), seq[0])

  return qst
}

// fold has the following differences from reduce:
//
//  * it is guaranteed to proceed through the sequence from
//    first element to last.
//  * it passes an initial base value to the function with the
//    first element in place of the previous reduction result.
//
q.fold = (db, qst, args) => {
  const [startVal, reduceFn] = args

  if (args.length < 2) {
    throw mmErrArgumentsNumber('fold', 2, args.length)
  }

  qst.target = qst.target
    .reduce((st, arg) => spend(db, qst, reduceFn, [st, arg]), startVal)

  return qst
}

q.forEach =  (db, qst, args) => {
  const [forEachRow] = args

  if (args.length !== 1) {
    throw mmErrArgumentsNumber('forEach', 1, args.length)
  }

  qst.target = qst.target.reduce((st, arg) => {
    const result = spend(db, qst, forEachRow, [arg])

    return mmDbStateAggregate(st, result)
  }, {})

  return qst
}

q.getCursor = (db, qst, args) => {
  // returning the changes()-defined 'target' here causes node to hang un-predictably
  if (qst.target instanceof Readable
      && 'changesTarget' in qst) {

    qst.target.close()
    qst.target = qst.changesTarget
  }

  const dbName = mockdbReqlQueryOrStateDbName(qst, db)
  const tableName = qst.tablename
  const queryTarget = qst.target
  const queryTargetFuture = qst.target || {
    [mmDbStateTableGetPrimaryKey(db, dbName, tableName)]: qst.primaryKeyValue
  }
  const queryOptions = queryArgsOptions(args)
  const cursorTargetType = tableName
    ? (Array.isArray(queryTarget) ? 'table' : 'doc') : 'expr'

  let cursors = null

  if (typeof queryOptions.maxBatchRows !== 'number') {
    queryOptions.maxBatchRows = Math.Infinite
  }

  if (cursorTargetType === 'doc') {
    cursors = mmDbStateTableDocCursorsGetOrCreate(
      db, dbName, tableName, queryTargetFuture)
  } else if (cursorTargetType === 'table') {
    cursors = mmDbStateTableCursorsGetOrCreate(
      db, dbName, tableName)
  }
  const cursorIndex = cursors ? cursors.length : null
  const initialDocs = []

  if (!qst.isChanges || qst.includeInitial) {
    asList(queryTarget).map(item => {
      if (cursorTargetType === 'doc' || item || isBoolNumStrRe.test(typeof item)) {
        initialDocs.push({
          new_val: item
        })
      }
    })
  }

  if (qst.error) {
    initialDocs.push({ error: qst.error })
  }

  const cursor = mmStream(initialDocs, !qst.isChanges)

  cursor.close = () => {
    cursor.destroy()

    if (cursorTargetType === 'table')
      db = mmDbStateTableCursorSplice(db, dbName, tableName, cursorIndex)
    if (cursorTargetType === 'doc')
      db = mmDbStateTableDocCursorSplice(db, dbName, tableName, queryTargetFuture, cursorIndex)

    return new Promise((resolve /*, reject*/) => resolve())
  }

  if (cursorTargetType === 'table') {
    db = mmDbStateTableCursorSet(db, dbName, tableName, cursor)
  } else if (cursorTargetType === 'doc') {
    db = mmDbStateTableDocCursorSet(db, dbName, tableName, queryTargetFuture, cursor)
  }

  qst.target = cursor

  return qst
}

export default Object.assign(spend, q)
