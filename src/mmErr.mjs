const mmErrStringify = obj => JSON.stringify(obj, null, '\t')

const mmErrDuplicatePrimaryKey = (existingDoc, conflictDoc) => new Error (
  'Duplicate primary key `id`:\n :existingDoc\n:conflictDoc'
    .replace(/:existingDoc/, mmErrStringify(existingDoc))
    .replace(/:conflictDoc/, mmErrStringify(conflictDoc)))

const mmErrArgumentsNumber = (queryId, takesArgs = 0, givenArgs = 1, atLeast = false) => new Error(
  '`:queryId` takes :takesArgs :argument, :givenArgs provided.'
    .replace(/:queryId/, queryId)
    .replace(/:argument/, takesArgs === 1 ? 'argument' : 'arguments')
    .replace(/:takesArgs/, atLeast ? `at least ${takesArgs}` : takesArgs)
    .replace(/:givenArgs/, givenArgs))

const mmErrIndexOutOfBounds = index => new Error(
  `ReqlNonExistanceError: Index out of bounds: ${index}`)

const mmErrUnrecognizedOption = key => new Error(
  'Unrecognized optional argument `:key`.'
    .replace(/:key/, key))

const mmErrInvalidTableName = tableName => new Error(
  'RethinkDBError [ReqlLogicError]: Table name `:tableName` invalid (Use A-Z, a-z, 0-9, _ and - only)'
    .replace(/:tableName/, tableName))

const mmErrInvalidDbName = dbName => new Error(
  'Database name `:dbName` invalid (Use A-Z, a-z, 0-9, _ and - only)'
    .replace(/:dbName/, dbName))

const mmErrTableExists = (dbName, tableName) => new Error(
  'Table `:tableName` already exists.'
    .replace(/:tableName/, [dbName, tableName].join('.')))

const mmErrTableDoesNotExist = (dbName, tableName) => new Error(
  'Table `:tableName` does not exist.'
    .replace(/:tableName/, [dbName, tableName].join('.')))

const mmErrSecondArgumentOfQueryMustBeObject = queryType => new Error(
  'Second argument of `:queryType` must be an object.'
    .replace(/:queryType/, queryType))

const mmErrPrimaryKeyWrongType = primaryKey => new Error(
  'Primary keys must be either a number, string, bool, pseudotype or array (got type :type)'
    .replace(/:type/, String(typeof primaryKey).toUpperCase()))

const mmErrNotATIMEpsudotype = () => new Error(
  'Not a TIME pseudotype: `null`')

const mmErrCannotUseNestedRow = () => new Error(
  'Cannot user r.row in nested queries. Use functions instead')

const mmErrNoMoreRowsInCursor = () => new Error(
  'No more rows in the cursor.')

const mmErrNoAttributeInObject = propertyName => new Error(
  'No attribute `:propertyName` in object'
    .replace(/:propertyName/, propertyName))

const mmErrExpectedTypeFOOButFoundBAR = (foo, bar) => new Error(
  `Expected type ${foo} but found ${bar}`)

// live rethinkdb inst returns sequence of 0 as error
const mmErrCannotReduceOverEmptyStream = () => new Error(
  `Cannot reduce over an empty stream.`)

const mmErrCannotCallFOOonBARTYPEvalue = (foo, bar) => new Error(
  `Cannot call ${foo} on ${bar} value.`)

export {
  mmErrStringify,
  mmErrDuplicatePrimaryKey,
  mmErrArgumentsNumber,
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
  mmErrCannotReduceOverEmptyStream,
  mmErrCannotCallFOOonBARTYPEvalue,
  mmErrNoMoreRowsInCursor,
  mmErrNoAttributeInObject,
  mmErrExpectedTypeFOOButFoundBAR
}
