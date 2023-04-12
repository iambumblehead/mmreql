const mmEnumQueryArgTypeROWFN = 'reqlARGSSUSPENDFN'
const mmEnumQueryArgTypeROW = 'reqlARGSSUSPEND'
const mmEnumQueryArgTypeARGS = 'reqlARGSRESULT'

const mmEnumQueryArgTypeROWIsRe = new RegExp(`^${mmEnumQueryArgTypeROW}`)
const mmEnumQueryArgTypeROWHasRe = new RegExp(`${mmEnumQueryArgTypeROW}`)

const mmEnumTypeERROR = 'reqlERROR'

const mmEnumQueryNamesResolving = [
  'serialize', 'drain', 'run',
  'getCursor', 'getPoolMaster',
  'connect', 'connectPool']

const mmEnumQueryNamesFirstTerm = [
  'desc', 'asc']

// eslint-disable-next-line security/detect-non-literal-regexp
const mmEnumQueryNameIsRESOLVINGRe = new RegExp(
  `^(${mmEnumQueryNamesResolving.join('|')})$`)
// eslint-disable-next-line security/detect-non-literal-regexp
const mmEnumQueryNameIsFIRSTTERMRe = new RegExp(
  `^(${mmEnumQueryNamesFirstTerm.join('|')})$`)

const mmEnumQueryNameIsCURSORORDEFAULTRe = /getCursor|default/

const mmEnumIsLookObj = obj => obj
  && typeof obj === 'object' && !(obj instanceof Date)

const mmEnumIsRow = obj => mmEnumIsLookObj(obj)
  && mmEnumQueryArgTypeROWIsRe.test(obj.type)

const mmEnumIsRowShallow = obj => mmEnumIsLookObj(obj) && (
  mmEnumQueryArgTypeROWIsRe.test(obj.type) ||
    mmEnumQueryArgTypeROWHasRe.test(Object.values(obj).join()))

const mmEnumIsQueryArgsResult = obj => mmEnumIsLookObj(obj)
  && Boolean(mmEnumQueryArgTypeARGS in obj)

export {
  mmEnumQueryArgTypeARGS,
  mmEnumQueryArgTypeROW,
  mmEnumQueryArgTypeROWFN,
  mmEnumQueryArgTypeROWIsRe,
  mmEnumQueryArgTypeROWHasRe,

  mmEnumQueryNameIsRESOLVINGRe,
  mmEnumQueryNameIsFIRSTTERMRe,
  mmEnumQueryNameIsCURSORORDEFAULTRe,

  mmEnumTypeERROR,

  mmEnumIsRow,
  mmEnumIsRowShallow,
  mmEnumIsQueryArgsResult
}
