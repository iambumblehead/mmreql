const mmEnumRecTypeCHAIN = 'reqlCHAIN'
const mmEnumRecTypeROW = 'reqlARGSSUSPEND'

const mmEnumRecTypeCHAINHasRe = new RegExp(
  `${mmEnumRecTypeCHAIN}`)

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

export {
  mmEnumRecTypeCHAIN,
  mmEnumRecTypeROW,
  mmEnumRecTypeCHAINHasRe,

  mmEnumQueryArgTypeARGS,
  mmEnumQueryArgTypeROW,
  mmEnumQueryArgTypeROWFN,
  mmEnumQueryArgTypeROWIsRe,
  mmEnumQueryArgTypeROWHasRe,

  mmEnumQueryNameIsRESOLVINGRe,
  mmEnumQueryNameIsFIRSTTERMRe,

  mmEnumTypeERROR
}
