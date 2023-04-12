const mmEnumQueryArgTypeCHAINFN = 'reqlARGSCHAINFN'
const mmEnumQueryArgTypeCHAIN = 'reqlARGSCHAIN'
const mmEnumQueryArgTypeARGS = 'reqlARGSRESULT'
const mmEnumQueryArgTypeARGSIG = 'reqlARGSIG'

const mmEnumQueryArgTypeCHAINIsRe = new RegExp(`^${mmEnumQueryArgTypeCHAIN}`)
const mmEnumQueryArgTypeCHAINHasRe = new RegExp(`${mmEnumQueryArgTypeCHAIN}`)

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

const mmEnumIsChain = obj => mmEnumIsLookObj(obj)
  && mmEnumQueryArgTypeCHAINIsRe.test(obj.type)

const mmEnumIsChainShallow = obj => mmEnumIsLookObj(obj) && (
  mmEnumQueryArgTypeCHAINIsRe.test(obj.type) ||
    mmEnumQueryArgTypeCHAINHasRe.test(Object.values(obj).join()))

const mmEnumIsQueryArgsResult = obj => mmEnumIsLookObj(obj)
  && Boolean(mmEnumQueryArgTypeARGS in obj)

export {
  mmEnumQueryArgTypeARGS,
  mmEnumQueryArgTypeARGSIG,
  mmEnumQueryArgTypeCHAIN,
  mmEnumQueryArgTypeCHAINFN,
  mmEnumQueryArgTypeCHAINIsRe,
  mmEnumQueryArgTypeCHAINHasRe,

  mmEnumQueryNameIsRESOLVINGRe,
  mmEnumQueryNameIsFIRSTTERMRe,
  mmEnumQueryNameIsCURSORORDEFAULTRe,

  mmEnumTypeERROR,

  mmEnumIsChain,
  mmEnumIsChainShallow,
  mmEnumIsQueryArgsResult
}
