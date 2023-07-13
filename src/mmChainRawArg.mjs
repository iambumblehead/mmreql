import mmConn from './mmConn.mjs'
import mmChainRecNext from './mmChainRec.mjs'

import {
  mmEnumQueryArgTypeCHAINIsRe,
  mmEnumQueryArgTypeARGSIG
} from './mmEnum.mjs'

const isBoolNumUndefRe = /boolean|number|undefined/

// ex, "table => r.db('cmdb').tableCreate(table)"
//   [ "table =>", "table " ]
//
// ex, "function (hello, world) { return hello; }"
//   [ "function (hello, world", "hello, world" ]
//
// ex, "(row1, row2) => (",
//   [ "(row1, row2) =>", "row1, row2" ]
const fnStrEs5Re = /^function/
const fnStrEs5ArgsRe = /^function[^(]*\(([^)]*)/
const fnStrEs6ArgsRe = /\(?([^)]*)\)?\s*\=\>/
const fnStrCreateArgs = fnStr => [
  fnStr, Array(fnStr.length).map((_, i) => String(i)).join(', ')]
const fnStrParseArgs = fnStr => (
  String(fnStr)
    .match(fnStrEs5Re.test(fnStr)
      ? fnStrEs5ArgsRe
      : fnStrEs6ArgsRe
    ) || fnStrCreateArgs(fnStr))[1].trim().split(/,\s*/)

// gennerates spec from chain. when chain includes row functions,
// applies function to row-chain to extract row-spec from row-chain
//
// chain arg is used rather than importing chain, because
// recursive error between this function and chain
const mockdbChainSuspendArgFn = (chainCreate, recId, arg) => {
  const fnArgNames = fnStrParseArgs(arg)
  const fnArgSig = fnArgNames.join()
  const fnRecId = (recId || '') + '(' + fnArgSig + ')'

  let argchain = arg(
    ...fnArgNames.map((argName, i) => (
      chainCreate()
        .row(mmEnumQueryArgTypeARGSIG, fnRecId, i, argName.trim())
    ))
  )

  // if raw data are returned, convert to chain
  if (!mmEnumQueryArgTypeCHAINIsRe.test(argchain)) {
    argchain = chainCreate().expr(argchain)
  }

  return Array.isArray(argchain)
    ? argchain.map(argc => mmChainRecNext(argc, null, fnRecId))
    : mmChainRecNext(argchain, null, fnRecId)
}

// deeply recurse data converting chain leaves to spec
const mmChainRawArg = (arg, recId, chainCreate, type = typeof arg) => {
  if (isBoolNumUndefRe.test(type)
    || arg instanceof Date || arg instanceof mmConn || !arg) {
    arg = arg
  } else if (Array.isArray(arg)) {
    arg = arg.map(a => mmChainRawArg(a, recId, chainCreate))
  } else if (mmEnumQueryArgTypeCHAINIsRe.test(arg)) {
    arg = mmChainRecNext(arg)
  } else if (typeof arg === 'function') {
    arg = mockdbChainSuspendArgFn(chainCreate, recId, arg)
  } else if (type === 'object') {
    arg = Object.keys(arg).reduce((a, k) => (
      a[k] = mmChainRawArg(arg[k], recId, chainCreate),
      a), {})
  }

  return arg
}

export default mmChainRawArg
