import mmConn from './mmConn.mjs'

import {
  mmEnumQueryArgTypeCHAIN,
  mmEnumQueryArgTypeCHAINIsRe
} from './mmEnum.mjs'

import {
  mmChainRecNext,
  mmChainRecRowFnCreate
} from './mmChainRec.mjs'
/*
import {
  mmRecChainRowCreate,
  mmRecChainRowFnCreate
} from './mmRecChain.mjs'  
*/
const isBoolNumUndefRe = /boolean|number|undefined/

// ex, "table => r.db('cmdb').tableCreate(table)"
//   [ "table =>", "table " ]
//
// ex, "function (hello, world) { return hello; }"
//   [ "function (hello, world", "hello, table" ]
//
// ex, "(row1, row2) => (",
//   [ "(row1, row2) =>", "row1, row2" ]
const fnStrEs5Re = /^function/
const fnStrEs5ArgsRe = /^function \(([^)]*)/
const fnStrEs6ArgsRe = /\(?([^)]*)\)?\s*\=\>/
const fnStrParseArgs = fnStr => String(fnStr)
  .match(fnStrEs5Re.test(fnStr)
    ? fnStrEs5ArgsRe
    : fnStrEs6ArgsRe
  )[1].trim().split(/,\s*/)

// gennerates spec from chain. when chain includes row functions,
// applies function to row-chain to extract row-spec from row-chain
//
// chain arg is used rather than importing chain, because
// recursive error between this function and chain
const mockdbChainSuspendArgFn = (chainCreate, arg) => {
  const fnArgNames = fnStrParseArgs(arg)
  const fnArgSig = `reqlARGSIG.${fnArgNames}`

  let argchain = arg(
    ...fnArgNames.map((argName, i) => (
      chainCreate()
        .row(mmEnumQueryArgTypeCHAIN, fnArgSig, i, argName.trim())
    ))
  )

  // if raw data are returned, convert to chain
  if (!mmEnumQueryArgTypeCHAINIsRe.test(String(argchain))) {
    argchain = chainCreate().expr(argchain)
  }

  return Array.isArray(argchain)
    ? argchain.map(argc => mmChainRecRowFnCreate(argc, fnArgSig))
    : mmChainRecRowFnCreate(argchain, fnArgSig)
}

/*
// deeply recurse data converting chain leaves to spec
const mmChainRawArg = (arg, chainCreate, type = typeof arg) => {
  if (isBoolNumUndefRe.test(type)
    || arg instanceof Date || arg instanceof mmConn || !arg) {
    arg = arg
  } else if (typeof arg === 'function') {
    arg = arg.isReql
      ? mmRecChainRowCreate(arg)
      : mockdbChainSuspendArgFn(chainCreate, arg)
  } else if (Array.isArray(arg)) {
    arg = arg.map(a => mmChainRawArg(a, chainCreate))
  } else if (type === 'object') {
    arg = Object.keys(arg)
      .reduce((a, k) => (a[k] = mmChainRawArg(arg[k], chainCreate), a), {})
  }

  return arg
}
*/

// should be renamed
const isChain = obj => Boolean(
  obj && /object|function/.test(typeof obj) && obj.isReql)

// deeply recurse data converting chain leaves to spec
const mmChainRawArg = (arg, chainCreate, type = typeof arg) => {
  if (isBoolNumUndefRe.test(type)
    || arg instanceof Date || arg instanceof mmConn || !arg) {
    arg = arg
  } else if (isChain(arg)) {
    arg = mmChainRecNext(arg)
  } else if (typeof arg === 'function') {
    arg = mockdbChainSuspendArgFn(chainCreate, arg)
  } else if (Array.isArray(arg)) {
    arg = arg.map(a => mmChainRawArg(a, chainCreate))
  } else if (type === 'object') {
    arg = Object.keys(arg)
      .reduce((a, k) => (a[k] = mmChainRawArg(arg[k], chainCreate), a), {})
  }

  return arg
}

export default mmChainRawArg
