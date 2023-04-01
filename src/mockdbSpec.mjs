import mmConn from './mmConn.mjs';

import {
  mmEnumQueryArgTypeROW,
  mmEnumQueryArgTypeROWIsRe,
  mmEnumQueryArgTypeROWHasRe,
  mmEnumRecTypeCHAINHasRe
} from './mmEnum.mjs';

import {
  mmRecChainRowCreate,
  mmRecChainRowFnCreate
} from './mmRecChain.mjs';

const isBoolNumUndefRe = /boolean|number|undefined/;

const mockdbSpecIsSuspendNestedShallow = obj => obj
  && typeof obj === 'object'
  && mmEnumQueryArgTypeROWHasRe.test(Object.values(obj).join());

const mockdbSpecIs = obj => Boolean(
  obj && mmEnumQueryArgTypeROWIsRe.test(obj.type));

// ex, "table => r.db('cmdb').tableCreate(table)"
//   [ "table =>", "table " ]
//
// ex, "function (hello, world) { return hello; }"
//   [ "function (hello, world", "hello, table" ]
//
// ex, "(row1, row2) => (",
//   [ "(row1, row2) =>", "row1, row2" ]

const fnStrEs5Re = /^function/;
const fnStrEs5ArgsRe = /^function \(([^)]*)/;
const fnStrEs6ArgsRe = /\(?([^)]*)\)?\s*\=\>/;
const fnStrParseArgs = fnStr => String(fnStr)
  .match(fnStrEs5Re.test(fnStr)
    ? fnStrEs5ArgsRe
    : fnStrEs6ArgsRe
  )[1].trim().split(/,\s*/);

// gennerates spec from chain. when chain includes row functions,
// applys function to row-chain to extract row-spec from row-chain
//
// chain arg is used rather than importing chain, because
// recursive error between this function and chain
const mockdbChainSuspendArgFn = (chainCreate, arg) => {
  const fnArgNames = fnStrParseArgs(arg);
  const fnArgSig = `reqlARGSIG.${fnArgNames}`;

  let argchain = arg(
    ...fnArgNames.map((argName, i) => (
      chainCreate()
        .row(mmEnumQueryArgTypeROW, fnArgSig, i, argName.trim())
    ))
  );

  // if raw data are returned, convert to chain
  if (!mmEnumRecTypeCHAINHasRe.test(String(argchain))) {
    // 'insert' is evaluated here...
    // need to store the arg names
    argchain = chainCreate().expr(argchain);
  }

  return Array.isArray(argchain)
    ? argchain.map(argc => mmRecChainRowFnCreate(argc, fnArgSig))
    : mmRecChainRowFnCreate(argchain, fnArgSig);
};


// should be renamed
const isChain = obj => Boolean(
  obj && /object|function/.test(typeof obj) && obj.isReql);

// deeply recurse data converting chain leaves to spec
const specFromRawArg = (arg, chainCreate, type = typeof arg) => {
  if (isBoolNumUndefRe.test(type)
    || arg instanceof Date || arg instanceof mmConn || !arg) {
    arg = arg;
  } else if (isChain(arg)) {
    // arg = mockdbSpecFromChain(mmEnumQueryArgTypeROW, arg);
    arg = mmRecChainRowCreate(arg);
  } else if (typeof arg === 'function') {
    arg = mockdbChainSuspendArgFn(chainCreate, arg);
  } else if (Array.isArray(arg)) {
    arg = arg.map(a => specFromRawArg(a, chainCreate));
  } else if (type === 'object') {
    arg = Object.keys(arg)
      .reduce((a, k) => (a[k] = specFromRawArg(arg[k], chainCreate), a), {});
  }

  return arg;
};

export {
  mockdbSpecIsSuspendNestedShallow,
  mockdbSpecIs,
  
  specFromRawArg as mockdbSpecFromRawArg
}
