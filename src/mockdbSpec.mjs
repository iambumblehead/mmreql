const mockdbSpecARGSRESULT = 'reqlARGSRESULT';
const mockdbSpecARGSSUSPEND = 'reqlARGSSUSPEND';
const mockdbSpecARGSSUSPENDFN = 'reqlARGSSUSPENDFN';
const mockdbSpecARGSISSUSPENDRe = /reqlARGSSUSPEND/;

const isBoolNumUndefRe = /boolean|number|undefined/;
// const isCursorDefaultRe = /getCursor|default/;

//const mockdbRecIsCursorOrDefault = rec => (
//  isCursorDefaultRe.test(rec.queryName));

const mockdbSpecIsSuspendNestedShallow = obj => obj
  && typeof obj === 'object'
  && mockdbSpecARGSISSUSPENDRe.test(Object.values(obj).join());

// const isReqlObj = obj => Boolean(
//   obj && /object|function/.test(typeof obj) && obj.isReql);
const mockdbSpecIs = obj => Boolean(
  obj && mockdbSpecARGSISSUSPENDRe.test(obj.type));

const mockdbSpecSignatureArg = (recarg, type = typeof recarg) => {
  if (type === 'string') {
    recarg = /^reqlARGSSUSPEND/.test(recarg) ? 'row' : `"${recarg}"`;
  } else if (type === 'object') {
    recarg = '...';
  } else if (Array.isArray(recarg)) {
    recarg = 'arr';
  }

  return recarg;
};

const mockdbSpecSignatureArgs = rec => rec.queryArgs[0] === 'reqlARGSSUSPEND'
  ? rec.queryArgs[3]
  : rec.queryArgs.map(arg => mockdbSpecSignatureArg(arg)).join(', ');

// returns a human readable signature from reqlOb,
//
// ex, '.row("rose")("petal")'
const mockdbSpecSignature = reqlObj => (
  reqlObj && reqlObj.record.reduce((prev, rec) => (
    prev + (/\.fn/.test(rec.queryName)
      ? `(${mockdbSpecSignatureArgs(rec)})`
      : `.${rec.queryName}(${mockdbSpecSignatureArgs(rec)})`)), ''));


// default record used to wrap raw data with no chain
const mockdbSpecRecordDefault = [ {
  queryName: 'expr',
  queryArgs: [ mockdbSpecARGSSUSPEND + 0 ]
} ];

// 'spec' arrives here from nested sub query as a reqlCHAIN or pojo. ex,
//  * `map(hero => hero('name'))`
//  * `map(hero => ({ heroName: hero('name') }))`
//
// maybe these should be evaluated in advance... called with an empty chain
const mockdbSpecFromChain = (type, chain, recId) => ({
  type: type,
  toString: () => type,
  recs: chain.recs || mockdbSpecRecordDefault.slice(),
  recIndex: chain.recIndex || 1,
  recHist: chain.recHist || [ [] ],
  recId: recId || ('orphan' + Date.now())
});

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
// /\(?([^)]*)\=\>/;
const fnStrParseArgs = fnStr => String(fnStr)
  .match(fnStrEs5Re.test(fnStr)
    ? fnStrEs5ArgsRe
    : fnStrEs6ArgsRe
  )[1].trim().split(/,\s*/);

// const mockdbSpecArgsIdRe = /^function \(([^)]*)|\(?(.*)\)?\=\>/

// gennerates spec from chain. when chain includes row functions,
// applys function to row-chain to extract row-spec from row-chain
//
// chain arg is used rather than importing chain, because
// recursive error between this function and chain
const mockdbChainSuspendArgFn = (chainCreate, arg) => {
  const fnArgNames = fnStrParseArgs(arg);
  const fnArgSig = `reqlARGSIG.${fnArgNames}`;

  let argchain = arg(
    ...fnArgNames.map((argName, i) => {
      const chain = chainCreate()
        .row(mockdbSpecARGSSUSPEND, fnArgSig, i, argName.trim());

      return chain;
    })
  );

  // if raw data are returned, convert to chain
  if (!/reqlCHAIN/.test(String(argchain))) {
    // 'insert' is evaluated here...
    // need to store the arg names
    argchain = chainCreate().expr(argchain);
  }

  return Array.isArray(argchain)
    ? argchain.map(argc => (
      mockdbSpecFromChain(mockdbSpecARGSSUSPENDFN, argc, fnArgSig)))
    : mockdbSpecFromChain(mockdbSpecARGSSUSPENDFN, argchain, fnArgSig);
};


// should be renamed
const isChain = obj => Boolean(
  obj && /object|function/.test(typeof obj) && obj.isReql);

// deeply recurse data converting chain leaves to spec
const specFromRawArg = (arg, chainCreate, type = typeof arg) => {
  if (isBoolNumUndefRe.test(type) || arg instanceof Date || !arg) {
    arg = arg;
  } else if (isChain(arg)) {
    arg = mockdbSpecFromChain(mockdbSpecARGSSUSPEND, arg);
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

// needs to return not just arg but row details...
// 

// mockdbSpecFromRawArgs
// const mockdbSpecFromRawArgs = (args, rowchain) => args
//  .map(arg => mockdbSpecFromRawArg(arg, rowchain));

export {
  // mockdbRecIsCursorOrDefault,
  mockdbSpecARGSRESULT,
  mockdbSpecARGSSUSPEND,
  mockdbSpecARGSSUSPENDFN,
  mockdbSpecARGSISSUSPENDRe,

  mockdbSpecIsSuspendNestedShallow,
  mockdbSpecIs,
  
  mockdbSpecSignatureArgs,
  mockdbSpecSignature,

  // mockdbSpecFromRawArgs,
  specFromRawArg as mockdbSpecFromRawArg
}
