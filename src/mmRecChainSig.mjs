import {
  mmEnumQueryArgTypeROW,
  mmEnumQueryArgTypeROWIsRe
} from './mmEnum.mjs'

const mmRecChainSigArg = (recarg, type = typeof recarg) => {
  if (type === 'string') {
    recarg = mmEnumQueryArgTypeROWIsRe.test(recarg) ? 'row' : `"${recarg}"`
  } else if (type === 'object') {
    recarg = '...'
  } else if (Array.isArray(recarg)) {
    recarg = 'arr'
  }

  return recarg
}

const mmRecChainSigArgs = rec => rec.queryArgs[0] === mmEnumQueryArgTypeROW
  ? rec.queryArgs[3]
  : rec.queryArgs.map(arg => mmRecChainSigArg(arg)).join(', ')

// returns a human readable signature from reqlOb,
//
// ex, '.row("rose")("petal")'
const mmRecChainSig = reqlObj => (
  reqlObj && reqlObj.recs.reduce((prev, rec) => (
    prev + (/\.fn/.test(rec.queryName)
      ? `(${mmRecChainSigArgs(rec)})`
      : `.${rec.queryName}(${mmRecChainSigArgs(rec)})`)), ''))

export default mmRecChainSig
