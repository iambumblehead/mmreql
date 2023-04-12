import {
  mmEnumQueryArgTypeROW,
  mmEnumQueryArgTypeROWIsRe
} from './mmEnum.mjs'

const mmChainSigArg = (recarg, type = typeof recarg) => {
  if (type === 'string') {
    recarg = mmEnumQueryArgTypeROWIsRe.test(recarg) ? 'row' : `"${recarg}"`
  } else if (type === 'object') {
    recarg = '...'
  } else if (Array.isArray(recarg)) {
    recarg = 'arr'
  }

  return recarg
}

const mmChainSigArgs = rec => rec[1][0] === mmEnumQueryArgTypeROW
  ? rec[1][3]
  : rec[1].map(arg => mmChainSigArg(arg)).join(', ')

// returns a human readable signature from reqlOb,
//
// ex, '.row("rose")("petal")'
const mmChainSig = reqlObj => (
  reqlObj && reqlObj.recs.reduce((prev, rec) => (
    prev + (/\.fn/.test(rec[0])
      ? `(${mmChainSigArgs(rec)})`
      : `.${rec[0]}(${mmChainSigArgs(rec)})`)), ''))

export default mmChainSig
