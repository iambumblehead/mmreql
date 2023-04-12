const isCursorDefaultRe = /getCursor|default/

const mmRecIsCursorOrDefault = rec => (
  isCursorDefaultRe.test(rec.queryName))

export {
  mmRecIsCursorOrDefault
}
