const mmRecCHAIN = 'reqlCHAIN';
const reqlARGSSUSPEND = 'reqlARGSSUSPEND';

// when a query is used to compose multiple, longer query chains
// recordindex is a unique point at the chain used to recover
// the record list from that time, rather than using recs
// added from external chains that might have added queries
// to this base chain

const mmRecChainIndexGet = chain => chain.recHist.length;

const mmRecChainFromIndex = (chain, index) => (
  chain.recHist[index].slice());


const mmRecChainPush = (chain, rec) => {
  chain.recs.push(rec);
  chain.recHist.push(chain.recs.slice());
  chain.recIndex = +chain.recHist.length;

  return chain;
};

const mmRecChainRecsGet = chain => (
  chain.recHist[Math.max(chain.recIndex - 1, 0)] || []).slice();

const mmRecChainClear = chain => {
  chain.recs.splice(0, chain.recs.length);
  chain.recHist = [];

  return chain;
};

const mmRecChainSubCreate = chain => ({
  type: reqlARGSSUSPEND,
  recs: chain.recs.slice(),
  recHist: chain.recHist || [],
  toString: () => reqlARGSSUSPEND
});

const mmRecChainCreate = chain => ({
  state: chain.state || {},
  recs: [],
  recIndex: 0,
  recHist: [],
  toString: () => mmRecCHAIN
});

const mmRecChainCreateNext = (chain, rec) => mmRecChainPush({
  ...chain,
  recIndex: +chain.recIndex,
  recs: mmRecChainRecsGet(chain)
}, rec);

const mmRecChainNext = (chain, rec) => {
  // should factor this spread away (create new object instead)
  chain = {
    ...chain,
    recs: mmRecChainRecsGet(chain)
  };

  chain = mmRecChainPush(chain, rec);

  return chain;
};

export {
  mmRecChainIndexGet,
  mmRecChainFromIndex,
  mmRecChainClear,
  mmRecChainPush,
  mmRecChainRecsGet,
  mmRecChainCreate,
  mmRecChainSubCreate,
  mmRecChainCreateNext,
  mmRecChainNext
}
