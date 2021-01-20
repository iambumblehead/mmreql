// 'queryFilter' may not be the best label.
// rename as this area becomes more well-understood.

// ex, { method: 'default', args: [Array] }
const queryFilterIsDefault = qfilter => (
    qfilter
        && qfilter.method === 'default' );

const queryFilterFindDefault = qfilters => qfilters
    .find( queryFilterIsDefault );

export {
    queryFilterIsDefault,
    queryFilterFindDefault
};
