// eslint-disable-next-line filenames/match-exported
import rethinkDBMocked, {
    PseudoQuery,
    unwrap
} from './template-js-rethinkdb-mocked-reql.js';

import thinkyMocked from './template-js-rethinkdb-mocked-thinky.js';

export default rethinkDBMocked;

export {
    PseudoQuery,
    unwrap,
    thinkyMocked,
    rethinkDBMocked
};
