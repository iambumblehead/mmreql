// eslint-disable-next-line filenames/match-exported
import rethinkDBMocked, {
    PseudoQuery,
    unwrap
} from './template-js-rethinkdb-mocked-reql.js';

import thinkyMocked, {
    thinkyMockedDB,
    thinkyMockedDBObject,
    thinkyMockedDBDocGen
} from './template-js-rethinkdb-mocked-thinky.js';

export default rethinkDBMocked;

export {
    PseudoQuery,
    unwrap,
    rethinkDBMocked,
    thinkyMocked,
    thinkyMockedDB,
    thinkyMockedDBObject,
    thinkyMockedDBDocGen
};
