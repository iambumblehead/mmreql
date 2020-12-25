import rethinkDBMocked, {
    PseudoQuery,
    unwrap
} from './template-js-rethinkdb-mocked-reql.js';

import thinkyMocked, {
    thinkyMockedDB,
    thinkyMockedDBObject,
    thinkyMockedDBDocGen
} from './template-js-rethinkdb-mocked-thinky.js';

export default tables => {
    const mockedDB = thinkyMockedDB();

    const tableMap = tables.reduce( ( map, tablelist ) => {
        map[tablelist[0]] = thinkyMockedDBDocGen(
            mockedDB,
            thinkyMockedDBObject( tablelist[0], () => tablelist.slice( 1 ) )
        );

        return map;
    }, {});

    return thinkyMocked( tableMap );
};

export {
    PseudoQuery,
    unwrap,
    rethinkDBMocked,
    thinkyMocked,
    thinkyMockedDB,
    thinkyMockedDBObject,
    thinkyMockedDBDocGen
};
