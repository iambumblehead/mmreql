import thinkyMocked from './mockdbThinky.js';

import {
    mockdbStateTableCreate
} from './mockdbState.js';

export default startTables => {
    const tables = Array.isArray( startTables ) ? startTables : [];
    const { tableMap, dbState } = tables.reduce( ( map, tablelist ) => {
        map.tableMap[tablelist[0]] = tablelist.slice( 1 );
        map.dbState = mockdbStateTableCreate( map.dbState, tablelist[0]);

        return map;
    }, {
        tableMap: {},
        dbState: {}
    });

    return thinkyMocked( tableMap, dbState );
};
