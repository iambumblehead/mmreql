const mockdbStateTableCreateIndexTuple = ( name, fields = [], config = {}) => (
    [ name, fields, config ]);

const mockdbStateCreate = () => ({
    tables: {}
});

const mockdbStateTableCreate = ( db, tableName ) => {
    db[tableName] = {
        indexes: [ mockdbStateTableCreateIndexTuple( 'id' ) ]
    };

    return db;
};

const mockdbStateTableGet = ( db, tableName ) => (
    db[tableName]);

const mockdbStateTableGetIndexNames = ( db, tableName ) => {
    const mocktable = mockdbStateTableGet( db, tableName );

    return mocktable ? mocktable.indexes.map( i => i[0]) : [];
};

const mockdbStateTableIndexExists = ( db, tableName, indexName ) => {
    const indexNames = mockdbStateTableGetIndexNames( db, tableName );

    return indexNames.includes( indexName );
};

const mockdbStateTableGetOrCreate = ( db, tableName ) => {
    if ( !db[tableName])
        db = mockdbStateTableCreate( db, tableName );

    return mockdbStateTableGet( db, tableName );
};

const mockdbStateTableIndexAdd = ( db, tableName, indexName, fields, config ) => {
    const mocktable = mockdbStateTableGetOrCreate( db, tableName );

    mocktable.indexes.push(
        mockdbStateTableCreateIndexTuple( indexName, fields, config )
    );

    return mocktable;
};

const mockdbStateTableIndexList = ( db, tableName ) => {
    db = mockdbStateTableGetOrCreate( db, tableName );

    return mockdbStateTableGetIndexNames( db, tableName );
};

// by default, a tuple for primaryKey 'id' is returned,
// this should be changed. ech table config should provide a primary key
// using 'id' as the defautl for each one.
const mockdbStateTableGetIndexTuple = ( db, tableName, indexName ) => {
    const mocktable = mockdbStateTableGet( db, tableName );

    return ( mocktable && mocktable.indexes )
        && mocktable.indexes.find( i => i[0] === indexName );
//         : mockdbStateTableCreateIndexTuple( 'id' );
};

export {
    mockdbStateCreate,
    mockdbStateTableCreate,
    mockdbStateTableGet,
    mockdbStateTableGetOrCreate,
    mockdbStateTableIndexAdd,
    mockdbStateTableIndexList,
    mockdbStateTableIndexExists,
    mockdbStateTableGetIndexTuple
};
