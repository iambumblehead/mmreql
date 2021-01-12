const mockdbStateCreate = () => ({
    tables: {}
});

const mockdbStateTableCreate = ( db, tableName ) => {
    db[tableName] = {
        indexes: []
    };

    return db;
};

const mockdbStateTableGet = ( db, tableName ) => (
    db[tableName]);

const mockdbStateTableGetOrCreate = ( db, tableName ) => {
    if ( !db[tableName])
        db = mockdbStateTableCreate( db, tableName );

    return mockdbStateTableGet( db, tableName );
};

const mockdbStateTableIndexAdd = ( db, tableName, indexName ) => {
    const mocktable = mockdbStateTableGetOrCreate( db, tableName );

    mocktable.indexes.push( indexName );

    return mocktable;
};

const mockdbStateTableIndexList = ( db, tableName ) => {
    const mocktable = mockdbStateTableGetOrCreate( db, tableName );

    return mocktable.indexes;
};

const mockdbStateTableIndexExists = ( db, tableName, indexName ) => {
    const mocktable = mockdbStateTableGet( db, tableName );

    return Boolean( mocktable && mocktable.indexes.includes( indexName ) );
};

export {
    mockdbStateCreate,
    mockdbStateTableCreate,
    mockdbStateTableGet,
    mockdbStateTableGetOrCreate,
    mockdbStateTableIndexAdd,
    mockdbStateTableIndexList,
    mockdbStateTableIndexExists
};
