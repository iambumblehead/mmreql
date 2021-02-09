const mockdbStateTableCreateIndexTuple = ( name, fields = [], config = {}) => (
    [ name, fields, config ]);

const mockdbStateCreate = () => ({
    tables: {}
});

const mockdbStateTableCreate = ( db, tableName ) => {
    db[tableName] = {
        indexes: [ mockdbStateTableCreateIndexTuple( 'id' ) ],
        cursors: []
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

const mockdbStateTableIndexListSecondary = ( db, tableName ) => {
    const mocktable = mockdbStateTableGet( db, tableName );
    const indexes = mocktable ? mocktable.indexes.map( i => i[0]) : [];

    // should later support custom primary indexes
    return indexes.filter( index => index !== 'id' );
};


// by default, a tuple for primaryKey 'id' is returned,
// this should be changed. ech table config should provide a primary key
// using 'id' as the defautl for each one.
const mockdbStateTableGetIndexTuple = ( db, tableName, indexName ) => {
    const mocktable = mockdbStateTableGet( db, tableName );

    const indexTuple = ( mocktable && mocktable.indexes )
        && mocktable.indexes.find( i => i[0] === indexName );

    if ( !indexTuple ) {
        console.warn(`table index not found. ${tableName}, ${indexName}`);
    }

    return indexTuple;
};

const mockdbStateTableCursorSplice = ( db, tableName, cursorIndex ) => {
    db[tableName].cursors.splice( cursorIndex, 1 );

    return db;
};

const mockdbStateTableDocCursorSplice = ( db, tableName, doc, cursorIndex ) => {
    const tableDocId = [ tableName, doc.id ].join( '-' );

    db[tableDocId] = db[tableDocId] || {};
    db[tableDocId].cursors.splice( cursorIndex, 1 );

    return db;
};

const mockdbStateTableCursorSet = ( db, tableName, cursor ) => {
    db[tableName].cursors.push( cursor );

    return db;
};

const mockdbStateTableDocCursorSet = ( db, tableName, doc, cursor ) => {
    const tableDocId = [ tableName, doc.id ].join( '-' );

    db[tableDocId] = db[tableDocId] || {};
    db[tableName].cursors.push( cursor );

    return db;
};

const mockdbStateTableCursorsPushChanges = ( db, tableName, changes ) => {
    db[tableName].cursors.map( c => {
        changes.map( d => c.push( d ) );
    });

    return db;
};

const mockdbStateTableCursorsGetOrCreate = ( db, tableName ) => {
    db[tableName].cursors = db[tableName].cursors || [];

    return db[tableName].cursors;
};

const mockdbStateTableDocCursorsGetOrCreate = ( db, tableName, doc ) => {
    const tableDocId = [ tableName, doc.id ].join( '-' );

    db[tableDocId] = db[tableDocId] || {};
    db[tableDocId].cursors = db[tableDocId].cursors || [];

    return db[tableDocId].cursors;
};

export {
    mockdbStateCreate,
    mockdbStateTableCreate,
    mockdbStateTableGet,
    mockdbStateTableGetOrCreate,
    mockdbStateTableGetIndexNames,
    mockdbStateTableIndexAdd,
    mockdbStateTableIndexListSecondary,
    mockdbStateTableIndexExists,
    mockdbStateTableGetIndexTuple,
    mockdbStateTableCursorSet,
    mockdbStateTableDocCursorSet,
    mockdbStateTableCursorSplice,
    mockdbStateTableDocCursorSplice,
    mockdbStateTableCursorsPushChanges,
    mockdbStateTableCursorsGetOrCreate,
    mockdbStateTableDocCursorsGetOrCreate
};
