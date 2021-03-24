import { v4 as uuidv4 } from 'uuid';

const mockdbTableGetDocument = ( table, id, key = 'id' ) => table
    .find( doc => doc[key] === id );

const mockdbTableGetDocuments = ( table, ids = [], primaryKey = 'id' ) => {
    // eslint-disable-next-line security/detect-non-literal-regexp
    const idsRe = new RegExp( `^(${ids.join( '|' )})$` );

    return table.filter( doc => idsRe.test( doc[primaryKey]) );
};

const mockdbTableRmDocument = ( table, doc, primaryKey = 'id' ) => {
    if ( doc && doc.id ) {
        const existingIndex = table
            .findIndex( d => d[primaryKey] === doc[primaryKey]);

        if ( existingIndex >= 0 )
            table.splice( existingIndex, 1 );
    }

    return [ table ];
};

const mockdbTableDocEnsurePrimaryKey = ( doc, primaryKey ) => {
    if ( doc && !doc[primaryKey])
        doc[primaryKey] = uuidv4();

    return doc;
};

const mockdbTableSetDocument = ( table, doc, primaryKey = 'id' ) => {
    [ table ] = mockdbTableRmDocument( table, doc, primaryKey );
    
    table.push(
        mockdbTableDocEnsurePrimaryKey( doc, primaryKey ) );

    return [ table, doc ];
};

const mockdbTableSetDocuments = ( table, docs ) => {
    docs = docs.map( doc => {
        [ table, doc ] = mockdbTableSetDocument( table, doc );

        return doc;
    });

    return [ table, docs ];
};

const mockdbTableRmDocumentsAll = table => {
    table.length = 0;

    return [ table ];
};

// set the entire table, replace existing documents
const mockdbTableSet = ( table, docs ) => {
    table.length = 0;

    docs.forEach( doc => table.push( doc ) );

    return [ table ];
};

const mockdbTableDocGetIndexValue = ( doc, indexTuple, spend, indexValueDefault, reqlChain ) => {
    const [ indexName, fields /* , options */ ] = indexTuple;

    if ( fields.length ) {
        indexValueDefault = fields
            .map( field => spend( field, reqlChain, doc ) )
            .join( ',' );
    } else {
        indexValueDefault = doc[indexName];
    }

    return indexValueDefault;
};

export {
    mockdbTableGetDocument,
    mockdbTableGetDocuments,
    mockdbTableSetDocument,
    mockdbTableSetDocuments,
    mockdbTableDocGetIndexValue,
    mockdbTableDocEnsurePrimaryKey,
    mockdbTableRmDocumentsAll,
    mockdbTableSet
};
