import { v4 as uuidv4 } from 'uuid';
import { unwrap } from './mockdbReql.js';

const mockdbTableGetDocument = ( table, id ) => table
    .find( doc => doc.id === id );

const mockdbTableGetDocuments = ( table, ids = []) => {
    // eslint-disable-next-line security/detect-non-literal-regexp
    const idsRe = new RegExp( `^(${ids.join( '|' )})$` );

    return table.filter( doc => idsRe.test( doc.id ) );
};

const mockdbTableSetDocument = ( table, doc ) => {
    if ( doc && !doc.id )
        doc.id = uuidv4();

    table.push( doc );

    return [ table, doc ];
};

const mockdbTableSetDocuments = ( table, docs ) => {
    docs = docs.map( doc => {
        [ table, doc ] = mockdbTableSetDocument( table, doc );

        return doc;
    });

    return [ table, docs ];
};

const mockdbTableDocGetIndexValue = ( doc, indexTuple, indexValueDefault ) => {
    const [ indexName, fields /* , options */ ] = indexTuple;

    if ( fields.length ) {
        indexValueDefault = fields
            .map( field => unwrap( field, doc ) )
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
    mockdbTableDocGetIndexValue
};
