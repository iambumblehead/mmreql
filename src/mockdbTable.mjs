import { randomUUID } from 'crypto';
import {
  mockdbSpecIs,
  mockdbSpecIsSuspendNestedShallow
} from './mockdbSpec.mjs';

const mockdbTableDocIsPrimaryKey = (doc, primaryKey) => Boolean(
  doc && /number|string/.test(typeof doc[primaryKey]));

const mockdbTableGetDocument = (table, id, key = 'id') => table
  .find(doc => doc[key] === id);

const mockdbTableGetDocuments = (table, ids = [], primaryKey = 'id') => {
  // eslint-disable-next-line security/detect-non-literal-regexp
  const idsRe = new RegExp(`^(${ids.join('|')})$`);

  return table.filter(doc => idsRe.test(doc[primaryKey]));
};

const mockdbTableRmDocument = (table, doc, primaryKey = 'id') => {
  if (!mockdbTableDocIsPrimaryKey(doc, primaryKey))
    return [ table ];

  const existingIndex = table
    .findIndex(d => d[primaryKey] === doc[primaryKey]);

  if (existingIndex > -1)
    table.splice(existingIndex, 1);

  return [ table ];
};

const mockdbTableDocEnsurePrimaryKey = (doc, primaryKey) => {
  if (!mockdbTableDocIsPrimaryKey(doc, primaryKey))
    doc[primaryKey] = randomUUID();

  return doc;
};

const mockdbTableSetDocument = (table, doc, primaryKey = 'id') => {
  [ table ] = mockdbTableRmDocument(table, doc, primaryKey);

  table.push(
    mockdbTableDocEnsurePrimaryKey(doc, primaryKey));

  return [ table, doc ];
};

const mockdbTableSetDocuments = (table, docs, primaryKey = 'id') => {
  docs = docs.map(doc => {
    [ table, doc ] = mockdbTableSetDocument(table, doc, primaryKey);

    return doc;
  });

  return [ table, docs ];
};

const mockdbTableRmDocumentsAll = table => {
  table.length = 0;

  return [ table ];
};

// set the entire table, replace existing documents
const mockdbTableSet = (table, docs) => {
  table.length = 0;

  docs.forEach(doc => table.push(doc));

  return [ table ];
};

const mockdbTableDocGetIndexValue = (doc, indexTuple, spend, qst, dbState, indexValueDefault) => {
  const [ indexName, spec ] = indexTuple;

  if (mockdbSpecIs(spec)) {
    indexValueDefault = spend(dbState, qst, spec, [ doc ]);
  } else if (Array.isArray(spec) && mockdbSpecIsSuspendNestedShallow(spec)) {
    indexValueDefault = spec.map(field => spend(dbState, qst, field, [ doc ]));
  } else {
    indexValueDefault = doc[indexName];
  }

  return indexValueDefault;
};

const mockdbTableDocHasIndexValueFn = (tableIndexTuple, indexValues, dbState) => {
  const targetIndexMulti = Boolean(tableIndexTuple[2].multi);
  // eslint-disable-next-line security/detect-non-literal-regexp
  const targetValueRe = targetIndexMulti || new RegExp(`^(${indexValues.join('|')})$`);
  const targetValueIs = valueResolved => targetValueRe.test(valueResolved);

  return (doc, spend, qst) => {
    const indexValueResolved = mockdbTableDocGetIndexValue(
      doc, tableIndexTuple, spend, qst, dbState);

    if (!targetIndexMulti)
      return targetValueIs(indexValueResolved);

    return indexValues.every(value => (
      Array.isArray(indexValueResolved)
        ? indexValueResolved.flat().includes(value)
        : indexValueResolved === value));
  };
};

export {
  mockdbTableGetDocument,
  mockdbTableGetDocuments,
  mockdbTableSetDocument,
  mockdbTableSetDocuments,
  mockdbTableRmDocument,
  mockdbTableRmDocumentsAll,
  mockdbTableDocGetIndexValue,
  mockdbTableDocEnsurePrimaryKey,
  mockdbTableDocIsPrimaryKey,
  mockdbTableDocHasIndexValueFn,
  mockdbTableSet
};
