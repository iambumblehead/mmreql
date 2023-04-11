import { randomUUID } from 'crypto';
import { mmEnumIsRowShallow } from './mmEnum.mjs';

const mmTableDocIsPrimaryKey = (doc, primaryKey) => Boolean(
  doc && /number|string/.test(typeof doc[primaryKey]));

const mmTableDocsGet = (table, ids = [], primaryKey = 'id') => {
  // eslint-disable-next-line security/detect-non-literal-regexp
  const idsRe = new RegExp(`^(${ids.join('|')})$`);

  return table.filter(doc => idsRe.test(doc[primaryKey]));
};

const mmTableDocsRm = (table, docs, primaryKey = 'id') => docs.map(doc => {
  if (!mmTableDocIsPrimaryKey(doc, primaryKey))
    return [ table ];

  const existingIndex = table
    .findIndex(d => d[primaryKey] === doc[primaryKey]);

  if (existingIndex > -1)
    table.splice(existingIndex, 1);

  return [ table ];
}) && [ table ];

const mmTableDocEnsurePrimaryKey = (doc, primaryKey) => {
  if (!mmTableDocIsPrimaryKey(doc, primaryKey))
    doc[primaryKey] = randomUUID();

  return doc;
};

const mmTableDocsSet = (table, docs, primaryKey = 'id') => {
  docs = docs.map(doc => {
    [ table ] = mmTableDocsRm(table, [ doc ], primaryKey);

    table.push(
      mmTableDocEnsurePrimaryKey(doc, primaryKey));

    return doc;
  });

  return [ table, docs ];
};

const mmTableDocsRmAll = table => {
  table.length = 0;

  return [ table ];
};

// set the entire table, replace existing documents
const mmTableSet = (table, docs) => {
  table.length = 0;

  docs.forEach(doc => table.push(doc));

  return [ table ];
};

const mmTableDocGetIndexValue = (doc, indexTuple, spend, qst, dbst, indexValueDefault) => {
  const [ indexName, spec ] = indexTuple;

  if (mmEnumIsRowShallow(spec)) {
    indexValueDefault = Array.isArray(spec)
      ? spec.map(field => spend(dbst, qst, field, [ doc ]))
      : spend(dbst, qst, spec, [ doc ]);
  } else {
    indexValueDefault = doc[indexName];
  }

  return indexValueDefault;
};

const mmTableDocHasIndexValueFn = (tableIndexTuple, indexValues, dbst) => {
  const targetIndexMulti = Boolean(tableIndexTuple[2].multi);
  // eslint-disable-next-line security/detect-non-literal-regexp
  const targetValueRe = targetIndexMulti || new RegExp(`^(${indexValues.join('|')})$`);
  const targetValueIs = valueResolved => targetValueRe.test(valueResolved);

  return (doc, spend, qst) => {
    const indexValueResolved = mmTableDocGetIndexValue(
      doc, tableIndexTuple, spend, qst, dbst);

    if (!targetIndexMulti)
      return targetValueIs(indexValueResolved);

    return indexValues.every(value => (
      Array.isArray(indexValueResolved)
        ? indexValueResolved.flat().includes(value)
        : indexValueResolved === value));
  };
};

export {
  mmTableDocsGet,
  mmTableDocsSet,
  mmTableDocsRm,
  mmTableDocsRmAll,
  mmTableDocGetIndexValue,
  mmTableDocEnsurePrimaryKey,
  mmTableDocIsPrimaryKey,
  mmTableDocHasIndexValueFn,
  mmTableSet
};
