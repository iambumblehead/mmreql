// rethinkdb response values are never 'undefined'
// remove 'undefined' definitions from object
const mockdbFilterUndefined = obj => Object.keys( obj )
    .reduce( ( filtered, key ) => (
        typeof obj[key] !== 'undefined'
            ? { [key]: obj[key], ...filtered }
            : filtered
    ), {});

const mockdbResChangesFieldCreate = opts => mockdbFilterUndefined({
    deleted: 0,
    errors: 0,
    inserted: 0,
    replaced: 0,
    skipped: 0,
    unchanged: 0,
    ...opts
});

const mockdbResStringify = obj => JSON.stringify( obj, null, '\t' );

const mockdbResErrorDuplicatePrimaryKey = ( existingDoc, conflictDoc ) => (
    'Duplicate primary key `id`:\n :existingDoc\n:conflictDoc'
        .replace( /:existingDoc/, mockdbResStringify( existingDoc ) )
        .replace( /:conflictDoc/, mockdbResStringify( conflictDoc ) ) );

export {
    mockdbResChangesFieldCreate,
    mockdbResStringify,
    mockdbResErrorDuplicatePrimaryKey
};
