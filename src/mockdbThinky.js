/* eslint-disable max-classes-per-file */
/* eslint-disable filenames/match-exported */
import casual from 'casual';
import createDebug from 'debug';
import { inspect } from 'util';
import { stub as baseStub, spy as baseSpy } from 'sinon';
import {
    isEqual,
    isPlainObject,
    isMatch,
    entries,
    flatten,
    orderBy,
    pick,
    isEqualWith,
    pipe,
    filter,
    fromPairs,
    cloneDeep,
    mapValues,
    map,
    merge,
    shuffle,
    groupBy,
    uniqBy
} from 'lodash/fp.js';

import {
    mockdbStateCreate,
    mockdbStateTableIndexExists
} from './mockdbState.js';

import {
    indexCreate,
    indexList,
    indexWait,
    insert
} from './mockdbTableQuery.js';

import rethinkDBMocked, {
    PseudoQuery,
    unwrap
} from './mockdbReql.js';

export const inspectStripSinon = obj => {
    if ( typeof obj !== 'object' )
        return obj;
    return mapValues( val => {
        if ( typeof val === 'function' && typeof val.callCount === 'number' )
            return val.id;
        if ( Array.isArray( val ) )
            return val.map( inspectStripSinon );
        if ( val && typeof val === 'object' ) {
            if ( val._sourceObject ) // eslint-disable-line no-underscore-dangle
                return val; // Inspect will catch this.
            return inspectStripSinon( val );
        }
        return val;
    }, obj );
};

const debug = createDebug( 'thinkyMock' );
const debugDetailed = createDebug( 'thinkyMock:detailed' );

const debugDir = ( val, options ) => {
    debugDetailed( inspect( val, { colors: true, depth: Infinity, ...options }) );
};

/* eslint-disable no-underscore-dangle */

class DocumentNotFound extends Error {
    constructor ( message = 'r.table("SomeTable")' ) {
        super( message );
        this.name = 'DocumentNotFound';
    }
}

const spies = [];

const stub = () => {
    const sinonStub = baseStub();
    spies.push( sinonStub );
    return sinonStub;
};

const spy = func => {
    const sinonSpy = baseSpy( func );
    spies.push( sinonSpy );
    return sinonSpy;
};

function proxyIgnore ( prop ) {
    return [
        'toFunction',
        '_getResults',
        'run',
        'runAndCheckErrors',
        'execute'
    ].includes( prop );
}

// thinky.type.number().default(3).min(2).max(4)
class Typedef {
    constructor ( typeName, funcs, options = {}) {
        const onAllTypes = {
            required: val => val !== undefined,
            optional: () => true,
            allowNull: ( val, [ allow ]) => val !== null || allow,
            validator: ( val, [ validator ]) => validator.call( val, val ),
            default: () => true
        };

        this.validators = [];
        this.type = typeName;
        this.options = options;

        Object.entries({ ...funcs, ...onAllTypes }).forEach( ([ funcName, validator ]) => {
            this[funcName] = ( ...args ) => {
                this[`_${funcName}`] = args.length < 2 ? args[0] : args;
                const check = val => {
                    if ( !validator( val, args, this ) )
                        throw new Error( `Validator "${funcName}" failed for type ${typeName}, value was ${inspect( val, { depth: 0 })}` );
                };
                this[`_${funcName}Check`] = check;
                this.validators.push({ name: funcName, func: validator, args, check });
                return this;
            };
        });

        if ( this._check && !options.ignoreTypeCheck )
            this._check(); // Add type check

        if ( funcs._init )
            funcs._init( this );
    }

    _checkType ( val ) {
        if ( val === undefined ) {
            if ( this._required )
                this._requiredCheck();
            return;
        }

        if ( val === null ) {
            if ( this._allowNull )
                this._allowNullCheck();
            return;
        }

        this.validators.forEach( ({ check }) => {
            check( val );
        });
    }
}

const mockdb = mockdbStateCreate();

const types = {
    string: {
        _check: val => typeof val === 'string',
        min: ( val, [ min ]) => val.length >= min,
        max: ( val, [ max ]) => val.length <= max,
        length: ( val, [ length ]) => val.length === length,
        alphanum: val => /^[a-zA-Z0-9]+$/.test( val ),
        regex: ( val, [ re ]) => re.test( val ),
        lowercase: val => val.toLowerCase() === val,
        uppercase: val => val.toUpperCase() === val,
        enum: ( val, [ validValues ]) => validValues.includes( val ),
        uuid: val => /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test( val )
    },
    boolean: {
        _check: val => typeof val === 'boolean'
    },
    number: {
        _check: val => typeof val === 'number',
        min: ( val, [ min ]) => val >= min,
        max: ( val, [ max ]) => val <= max,
        integer: val => Number.isInteger( val )
    },
    date: {
        _check: val => typeof val === 'number'
            || ( typeof val === 'function' && val.$mustInterpret )
            || (
                val instanceof Date
                && typeof val.valueOf() === 'number' // on-change error check
                && Object.prototype.toString.call( val ) === '[object Date]' // on-change error check
            ),
        min: ( val, [ min ]) => val.valueOf() >= min.valueOf(),
        max: ( val, [ max ]) => val.valueOf() <= max.valueOf()
    },
    buffer: { _check: val => Buffer.isBuffer( val ) },
    object: {
        _check: val => isPlainObject( val ),
        _init: typedef => {
            // This is how thinky works :/
            typedef._schema = {};
        },
        schema: ( val, [ schema ]) => {
            Object.entries( schema ).forEach( ([ key, typedef ]) => {
                if ( isPlainObject( typedef ) )
                    typedef = new Typedef( 'object', types.object ).schema( typedef );
                try {
                    typedef._checkType( val[key]);
                } catch ( err ) {
                    throw new Error( `Schema validation failed for ${key}: ${err.message}` );
                }
            });
            return true; // Will throw
        }
    },
    array: {
        _check: val => Array.isArray( val ),
        _init: typedef => {
            typedef._schema = new Typedef( 'any', types.any );
        },
        schema: ( val, [ schema ]) => {
            val.forEach( ( inner, i ) => {
                try {
                    schema._checkType( inner );
                } catch ( err ) {
                    throw new Error( `Schema validation failed for element ${i}: ${err.message}` );
                }
            });
            return true; // Will throw
        }
    },
    point: {},
    virtual: {},
    any: {}
};

const thinkyType = Object.entries( types ).reduce( ( obj, [ typeName, funcs ]) => Object.assign( obj, {
    [typeName]: options => new Typedef( typeName, funcs, options )
}), {});

const getTypedefInstance = typedef => typedef && typedef.type;

// eslint-disable-next-line camelcase
const getTypedef_default = ( typedef, def = null ) => (
    typedef && typedef._default ) || def;

// eslint-disable-next-line camelcase
const getTypedef_schema = ( typedef, def = null ) => (
    typedef && typedef._schema ) || def;

Object.assign( thinkyType, {
    isVirtual: typedefInstance => getTypedefInstance( typedefInstance ) === 'virtual',
    isArray: typedefInstance => getTypedefInstance( typedefInstance ) === 'array',
    isObject: typedefInstance => getTypedefInstance( typedefInstance ) === 'object',
    isDate: typedefInstance => getTypedefInstance( typedefInstance ) === 'date'
});

let idCounter = 0;

// eslint-disable-next-line no-unused-vars
const createFunctionQuery = ( model, options ) => {
    const query = createQuery( model, options );

    function functionQuery ( /* val */ ) {
        // const p = unwrap( val );
        throw new Error( 'not yet implemented' );
        // return createQuery( model, {
        //    ...options,
        //    _filters: [
        //        ...query._filters,
        //        // { method: prop, args: val }
        //        { args: val }
        //    ]
        // });
    }

    Object.keys( query ).reduce( ( prev, key ) => {
        prev[key] = query[key];

        return prev;
    }, functionQuery );

    return functionQuery;
};

function createQuery ( model, options = {}) {
    const id = idCounter;
    idCounter += 1;
    const query = new Proxy({
        _filters: options._filters || [],
        id
    }, {
        get ( target, prop, receiver ) {
            const existing = Reflect.get( target, prop, receiver );
            if ( existing !== undefined )
                return existing;

            if ( proxyIgnore( prop ) ) return undefined;

            return ( ...args ) => {
                if ( args.some( a => a === undefined ) )
                    throw new Error( `Cannot call ${prop} with an argument of 'undefined'.` );

                // return createFunctionQuery( model, {
                return createQuery( model, {
                    ...options,
                    _filters: [
                        ...query._filters,
                        { method: prop, args }
                    ]
                });
            };
        },
        set ( target, prop, value, receiver ) {
            return Reflect.set( target, prop, value, receiver );
        }
    });

    const {
        thinky = true
    } = options;

    query.toString = () => query._filters.reduce( ( str, { method, args }) => {
        if ( method === 'context' )
            return str;

        function stringifyArg ( arg, depth = 0 ) {
            if ( depth > 30 )
                return 'deep';

            if ( arg === null )
                return 'null';

            if ( arg === undefined )
                return 'undefined';

            if ( arg._getResults )
                return arg.toString();

            if ( typeof arg === 'function' && arg.name !== 'bound row' ) {
                try {
                    const argResult = arg( new PseudoQuery() );
                    if ( isPlainObject( argResult ) )
                        return `obj => (${stringifyArg( argResult )})`;
                    return `obj => ${argResult.toString()}`;
                } catch ( err ) {
                    try {
                        const innerSequence = arg( createQuery( model._modelStore.$$null ) ).toString();
                        return innerSequence.replace( /\[Query \d+\]: r\.table\('\$\$null'\)/, 'seq => seq' ); // Simulate a sequence
                    } catch ( err2 ) {
                        try {
                            return arg.toString();
                        } catch ( err3 ) {
                            throw new Error( `Couldn't stringify ${arg}: ${err.message}, then ${err2.message}\n First Try: ${err.stack} Second Try: \n ${err2.stack}` );
                        }
                    }
                }
            }

            if ( typeof arg === 'string' )
                return `'${arg}'`;

            if ( isPlainObject( arg ) ) {
                const values = Object.entries( mapValues( v => stringifyArg( v, depth + 1 ), arg ) ).map( ([ key, value ]) => `"${key}": ${value}` ).join( ', ' );
                return values.length ? `{ ${values} }` : '{}';
            }

            if ( Array.isArray( arg ) ) {
                const values = arg.map( a => ( a._values ? a.toString() : stringifyArg( a, depth + 1 ) ) ).join( ', ' );
                return values.length ? `[ ${values} ]` : '[]';
            }

            return arg.toString();
        }

        // Flatten r.args
        const flattenedArgs = args.reduce( ( arr, item ) => {
            if ( isPlainObject( item ) && item.args )
                return arr.concat( item.args );
            return [ ...arr, item ];
        }, []);

        const argsStr = ( flattenedArgs || []).map( a => stringifyArg( a ) ).join( ', ' );

        return `${str}.${method}(${argsStr})`;
    }, `[Query ${query.id}]: r${model.getTableName() === '$$null' ? '' : `.table(${`'${model.getTableName()}'`})`}` );

    query.thinky = thinky;

    model._staticMethods.forEach( method => { query[method] = stub().returns( query ); });

    query._getResults = ({ wrap, thinky: resultsThinky = thinky }) => {
        if ( debugDetailed.enabled ) {
            debugDetailed( 'new query ----------------' );
            debugDetailed( query.toString() );
        }

        const storedData = model._modelStore._data;
        const { r } = model._modelStore;
        const table = storedData && storedData[model.getTableName()];

        if ( !table )
            throw new Error( `No data for ${model.getTableName()}` );

        const rethinkMap = ( data, func ) => {
            // .map( 'someProperty' )
            if ( typeof func === 'string' ) {
                const property = func;
                func = obj => obj( property );
            }

            if ( Array.isArray( data ) )
                return data.map( item => rethinkMap( item, func ) );

            return unwrap( func, data );
        };

        if ( table ) { // Try to find it in mocked table.
            let filteredData = table.map( cloneDeep );
            let isSingle = false;
            let isGrouped = false;

            const runOneFilter = ( oneFilter, data ) => {
                const { args, method } = oneFilter;

                switch ( method ) {
                case 'get': {
                    const pk = unwrap( args[0]);
                    const obj = data.find( item => item[model._pk] === pk );
                    if ( !obj && resultsThinky )
                        throw new DocumentNotFound();
                    return {
                        data: [ obj ],
                        isSingle: true
                    };
                }
                case 'getAll': {
                    let index = model._pk;
                    let values = Array.from( args );

                    // Last argument can be { index: 'some-index' } to override default of the primary key
                    if ( typeof values[values.length - 1] === 'object' && values[values.length - 1].index )
                        ({ index } = values.pop() );

                    // r.args() mocked return { args: [ the-passed-in-args ] } so expand it
                    values = values.reduce(
                        ( arr, val ) => ( val.args ? arr.concat( val.args ) : [ ...arr, val ]),
                        []);

                    data = data.filter( item => {
                        const itemValues = values.map( a => unwrap( a, item ) );

                        if ( !model._indexes[index] && !mockdbStateTableIndexExists( mockdb, model.getTableName(), index ) ) {
                            throw new Error( `There is no index ${index} for table ${model._tableName}` );
                        }

                        if ( args[1] && args[1].index && mockdbStateTableIndexExists( mockdb, model.getTableName(), index ) ) {
                            const tableDocs = model._modelStore._data[model.getTableName()];
                            return {
                                data: tableDocs.filter( doc => doc[args[1].index] === args[0]),
                                wrap: true,
                                isSingle: false
                            };
                        }

                        if ( model._indexes[index].multi ) {
                            // Multi-index
                            return itemValues.some( value => {
                                const indexValue = model._getIndexValue( item, index );
                                return indexValue.some( isEqualWith(
                                    ( a, b ) => {
                                        if ( unwrap( a, item ) === unwrap( b, item ) )
                                            return true;
                                        return undefined;
                                    },
                                    value
                                ) );
                            });
                        }

                        // May be r.getAll([ r.row( 'a' ), 'some-fixed-value' ], { index: 'a-compound-index' })
                        return itemValues.some( value => isEqualWith(
                            ( a, b ) => {
                                if ( unwrap( a, item ) === unwrap( b, item ) )
                                    return true;
                                return undefined;
                            },
                            value,
                            model._getIndexValue( item, index )
                        ) );
                    });

                    // when using getAll, the input array does not map to the output array,
                    // so shuffle so that we can be sure we're handling this correctly!
                    return shuffle( data );
                }
                case 'filter': {
                    const [ predicate ] = args;
                    return data.filter( item => {
                        const itemPredicate = unwrap( predicate, item );
                        if ( isPlainObject( itemPredicate ) )
                            return isMatch( itemPredicate, item );
                        return itemPredicate;
                    });
                }
                case 'slice': {
                    let [ begin, end ] = args;
                    begin = unwrap( begin );
                    end = unwrap( end );
                    return data.slice( begin, end ); // Don't support changing bounds yet
                }
                case 'skip': {
                    const count = unwrap( args[0]);
                    return data.slice( count );
                }
                case 'merge': {
                    return data.map( item => {
                        const merges = args.map( arg => unwrap( arg, item ) );
                        return Object.assign({}, item, ...merges );
                    });
                }
                case 'count': {
                    return {
                        isSingle: true,
                        wrap: false,
                        data: [ data.length ]
                    };
                }
                case 'pluck': {
                    const props = flatten( args.map( unwrap ).map( arg => arg.args || arg ) );
                    return data.map( pick( props ) );
                }
                case 'append': {
                    // const props = flatten( args.map( unwrap ).map( arg => arg.args || arg ) );

                    throw new Error( 'not yet implemented' );
                    // return {
                    //     data: [ table[args[0]] ],
                    //     isSingle: true,
                    //     wrap: false
                    // };
                }
                case 'getJoin': {
                    const [ joins ] = args;
                    entries( joins ).forEach( ([ join, joinValue ]) => {
                        // model._joins[fieldName] = { model, leftKey, rightKey, belongs: true, many: true };
                        const joinInfo = model._joins[join];
                        if ( !joinInfo ) return;

                        data.forEach( item => {
                            if ( joinInfo.many && joinInfo.belongs ) { // hasAndBelongsToMany
                                const otherIds = []
                                    .concat( item[`$${joinInfo.model.getTableName()}_ids`] || [])
                                    .concat( createQuery( joinInfo.joinModel )
                                        .filter({ [`${model._tableName}_${model._pk}`]: item[model._pk] })
                                        .map( row => row( `${joinInfo.model._tableName}_${joinInfo.model._pk}` ) )
                                        ._getResults({ wrap: false }) );

                                if ( !otherIds.length )
                                    item[join] = [];

                                let innerQuery = createQuery( joinInfo.model )
                                    .filter( other => r.expr( otherIds ).contains( other( joinInfo.rightKey ) ) );

                                if ( typeof joinValue === 'object' )
                                    innerQuery = innerQuery.getJoin( joinValue );

                                if ( typeof joinValue._apply === 'function' )
                                    innerQuery = joinValue._apply( innerQuery );

                                item[join] = innerQuery._getResults({ wrap });
                            } else if ( joinInfo.many ) { // hasMany
                                let innerQuery = createQuery( joinInfo.model )
                                    .filter({ [joinInfo.rightKey]: item[joinInfo.leftKey] });

                                if ( typeof joinValue === 'object' )
                                    innerQuery = innerQuery.getJoin( joinValue );

                                if ( typeof joinValue._apply === 'function' )
                                    innerQuery = joinValue._apply( innerQuery );

                                item[join] = innerQuery._getResults({ wrap });
                            } else if ( joinInfo.belongs ) { // belongsTo
                                let innerQuery = createQuery( joinInfo.model )
                                    .filter({ [joinInfo.rightKey]: item[joinInfo.leftKey] });

                                if ( typeof joinValue === 'object' )
                                    innerQuery = innerQuery.getJoin( joinValue );

                                if ( typeof joinValue._apply === 'function' )
                                    innerQuery = joinValue._apply( innerQuery );

                                item[join] = innerQuery._getResults({ wrap })[0] || null;
                            } else { // hasOne
                                let innerQuery = createQuery( joinInfo.model )
                                    .filter({ [joinInfo.rightKey]: item[joinInfo.leftKey] });

                                if ( typeof joinValue === 'object' )
                                    innerQuery = innerQuery.getJoin( joinValue );

                                if ( typeof joinValue._apply === 'function' )
                                    innerQuery = joinValue._apply( innerQuery );

                                item[join] = innerQuery._getResults({ wrap })[0] || null;
                            }
                        });
                    });
                    return data;
                }
                case 'eqJoin': {
                    wrap = false;

                    let [ leftKey, otherSequence ] = args; // eslint-disable-line prefer-const
                    leftKey = unwrap( leftKey );

                    const otherData = otherSequence._getResults({ wrap });
                    return data.map( item => ({
                        left: item,
                        right: otherData.find( other => other.id === item[leftKey])
                    }) ).filter( ({ right }) => right );
                }
                case 'concatMap': {
                    const [ func ] = args;
                    const mapped = rethinkMap( data, func );
                    const flattened = flatten( mapped );
                    return flattened.map( unwrap );
                }
                case 'add': {
                    const values = args.map( unwrap );
                    // Only array supported right now
                    return [ ...data, ...values ];
                }
                case 'orderBy': {
                    let [ arg ] = args;
                    if ( !isPlainObject( arg ) )
                        arg = { sortBy: arg, sortDirection: 'asc' };

                    const { sortBy, sortDirection } = arg;

                    return orderBy( obj => {
                        const val = typeof sortBy === 'function' && !sortBy.toFunction
                            ? unwrap( sortBy, obj )
                            : obj[unwrap( sortBy, obj )];

                        return val instanceof Date ? val.valueOf() : val;
                    }, sortDirection, data );
                }
                case 'limit': {
                    const limit = unwrap( args[0]);
                    return data.slice( 0, limit );
                }
                case 'coerceTo':
                    return data; // We don't care
                case 'map': {
                    const [ func ] = args;
                    return rethinkMap( data, func );
                }
                case 'hasFields': {
                    return data.filter( item => {
                        if ( !item ) return false;
                        return args.every( name => Object.prototype.hasOwnProperty.call( item, name ) );
                    });
                }
                case 'innerJoin': {
                    // Nested loop inner join
                    const [ otherSequence, joinFunc ] = args;
                    const otherItems = unwrap( otherSequence );

                    return flatten( data.map( item => otherItems.map( otherItem => {
                        const resultFunc = joinFunc( new PseudoQuery( item ), new PseudoQuery( otherItem ) );
                        const match = resultFunc.toFunction()();
                        return match ? {
                            left: item,
                            right: otherItem
                        } : null;
                    }) ).filter( x => x !== null ) );
                }
                case 'delete': {
                    let deleted = 0;
                    data.forEach( doc => {
                        const index = table.findIndex( item => item[model._pk] === doc[model._pk] || isEqual( item, doc ) );
                        if ( index !== -1 ) {
                            table.splice( index, 1 );
                            deleted += 1;
                        }
                    });

                    return {
                        data: [ { deleted } ],
                        isSingle: true,
                        wrap: false
                    };
                }
                case 'nth': {
                    if ( args[0] >= table.length )
                        throw new Error( `ReqlNonExistanceError: Index out of bounds: ${args[0]}` );

                    return {
                        data: [ table[args[0]] ],
                        isSingle: true,
                        wrap: false
                    };
                }
                case 'insert': {
                    return insert( mockdb, model.getTableName(), args, table );
                }

                case 'contains': {
                    if ( !args.length ) throw new Error( 'Rethink supports contains(0) but rethinkdbdash does not.' );

                    return {
                        data: [ args.every( predicate => {
                            // A PseudoQuery argument is treated as a scalar argument,
                            // and a direct function argument is treated as a predicate
                            if ( typeof predicate === 'function' && !predicate.toFunction )
                                return data.some( item => unwrap( predicate, item ) );
                            return data.includes( unwrap( predicate ) );
                        }) ],
                        isSingle: true,
                        wrap: false
                    };
                }
                case 'not': {
                    if ( isSingle && typeof data[0] === 'boolean' )
                        return [ !data[0] ];
                    throw new Error( 'Cannot call not() on non-boolean value.' );
                }
                case 'gt': {
                    if ( !isSingle ) throw new Error( 'Cannot gt on sequence.' );
                    let [ val ] = args;
                    val = unwrap( val, data[0]);
                    return [ data[0] > val ];
                }
                case 'lt': {
                    if ( !isSingle ) throw new Error( 'Cannot lt on sequence.' );
                    let [ val ] = args;
                    val = unwrap( val, data[0]);
                    return [ data[0] < val ];
                }
                case 'eq': {
                    if ( !isSingle ) throw new Error( 'Cannot eq on sequence.' );
                    let [ val ] = args;
                    val = unwrap( val, data[0]);
                    return [ data[0] === val ];
                }
                case 'ne': {
                    if ( !isSingle ) throw new Error( 'Cannot ne on sequence.' );
                    let [ val ] = args;
                    val = unwrap( val, data[0]);
                    return [ data[0] !== val ];
                }
                case 'expr': {
                    wrap = false;
                    const [ value ] = args;
                    const resolved = unwrap( value );
                    if ( Array.isArray( resolved ) ) {
                        return { isSingle: false, data: resolved };
                    }
                    return { isSingle: true, data: [ resolved ] };
                }
                case 'union': {
                    return args.reduce( ( argData, value ) => {
                        value = unwrap( value );
                        return argData.concat( value );
                    }, data );
                }
                case 'context':
                    return data;
                case 'update': {
                    const [ setData ] = args;
                    let replaced = 0;
                    for ( const item of data.filter( i => !!i[model._pk]) ) {
                        const storedItem = storedData.find( s => s[model._pk] === item[model._pk]);
                        if ( storedItem ) {
                            Object.assign( storedItem, merge( storedItem, setData ) );
                            replaced += 1;
                        }
                    }
                    return {
                        isSingle: true,
                        wrap: false,
                        data: [ {
                            replaced,
                            unchanged: 0,
                            skipped: 0,
                            errors: 0
                        } ]
                    };
                }
                case 'isEmpty': {
                    return {
                        data: [ data.length === 0 ],
                        isSingle: true,
                        wrap: false
                    };
                }
                case 'hours': {
                    return {
                        data: new Date( data[0]).getHours(),
                        isSingle: true,
                        wrap: false
                    };
                }
                case 'minutes': {
                    return {
                        data: new Date( data[0]).getMinutes(),
                        isSingle: true,
                        wrap: false
                    };
                }
                case 'group': {
                    const [ func ] = args;
                    const groupedData = groupBy( item => rethinkMap( item, func ), data );
                    const rethinkFormat = Object.entries( groupedData )
                        .map( ([ group, reduction ]) => ({ group, reduction }) );
                    return {
                        isGrouped: true,
                        data: rethinkFormat
                    };
                }
                case 'ungroup':
                    return { isGrouped: false, data };
                case 'or':
                    return {
                        data: [ args.reduce( ( current, value ) => !!( current || unwrap( value ) ), !!data[0]) ],
                        isSingle: true
                    };
                case 'and':
                    return {
                        data: [ args.reduce( ( current, value ) => !!( current && unwrap( value ) ), !!data[0]) ],
                        isSingle: true
                    };
                case 'distinct':
                    // Rethink has its own alg for finding distinct, but unique by ID should be sufficient here.
                    return uniqBy( 'id', data );

                case 'default':
                    return {
                        data: [ args.reduce( ( current, value ) => (
                            typeof current === 'undefined' ? unwrap( value ) : current
                        ), data[0]) ],
                        isSingle: true
                    };

                case 'indexCreate':
                    return indexCreate( mockdb, model.getTableName(), args );

                case 'indexWait':
                    return indexWait( mockdb, model.getTableName(), args );

                case 'indexList':
                    return indexList( mockdb, model.getTableName(), args );

                default:
                    console.warn( `Unimplemented rethink method ${method} - query results are likely inaccurate.` );
                    return data;
                }
            };

            for ( const oneFilter of query._filters ) {
                debugDetailed( '-------------------' );
                debugDetailed( `enter: ${oneFilter.method}` );

                /* eslint-disable no-loop-func */
                const checkToggles = results => {
                    if ( Array.isArray( results ) )
                        return;

                    if ( results.isSingle !== undefined )
                        ({ isSingle } = results );

                    if ( results.wrap !== undefined )
                        ({ wrap } = results );

                    if ( results.isGrouped !== undefined )
                        ({ isGrouped } = results );
                };
                /* eslint-enable no-loop-func */

                if ( isGrouped ) { // If we're grouped, then subsequent commands run once for each group
                    const results = filteredData.map( group => ({
                        group: group.group,
                        result: runOneFilter( oneFilter, group.reduction )
                    }) );

                    for ( const { result } of results ) {
                        checkToggles( result );
                    }

                    filteredData = results.map( ({ group, result }) => ({
                        group,
                        reduction: Array.isArray( result ) ? result : result.data
                    }) );
                } else {
                    const filterResults = runOneFilter( oneFilter, filteredData );
                    checkToggles( filterResults );
                    filteredData = Array.isArray( filterResults ) ? filterResults : filterResults.data;
                }

                debugDetailed( `results: ${oneFilter.method}` );
                debugDir( filteredData );
            }

            if ( debug.enabled )
                debug( `Executed ${query.toString()} - ${filteredData.length} result${filteredData.length === 1 ? '' : 's'}.` );

            model._addFinishedQuery({ query, data: [ ...filteredData ] });

            if ( wrap && resultsThinky ) {
                const make = data => {
                    const instance = model._parseSync( data );

                    model._callPostHook( 'retrieve', instance );

                    return instance;
                };

                return isSingle ? make( filteredData[0]) : filteredData.map( make );
            }
            return isSingle ? filteredData[0] : filteredData;
        }

        // Fall back to mocking

        // Find a return value with filters that all match the filters for any query.
        const result = model._returnValues
            .find( returnInfo => !returnInfo.filters || returnInfo.filters.every( testFilter => {
                // Some filters (like `filter`) can be applied multiple times - so just test that one of them matches
                const appFilters = query._filters.filter( f => f.method === testFilter.method );
                return appFilters.some( appFilter => appFilter && testFilter.args.every( ( testArg, i ) => {
                    const appArg = appFilter.args[i];
                    return typeof testArg === 'function'
                        ? testArg( appFilter.args[i]) // If the test filter is a function, check the arg against the function
                        : isEqual( testArg, appArg ); // Otherwise test that the app filter and the test filter are deep equal
                }) );
            }) );

        model._addFinishedQuery({ query });

        if ( !result )
            return model._defaultReturnValue();

        if ( result.returnValue === null && thinky )
            throw new DocumentNotFound();

        let { returnValue } = result;

        if ( result.options ) {
            if ( result.options.after && typeof returnValue === 'function' ) {
                returnValue = returnValue();
            }

            if ( result.options.wrap ) {
                if ( Array.isArray( returnValue ) ) {
                    // eslint-disable-next-line new-cap
                    returnValue = returnValue.map( val => new model( val ) );
                } else {
                    // eslint-disable-next-line new-cap
                    returnValue = new model( returnValue );
                }
            }
        }

        return returnValue;
    };

    if ( query.thinky ) {
        query.execute = async () => query._getResults({ wrap: false });
    }

    // query.delete = async ( ...args ) => {
    //     not supported
    // };

    query.run = async () => {
        if ( query.thinky ) {
            const lastFilter = query._filters && query._filters[query._filters.length - 1];
            if ( lastFilter && lastFilter.method === 'delete' )
                throw new Error( 'You are calling .run() after .delete() in a thinky query. This will throw a ValidationError.' );
        }

        return query._getResults({ wrap: query.thinky });
    };
    query.runAndCheckErrors = query.run;

    query.runOrNull = async ( ...args ) => { // models/thinky.js
        try {
            return await query.run( ...args );
        } catch ( err ) {
            if ( err instanceof DocumentNotFound )
                return null;
            throw err;
        }
    };

    // eslint-disable-next-line promise/prefer-await-to-then
    query.then = ( ...args ) => query.run().then( ...args );

    // In thinky, this gets the raw reql query that it's building,
    // here we just return itself so we can execute it.
    query._query = query;

    return query;
}

const documentMethods = ( instance, model ) => {
    const chain = () => Promise.resolve( instance );
    const methods = {
        getModel: () => model,
        merge: obj => Object.assign( instance, obj ),
        validate: () => {},
        validateAll: () => {},
        isSaved: () => instance.__isSaved,
        setSaved: ( saved = true ) => {
            Object.defineProperty( instance, '__isSaved', { value: saved, enumerable: false });
        },
        getOldValue: () => {
            if ( !instance._isSaved )
                return null;
            return instance._sourceObject;
        },
        getFeed: () => spy()
    };

    [ 'delete', 'deleteAll', 'purge' ].forEach( method => {
        methods[method] = async () => {
            // Remove the document from the model store's data.
            // Make sure you're using one-modelStore-per-test
            const tableData = model._modelStore._data && model._modelStore._data[model._tableName];
            const matchIndex = tableData.findIndex( item => item[model._pk] === instance[model._pk] || isEqual( item, instance ) );

            if ( matchIndex === -1 )
                throw new Error( 'Tried to delete a document that is not in the database.' );

            tableData.splice( matchIndex, 1 );

            instance._isSaved = false;
            return instance;
        };
    });

    methods.addRelation = spy( async ( propertyName, otherInstance ) => {
        const { joinTableName } = model._joins[propertyName];

        if ( !model._modelStore._data[joinTableName])
            model._modelStore._data[joinTableName] = [];

        const existing = model._modelStore._data[joinTableName].find( row => (
            row[`${model._tableName}_${model._pk}`] === instance[model._pk]
            && row[`${otherInstance.getModel()._tableName}_${otherInstance.getModel()._pk}`] === otherInstance[otherInstance.getModel()._pk]
        ) );

        if ( !existing ) {
            model._modelStore._data[joinTableName].push({
                id: casual.uuid,
                [`${model._tableName}_${model._pk}`]: instance[model._pk],
                [`${otherInstance.getModel()._tableName}_${otherInstance.getModel()._pk}`]: otherInstance[otherInstance.getModel()._pk]
            });
        }
    });

    methods.removeRelation = spy( async ( propertyName, otherInstance ) => {
        const { joinTableName } = model._joins[propertyName];
        const index = model._modelStore._data[joinTableName].findIndex( row => (
            row[`${model._tableName}_${model._pk}`] === instance[model._pk]
            && row[`${otherInstance.getModel()._tableName}_${otherInstance.getModel()._pk}`] === otherInstance[otherInstance.getModel()._pk]
        ) );

        if ( index >= 0 )
            model._modelStore._data[joinTableName].splice( index, 1 );
    });

    [ 'closeFeed', 'on' ].forEach( method => {
        methods[method] = spy( chain );
    });

    [ 'save', 'saveAll' ].forEach( method => {
        methods[method] = spy( async ( ...args ) => {
            await model._callAsyncPreHook( 'save', instance );

            model._checkSchema( instance );
            model._interpretValues( instance );

            const data = instance.$toPlainObject();

            const rows = model._modelStore._data[model.getTableName()];
            if ( !rows )
                throw new Error( `Missing data for ${model.getTableName()}` );

            const existing = rows.find( row => row[model._pk] === data[model._pk]);

            if ( existing ) {
                Object.assign( existing, data );
            } else {
                rows.push( data );
            }
            instance.setSaved( true );

            // Feed any computed values back into the instance, for example id
            Object.entries( data ).forEach( ([ key, value ]) => {
                const { writable } = Object.getOwnPropertyDescriptor( instance, key );
                if ( writable && typeof value !== 'function' ) {
                    instance[key] = value;
                }
            });

            if ( method === 'saveAll' ) {
                const saveJoins = args[0];
                if ( saveJoins ) {
                    for ( const [ join ] of Object.entries( saveJoins ) ) {
                        // model._joins[fieldName] = { model, leftKey, rightKey, belongs: true, many: true };
                        const joinInfo = model._joins[join];
                        let otherObj = instance[join];

                        const resolveOtherObj = o => {
                            if ( !o.save ) {
                                if ( !o[joinInfo.model._pk])
                                    throw new Error( 'saveAll with unknown obj' );
                                o = model._modelStore._data[joinInfo.model.getTableName()].find( row => row[joinInfo.model._pk] === o[joinInfo.model._pk]);

                                if ( !o )
                                    throw new Error( `Could not find ${joinInfo.model.getTableName()} ${o[joinInfo.model._pk]}` );
                            }

                            return o;
                        };

                        if ( Array.isArray( otherObj ) )
                            otherObj = otherObj.map( resolveOtherObj );
                        else
                            otherObj = resolveOtherObj( otherObj );

                        if ( joinInfo.many && joinInfo.belongs ) {
                            for ( const other of otherObj ) {
                                // ********* Old method *********
                                if ( !Array.isArray( instance[`$${joinInfo.model.getTableName()}_ids`]) ) {
                                    if ( typeof instance[`$${joinInfo.model.getTableName()}_ids`] === 'string' ) {
                                        instance[`$${joinInfo.model.getTableName()}_ids`] = [
                                            instance[`$${joinInfo.model.getTableName()}_ids`]
                                        ];
                                    } else {
                                        instance[`$${joinInfo.model.getTableName()}_ids`] = [];
                                    }
                                }
                                instance[`$${joinInfo.model.getTableName()}_ids`].push( other[joinInfo.model._pk]);

                                if ( !other[`$${model.getTableName()}_ids`]) {
                                    other[`$${model.getTableName()}_ids`] = [];
                                }
                                other[`$${model.getTableName()}_ids`].push( instance[model._pk]);
                                // ********* End old method *********

                                await instance.save();
                                await other.save();
                            }
                            // ********* Correct method *********
                            const { joinTableName } = model._joins[join];

                            if ( !model._modelStore._data[joinTableName])
                                model._modelStore._data[joinTableName] = [];

                            const joinsForOtherObjs = model._modelStore._data[joinTableName].filter( obj => obj[`${model._tableName}_${model._pk}`] !== instance[model._pk]);
                            const newJoinsList = otherObj.map( other => ({
                                id: casual.uuid,
                                [`${model._tableName}_${model._pk}`]: instance[model._pk],
                                [`${joinInfo.model._tableName}_${joinInfo.model._pk}`]: other[joinInfo.model._pk]
                            }) );

                            model._modelStore._data[joinTableName].splice( 0, model._modelStore._data[joinTableName].length, ...joinsForOtherObjs, ...newJoinsList );
                            // ********* End correct method *********
                        } else if ( joinInfo.many ) { // hasMany
                            for ( const other of otherObj ) {
                                other[joinInfo.rightKey] = instance[model._pk];
                                await other.save();
                            }
                        } else if ( joinInfo.belongs ) { // belongsTo or hasOne
                            instance[joinInfo.leftKey] = otherObj[joinInfo.model._pk];
                            await instance.save();
                        } else { // hasOne
                            otherObj[joinInfo.rightKey] = instance[model._pk];
                            await otherObj.save();
                        }
                    }
                }
            }

            await model._callAsyncPostHook( 'save', instance );
            return instance;
        });
    });

    Object.keys( model._instanceOverrides ).forEach( method => { methods[method] = model._instanceOverrides[method]; });

    const spiedMethods = Object.keys( methods ).reduce( ( o, k ) => {
        o[k] = spy( methods[k]);
        return o;
    }, {});

    const $toPlainObject = () => pipe(
        entries,
        map( ([ key, value ]) => {
            if ( value && value.toFunction ) // e.g. r.now() returns a PseudoQuery
                return [ key, unwrap( value ) ];
            return [ key, value ];
        }),
        filter( ([ key, value ]) => key.startsWith( '_' ) === false && typeof value !== 'function' ),
        fromPairs
    )( instance );

    return {
        ...spiedMethods,
        $toPlainObject,
        ...model._documentMethods.reduce( ( obj, { name, func }) => Object.assign( obj, { [name]: func.bind( instance ) }), {})
    };
};

// Generate virtual functions from the schema.
function virtuals ( instance, schema ) {
    entries( schema ).forEach( ([ field, typedef ]) => {
        if ( !typedef ) {
            throw new Error( 'Missing type for schema field?' );
        }

        if ( typedef.type === 'virtual' ) {
            Object.defineProperty( instance, field, {
                get: () => {
                    const typedefDefault = getTypedef_default( typedef );

                    if ( typedefDefault && typedefDefault.toFunction )
                        return typedef._default.toFunction()();

                    if ( typeof typedefDefault === 'function' )
                        return typedefDefault.call( instance );

                    return typedef._default;
                },
                configurable: true
            });
        }

        const isObjectField = instance[field] && typeof instance[field] === 'object' && typedef.type === 'object';
        const typedefSchema = getTypedef_schema( typedef );
        if ( typedefSchema && typedefSchema._schema && isObjectField ) {
            virtuals( instance[field], typedef._schema._schema );
        }

        const isArrayField = instance[field] && Array.isArray( instance[field])
              && typedef.type === 'array'
              && typedefSchema && typedefSchema.type === 'object';

        if ( typedefSchema && typedefSchema._schema && isArrayField ) {
            instance[field].forEach( item => {
                virtuals( item, typedef._schema._schema );
            });
        }
    });
}

// Set defaults values from the schema
function defaults ( instance, schema ) {
    entries( schema ).forEach( ([ field, typedef ]) => {
        if ( typedef.type === 'virtual' )
            return;

        const typedefDefault = getTypedef_default( typedef );
        if ( typedefDefault !== undefined && instance[field] === undefined ) {
            if ( typedefDefault && typedefDefault.toFunction ) {
                instance[field] = typedef._default.toFunction()();

                if ( typedef._default.toString().includes( '{now}' ) ) {
                    const dateValue = typedef._default.toFunction()();
                    instance[field] = () => ({
                        $actualValue: dateValue,
                        toJSON () {
                            throw new Error( 'Attempted to serialize an r.now() value.' );
                        }
                    });
                    instance[field].$mustInterpret = true;
                }
            } else if ( typeof typedef._default === 'function' ) {
                instance[field] = typedef._default.call( instance );
            } else {
                instance[field] = typedef._default;
            }
        }

        const isObjectField = instance[field] && typeof instance[field] === 'object' && typedef.type === 'object';
        const typedefSchema = getTypedef_schema( typedef );
        if ( typedefSchema && isObjectField ) {
            defaults( instance[field], typedef._schema );
        }

        const isArrayField = instance[field] && Array.isArray( instance[field])
              && typedef.type === 'array'
              && typedefSchema && typedefSchema.type === 'object';

        if ( typedefSchema && typedefSchema._schema && isArrayField ) {
            instance[field].forEach( item => {
                defaults( item, typedef._schema._schema );
            });
        }
    });
}

// var User = thinky.createModel( 'Users', { id: etc });
function createModel ( modelStore ) {
    return ( tableName, schema, otherOptions = {}) => {
        let model = function Model ( newObj ) {
            if ( !newObj )
                throw new Error( 'Thinky requires that new Foo() takes an object: new Foo({}).' );

            Object.defineProperty( this, '_sourceObject', {
                writable: true,
                value: newObj,
                enumerable: false,
                configurable: true
            });

            Object.assign( this,
                newObj,
                documentMethods( this, model )
            );

            virtuals( this, schema );
            defaults( this, schema );

            model._instances.push( this );

            this[inspect.custom] = () => {
                const obj = Object.create( model.prototype );
                return Object.assign( obj, inspectStripSinon( this ) );
            };

            model._callPostHook( 'init', this );
        };

        function wrapObjects ( innerSchema ) {
            // Wrap bare objects in schema
            Object.keys( innerSchema ).forEach( key => {
                if ( isPlainObject( innerSchema[key]) )
                    innerSchema[key] = thinkyType.object().schema( innerSchema[key]);

                const typedefSchema = getTypedef_schema( innerSchema[key]);
                if ( typedefSchema && typedefSchema._schema ) {
                    wrapObjects( innerSchema[key]._schema._schema );
                } else if ( innerSchema[key] && innerSchema[key]._schema ) {
                    wrapObjects( innerSchema[key]._schema );
                }
            });
        }

        wrapObjects( schema );

        Object.defineProperty( model, 'name', {
            value: `${tableName} Instance`,
            configurable: true
        });

        Object.assign( model, {
            schema,
            _schema: { ...schema, _schema: schema }, // thinky compatibility
            _modelStore: modelStore,
            _staticMethods: [],
            _joins: {},
            _otherOptions: otherOptions,
            _pk: otherOptions.pk || 'id',
            _tableName: tableName,
            _isSaved: false,
            _defaultReturnValue: () => modelStore._defaultReturnValue,
            _documentMethods: [],
            _preHooks: [],
            _postHooks: [],
            _indexes: {},

            _callPreHook: ( name, instance ) => {
                model._preHooks.filter( hook => hook.event === name ).forEach( hook => {
                    const next = spy();
                    hook.action.call( instance, next );
                });
            },
            _callPostHook: ( name, instance ) => {
                model._postHooks.filter( hook => hook.event === name ).forEach( hook => {
                    const next = spy();
                    hook.action.call( instance, next );
                });
            },
            _callAsyncPreHook: async ( name, instance ) => {
                await Promise.all( model._preHooks.filter( hook => hook.event === name ).map( hook => new Promise( ( resolve, reject ) => {
                    const next = err => ( err ? reject( err ) : resolve() );
                    hook.action.call( instance, next );
                }) ) );
            },
            _callAsyncPostHook: async ( name, instance ) => {
                await Promise.all( model._postHooks.filter( hook => hook.event === name ).map( hook => new Promise( ( resolve, reject ) => {
                    const next = err => ( err ? reject( err ) : resolve() );
                    hook.action.call( instance, next );
                }) ) );
            },

            defineStatic: ( name, func ) => {
                model[name] = func;
            },

            define: ( name, func ) => {
                model._documentMethods.push({ name, func });
            },

            pre: ( event, action ) => {
                model._preHooks.push({ event, action });
            },

            post: ( event, action ) => {
                model._postHooks.push({ event, action });
            },

            ensureIndex: ( indexName, indexFunction, options ) => {
                if ( isPlainObject( indexFunction ) ) {
                    options = indexFunction;
                    indexFunction = undefined;
                }

                if ( !indexFunction ) {
                    indexFunction = modelStore.r.row( indexName );
                }

                model._indexes[indexName] = {
                    name: indexName,
                    func: indexFunction,
                    ...options
                };
            },

            hasOne: ( OtherModel, fieldName, leftKey, rightKey ) => {
                model._joins[fieldName] = { model: OtherModel, leftKey, rightKey, type: 'hasOne' };
            },

            belongsTo: ( OtherModel, fieldName, leftKey, rightKey ) => {
                model._joins[fieldName] = { model: OtherModel, leftKey, rightKey, belongs: true, type: 'belongsTo' };
            },

            hasMany: ( OtherModel, fieldName, leftKey, rightKey ) => {
                model._joins[fieldName] = { model: OtherModel, leftKey, rightKey, many: true, type: 'hasMany' };
            },

            hasAndBelongsToMany: ( OtherModel, fieldName, leftKey, rightKey ) => {
                // Automatically create join model with indexes
                let joinModel = modelStore[`${model._tableName}_${OtherModel._tableName}`]
                    || modelStore[`${OtherModel._tableName}_${model._tableName}`];

                if ( !joinModel ) {
                    joinModel = createModel( modelStore )( `${model._tableName}_${OtherModel._tableName}`, {
                        id: thinkyType.string().uuid( 4 ),
                        [`${model._tableName}_${model._pk}`]: thinkyType.string().uuid( 4 ),
                        [`${OtherModel._tableName}_${OtherModel._pk}`]: thinkyType.string().uuid( 4 )
                    });
                    joinModel.ensureIndex( `${model._tableName}_${model._pk}` );
                    joinModel.ensureIndex( `${OtherModel._tableName}_${OtherModel._pk}` );

                    if ( !model._modelStore._data[joinModel._tableName])
                        model._modelStore._data[joinModel._tableName] = [];
                }

                model._joins[fieldName] = {
                    model: OtherModel,
                    leftKey,
                    rightKey,
                    belongs: true,
                    many: true,
                    type: 'hasAndBelongsToMany',
                    joinTableName: joinModel._tableName,
                    joinModel
                };
            },

            // Save is special because it doesn't need an execute, we'll just use that here to simulate not needing it
            save: spy( ( ...args ) => createQuery( model ).save( ...args ).execute() ),

            getTableName: () => tableName,

            _addFinishedQuery: ({ query, data }) => {
                model._finishedQueries.push({ query, data });
                modelStore._addFinishedQuery( tableName, { query, data });
            },

            _reset: () => {
                model._returnValues = [];
                model._finishedQueries = [];
                model._instances = [];
                model._instanceOverrides = {};
            },

            _setReturnValue: ( returnValue, filters, options ) => {
                model._returnValues.push({ returnValue, filters, options });
            },

            _overrideInstanceMethod: ( method, func ) => {
                model._instanceOverrides[method] = func;
            },

            _newId: () => modelStore._newId(),

            _checkSchema: instance => {
                try {
                    thinkyType.object({ ignoreTypeCheck: true }).schema( schema )._checkType( instance );
                } catch ( err ) {
                    throw new Error( `Schema validation failed for ${tableName}: ${err.message}` );
                }
            },

            _interpretValues: instance => {
                const interpretValues = ( obj, typedef ) => {
                    if ( !obj )
                        return obj;

                    if ( obj && obj.$mustInterpret )
                        obj = obj();

                    if ( obj && obj.$actualValue )
                        obj = obj.$actualValue;

                    if ( ( isPlainObject( typedef ) || ( typedef.type === 'object' && typedef._schema ) )
                        && typeof obj === 'object' ) {
                        Object.entries( typedef._schema || typedef ).forEach( ([ key, innerTypedef ]) => {
                            if ( innerTypedef.type !== 'virtual' )
                                obj[key] = interpretValues( obj[key], innerTypedef );
                        });
                    } else if ( typedef.type === 'array' && typedef._schema && Array.isArray( obj ) ) {
                        obj.forEach( ( arrayVal, i ) => {
                            obj[i] = interpretValues( arrayVal, typedef._schema );
                        });
                    } else if ( typedef.type === 'date' ) {
                        if ( obj instanceof Date )
                            return obj;
                        if ( typeof obj === 'number' )
                            return new Date( obj );
                    }
                    return obj;
                };
                const rootTypedef = thinkyType.object({ ignoreTypeCheck: true }).schema( schema );
                interpretValues( instance, rootTypedef );
            },

            _parseSync: obj => {
                // eslint-disable-next-line new-cap
                const instance = new model( obj );
                instance.setSaved( true );
                return instance;
            },

            _parse: async obj => model._parseSync( obj ),

            _getModel: () => model,

            _getIndexValue: ( instance, index ) => {
                const indexData = model._indexes[index];
                if ( !indexData )
                    throw new Error( `Attempted to access index ${index} of ${model._tableName}, but the index does not exist.` );

                return unwrap( indexData.func, instance );
            }
        });

        // Creates properties
        model._reset();

        // id and the primary key (usually the same) automatically has an index in rethinkdb
        model.ensureIndex( 'id' );
        model.ensureIndex( model._pk );

        model = new Proxy( model, {
            get ( target, prop, receiver ) {
                const existing = Reflect.get( target, prop, receiver );
                if ( existing )
                    return existing;

                if ( proxyIgnore( prop ) ) return undefined;

                return ( ...args ) => createQuery( model )[prop]( ...args );
            }
        });

        Object.keys( model ).forEach( key => {
            if ( /^_/.test( key ) ) {
                Object.defineProperty( model, key, {
                    value: model[key],
                    enumerable: false
                });
            }
        });

        modelStore[tableName] = model;
        return model;
    };
}

export const thinkyMockedDB = opts => ({
    ...opts,
    adminOptions: {}
});

export const thinkyMockedDBObject = ( table, dataFn ) => {
    const constructor = ( options = {}, data = dataFn() ) => (
        Array.isArray( data )
            ? data.map( d => Object.assign( d, options ) )
            : [ Object.assign( data, options ) ]);

    constructor.table = table;
    return constructor;
};

function gen ( generator, count = 1, options = {}) {
    return Array( Math.max( count, 0 ) || 0 ).fill().map( () => generator( options ) );
}

/* eslint-disable no-return-assign */
export const thinkyMockedDBDocGen = ( mockedDB, constructor ) => {
    const tableData = constructor();

    return mockedDB[constructor.table] || [
        ...tableData,
        ...gen( constructor, mockedDB[`add${constructor.table}`] || ( mockedDB[`add${constructor.table}`] = 0 ) )
    ];
};
/* eslint-enable no-return-assign */

export default function thinkyMock ( tables = {}) {
    const data = tables;

    // not sure why this is needed yet but tsts break w/out it
    if ( !tables.tables ) {
        tables.tables = tables;
    }
    const modelStore = {
        _idCounter: 0,
        _finishedQueries: [],
        _data: {
            ...data,
            $$null: []
        },

        _addFinishedQuery: ( tableName, { query, data: finishedQueryData }) => {
            modelStore._finishedQueries.push({ query, table: tableName, data: finishedQueryData });
        },

        _getFinishedQueries: table => {
            if ( !table )
                return modelStore._finishedQueries;

            const queries = modelStore._finishedQueries
                .filter( query => query.table === table )
                .map( query => ({ filters: query.query._filters, data: query.data }) );

            // So we can call queries[0].update to get the args passed to `update(args)`
            return queries.map( query => new Proxy( query, {
                get ( target, prop, receiver ) {
                    const existing = Reflect.get( target, prop, receiver );
                    if ( existing )
                        return existing;

                    if ( proxyIgnore( prop ) ) return undefined;

                    const targetFilter = target.filters.find( q => q.method === prop );
                    return targetFilter && targetFilter.args && ( targetFilter.args.length === 1 ? targetFilter.args[0] : targetFilter.args );
                }
            }) );
        },

        _newId: () => {
            const val = modelStore._idCounter;
            modelStore._idCounter += 1;
            return val;
        },

        _defaultReturnValue: undefined
    };

    function setTableReturnValue ( tableName, returnValue, filters, options ) {
        modelStore[tableName]._setReturnValue( returnValue, filters, options );
    }

    function reset ( newData ) {
        for ( const sinonSpy of spies ) {
            if ( sinonSpy.reset )
                sinonSpy.reset();
            else if ( sinonSpy.resetHistory )
                sinonSpy.resetHistory();
        }

        for ( const key of Object.keys( modelStore ) ) {
            if ( key.charAt( 0 ) !== '_' && modelStore[key]._reset )
                modelStore[key]._reset();
        }

        modelStore._finishedQueries = [];
        modelStore._idCounter = 0;
        modelStore._defaultReturnValue = undefined;

        // Make sure we have empty entries for all the join tables.
        const joinTablesObj = flatten(
            Object.values( modelStore )
                .filter( val => val && val._joins )
                .map( val => Object.values( val._joins )
                    .filter( joinInfo => joinInfo.joinTableName )
                    .map( joinInfo => joinInfo.joinTableName )
                )
        ).reduce( ( obj, name ) => Object.assign( obj, { [name]: [] }), {});

        modelStore._data = {
            ...joinTablesObj,
            ...newData,
            $$null: []
        };
    }

    const r = {
        ...rethinkDBMocked(),
        table ( tableName ) {
            let model = modelStore[tableName];
            if ( !model ) {
                createModel( modelStore )( tableName, {});
                model = modelStore[tableName];
            }
            return createQuery( model, { thinky: false });
        },
        connectPool: opts => ({
            connParam: {
                db: opts.db,
                user: opts.user || 'admin',
                password: opts.password,
                buffer: 1,
                max: 1,
                timeout: 20,
                pingInterval: -1,
                timeoutError: 1000,
                timeoutGb: 3600000,
                maxExponent: 6,
                silent: false,
                log: [ () => {} ]
            },
            servers: [ { host: opts.host, port: opts.port } ]
        }),
        expr ( val ) {
            const model = modelStore.$$null;
            return createQuery( model, { thinky: false }).expr( val );
        }
    };
    modelStore.r = r;

    // A null model which has no data, for example r.expr() uses the null table, and then adds the values passed into expr
    createModel( modelStore )( '$$null', {});

    // require( 'thinky' )
    const mockInstance = {
        createModel: createModel( modelStore ),
        type: thinkyType,
        r,
        Errors: { DocumentNotFound },
        _setTableReturnValue: setTableReturnValue,
        _reset: reset,
        _getTables: () => modelStore._data,
        _modelStore: modelStore,
        _getQueries: table => modelStore._getFinishedQueries( table ),
        _getModel: table => modelStore[table],
        _getInstances: table => modelStore[table]._instances,
        _overrideInstanceMethod: ( table, method, func ) => modelStore[table]._overrideInstanceMethod( method, func ),
        _wrapModel: ( obj, table ) => modelStore[table]._parseSync( obj ),
        _setDefaultReturnValue: returnValue => { modelStore._defaultReturnValue = returnValue; }
    };

    return mockInstance;
}

/* eslint-enable no-underscore-dangle */
/* eslint-enable max-classes-per-file */
/* eslint-enable filenames/match-exported */
