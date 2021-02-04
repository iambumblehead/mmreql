import { v4 as uuidv4 } from 'uuid';

import {
    mockdbStateTableIndexAdd,
    mockdbStateTableIndexList,
    mockdbStateTableGetIndexTuple
} from './mockdbState.js';

import {
    mockdbTableGetDocument,
    mockdbTableSetDocument,
    mockdbTableGetDocuments,
    mockdbTableSetDocuments,
    mockdbTableDocGetIndexValue,
    mockdbTableSet
} from './mockdbTable.js';

import {
    mockdbResChangesFieldCreate,
    mockdbResErrorDuplicatePrimaryKey
} from './mockdbRes.js';

const isReqlObj = obj => Boolean(
    obj && /object|function/.test( typeof obj ) && obj.isReql );

const isConfigObj = ( obj, objType = typeof obj ) => obj
      && /object|function/.test( objType )
      && !isReqlObj( obj )
      && !Array.isArray( obj );

// return last query argument (optionally) provides query configurations
const queryArgsOptions = ( queryArgs, queryOptionsDefault = {}) => {
    const queryOptions = queryArgs.slice( -1 )[0] || {};

    return ( isConfigObj( queryOptions ) )
        ? queryOptions
        : queryOptionsDefault;
};

// use when order not important and sorting helps verify a list
const compare = ( a, b, prop ) => {
    if ( a[prop] < b[prop]) return -1;
    if ( a[prop] > b[prop]) return 1;
    return 0;
};

const isReqlArgs = value => (
    isReqlObj( value ) && value.queryName === 'args' );

export const spend = ( value, reqlChain, doc, type = typeof value, f = null ) => {
    if ( value === f ) {
        f = value;
    } else if ( isReqlObj( value ) ) {
        if ( value.record ) {
            f = value.run( doc );
        } else {
            f = value.run();
        }
    } else if ( /string|boolean|number|undefined/.test( type ) ) {
        f = value;
        if ( doc && !doc.playbackStub ) {
            f = doc[value];
        }
    } else if ( Array.isArray( value ) ) {
        // detach if value is has args
        if ( isReqlArgs( value.slice( -1 )[0]) ) {
            f = value.slice( -1 )[0].run();
        } else {
            f = value.map( v => spend( v, reqlChain ) );
        }
    } else if ( type === 'function' ) {
        if ( doc ) {
            f = value( reqlChain().expr( doc ) ).run();
        } else {
            f = value().run();
        }
    } else if ( value instanceof Date ) {
        f = value;
    } else {
        f = Object.keys( value ).reduce( ( prev, key ) => {
            prev[key] = spend( value[key], reqlChain );

            return prev;
        }, {});
    }

    return f;
};

const reql = {};

reql.connectPool = opts => ({
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
    servers: [ {
        host: opts.host,
        port: opts.port
    } ]
});

reql.indexCreate = ( queryState, args, reqlChain, dbState ) => {
    const [ indexName ] = args;
    const fields = spend( Array.isArray( args[1]) ? args[1] : [], reqlChain );
    const config = queryArgsOptions( args );

    mockdbStateTableIndexAdd(
        dbState, queryState.tablename, indexName, fields, config );

    // should throw ReqlRuntimeError if index exits already
    queryState.target = { created: 1 };

    return queryState;
};

reql.indexWait = ( queryState, args, reqlChain, dbState ) => {
    const tableIndexList = mockdbStateTableIndexList(
        dbState, queryState.tablename );

    queryState.target = tableIndexList.map( indexName => ({
        index: indexName,
        ready: true,
        function: 1234,
        multi: false,
        geo: false,
        outdated: false
    }) );

    return queryState;
};

reql.indexList = ( queryState, args, reqlChain, dbState ) => {
    queryState.target = mockdbStateTableIndexList(
        dbState, queryState.tablename );

    return queryState;
};

reql.insert = ( queryState, args, reqlChain ) => {
    let documents = spend( args.slice( 0, 1 ), reqlChain );
    let table = queryState.tablelist;
    const options = args[1] || {};
    const [ existingDoc ] = mockdbTableGetDocuments(
        queryState.tablelist, documents.map( doc => doc.id ) );

    if ( existingDoc ) {
        queryState.target = mockdbResChangesFieldCreate({
            errors: 1,
            firstError: mockdbResErrorDuplicatePrimaryKey(
                existingDoc,
                documents.find( doc => doc.id === existingDoc.id )
            )
        });

        return queryState;
    }

    [ table, documents ] = mockdbTableSetDocuments(
        table, documents.map( doc => spend( doc, reqlChain ) ) );

    queryState.target = mockdbResChangesFieldCreate({
        inserted: documents.length,
        generated_keys: documents.map( doc => doc.id ),
        changes: options.returnChanges === true
            ? documents.map( doc => ({
                old_val: null,
                new_val: doc
            }) )
            : undefined
    });

    return queryState;
};

reql.update = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const queryTable = queryState.tablelist;
    const updateProps = spend( args[0], reqlChain );

    const updateTarget = targetDoc => {
        let tableDoc = mockdbTableGetDocument( queryTable, targetDoc.id );

        if ( tableDoc ) {
            Object.assign( tableDoc, updateProps || {});
            [ , tableDoc ] = mockdbTableSetDocument( queryTable, tableDoc );
        }

        return tableDoc;
    };

    const updatedDocs = (
        Array.isArray( queryTarget )
            ? queryTarget
            : [ queryTarget ]
    ).reduce( ( updated, targetDoc ) => {
        const tableDoc = updateTarget( targetDoc );

        if ( tableDoc )
            updated.push( tableDoc );

        return updated;
    }, []);

    queryState.target = mockdbResChangesFieldCreate({
        replaced: updatedDocs.length
    });

    return queryState;
};

reql.get = ( queryState, args, reqlChain ) => {
    const primaryKeyValue = spend( args[0], reqlChain );
    const tableDoc = mockdbTableGetDocument( queryState.target, primaryKeyValue );

    queryState.target = tableDoc || null;

    return queryState;
};

reql.get.fn = ( queryState, args, reqlChain ) => {
    queryState.target = reqlChain()
        .expr( queryState.target ).getField( args[0]).run();

    return queryState;
};

reql.getAll = ( queryState, args, reqlChain, dbState ) => {
    const queryOptions = queryArgsOptions( args );
    const { tablename } = queryState;
    const indexName = queryOptions.index || 'id';
    const indexTargetValue = Array.isArray( args[0]) ? args[0].join() : args[0];
    const tableIndexTuple = mockdbStateTableGetIndexTuple( dbState, tablename, indexName );

    queryState.target = queryState.target.filter( doc => (
        mockdbTableDocGetIndexValue( doc, tableIndexTuple, spend, reqlChain ) === indexTargetValue
    ) ).sort( () => 0.5 - Math.random() );
    // rethink output array is not in-order

    return queryState;
};

reql.replace = ( queryState, args, reqlChain ) => {
    let replaced = 0;
    const replacement = spend( args[0], reqlChain );
    const targetIndexName = 'id';
    const targetIndex = queryState.tablelist
        .findIndex( doc => doc[targetIndexName] === replacement[targetIndexName]);

    if ( targetIndex > -1 ) {
        replaced += 1;
        queryState.tablelist[targetIndex] = replacement;
    }

    queryState.target = mockdbResChangesFieldCreate({
        replaced
    });

    return queryState;
};

reql.nth = ( queryState, args, reqlChain ) => {
    const nthIndex = spend( args[0], reqlChain );

    if ( nthIndex >= queryState.target.length ) {
        queryState.error = `ReqlNonExistanceError: Index out of bounds: ${nthIndex}`;
        queryState.target = null;
    } else {
        queryState.target = queryState.target[args[0]];
    }

    return queryState;
};

// r.row → value
reql.row = ( queryState, args, reqlChain ) => {
    const rowFieldName = spend( args[0], reqlChain );

    queryState.target = queryState.target
        ? queryState.target[rowFieldName]
        : rowFieldName;

    return queryState;
};

reql.default = ( queryState, args, reqlChain ) => {
    if ( queryState.target === null ) {
        queryState.error = null;
        queryState.target = spend( args[0], reqlChain );
    }

    return queryState;
};

// time.during(startTime, endTime[, {leftBound: "closed", rightBound: "open"}]) → bool
reql.during = ( queryState, args, reqlChain ) => {
    const [ start, end ] = args;
    const startTime = spend( start, reqlChain );
    const endTime = spend( end, reqlChain );

    queryState.target = (
        queryState.target.getTime() > startTime.getTime()
            && queryState.target.getTime() < endTime.getTime()
    );

    return queryState;
};

reql.append = ( queryState, args, reqlChain ) => {
    queryState.target = spend( args, reqlChain ).reduce( ( list, val ) => {
        list.push( val );

        return list;
    }, queryState.target );

    return queryState;
};

// NOTE rethinkdb uses re2 syntax
// re using re2-specific syntax will fail
reql.match = ( queryState, args, reqlChain ) => {
    let regexString = spend( args[0], reqlChain );

    let flags = '';
    if ( regexString.startsWith( '(?i)' ) ) {
        flags = 'i';
        regexString = regexString.slice( '(?i)'.length );
    }

    // eslint-disable-next-line security/detect-non-literal-regexp
    const regex = new RegExp( regexString, flags );

    queryState.target = regex.test( queryState.target );

    return queryState;
};

reql.delete = ( queryState, args, reqlChain, dbState ) => {
    const queryTarget = queryState.target;
    const queryTable = queryState.tablelist;
    const indexName = 'id';
    const tableIndexTuple = mockdbStateTableGetIndexTuple(
        dbState, queryState.tablename, indexName );
    const targetIds = ( Array.isArray( queryTarget ) ? queryTarget : [ queryTarget ])
        .map( doc => mockdbTableDocGetIndexValue( doc, tableIndexTuple, spend ) );
    // eslint-disable-next-line security/detect-non-literal-regexp
    const targetIdRe = new RegExp( `^(${targetIds.join( '|' )})$` );
    const tableFiltered = queryTable.filter( doc => !targetIdRe.test(
        mockdbTableDocGetIndexValue( doc, tableIndexTuple, spend ) ) );
    const deleted = queryTable.length - tableFiltered.length;

    mockdbTableSet( queryTable, tableFiltered );

    queryState.target = mockdbResChangesFieldCreate({
        deleted
    });

    return queryState;
};

reql.contains = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;

    if ( !args.length ) {
        throw new Error( 'Rethink supports contains(0) but rethinkdbdash does not.' );
    }

    queryState.target = args.every( predicate => (
        queryTarget.includes( spend( predicate, reqlChain ) ) ) );

    return queryState;
};

reql.getField = ( queryState, args, reqlChain ) => {
    const [ fieldName ] = spend( args, reqlChain );

    queryState.target = queryState.target[fieldName];

    return queryState;
};

reql.filter = ( queryState, args, reqlChain ) => {
    const [ predicate ] = args;

    queryState.target = queryState.target.filter( item => {
        const finitem = spend( predicate, reqlChain, item );

        if ( finitem && typeof finitem === 'object' ) {
            return Object
                .keys( finitem )
                .every( key => finitem[key] === item[key]);
        }

        return finitem;
    });

    return queryState;
};

reql.count = queryState => {
    queryState.target = queryState.target.length;

    return queryState;
};

reql.pluck = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const pluckObj = ( obj, props ) => props.reduce( ( plucked, prop ) => {
        plucked[prop] = obj[prop];
        return plucked;
    }, {});

    args = spend( args, reqlChain );

    queryState.target = Array.isArray( queryTarget )
        ? queryTarget.map( t => pluckObj( t, args ) )
        : pluckObj( queryTarget, args );

    return queryState;
};

reql.hasFields = ( queryState, args ) => {
    const queryTarget = queryState.target;

    queryState.target = queryTarget.filter( item => {
        if ( !item ) return false;
        return args.every( name => Object.prototype.hasOwnProperty.call( item, name ) );
    });

    return queryState;
};

reql.slice = ( queryState, args, reqlChain ) => {
    const [ begin, end ] = spend( args.slice( 0, 2 ), reqlChain );

    if ( queryState.isGrouped ) { // slice from each group
        queryState.target = queryState.target.map( targetGroup => {
            targetGroup.reduction = targetGroup.reduction.slice( begin, end );

            return targetGroup;
        });
    } else {
        queryState.target = queryState.target.slice( begin, end );
    }

    return queryState;
};

reql.skip = ( queryState, args, reqlChain ) => {
    const count = spend( args[0], reqlChain );

    queryState.target = queryState.target.slice( count );

    return queryState;
};

reql.limit = ( queryState, args ) => {
    queryState.target = queryState.target.slice( 0, args[0]);

    return queryState;
};

reql.eqJoin = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const [ leftKey, otherSequence ] = spend( args.slice( 0, 2 ), reqlChain );

    queryState.target = queryTarget.map( item => ({
        left: item,
        right: otherSequence.find( other => other.id === item[leftKey])
    }) ).filter( ({ right }) => right );

    return queryState;
};

reql.innerJoin = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const [ otherSequence, joinFunc ] = args;
    const otherTable = spend( otherSequence, reqlChain );

    queryState.target = queryTarget.map( item => otherTable.map( otherItem => {
        const isJoined = spend( first => {
            const second = reqlChain().expr( otherItem );

            return joinFunc( first, second );
        }, reqlChain, item );

        return {
            left: item,
            right: isJoined ? otherItem : null
        };
    }) ).flat().filter( ({ right }) => right );

    return queryState;
};

reql.now = queryState => {
    queryState.target = new Date();

    return queryState;
};

reql.toEpochTime = queryState => {
    queryState.target = ( new Date( queryState.target ) ).getTime() / 1000;

    return queryState;
};

reql.epochTime = ( queryState, args ) => {
    queryState.target = new Date( args[0] * 1000 );

    return queryState;
};

reql.not = queryState => {
    const queryTarget = queryState.target;

    if ( typeof queryTarget !== 'boolean' )
        throw new Error( 'Cannot call not() on non-boolean value.' );

    queryState.target = !queryTarget;

    return queryState;
};

reql.gt = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target > argTarget;

    return queryState;
};

reql.ge = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target >= argTarget;

    return queryState;
};

reql.lt = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target < argTarget;

    return queryState;
};

reql.le = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target <= argTarget;

    return queryState;
};

reql.eq = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target === argTarget;

    return queryState;
};

reql.ne = ( queryState, args, reqlChain ) => {
    const argTarget = spend( args[0], reqlChain );

    queryState.target = queryState.target !== argTarget;

    return queryState;
};

reql.max = ( queryState, args ) => {
    const targetList = queryState.target;
    const getListMax = ( list, prop ) => list.reduce( ( maxDoc, doc ) => (
        maxDoc[prop] > doc[prop] ? maxDoc : doc
    ), targetList );

    const getListMaxGroups = ( groups, prop ) => (
        groups.reduce( ( prev, target ) => {
            prev.push({
                ...target,
                reduction: getListMax( target.reduction, prop )
            });

            return prev;
        }, [])
    );

    queryState.target = queryState.isGrouped
        ? getListMaxGroups( targetList, args[0])
        : getListMax( targetList, args[0]);

    return queryState;
};

reql.max.fn = ( queryState, args, reqlChain ) => {
    const field = spend( args[0], reqlChain );

    if ( queryState.isGrouped ) {
        queryState.target = queryState.target.map( targetGroup => {
            targetGroup.reduction = targetGroup.reduction[field];

            return targetGroup;
        });
    } else {
        queryState.target = queryState.target[field];
    }

    return queryState;
};

reql.min = ( queryState, args ) => {
    const targetList = queryState.target;
    const getListMin = ( list, prop ) => list.reduce( ( maxDoc, doc ) => (
        maxDoc[prop] < doc[prop] ? maxDoc : doc
    ), targetList );

    const getListMinGroups = ( groups, prop ) => (
        groups.reduce( ( prev, target ) => {
            prev.push({
                ...target,
                reduction: getListMin( target.reduction, prop )
            });

            return prev;
        }, [])
    );

    queryState.target = queryState.isGrouped
        ? getListMinGroups( targetList, args[0])
        : getListMin( targetList, args[0]);

    return queryState;
};

reql.merge = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const merges = args.map( arg => spend( arg, reqlChain ) );

    queryState.target = merges
        .reduce( ( p, next ) => Object.assign( p, next ), queryTarget );

    return queryState;
};

reql.concatMap = ( queryState, args, reqlChain ) => {
    const [ func ] = args;

    queryState.target = queryState
        .target.map( t => spend( func, reqlChain, t ) ).flat();

    return queryState;
};

reql.isEmpty = queryState => {
    queryState.target = queryState.target.length === 0;

    return queryState;
};

reql.add = ( queryState, args, reqlChain ) => {
    const { target } = queryState;
    const values = spend( args, reqlChain );

    let result;

    if ( typeof target === 'undefined' ) {
        if ( Array.isArray( values ) ) {
            result = values.slice( 1 ).reduce( ( prev, val ) => prev + val, values[0]);
        } else {
            result = values;
        }
    } else if ( /number|string/.test( typeof target ) ) {
        result = values.reduce( ( prev, val ) => prev + val, target );
    } else if ( Array.isArray( target ) ) {
        result = [ ...target, ...values ];
    }

    queryState.target = result;

    return queryState;
};

reql.group = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const [ arg ] = args;
    const groupedData = queryTarget.reduce( ( group, item ) => {
        const key = spend( arg, reqlChain );
        const groupKey = item[key];

        group[groupKey] = group[groupKey] || [];
        group[groupKey].push( item );

        return group;
    }, {});
    const rethinkFormat = Object.entries( groupedData )
        .map( ([ group, reduction ]) => ({ group, reduction }) );

    queryState.isGrouped = true;
    queryState.target = rethinkFormat;

    return queryState;
};

// array.sample(number) → array
reql.sample = ( queryState, args ) => {
    queryState.target = queryState.target
        .sort( () => 0.5 - Math.random() )
        .slice( 0, args );

    return queryState;
};

reql.ungroup = queryState => {
    queryState.isGrouped = false;

    return queryState;
};

reql.orderBy = ( queryState, args, reqlChain, dbState ) => {
    const queryTarget = queryState.target;
    const queryOptions = typeof args[0] === 'function'
        ? args[0]
        : queryArgsOptions( args );

    const queryOptionsIndex = spend( queryOptions.index, reqlChain );
    const indexSortBy = typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortBy;
    const indexSortDirection = ( typeof queryOptionsIndex === 'object' && queryOptionsIndex.sortDirection ) || 'asc';
    const indexString = typeof queryOptionsIndex === 'string' && queryOptionsIndex;
    const argsSortPropValue = typeof args[0] === 'string' && args[0];
    const indexName = indexSortBy || indexString || 'id';
    const tableIndexTuple = mockdbStateTableGetIndexTuple( dbState, queryState.tablename, indexName );
    const sortDirection = isAscending => (
        isAscending * ( indexSortDirection === 'asc' ? 1 : -1 ) );

    const getSortFieldValue = doc => {
        let value;

        if ( typeof queryOptions === 'function' ) {
            value = spend( queryOptions, reqlChain, doc );
        } else if ( argsSortPropValue ) {
            value = doc[argsSortPropValue];
        } else {
            value = mockdbTableDocGetIndexValue( doc, tableIndexTuple, spend );
        }

        return value;
    };

    queryState.target = queryTarget.sort( ( doca, docb ) => {
        const docaField = getSortFieldValue( doca, tableIndexTuple );
        const docbField = getSortFieldValue( docb, tableIndexTuple );

        return sortDirection( docaField < docbField ? -1 : 1 );
    });

    return queryState;
};

// Return the hour in a time object as a number between 0 and 23.
reql.hours = queryState => {
    queryState.target = new Date( queryState.target ).getHours();

    return queryState;
};

reql.minutes = queryState => {
    queryState.target = new Date( queryState.target ).getMinutes();

    return queryState;
};

reql.uuid = queryState => {
    queryState.target = uuidv4();

    return queryState;
};

reql.expr = ( queryState, args ) => {
    const [ value ] = args;
    const resolved = spend( value );

    queryState.targetOriginal = resolved;
    queryState.target = resolved;

    return queryState;
};

reql.expr.fn = ( queryState, args ) => {
    queryState.target = queryState.target[args[0]];

    return queryState;
};

reql.coerceTo = ( queryState, args, reqlChain ) => {
    const [ coerceType ] = args;
    let resolved = spend( queryState.target, reqlChain );

    if ( coerceType === 'string' )
        resolved = String( resolved );

    queryState.target = resolved;

    return queryState;
};

reql.upcase = queryState => {
    queryState.target = String( queryState.target ).toUpperCase();

    return queryState;
};

reql.downcase = queryState => {
    queryState.target = String( queryState.target ).toLowerCase();

    return queryState;
};

reql.map = ( queryState, args, reqlChain ) => {
    const [ func ] = args;

    queryState.target = queryState
        .target.map( t => spend( func, reqlChain, t ) );

    return queryState;
};

reql.without = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;

    args = spend( args, reqlChain );

    queryState.target = args.reduce( arg => {
        delete queryTarget[arg];

        return queryTarget;
    }, queryTarget );

    return queryState;
};

reql.or = ( queryState, args, reqlChain ) => {
    queryState.target = args.reduce( ( current, value ) => Boolean(
        current || spend( value, reqlChain )
    ), queryState.target );

    return queryState;
};

reql.and = ( queryState, args, reqlChain ) => {
    queryState.target = args.reduce( ( current, value ) => Boolean(
        current && spend( value, reqlChain )
    ), queryState.target );

    return queryState;
};

// if the conditionals return any value but false or null (i.e., “truthy” values),
// reql.branch = {};
reql.branch = ( queryState, args, reqlChain ) => {
    const queryTarget = queryState.target;
    const isResultTruthy = result => (
        result !== false && result !== null );

    const nextCondition = ( condition, branches ) => {
        const conditionResult = spend( condition, reqlChain, queryState.target );

        if ( branches.length === 0 )
            return conditionResult;

        if ( isResultTruthy( conditionResult ) ) {
            return spend( branches[0], reqlChain, queryTarget );
        }

        return nextCondition( branches[1], branches.slice( 2 ) );
    };

    queryState.target = nextCondition( args[0], args.slice( 1 ) );

    return queryState;
};

// Rethink has its own alg for finding distinct,
// but unique by ID should be sufficient here.
reql.distinct = queryState => {
    queryState.target = queryState.target.filter(
        ( item, pos, self ) => self.indexOf( item ) === pos );

    return queryState;
};

reql.union = ( queryState, args, reqlChain ) => {
    const queryOptions = queryArgsOptions( args );

    if ( queryOptions )
        args.splice( -1, 1 );

    let res = args.reduce( ( argData, value ) => {
        value = spend( value, reqlChain );
        return argData.concat( value );
    }, queryState.target );

    if ( queryOptions && queryOptions.interleave ) {
        res = res.sort(
            ( a, b ) => compare( a, b, queryOptions.interleave )
        );
    }

    queryState.target = res;

    return queryState;
};

reql.table = ( queryState, args, reqlChain, dbState, tables ) => {
    const [ tablename ] = args;
    const tablelist = tables[tablename];

    queryState.tablename = tablename;
    queryState.tablelist = tablelist;
    queryState.target = tablelist.slice();

    return queryState;
};

// r.args(array) → special
reql.args = ( queryState, args, reqlChain ) => {
    args = spend( args[0], reqlChain );

    if ( !Array.isArray( args ) ) {
        throw new Error( 'args must be an array' );
    }

    queryState.target = args;

    return queryState;
};

reql.desc = ( queryState, args, reqlChain ) => {
    queryState.target = {
        sortBy: spend( args[0], reqlChain ),
        sortDirection: 'desc'
    };

    return queryState;
};

reql.asc = ( queryState, args, reqlChain ) => {
    queryState.target = {
        sortBy: spend( args[0], reqlChain ),
        sortDirection: 'asc'
    };

    return queryState;
};

reql.run = queryState => {
    if ( queryState.error ) {
        throw new Error( queryState.error );
    }

    return queryState.target;
};

reql.serialize = queryState => JSON.stringify( queryState.chain );

reql.isReql = true;

export default reql;
