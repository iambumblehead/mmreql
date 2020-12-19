import { mapValues, isPlainObject, isMatch } from 'lodash/fp.js';
import casual from 'casual';

const internalError = Symbol( 'internalError' );

export class PseudoQuery {
    constructor ( finalObj ) {
        this.target = finalObj;
        this.currentTarget = this.target;
        this.func = obj => obj;
        this.str = this.target !== undefined && this.target.toString() !== '[object Object]' ? unwrapString( this.target ) : 'obj';
        return this.bound( this.row );
    }

    // If you can think of a better way to do this, then please do.
    // This enables chaining like so: r.row('prop1')('prop2').eq('val1')
    bound ( func ) {
        const boundFunc = func.bind( this );
        Object.getOwnPropertyNames( PseudoQuery.prototype ).forEach( classFunc => {
            boundFunc[classFunc] = this[classFunc].bind( this );
        });
        boundFunc.self = this;
        return boundFunc;
    }

    resolve ( val, resolveObj, options ) {
        return unwrap( val, this.currentTarget !== undefined ? this.currentTarget : resolveObj, { unwrapDate: true, ...options });
    }

    replace ( apply, newStr ) {
        const newQuery = new PseudoQuery();
        newQuery.self.target = this.target;
        newQuery.self.currentTarget = this.currentTarget;
        newQuery.self.func = obj => apply( this.func( obj ) );
        newQuery.self.str = newStr;

        return newQuery;
    }

    row ( property ) {
        notUndefined( property );
        return this.replace( obj => {
            if ( !obj || typeof obj !== 'object' )
                throw new Error( `obj is ${obj} - cannot get property ${property}.` );

            if ( !Object.prototype.hasOwnProperty.call( obj, property ) || obj[property] === undefined ) {
                return {
                    [internalError]: {
                        type: 'noAttribute',
                        err: new Error( `No attribute ${property} in obj ${JSON.stringify( obj )}.` ),
                        value: this.resolve( obj[property], null )
                    }
                };
            }

            return this.resolve( obj[property], null );
        }, `${this.str}[${property}]` );
    }

    eq ( val ) {
        notUndefined( val );
        return this.replace( obj => obj === this.resolve( val, null ),
            `${this.str} === (${unwrapString( val )})` );
    }

    ne ( val ) {
        notUndefined( val );
        return this.replace( obj => obj !== this.resolve( val, null ),
            `${this.str} !== (${unwrapString( val )})` );
    }

    gt ( val ) {
        notUndefined( val );
        return this.replace( obj => obj > this.resolve( val, null ),
            `${this.str} > (${unwrapString( val )})` );
    }

    lt ( val ) {
        notUndefined( val );
        return this.replace( obj => obj < this.resolve( val, null ),
            `${this.str} < (${unwrapString( val )})` );
    }

    ge ( val ) {
        notUndefined( val );
        return this.replace( obj => obj >= this.resolve( val, null ),
            `${this.str} >= (${unwrapString( val )})` );
    }

    le ( val ) {
        notUndefined( val );
        return this.replace( obj => obj <= this.resolve( val, null ),
            `${this.str} <= (${unwrapString( val )})` );
    }

    or ( ...otherFuncs ) {
        return this.replace( obj => otherFuncs.reduce( ( r, func ) => !!( r || this.resolve( func, obj ) ), obj ),
            `(${this.str}) || (${otherFuncs.map( unwrapString ).join( ' || ' )})` );
    }

    and ( ...otherFuncs ) {
        return this.replace( obj => otherFuncs.reduce( ( r, func ) => !!( r && this.resolve( func, obj ) ), obj ),
            `(${this.str}) && (${otherFuncs.map( unwrapString ).join( ' && ' )})` );
    }

    not () {
        return this.replace( obj => !obj, `!(${this.str})` );
    }

    branch ( test, onTrue, onFalse ) {
        return this.replace( obj => {
            const isTrue = this.resolve( test, this.currentTarget !== undefined ? this.currentTarget : obj );
            return this.resolve( isTrue ? onTrue : onFalse, this.currentTarget !== undefined ? this.currentTarget : obj, { allowFunction: false });
        }, `(${test.toString()}) ? ${onTrue.toString()} : ${onFalse.toString()}` );
    }

    contains ( val ) {
        return this.replace( obj => {
            if ( obj.includes ) {
                return obj.includes( this.resolve( val ) );
            }
            throw new Error( `contains does not support ${typeof obj}` );
        }, `${this.str} includes (${unwrapString( val )})` );
    }

    downcase () {
        return this.replace( obj => {
            if ( typeof obj !== 'string' )
                throw new Error( `Cannot downcase a non-string ${typeof obj}` );
            return obj.toLowerCase();
        }, `${this.str}.downcase()` );
    }

    merge ( other ) {
        return this.replace(
            obj => ({ ...obj, ...this.resolve( other ) }),
            `{ ...(${this.str}), ...(${unwrapString( other )} }` );
    }

    match ( val ) {
        return this.replace( obj => {
            obj = obj || '';

            let regexString = this.resolve( val );
            let flags = '';
            if ( regexString.startsWith( '(?i)' ) ) { // case-insensitive
                flags = 'i';
                regexString = regexString.slice( '(?i)'.length );
            }

            // eslint-disable-next-line security/detect-non-literal-regexp
            const regex = new RegExp( regexString, flags );
            return regex.test( obj );
        }, `/${unwrapString( val )}/.test(${this.str})` );
    }

    hasFields ( ...fields ) {
        return this.replace( obj => fields.reduce( ( has, field ) => has && obj[field] != null, true ),
            `${this.str} has fields (${fields.map( unwrapString ).join( ', and ' )})` );
    }

    filter ( predicate ) {
        return this.replace( obj => obj.filter( item => {
            const itemPredicate = unwrap( predicate, item );
            if ( isPlainObject( itemPredicate ) )
                return isMatch( itemPredicate, item );
            return itemPredicate;
        }), `${this.str}.filter(${unwrapString( predicate )})` );
    }

    isEmpty () {
        return this.replace( obj => obj.length === 0, `${this.str}.isEmpty()` );
    }

    default ( val ) {
        return this.replace( obj => {
            if ( obj && obj[internalError]?.type === 'noAttribute' ) {
                // row => row('not_exist').default('etc')
                // Normally row('not_exist') errors, but if you have a '.default' after it,
                // then the default returns instead.
                obj = obj[internalError].value;
            }
            return obj === undefined || obj === null ? this.resolve( val ) : obj;
        }, `${this.str} (default ${val})` );
    }

    toFunction ( target ) {
        const newQuery = new PseudoQuery();
        newQuery.self.target = this.target !== undefined ? this.target : ( this.currentTarget !== undefined ? this.currentTarget : target );
        newQuery.self.currentTarget = this.target !== undefined ? this.target : ( this.currentTarget !== undefined ? this.currentTarget : target );
        newQuery.self.func = this.func;
        newQuery.self.str = this.str;

        return function ( obj ) {
            if ( this.target === undefined )
                this.currentTarget = obj;
            return this.func( this.currentTarget );
        }.bind( newQuery.self );
    }

    toString () {
        return this.str;
    }

    async run () {
        // Not quite right but should get the job done for our purposes
        let result = this.toFunction()();
        if ( Array.isArray( result ) ) {
            result = result.map( r => ( r?.execute ? r.execute() : r ) );
            return Promise.all( result );
        }
        return result;
    }

    now () { return this.replace( () => new Date(), '{now}' ); }

    uuid () { return this.replace( () => casual.uuid, '{uuid}' ); }

    hours () {
        return this.replace( obj => new Date( obj ).getHours(), `${this.str}.hours()` );
    }

    minutes () {
        return this.replace( obj => new Date( obj ).getMinutes(), `${this.str}.minutes()` );
    }
}

function unwrapString ( other ) {
    if ( !other )
        return other;

    if ( typeof other === 'function' && other.name !== 'bound row' )
        return `obj => ${unwrapString( other( new PseudoQuery() ) )}`;

    return other.toString();
}

function notUndefined ( value ) {
    if ( value === undefined )
        throw new Error( 'Attempt to call reql method with an undefined argument. See stack trace.' );
}

// Unwraps a PseudoQuery or sub-query into a concrete value or series of values.
export function unwrap ( val, target, options = {}) {
    const {
        wrap = false,
        allowFunction = true,
        unwrapDate = false
    } = options;

    // "bound row" is the name of the PseudoQuery map function
    if ( typeof val === 'function' && target && val.name !== 'bound row' ) {
        if ( !allowFunction )
            throw new Error( 'This method does not support predicate functions.' );
        val = val( new PseudoQuery( target ) );
    }

    if ( val?.toFunction )
        val = val.toFunction( target )( target );

    // eslint-disable-next-line no-underscore-dangle
    if ( val?._getResults )
        val = val._getResults({ wrap }); // eslint-disable-line no-underscore-dangle

    if ( Array.isArray( val ) )
        val = val.map( v => unwrap( v, target ) );

    if ( !val )
        return val;

    if ( val[internalError])
        throw val[internalError].err;

    if ( isPlainObject( val ) )
        val = mapValues( v => unwrap( v, target ), val );

    if ( val instanceof Date && unwrapDate )
        val = val.valueOf();

    return val;
}

export default () => ({
    uuid: () => new PseudoQuery().uuid(),
    now: () => new PseudoQuery().now(),
    args: x => ({ args: x }),
    asc: val => ({ sortBy: val, sortDirection: 'asc' }),
    desc: val => ({ sortBy: val, sortDirection: 'desc' }),

    and: ( ...values ) => new PseudoQuery( true ).and( ...values ),
    or: ( ...values ) => new PseudoQuery( false ).or( ...values ),

    row: field => new PseudoQuery()( field ),
    expr: val => new PseudoQuery( val ),
    branch: ( ...args ) => new PseudoQuery().branch( ...args )
});
