import { inspect } from 'util';

import {
    isPlainObject
} from 'lodash/fp.js';

/* eslint-disable no-underscore-dangle */
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

const thinkyType = {
    point: {},
    virtual: {},
    any: {}
};

thinkyType.string = {
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
};

thinkyType.boolean = {
    _check: val => typeof val === 'boolean'
};

thinkyType.number = {
    _check: val => typeof val === 'number',
    min: ( val, [ min ]) => val >= min,
    max: ( val, [ max ]) => val <= max,
    integer: val => Number.isInteger( val )
};

thinkyType.date = {
    _check: val => typeof val === 'number'
        || ( typeof val === 'function' && val.$mustInterpret )
        || (
            val instanceof Date
                && typeof val.valueOf() === 'number' // on-change error check
                && Object.prototype.toString.call( val ) === '[object Date]' // on-change error check
        ),
    min: ( val, [ min ]) => val.valueOf() >= min.valueOf(),
    max: ( val, [ max ]) => val.valueOf() <= max.valueOf()
};

thinkyType.buffer = { _check: val => Buffer.isBuffer( val ) };

thinkyType.object = {
    _check: val => isPlainObject( val ),
    _init: typedef => {
        // This is how thinky works :/
        typedef._schema = {};
    },
    schema: ( val, [ schema ]) => {
        Object.entries( schema ).forEach( ([ key, typedef ]) => {
            if ( isPlainObject( typedef ) )
                typedef = new Typedef( 'object', thinkyType.object ).schema( typedef );
            try {
                typedef._checkType( val[key]);
            } catch ( err ) {
                throw new Error( `Schema validation failed for ${key}: ${err.message}` );
            }
        });
        return true; // Will throw
    }
};

thinkyType.array = {
    _check: val => Array.isArray( val ),
    _init: typedef => {
        typedef._schema = new Typedef( 'any', thinkyType.any );
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
};

const thinkyTypeDefs = Object.entries( thinkyType )
    .reduce( ( obj, [ typeName, funcs ]) => Object.assign( obj, {
        [typeName]: options => new Typedef( typeName, funcs, options )
    }), {});

const getTypedefInstance = typedef => typedef && typedef.type;

Object.assign( thinkyTypeDefs, {
    isVirtual: typedefInstance => getTypedefInstance( typedefInstance ) === 'virtual',
    isArray: typedefInstance => getTypedefInstance( typedefInstance ) === 'array',
    isObject: typedefInstance => getTypedefInstance( typedefInstance ) === 'object',
    isDate: typedefInstance => getTypedefInstance( typedefInstance ) === 'date'
});

// eslint-disable-next-line camelcase
const getTypedef_default = ( typedef, def = null ) => (
    typedef && typedef._default ) || def;

// eslint-disable-next-line camelcase
const getTypedef_schema = ( typedef, def = null ) => (
    typedef && typedef._schema ) || def;
/* eslint-enable no-underscore-dangle */

export {
    Typedef,
    thinkyType,
    thinkyTypeDefs,
    getTypedef_default, // eslint-disable-line camelcase
    getTypedef_schema // eslint-disable-line camelcase
};
