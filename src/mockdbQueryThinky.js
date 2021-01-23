// defines queries that are thinky-only and not part of rethinkdb
import {
    entries
} from 'lodash/fp.js';

const thinky = {};

/* eslint-disable no-underscore-dangle */
thinky.getJoin = ( mockdb, tableName, targetDocuments, table, args, wrap, model, createQuery, r ) => {
    const [ joins ] = args;
    entries( joins ).forEach( ([ join, joinValue ]) => {
        // model._joins[fieldName] = { model, leftKey, rightKey, belongs: true, many: true };
        const joinInfo = model._joins[join];
        if ( !joinInfo ) return;

        targetDocuments.forEach( item => {
            if ( joinInfo.many && joinInfo.belongs ) { // hasAndBelongsToMany
                const otherIds = []
                    .concat( item[`$${joinInfo.model.getTableName()}_ids`] || [])
                    .concat( createQuery( joinInfo.joinModel )
                        .filter({ [`${tableName}_${model._pk}`]: item[model._pk] })
                        .map( row => row( `${joinInfo.tableName}_${joinInfo.model._pk}` ) )
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
    return targetDocuments;
};
/* eslint-enable no-underscore-dangle */

export default thinky;
