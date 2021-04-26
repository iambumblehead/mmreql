import test from 'ava';
import rethinkdbMocked from '../src/mockdb.mjs';

test( '`distinct` should work', async t => {
    const { r } = rethinkdbMocked();
    const result = await r
        .expr([ 1, 2, 3, 1, 2, 1, 3, 2, 2, 1, 4 ])
        .distinct()
        .orderBy( row => row )
        .run();

    t.deepEqual( result, [ 1, 2, 3, 4 ]);
});

test( '`r.distinct` should work', async t => {
    const { r } = rethinkdbMocked();
    const result = await r
        .distinct([ 1, 2, 3, 1, 2, 1, 3, 2, 2, 1, 4 ])
        .orderBy( row => row )
        .run();

    t.deepEqual( result, [ 1, 2, 3, 4 ]);
});

test( '`distinct` should work with an index', async t => {
    const { r } = rethinkdbMocked([
        { db: 'jobrunner' },
        [ 'JobEvents', {
            jobId : 1,
            name : 'job1'
        },{
            jobId : 2,
            name : 'job2'
        },{
            jobId : 1,
            name : 'job1-log'
        } ]
    ]);

    await r.db( 'jobrunner' ).table( 'JobEvents' ).indexCreate( 'jobId' ).run();
    
    const result = await r
        .db( 'jobrunner' )
        .table( 'JobEvents' )
        .distinct({ index: 'jobId' })
        .run();

    t.deepEqual( result, [ 1, 2 ]);
});
