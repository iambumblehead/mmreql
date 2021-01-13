import test from 'ava';
import mockedReql from '../src/mockdbReql.js';

test( 'supports add()', async t => {
    const r = mockedReql();
    const start = Date.now();

    t.is( await r.expr( 2 ).add( 2 ).run(), 4 );
    t.is( await r.expr( 'foo' ).add( 'bar', 'baz' ).run(), 'foobarbaz' );
    // r.args() not yet fully supported
    // t.is( await r.add( r.args([ 'bar', 'baz' ]) ).run(), 'barbaz' );
    t.true( ( new Date( await r.now().add( 365 ).run() ) ).getTime() <= start + 365 );
});
