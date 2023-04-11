import test from 'ava';
import msfrom from 'milliseconds';
import rethinkdbMocked from '../src/mmReql.mjs';

test('expressions is older than date, true', async t => {
  const { r } = rethinkdbMocked();
  const minAgeMs = msfrom.weeks(2);
  const givenAgeMs = Date.now() - msfrom.weeks(2.8);
    
  const res = await r
    .epochTime(new Date(givenAgeMs))
    .lt(r.epochTime(Date.now() - minAgeMs)).run();

  t.true(res);
});

test('expressions is older than date, false', async t => {
  const { r } = rethinkdbMocked();
  const minAgeMs = msfrom.weeks(3);
  const givenAgeMs = Date.now() - msfrom.weeks(2.8);
    
  const res = await r
    .epochTime(new Date(givenAgeMs))
    .lt(r.epochTime(Date.now() - minAgeMs)).run();

  t.false(res);
});

test('date filter, documents older than', async t => {
  const { r } = rethinkdbMocked([
    [ 'users', {
      id: 'userId-expired-1234',
      time_last_seen: new Date(Date.now() - msfrom.weeks(3.2))
    }, {
      id: 'userId-fresh-1234',
      time_last_seen: new Date(Date.now() - msfrom.weeks(2.8))
    } ]
  ]);

  // epoch time is seconds: https://rethinkdb.com/api/javascript/to_epoch_time/
  // javascript time is milliseconds
  //
  // convert javascript times to rethink times div 1000
  const minAgeMs = msfrom.weeks(3);
  const usersRowFunction = await r
    .table('users')
    .filter(row => row('time_last_seen').lt(
      r.epochTime((Date.now() - minAgeMs) / 1000)))
    .run();

  const usersRowEmbed = await r
    .table('users')
    .filter(r.row('time_last_seen').lt(
      r.epochTime((Date.now() - minAgeMs) / 1000)))
    .run();    

  t.is(usersRowFunction.length, 1);
  t.is(usersRowEmbed.length, 1);
});

