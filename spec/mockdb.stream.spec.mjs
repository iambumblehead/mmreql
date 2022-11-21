import test from 'ava';
import { Readable } from 'stream';
import { v4 as uuidv4 } from 'uuid';
import rethinkdbMocked from '../src/mockdb.mjs';

test('table().getCursor() should return a stream', async t => {
  const { r } = rethinkdbMocked([
    [ 'Rooms', {
      id: 'roomAId-1234',
      numeric_id: 755090
    }, {
      id: 'roomBId-1234',
      numeric_id: 123321
    } ]
  ]);
    
  const stream = await r
    .db('default')
    .table('Rooms')
    .getCursor();

  t.true(stream instanceof Readable);

  stream.close();
});

test('Arrays should return a stream', async t => {
  const { r } = rethinkdbMocked();
  const data = [ 10, 11, 12, 13, 14, 15, 16 ];
  const stream = await r.expr(data).getCursor();

  t.true(stream instanceof Readable);

  await new Promise(resolve => {
    let count = 0;
    stream.on('data', () => {
      count += 1;
      if (count === data.length) {
        resolve();
      }
    });
  });
});

test('changes() should return a stream', async t => {
  const { r } = rethinkdbMocked([
    [ 'TheTable', { n: 0 }, { n: -1 } ]
  ]);

  const data = [
    { n: 1 },
    { n: 2 },
    { n: 3 },
    { n: 4 },
    { n: 5 },
    { n: 6 },
    { n: 7 }
  ];

  // added include initial, so it won't hang on some extreame cases
  const stream = await r
    .db('default')
    .table('TheTable')
    .changes()
    .getCursor();

  t.true(stream instanceof Readable);

  const promise = new Promise(resolve => {
    let count = 0;
    stream.on('data', d => {
      if (d.new_val.n === -1) {
        throw new Error('initial value must not be sent');
      }

      if (typeof d.new_val.n === 'number') {
        count += 1;
        if (count === data.length) {
          resolve();
          stream.close();
        }
      }
    });
  });

  await r.db('default').table('TheTable').insert(data).run();

  await promise;
});

test('changes() should return a stream, initial value true', async t => {
  const { r } = rethinkdbMocked([
    [ 'TheTable', { n: 0 }, { n: -1 } ]
  ]);

  const data = [
    { n: 1 },
    { n: 2 },
    { n: 3 },
    { n: 4 },
    { n: 5 },
    { n: 6 },
    { n: 7 }
  ];

  // added include initial, so it won't hang on some extreame cases
  const stream = await r
    .db('default')
    .table('TheTable')
    .changes({ includeInitial: true })
    .getCursor();

  t.true(stream instanceof Readable);

  const promise = new Promise(resolve => {
    let count = 0;
    stream.on('data', d => {
      if (typeof d.new_val.n === 'number') {
        count += 1;
        if (count === data.length + 2) {
          resolve();
          stream.close();
        }
      }
    });
  });

  await r.db('default').table('TheTable').insert(data).run();

  await promise;
});

test('expr().getCursor() should return a stream', async t => {
  const { r } = rethinkdbMocked();
  const data = [ 10, 11, 12, 13, 14, 15, 16 ];

  const stream = await r
    .expr(data)
    .getCursor();

  t.true(stream instanceof Readable);

  await new Promise(resolve => {
    let count = 0;
    stream.on('data', () => {
      count += 1;
      if (count === data.length) {
        resolve();
      }
    });
  });
});

test('expr().changes().getCursor() should return a stream', async t => {
  const { r } = rethinkdbMocked([
    [ 'Rooms' ]
  ]);
  const data = [
    { n: 1 }, { n: 2 }, { n: 3 }, { n: 4 },
    { n: 5 }, { n: 6 }, { n: 7 }
  ];

  // added include initial, so it won't hang on some extreme cases
  const stream = await r
    .db('default')
    .table('Rooms')
    .changes({ includeInitial: true })
    .getCursor();

  t.true(stream instanceof Readable);

  const promise = new Promise(resolve => {
    let count = 0;
    stream.on('data', d => {
      if (!!d.new_val.n) {
        count += 1;
        if (count === data.length) {
          resolve();
          stream.close();
        }
      }
    });
  });

  await r
    .db('default')
    .table('Rooms')
    .insert(data)
    .run();

  await promise;
});

test('get().changes() should return a stream', async t => {
  const id = uuidv4();
  const { r } = rethinkdbMocked([ { db: 'cmdb' },
    [ 'Rooms' ]
  ]);

  await r
    .db('cmdb')
    .table('Rooms')
    .insert({ id })
    .run();

  const stream = await r
    .db('cmdb')
    .table('Rooms')
    .get(id)
    .changes()
    .getCursor();

  t.true(stream instanceof Readable);

  const promise = new Promise(resolve => {
    let count = 0;

    stream.on('data', () => {
      count += 1;
      if (count === 3) {
        resolve();
        stream.close();
      }
    });
  });

  await new Promise(resolve => setTimeout(resolve, 200));
  await r
    .db('cmdb')
    .table('Rooms')
    .get(id)
    .update({ update: 1 })
    .run();

  await r
    .db('cmdb')
    .table('Rooms')
    .get(id)
    .update({ update: 2 })
    .run();

  await r
    .db('cmdb')
    .table('Rooms')
    .get(id)
    .update({ update: 3 })
    .run();

  await promise;
});

test('`table` should return a stream - testing empty SUCCESS_COMPLETE', async t => {
  const { r } = rethinkdbMocked([
    [ 'Rooms' ]
  ]);

  const connection = await r.connect({
    host: 'localhost',
    port: 8080,
    user: 'user',
    password: 'pass'
  });

  const stream = await r
    .db('default')
    .table('Rooms')
    .getCursor(connection, { maxBatchRows: 1 });

  t.true(stream instanceof Readable);

  await stream.close();
  await connection.close();
});

test('Test flowing - event data', async t => {
  const { r } = rethinkdbMocked([
    [ 'Rooms', {
      id: 'roomAId-1234',
      numeric_id: 755090
    }, {
      id: 'roomBId-1234',
      numeric_id: 123321
    } ]
  ]);

  const connection = await r.connect({
    host: 'localhost',
    port: 8080,
    user: 'user',
    password: 'pass'
  });

  const stream = await r
    .db('default')
    .table('Rooms')
    .getCursor(connection, { maxBatchRows: 1 });

  t.true(stream instanceof Readable);

  await new Promise(resolve => {
    let count = 0;
    stream.on('data', () => {
      count += 1;
      if (count === 2) {
        resolve();
      }
    });
  });
  await stream.close();
  await connection.close();
});

test('Test read', async t => {
  const { r } = rethinkdbMocked([
    [ 'Rooms', {
      id: 'roomAId-1234',
      numeric_id: 755090
    }, {
      id: 'roomBId-1234',
      numeric_id: 123321
    } ]
  ]);

  const connection = await r.connect({
    host: 'localhost',
    port: 8080,
    user: 'user',
    password: 'pass'
  });

  const stream = await r
    .db('default')
    .table('Rooms')
    .getCursor(connection, { maxBatchRows: 1 });

  t.true(stream instanceof Readable);

  await new Promise((resolve, reject) => {
    stream.once('readable', () => {
      const doc = stream.read();
      if (doc === null) {
        reject(
          new Error(
            'stream.read() should not return null when readable was emitted'
          )
        );
      }
      let count = 1;
      stream.on('data', () => {
        count += 1;
        if (count === 2) {
          resolve();
        }
      });
    });
  });
  await stream.close();
  await connection.close();
});

test('Test flowing - event data (pause, resume)', async t => {
  const { r } = rethinkdbMocked([
    [ 'Rooms', {
      id: 'roomAId-1234',
      numeric_id: 755090
    }, {
      id: 'roomBId-1234',
      numeric_id: 123321
    } ]
  ]);

  const connection = await r.connect({
    host: 'localhost',
    port: 8080,
    user: 'user',
    password: 'pass'
  });

  const stream = await r
    .db('default')
    .table('Rooms')
    .getCursor(connection, { maxBatchRows: 1 });

  t.true(stream instanceof Readable);

  await new Promise((resolve, reject) => {
    let count = 0;
    stream.on('data', () => {
      count += 1;
      if (count === 2) {
        resolve();
      }
    });
    stream.pause();
    if (count > 0) {
      reject(
        new Error('The stream should have been paused')
      );
    }
    stream.resume();
  });
  await stream.close();
  await connection.close();
});

test('read with null value', async t => {
  const { r } = rethinkdbMocked([
    [ 'Rooms', [ { primaryKey: 'n' } ],
      { n: 1 },
      { n: 2 },
      { n: 3 },
      { n: 4 },
      { n: 5 },
      { n: 6 },
      { n: 7 },
      { n: 8 },
      { n: 9 },
      { n: 10 },
      { n: 11 }
    ]
  ]);

  const connection = await r.connect({
    host: 'localhost',
    port: 8080,
    user: 'user',
    password: 'pass'
  });

  const stream = await r
    .db('default')
    .table('Rooms')
    .limit(10)
    .union([ null ])
    .union(r
      .db('default')
      .table('Rooms')
      .limit(10)
    ).getCursor(connection, { maxBatchRows: 1 });

  t.true(stream instanceof Readable);

  await new Promise((resolve, reject) => {
    stream.once('readable', () => {
      let count = 0;
      stream.on('data', () => {
        count += 1;
        if (count === 20) {
          resolve();
        } else if (count > 20) {
          reject(new Error('Should not get null'));
        }
      });
    });
  });
  await stream.close();
  await connection.close();
});

test('stream grouped data', async t => {
  const { r } = rethinkdbMocked([
    [ 'Rooms', {
      id: 'roomAId-1234',
      numeric_id: 755090
    }, {
      id: 'roomBId-1234',
      numeric_id: 123321
    } ]
  ]);

  const connection = await r.connect({
    host: 'localhost',
    port: 8080,
    user: 'user',
    password: 'pass'
  });

  const stream = await r
    .db('default')
    .table('Rooms')
    .group({ index: 'id' })
    .getCursor();

  t.true(stream instanceof Readable);

  await new Promise((resolve, reject) => {
    stream.once('readable', () => {
      const doc = stream.read();
      if (doc === null) {
        reject(
          new Error(
            'stream.read() should not return null when readable was emitted'
          )
        );
      }
      let count = 1;
      stream.on('data', () => {
        count += 1;
        if (count === 2) {
          resolve();
        }
      });
    });
  });
  await stream.close();
  await connection.close();
});
