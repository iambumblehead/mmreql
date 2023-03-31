// boilerplate for connect objects

const connectionCreate = (db, host, port, user) => ({
  connectionOptions: {
    host,
    port
  },
  db,
  options: {
    host,
    port
  },
  clientPort: port,
  clientAddress: host,
  timeout: 20,
  pingInterval: -1,
  silent: false,
  socket: {
    runningQueries: {},
    isOpen: true,
    nextToken: 3,
    buffer: {
      type: 'Buffer',
      data: []
    },
    mode: 'response',
    connectionOptions: {
      host,
      port
    },
    user,
    password: {
      type: 'Buffer',
      data: [ 0 ]
    }
  },
  close: function () {
    this.open = false;
  }
});

const connectionPoolCreate = (db, host, port, user, pass) => ({
  draining: false,
  healthy: true,
  discovery: false,
  connParam: {
    db: db,
    user,
    password: '',
    buffer: 1,
    max: 1,
    timeout: 20,
    pingInterval: -1,
    timeoutError: 1000,
    timeoutGb: 3600000,
    maxExponent: 6,
    silent: false
  },
  servers: [ {
    host,
    port
  } ],
  serverPools: [ {
    draining: false,
    healthy: true,
    connections: [
      connectionCreate(db, host, port, user)
    ],
    timers: {},
    buffer: 1,
    max: 1,
    timeoutError: 1000,
    timeoutGb: 3600000,
    maxExponent: 6,
    silent: false,
    server: {
      host,
      port
    },
    connParam: {
      db,
      user,
      password: pass,
      timeout: 20,
      pingInterval: -1,
      silent: false
    }
  } ]
});

const connectionPoolMasterCreate = (db, host, port, user, pass) => ({
  isHealthy: true,
  _events: {},
  _eventsCount: 0,
  _maxListeners: undefined,
  draining: false,
  healthy: true,
  discovery: false,
  connParam: {
    db: db,
    user,
    password: pass || '',
    buffer: 1,
    max: 1,
    timeout: 20,
    pingInterval: -1,
    timeoutError: 1000,
    timeoutGb: 3600000,
    maxExponent: 6,
    silent: false
  },
  servers: [ {
    host,
    port
  } ],
  serverPools: [ {
    _events: {},
    _eventsCount: 4,
    _maxListeners: undefined,
    draining: false,
    healthy: true,
    connections: [],
    timers: {},
    buffer: 1,
    max: 1,
    timeoutError: 1000,
    timeoutGb: 3600000,
    maxExponent: 6,
    silent: false,
    log: console.log,
    server: {
      host,
      port
    },
    connParam: [ {
      db: db,
      user,
      password: pass || '',
      timeout: 20,
      pingInterval: -1,
      silent: false
    } ]
  } ]
});

const connectionTypes = {
  connection: connectionCreate,
  connectionPool: connectionPoolCreate,
  connectionPoolMaster: connectionPoolMasterCreate
};

export default function mmConn (type, db, host, port, user, pass) {
  return Object.assign(
    this, connectionTypes[type](db, host, port, user, pass));
}
