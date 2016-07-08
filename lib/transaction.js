// Copyright IBM Corp. 2015. All Rights Reserved.
// Node module: loopback-connector-postgresql
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

var debug = require('debug')('loopback:connector:postgresql:transaction');

module.exports = mixinTransaction;

/*!
 * @param {PostgreSQL} PostgreSQL connector class
 */
function mixinTransaction(PostgreSQL) {

  /**
   * Begin a new transaction
   * @param isolationLevel
   * @param cb
   */
  PostgreSQL.prototype.beginTransaction = function(isolationLevel, cb) {
    var self = this;
    debug(Date.now() + ': Begin a transaction with isolation level: %s', isolationLevel);
    this.pg.connect(this.clientConfig, function(err, connection, done) {
      if (err) return cb(err);
      if (self.transactionPIDs[connection.processID]) {
        debug(Date.now() + ': Already have connection %d', connection.processID);
        setTimeout(function() {
          return self.beginTransaction(isolationLevel, cb);
        }, 200);
        return done();
      }
      self.transactionPIDs[connection.processID] = true;
      debug(Date.now() + ': Grabbed connection %d', connection.processID);
      connection.query('BEGIN TRANSACTION ISOLATION LEVEL ' + isolationLevel,
        function(err) {
          if (err) return cb(err);
          connection.release = done;
          cb(null, connection);
        });
    });
  };

  /**
   *
   * @param connection
   * @param cb
   */
  PostgreSQL.prototype.commit = function(connection, cb) {
    debug('Commit a transaction');
    var self = this;
    debug('Committing connection %d', connection.processID);
    connection.query('COMMIT', function(err) {
      self.releaseConnection(connection, err);
      cb(err);
    });
  };

  /**
   *
   * @param connection
   * @param cb
   */
  PostgreSQL.prototype.rollback = function(connection, cb) {
    debug('Rollback a transaction');
    var self = this;
    debug('Rolling back connection %d', connection.processID);
    connection.query('ROLLBACK', function(err) {
      //if there was a problem rolling back the query
      //something is seriously messed up.  Return the error
      //to the done function to close & remove this client from
      //the pool.  If you leave a client in the pool with an unaborted
      //transaction weird, hard to diagnose problems might happen.
      self.releaseConnection(connection, err);
      cb(err);
    });
  };

  PostgreSQL.prototype.releaseConnection = function(connection, err) {
    if (typeof connection.release === 'function') {
      debug('connection.release(%d)', connection.processID);
      connection.release(err);
      connection.release = null;
    } else {
      var pool = this.pg;
      if (err) {
        debug('pool.destroy(%d)', connection.processID);
        pool.destroy(connection);
      } else {
        debug('pool.release(%d)', connection.processID);
        pool.release(connection);
      }
    }
    this.transactionPIDs[connection.processID] = false;
  };
}
