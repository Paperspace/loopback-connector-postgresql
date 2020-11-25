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
    debug('Begin a transaction with isolation level: %s', isolationLevel);
    var self = this;
    self.pg.connect(function(err, connection, done) {
      if (err) return cb(err);
      debug('Begin transaction %d', connection.processID);
      connection.done = done;
      connection.query('BEGIN TRANSACTION ISOLATION LEVEL ' + isolationLevel,
        function(err) {
          if (err) {
            self.releaseConnection(connection, err);
            return cb(err);
          }
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
    debug('Release transaction connection %d', connection.processID);
    connection.done(err);
    connection.done = null;
  };
}
