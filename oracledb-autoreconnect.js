/* global module */

/**
 * =======================================================================================
 * Wrapper for Oracle official NodeJS driver {@see https://github.com/oracle/node-oracledb}
 * with autoreconnection feature.
 *
 * Aditional features:
 *     - Auto-connect
 *     - Auto-reconnect if connection lost.
 *     - Using promisies
 * =======================================================================================
 * @author Martin Zaloudek, CZ
 * @module ma-zal/node-oracledb-autoreconnect
 */

var Q = require('q');
/** @type {{getConnection:function}|*} */
var oracledb = require('oracledb');


/** Public API of module */
module.exports.setConnection = setConnection;
module.exports.query = query;
module.exports.connect = connect;
module.exports.disconnect = disconnect;
module.exports.transformToAssociated = transformToAssociated;


/**
 * If connection is in progres, holds promise for this connection try
 * @private
 * @type {Promise|null}
 */
var oracleConnectionPromise = null;

/**
 * Manual create connection to Oracle server. If already connected to server, it does NOT connect second one, but use the first one.
 * NOTE: In common use in not necessary to call.
 *
 * @returns {Promise} Oracledb connection object of official Oracledb driver
 */
function connect() {
	if (oracleConnectionPromise === null) {
		// disconnected. Connection is not in progress, so try to connect.
		var defer = Q.defer();
		oracledb.getConnection(oracleConnParams, function(error, value) {
			if (error) { defer.reject(error); } else { defer.resolve(value); }
		});
		oracleConnectionPromise = defer.promise.catch(function(err) {
			// Connection failed
			console.log("Error DB connecting: ", (err ? (err.message || err) : "no error message"));
			oracleConnectionPromise = null;
			return Q.reject(err && err.message ? err.message : err);
		});

	}
	return oracleConnectionPromise;
}

/**
 * Manual disconnect from DB.
 * NOTE: In common use in not necessary to call.
 * @returns {Promise}
 */
function disconnect() {
	if (oracleConnectionPromise === null) {
		return Q.resolve();
	}
	return oracleConnectionPromise.then(function(/*OracleConnection*/ oracleConnection) {
		oracleConnectionPromise = null;
		var defer = Q.defer();
		oracleConnection.release(function(error, value) {
			if (error) { defer.reject(error); } else { defer.resolve(value); }
		});
		return defer.promise.catch(function (err) {
			console.error('Oracle disconnect error: ', err.message);
			return Q.reject(err.message);
		});
	});
}

/**
 * Configuration of server connection parameters and credentials for future use in autoconnection/reconnection.
 * Note: Object with parameters is pushed directly into Oracle library into oracledb.getConnection.
 *
 * @param {OracleConnParams} _oracleConnParams
 */
function setConnection(_oracleConnParams) {
	oracleConnParams = _oracleConnParams;
}

/**
 * @type {OracleConnParams|null}
 * @description Internal store of server connection parameters and credentials.
 */
var oracleConnParams = null;


/**
 * Makes SQL query with autoconnection and reconnection on error
 * If oracle DB is not connected yet, method will try to connect automatically.
 * If DB is connected, but connection is lost (connection timeout), method will automatically try to reconnect.
 *
 * @param {string} sqlQuery - SQL query
 * @param {Array} queryParams - Array of values to SQL query
 * @returns {Promise} Result of SQL query
 */
function query(sqlQuery, queryParams) {

	return connect(oracleConnParams).then(function (/*OracleConnection*/ oracleConnection) {
		var defer = Q.defer();
		oracleConnection.execute(sqlQuery, queryParams, function(error, value) {
			if (error) { defer.reject(error); } else { defer.resolve(value); }
		});
		return defer.promise.catch(function(err) {
			// Some error
			console.log("Error executing query: ", err.message);
			if (err.message && err.message.match(/^ORA-(03114|03135|02396|01012)/)) {
				// Oracle errors:
				//     ORA-03114: not connected to ORACLE
				//     ORA-03135: connection lost contact
				//     ORA-02396: exceeded maximum idle time, please connect again
				//     ORA-01012: not logged on

				// existing connection is not active yet. Change state to disable
				console.info('Oracle connection lost. Trying to reconnect.');

				return disconnect().then(function () {
					// Second try to connect and send sql query
					return query(sqlQuery, queryParams);
				}).catch(function (err) {
					return Q.reject(err && err.message ? err.message : err);
				});

			} else {
				// Unknown error. Close this non-working connection and reject query
				return disconnect().finally(function () {
					return Q.reject(err && err.message ? err.message : err);
				});
			}
		});
	});
}

/**
 * Converts common SQL SELECT result into Array of rows with associated column names.
 * Example:
 *     Input:
 *     {
 *         metaData: [{name:"ID"},{name:"FIRSTNAME"}],
 *         rows: [[1, "JOHN"],[2,"JARYN"]]
 *     }
 *     Converted output:
 *     [
 *         {"ID":1, "FIRSTNAME":"JOHN"}
 *         {"ID":2, "FIRSTNAME":"JARYN"}
 *     ]
 *
 * @param {Object} sqlSelectResult
 * @returns {Array.<Object.<string, *>>}
 */
function transformToAssociated(sqlSelectResult) {
	var assocRows = [];
	var i, l = sqlSelectResult.metaData.length;
	sqlSelectResult.rows.map(function(row) {
		var assocRow = {};
		for (i=0; i<l; i++) {
			assocRow[sqlSelectResult.metaData[i].name] = row[i];
		}
		assocRows.push(assocRow);
	});
	return assocRows;
}

/**
 * @typedef {Object} OracleConnParams
 * @property {String} connectString
 * @property {String} user
 * @property {String} password
 */

/**
 * @typedef {Object} OracleConnection
 * @property {function} execute
 * @property {function} release
 */
