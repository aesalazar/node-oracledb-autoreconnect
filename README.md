Autoreconnect for [Oracle official NodeJS driver](https://github.com/oracle/node-oracledb)
=================================================

__This library is wrapper with aditional features, that uses [Oracle official NodeJS driver](https://github.com/oracle/node-oracledb) on background.__

Aditional features
------------------

- __Auto-connect__. So just call sql query and does not care of anything else. 
- __Auto-reconnect__ if connection get lost.
- __Promisies__, that official driver doesn't use. :-(


Installation
------------

Becasuse this small library is only wrapper for [Oracle official NodeJS driver](https://github.com/oracle/node-oracledb), so please read [Installing node-oracledb](https://github.com/oracle/node-oracledb/blob/master/INSTALL.md) instructions. 


Example using library
---------------------

    // Include this library
    
    var oracledbWrapper = require('oracledb-autoconnect');


    // Set connection parameters on application init.

    oracledbWrapper.setConnection({
        user          : "hr",
        password      : "welcome",
        connectString : "localhost/XE"
    });


    // Just make query anytime.
    // Don't care about connect to db. It is solved automatically.

    var query = "select ID, FIRSTNAME, LASTNAME FROM db.persons " WHERE FIRSTNAME LIKE :1 AND LASTNAME LIKE :2";
    var queryParams = ["John","Brown"];
    oracledbWrapper.oracleQuery(query, usernames521).then(function (dbResult) {
        dbResult.map(function(person) {
            console.log(person.FIRSTNAME + " " + person.LASTNAME + " has ID " + person.ID);
        });
    });


API of library
--------------

### function setOracleConnection(oracledbConnectionObject)

Configuration of server connection parameters and credentials for future use in autoconnection/reconnection.

Note: Object with parameters will be pushed directly into Oracle library into official `oracledb.getConnection` method.

* `@returns {undefined} - Does return nothing`


### function oracleQuery(query, queryParams)

Makes SQL query with autoconnection and reconnection on error
If oracle DB is not connected yet, method will try to connect automatically.
If DB is connected, but connection is lost (connection timeout), method will automatically try to reconnect.
 
* `@param {String} query - SQL query`
* `@param {Array} queryParams - Array of values to SQL query`
* `@returns {Promise} - Result of SQL query`


### function oracleConnect()

Manual create connection to Oracle server. If already connected to server, it does NOT connect second one, but use the first one.
NOTE: In commom use in not neccessary to call.

* `@returns {Promise} - Oracledb connection object of official Oracledb driver`


### function oracleDisconnect()

Manual disconnect from DB.

NOTE: In commom use in not neccessary to call.

* `@returns {Promise}`
