Autoreconnect for [Oracle official NodeJS driver](https://github.com/oracle/node-oracledb)
=================================================

__This library is wrapper with additional features, that uses [Oracle official NodeJS driver](https://github.com/oracle/node-oracledb) on background.__

Additional features
------------------

- __Auto-connect__. So just call sql query and does not care of anything else. 
- __Auto-reconnect__ if connection get lost.
- __Promises__, that official driver don't have. :-)


Installation
------------

    npm install oracledb-autoreconnect

Installation Prerequires
------------------------

Because this small library is only wrapper for [Oracle official NodeJS driver](https://github.com/oracle/node-oracledb), so please read [Installing node-oracledb](https://github.com/oracle/node-oracledb#-3-installation) or [Installing node-oracledb - extended version](https://github.com/oracle/node-oracledb/blob/master/INSTALL.md).
 
NOTE: If you will have some issues, try (as test) install `npm install oracledb` and solve, why it fails.


Example using library
---------------------

    // Include this library
    
    var db = require('oracledb-autoreconnect');


    // Set connection parameters on application init.

    db.setConnection({
        user          : "hr",
        password      : "welcome",
        connectString : "localhost/XE"
    });


    // Just make query anytime.
    // Don't care about connect to db. It is solved automatically.

    var queryString = "SELECT id, firstname, lastname FROM db.persons WHERE firstname LIKE :1 AND lastname LIKE :2";
    var queryParams = ["John","Brown"];
    db.query(queryString, queryParams).then(function (dbResult) {
    
        // Convert result to better structured object
        var assocDbResult = db.transformToAssociated(dbResult);
        
        // Print results to console
        assocDbResult.map(function(person) {
            console.log(person.firstname + " " + person.lastname + " has id " + person.id);
        });
    });


API of library
--------------

### function setConnection(oracledbConnectionObject)

Configuration of server connection parameters and credentials for future use in autoconnection/reconnection.

Note: Object with parameters will be pushed directly into Oracle library into official `oracledb.getConnection` method.

* `@returns {undefined} - Does return nothing`


### function query(query, queryParams)

Makes SQL query with autoconnection and reconnection on error
If oracle DB is not connected yet, method will try to connect automatically.
If DB is connected, but connection is lost (connection timeout), method will automatically try to reconnect.
 
* `@param {String} query - SQL query`
* `@param {Array} queryParams - Array of values to SQL query`
* `@returns {Promise} - Result of SQL query`

### function transformToAssociated(sqlResult);

Converts common SQL SELECT result into Array of rows with associated column names.

Example:

    Input:
    {
        metaData: [{name:"ID"},{name:"FIRSTNAME"}],
        rows: [[1, "JOHN"],[2,"JARYN"]]
    }
    Converted output:
    [
        {"ID":1, "FIRSTNAME":"JOHN"}
        {"ID":2, "FIRSTNAME":"JARYN"}
    ]
    
* `@param {Object} sqlSelectResult`
* `@returns {Array.<Object.<string,*>>}`


### function connect()

Manual create connection to Oracle server. If already connected to server, it does NOT connect second one, but use the first one.
NOTE: In common use in not necessary to call.

* `@returns {Promise} - Oracledb connection object of official Oracledb driver`


### function disconnect()

Manual disconnect from DB.

NOTE: In common use in not necessary to call.

* `@returns {Promise}`
