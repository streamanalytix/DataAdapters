package com.streamanalytix.extensions.utility;

/** The Interface MongoConfigConstant. */
public interface MongoConfigConstant {

    /** The host. */
    String HOST = "host";

    /** The portnumber. */
    String PORTNUMBER = "port";

    /** The dbname. */
    String DBNAME = "dbName";

    /** The collectionname. */
    String COLLECTIONNAME = "dbCollectionName";

    /** The outputcolumn. */
    String OUTPUTCOLUMN = "outPutColumns";

    /** The mongoconnectionuri. */
    String MONGOCONNECTIONURI = "mongo_Conn_Uri";

    // For Retry mechanism
    /** The numberofretry. */
    String NUMBEROFRETRY = "numberOfRetries";

    /** The timetowaitretry. */
    String TIMETOWAITRETRY = "timeToWait";

    // For authentication
    /** The username. */
    String USERNAME = "user";

    /** The password. */
    String PASSWORD = "password";

    /** The authdatabase. */
    String AUTHDATABASE = "authenticationDatabase";

}
