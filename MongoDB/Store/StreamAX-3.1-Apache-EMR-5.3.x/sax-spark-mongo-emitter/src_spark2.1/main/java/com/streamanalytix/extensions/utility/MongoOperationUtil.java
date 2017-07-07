/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.utility;

import java.util.Map;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/** The Interface MongoOperationUtil. */
public interface MongoOperationUtil {

    /** Gets the mongo conn.
     *
     * @param configMap
     *            "all config"
     * @return nothing
     * @throws Exception
     *             "exception throw" */
    MongoClient getMongoConn(Map<String, Object> configMap) throws Exception;

    /** Shard process.
     *
     * @param dbName
     *            the database name
     * @param collectionName
     *            the table name
     * @param db
     *            DB object
     * @param shardKey
     *            key for sharding
     * @throws Exception
     *             the Exception.Need to handle */
    void shardProcess(String dbName, String collectionName, DB db, String shardKey) throws Exception;

    /** @param collec
     *            coollection
     * @param indexStr
     *            the indexStr
     * @throws Exception
     *             the Exception */
    void setMultiIndexes(DBCollection collec, String indexStr) throws Exception;
}
