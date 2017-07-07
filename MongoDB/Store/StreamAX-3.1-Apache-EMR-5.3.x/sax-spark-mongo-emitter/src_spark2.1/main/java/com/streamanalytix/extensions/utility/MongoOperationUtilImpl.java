/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.utility;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/** The Class MongoOperationUtilImpl. */
public class MongoOperationUtilImpl implements MongoOperationUtil, MongoConfigConstant {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(MongoOperationUtilImpl.class);

    /** The mongo client. */
    private MongoClient mongoClient;

    /** The host. */
    private String host;

    /** The port. */
    private int port;

    /** The number of retries. */
    private int numberOfRetries;

    /** The time to wait. */
    private long timeToWait;

    /** Gets the mongo conn.
     *
     * @param configMap
     *            "All required configuration"
     * @return mongoClient "returning mongo connection"
     * @throws Exception
     *             "some exception" */
    @Override
    public MongoClient getMongoConn(Map<String, Object> configMap) throws Exception {
        RetryStrategyMongoImpl retryMongo = new RetryStrategyMongoImpl();
        host = configMap.get(HOST).toString();

        port = Integer.parseInt((String) configMap.get(PORTNUMBER));

        if (configMap.get(NUMBEROFRETRY) != null && !("".equals(configMap.get(NUMBEROFRETRY).toString().trim()))) {
            numberOfRetries = Integer.parseInt((String) configMap.get(NUMBEROFRETRY));
        }
        if (configMap.get(TIMETOWAITRETRY) != null && !("".equals(configMap.get(TIMETOWAITRETRY).toString().trim()))) {
            timeToWait = Long.parseLong((String) configMap.get(TIMETOWAITRETRY));
        }

        if (numberOfRetries != 0 && timeToWait != 0) {
            RetryStrategyMongoDao retry = retryMongo.retryProcess(mongoClient, numberOfRetries, timeToWait, configMap);
            mongoClient = retryDSProcess(retry, configMap);
        } else {
            RetryStrategyMongoDao retry = retryMongo.retryProcess(mongoClient, configMap);
            mongoClient = retryDSProcess(retry, configMap);
        }

        return mongoClient;
    }

    /** Retry ds process.
     *
     * @param retry
     *            "type  of RetryStrategyMongoDao"
     * @param configMap
     *            the COnfiguration
     * @return MongoClient
     * @throws Exception
     *             the Exception */

    public MongoClient retryDSProcess(RetryStrategyMongoDao retry, Map<String, Object> configMap) throws Exception {

        while (retry.shouldRetry()) {
            try {
                mongoClient = ldapConn(configMap, host, port);
                mongoClient.getAddress();
                break;
            } catch (Exception ex) {
                try {
                    retry.errorOccured();

                } catch (RetryException retryExep) {
                    LOGGER.info("Trying to connect but failed after trying all attempt" + " " + retryExep + "  : "
                            + "try to connect,actual reason for failure  ", ex);
                    throw retryExep;
                }
            }
        }
        return mongoClient;
    }

    /** Ldap conn.
     *
     * @param configMap
     *            "All required configuration"
     * @param host
     *            "host need to connect"
     * @param port
     *            "port number"
     * @return MongoClient "mongo client"
     * @throws Exception
     *             "some exception" */

    private MongoClient ldapConn(Map<String, Object> configMap, String host, int port) throws Exception {

        MongoAuthProConn mongoConn = new MongoAuthProConn();
        if (configMap.get(USERNAME) != null && !("".equals(configMap.get(USERNAME).toString().trim()))) {
            if (configMap.get(PASSWORD) != null && !("".equals(configMap.get(PASSWORD).toString().trim()))) {

                if (configMap.get(AUTHDATABASE) != null && !("".equals(configMap.get(AUTHDATABASE).toString().trim()))) {
                    mongoClient = mongoConn.getMongoClient(configMap);
                }
            }
        } else {
            mongoClient = new MongoClient(host, port);
        }
        return mongoClient;
    }

    /*
     * (non-Javadoc)
     * @see main.java.com.streamanalytix.extensions.utility.MongoOperationUtil#shardProcess(java.lang.String, java.lang.String, com.mongodb.DB,
     * java.lang.String)
     */
    public void shardProcess(String dbName, String collectionName, DB db, String shardKey) throws Exception {
        if (dbName != null && !("".equals(dbName.trim()))) {
            if (collectionName != null && !("".equals(collectionName.trim()))) {
                CommandResult shardProcess = null;
                DBObject shardDbCmd = new BasicDBObject("enablesharding", dbName);
                shardProcess = db.getSisterDB("admin").command(shardDbCmd);
                String shardCollection = dbName + "." + collectionName;
                DBObject shardCollcmd = new BasicDBObject("shardCollection", shardCollection).append("key", new BasicDBObject(shardKey, "hashed"));
                shardProcess = db.getSisterDB("admin").command(shardCollcmd);
            } else {
                LOGGER.error("please provide correct  coolectionName for sharding");
            }
        } else {
            LOGGER.error("please provide correct dbName for sharding ");
        }
    }

    /** this is comment of function.
     *
     * @param collec
     *            the collec
     * @param indexStr
     *            this is DBCollection and indexStr setMultiIndexes processed here
     * @throws Exception
     *             the exception */
    @Override
    public void setMultiIndexes(DBCollection collec, String indexStr) throws Exception {
        try {
            String[] keysAndOptions = indexStr.split("-");
            String keysStr = keysAndOptions[0];
            String optionsStr = keysAndOptions[1];

            String[] keys = keysStr.split(",");
            BasicDBObject dboptions = new BasicDBObject();
            BasicDBObject dbkeys = new BasicDBObject();

            for (String key : keys) {
                if (key.indexOf("=") != -1) {
                    String[] keyAndValue = key.split("=");
                    String indexKey = keyAndValue[0];
                    String ascOrDesc = keyAndValue[1];
                    try {
                        dbkeys.put(indexKey, Integer.parseInt(ascOrDesc));
                    } catch (NumberFormatException e) {
                        LOGGER.error("exception while converting number " + ascOrDesc, e);
                    }
                }
            }

            String[] options = optionsStr.split(",");
            for (String option : options) {
                if (option.indexOf("=") != -1) {
                    String[] optionAndValue = option.split("=");
                    String optionKey = optionAndValue[0];
                    String optionValue = optionAndValue[1];
                    String optionDataType = optionAndValue[2];
                    if ("boolean".equalsIgnoreCase(optionDataType))
                        dboptions.put(optionKey, Boolean.parseBoolean(optionValue));
                    if ("int".equalsIgnoreCase(optionDataType))
                        dboptions.put(optionKey, Integer.parseInt(optionValue));
                    if ("string".equalsIgnoreCase(optionDataType))
                        dboptions.put(optionKey, optionValue);
                }
            }
            collec.createIndex(dbkeys, dboptions);
        } catch (Exception e) {
            LOGGER.error("  exception while creating index  for " + indexStr, e);
        }

    }

}
