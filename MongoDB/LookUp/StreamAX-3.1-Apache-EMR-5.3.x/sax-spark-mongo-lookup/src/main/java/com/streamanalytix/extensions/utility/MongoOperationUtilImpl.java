package com.streamanalytix.extensions.utility;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.MongoClient;

/** The Class MongoOperationUtilImpl. */
public class MongoOperationUtilImpl implements MongoOperationUtil, MongoConfigConstant {

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(MongoOperationUtilImpl.class);

    /** The mongo client. */
    private MongoClient mongoClient;

    /** The host. */
    private String host;

    /** The port. */
    private int port;

    /** The number of retries. */
    private int numberOfRetries; // total number of tries

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
                    LOGGER.info("catch the exception and do recursion for trying to connect again");
                    retry.errorOccured();

                } catch (RetryException retryExep) {
                    LOGGER.error("Trying to connect but failed after trying all attempt" + " " + retryExep + "  : "
                            + "try to connect,actual reason for failure  " + "  " + ex);
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
}
