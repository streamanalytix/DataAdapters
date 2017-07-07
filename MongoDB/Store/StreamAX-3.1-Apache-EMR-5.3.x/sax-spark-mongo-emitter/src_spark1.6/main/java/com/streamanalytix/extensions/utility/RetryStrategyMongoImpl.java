/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.utility;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.MongoClient;

/** The Class RetryStrategyMongoImpl. */
public class RetryStrategyMongoImpl {
    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(RetryStrategyMongoImpl.class);
    private MongoOperationUtilImpl mongoOperation = new MongoOperationUtilImpl();

    /** @param mongoClient
     *            "mongo client "
     * @param configMap
     *            "configuration"
     * @return RetryStrategyMongoDao */

    public RetryStrategyMongoDao retryProcess(MongoClient mongoClient, Map<String, Object> configMap) {
        RetryStrategyMongoDao retry = new RetryStrategyMongoDao();
        return retry;

    }

    /** @param mongoClient
     *            "mongo client "
     * @param numberOfRetries
     *            "number try to connect"
     * @param timeToWait
     *            "waiting time "
     * @param configMap
     *            "all config"
     * @return RetryStrategyMongoDao */

    public RetryStrategyMongoDao retryProcess(MongoClient mongoClient, int numberOfRetries, long timeToWait, Map<String, Object> configMap) {
        RetryStrategyMongoDao retry = new RetryStrategyMongoDao(numberOfRetries, timeToWait);
        return retry;

    }

}
