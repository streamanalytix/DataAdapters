package com.streamanalytix.extensions.utility;

import java.util.Map;

import com.mongodb.MongoClient;

/** The Class RetryStrategyMongoImpl.
 *
 * @author abhijit.biswas */
public class RetryStrategyMongoImpl {

    /** Retry process.
     *
     * @param mongoClient
     *            "mongo client "
     * @param configMap
     *            "configuration"
     * @return RetryStrategyMongoDao */

    public RetryStrategyMongoDao retryProcess(MongoClient mongoClient, Map<String, Object> configMap) {
        RetryStrategyMongoDao retry = new RetryStrategyMongoDao();
        return retry;

    }

    /** Retry process.
     *
     * @param mongoClient
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
