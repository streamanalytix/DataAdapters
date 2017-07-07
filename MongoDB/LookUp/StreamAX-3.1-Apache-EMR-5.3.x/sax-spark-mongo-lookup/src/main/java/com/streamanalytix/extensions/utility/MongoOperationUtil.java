package com.streamanalytix.extensions.utility;

import java.util.Map;

import com.mongodb.MongoClient;

/** The Interface MongoOperationUtil. */
public interface MongoOperationUtil {
    /** @param configMap
     *            "all config"
     * @return nothing
     * @throws Exception
     *             "exception throw" */
    MongoClient getMongoConn(Map<String, Object> configMap) throws Exception;

}
