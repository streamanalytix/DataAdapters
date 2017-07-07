/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.utility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

/** The Class MongoAuthProConn. */
public class MongoAuthProConn implements MongoConfigConstant {

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(MongoAuthProConn.class);

    /** Gets the mongo client.
     *
     * @param configurationMap
     *            **configuration map **
     * @return MongoClient for auth mode */
    public MongoClient getMongoClient(Map<String, Object> configurationMap) {
        List<ServerAddress> seeds = mongoDBSeeds(configurationMap);
        return new MongoClient(seeds, Arrays.asList(mongoDBCredentials(configurationMap)));
    }

    /** Mongo db credentials.
     *
     * @param configurationMap
     *            **configuration map **
     * @return credential */

    private MongoCredential mongoDBCredentials(Map<String, Object> configurationMap) {

        MongoCredential credential = MongoCredential.createCredential(configurationMap.get(USERNAME).toString(), configurationMap.get(AUTHDATABASE)
                .toString(), configurationMap.get(PASSWORD).toString().toCharArray());
        return credential;
    }

    /** Mongo db seeds.
     *
     * @param configurationMap
     *            **configuration map **
     * @return seeds */

    private List<ServerAddress> mongoDBSeeds(Map<String, Object> configurationMap) {
        String host = configurationMap.get(HOST).toString();
        int port = Integer.parseInt((String) configurationMap.get(PORTNUMBER));
        List<ServerAddress> seeds = new ArrayList<ServerAddress>();
        seeds.add(new ServerAddress(host, port));
        return seeds;
    }

}
