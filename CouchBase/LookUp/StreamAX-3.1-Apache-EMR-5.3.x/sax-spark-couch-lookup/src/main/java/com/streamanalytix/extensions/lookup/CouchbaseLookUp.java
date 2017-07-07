/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.lookup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.streamanalytix.extensions.retry.RetryNTimes;
import com.streamanalytix.extensions.utils.CouchbaseConnectionManager;
import com.streamanalytix.extensions.utils.CustomConstants;
import com.streamanalytix.framework.api.udf.Function;

/** Couchbase_Lookup is an enricher functionality in which data will be processed in order to enrich it with desired properties. */
public class CouchbaseLookUp implements Function {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The cluster. */
    private CouchbaseCluster cluster;

    /** The bucket password. */
    private String bucketPassword;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(CouchbaseLookUp.class);

    /** This init method is used to initialize the configuration.
     *
     * @param configMap
     *            the config map */
    @Override
    public void init(Map<String, Object> configMap) {
        LOGGER.debug("Inside the CouchbaseLookUp : init.");
        Long timeOut = configMap.containsKey(CustomConstants.Literal.DELAY_BETWEEN_CONNECTION_RETRIES) ? Long.parseLong((String) configMap
                .get(CustomConstants.Literal.DELAY_BETWEEN_CONNECTION_RETRIES)) : CustomConstants.Values.DEFAULT_DELAY_BETWEEN_CONNECTION_RETRIES;
        String[] hostURLs = configMap.containsKey(CustomConstants.Literal.HOSTNAME) ? ((String) configMap.get(CustomConstants.Literal.HOSTNAME))
                .split(",") : new String[0];
        bucketPassword = configMap.containsKey(CustomConstants.Literal.BUCKET_PASSWORD) ? ((String) configMap
                .get(CustomConstants.Literal.BUCKET_PASSWORD)) : null;
        Integer connectionRetryCount = configMap.containsKey(CustomConstants.Literal.CONNECTION_RETRY_COUNT) ? Integer.parseInt((String) configMap
                .get(CustomConstants.Literal.CONNECTION_RETRY_COUNT)) : 2;

        StringBuffer sb = new StringBuffer();
        int totalURLs = hostURLs.length;
        int count = 0;
        for (String host : hostURLs) {
            count++;
            String node = host.split(":")[0];
            int port = Integer.parseInt(host.split(":")[1]);
            if (RetryNTimes.serviceAvailable(node, port, connectionRetryCount)) {
                if (count == totalURLs) {
                    sb.append(host);
                } else {
                    sb.append(host + ",");
                }
            } else {
                LOGGER.info("Couchbase : Services not available for host : " + host + ", port : " + port);
            }
        }
        LOGGER.info("Couchbase is up and running.. ");

        this.cluster = CouchbaseConnectionManager.getCouchbaseClient(timeOut, sb.toString().split(","));
        LOGGER.debug("Couchbase connection established.. ");
    }// end of init()

    /** The process method is used to write custom implementation.
     *
     * @param arguments
     *            the arguments
     * @return the object
     * @throws Exception
     *             the exception */
    @Override
    public Object execute(Object... arguments) throws Exception {
        LOGGER.debug("Inside the execute() of Couchbase-LookUp function");
        String bucketName = null != arguments[0] ? arguments[0].toString() : null;
        String query = null != arguments[1] ? arguments[1].toString() : null;

        if (null != bucketName && null != query) {
            List<Map<String, String>> returnedList = new ArrayList<Map<String, String>>();
            N1qlQueryResult result = null;
            try {
                Bucket bucket;
                if (StringUtils.isEmpty(bucketPassword)) {
                    bucket = cluster.openBucket(bucketName);
                } else {
                    bucket = cluster.openBucket(bucketName, bucketPassword);
                }
                result = bucket.query(N1qlQuery.simple(query));

                JsonObject jsonObject = (JsonObject) result.allRows().get(0).value().get(bucketName);
                Set<String> fieldNames = jsonObject.getNames();
                Map<String, String> map = new HashMap<String, String>();
                for (String key : fieldNames) {
                    String value = (String) jsonObject.get(key);
                    map.put(key, value);
                }
                returnedList.add(map);
            } catch (Exception e) {
                LOGGER.error("Exception encountered : " , e);
            }

            if (null != result && "success".equalsIgnoreCase(result.status())) {
                return returnedList.toArray();
            } else {
                return new ArrayList<Map<String, String>>().toArray();
            }
        } else {
            LOGGER.debug("Couchbase Lookup arguments are missing..");
            return new ArrayList<Map<String, String>>().toArray();
        }
    }// end of execute()

    /** The cleanup method to free the resources. */
    @Override
    public void cleanup() {
    }// end of cleanup()

}
