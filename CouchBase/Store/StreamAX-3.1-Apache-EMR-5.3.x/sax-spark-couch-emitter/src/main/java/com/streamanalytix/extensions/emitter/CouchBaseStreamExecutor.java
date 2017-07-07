/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.emitter;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.streamanalytix.extensions.exceptions.ServiceNotAvailableException;
import com.streamanalytix.extensions.retry.RetryNTimes;
import com.streamanalytix.extensions.utils.CouchbaseConnectionManager;
import com.streamanalytix.extensions.utils.CustomConstants;
import com.streamanalytix.framework.api.spark.processor.DStreamProcessor;

import net.minidev.json.JSONObject;

/** The Class CouchBaseStreamExecutor. Handles Stream insertion of documents into Couchbase bucket. */
public class CouchBaseStreamExecutor implements DStreamProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(CouchBaseBatchExecutor.class);

    /** The bucket. */
    private static Bucket bucket;

    /** The cluster. */
    private static Cluster cluster;

    /** The bucket name. */
    private static String bucketName;

    /** The bucket password. */
    private static String bucketPassword;

    /** The cluster nodes. */
    private static String[] clusterNodes;

    /** The connect timeout. */
    private static long connectTimeout;

    /** The cluster admin username. */
    private static String clusterAdminUsername;

    /** The cluster admin password. */
    private static String clusterAdminPassword;
    // Default replication factor is 0. Can have max 3 Replicas.
    /** The replicas. */
    private static int replicas = 0;
    // Default TTL is 5mins
    /** The ttl. */
    private static int ttl = CustomConstants.Values.DEFAULT_TTL;
    // Default Retry count is 3.
    /** The retry count. */
    private static int retryCount = CustomConstants.Values.DEFAULT_RETRY_COUNT;

    /** The create new bucket. */
    private static boolean createNewBucket = true;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.SparkProcessor#process(java.lang.Object, java.util.Map)
     */
    @Override
    public JavaDStream<JSONObject> process(JavaDStream<JSONObject> dStream, Map<String, Object> configMap) {
        if (cluster == null) {
            init(configMap);
        }

        JavaDStream<JSONObject> dStreamResponse = dStream.map(new Function<JSONObject, JSONObject>() {
            private static final long serialVersionUID = 1L;

            @Override
            public JSONObject call(JSONObject jsonObject) throws Exception {
                if (cluster != null && !jsonObject.toJSONString().isEmpty()) {
                    LOGGER.info(jsonObject.keySet().size());
                    JSONObject content = new JSONObject();
                    for (String key : jsonObject.keySet()) {
                        if (!key.equals(CustomConstants.Literal.SAX_MANDATORY_FIELD_1)) {
                            content.put(key, jsonObject.get(key));
                        }
                    }
                    if (bucket == null) {
                        bucket = CouchbaseConnectionManager.openBucket(cluster, clusterAdminUsername, clusterAdminPassword, bucketName,
                                bucketPassword, createNewBucket, replicas);
                    }
                    LOGGER.debug("Creating couchbase document " + content + " and Inserting into couchbase bucket: " + bucketName);
                    bucket.upsert(JsonDocument.create(UUID.randomUUID().toString(), ttl, JsonObject.fromJson(content.toJSONString())));
                    return content;
                }
                return jsonObject;
            }
        });
        return dStreamResponse;
    }

    /** This init method is used to initialize the configuration.
     * 
     * @param configMap
     *            This is a parameter to init as Map type containing all the input/value pairs from SAX UI for initializing the Couchbase Connection. */
    public static void init(Map<String, Object> configMap) {
        LOGGER.debug("Initializing Configuration  Map.");
        try {
            if (configMap.containsKey(CustomConstants.Literal.BUCKET_NAME)) {
                bucketName = (String) configMap.get(CustomConstants.Literal.BUCKET_NAME);
            }

            if (configMap.containsKey(CustomConstants.Literal.CREATE_NEW_BUCKET)) {
                createNewBucket = Boolean.valueOf((String) configMap.get(CustomConstants.Literal.CREATE_NEW_BUCKET));
            }

            if (configMap.containsKey(CustomConstants.Literal.BUCKET_PASSWORD)) {
                bucketPassword = (String) configMap.get(CustomConstants.Literal.BUCKET_PASSWORD);
            }

            if (configMap.containsKey(CustomConstants.Literal.CONNECT_TIMEOUT)) {
                connectTimeout = Long.parseLong((String) configMap.get(CustomConstants.Literal.CONNECT_TIMEOUT));
            }

            if (configMap.containsKey(CustomConstants.Literal.CLUSTER_NODES)) {
                clusterNodes = ((String) configMap.get(CustomConstants.Literal.CLUSTER_NODES)).split(CustomConstants.Delimiter.COMMA);
            } else {
                cleanup();
                throw new RuntimeException("You must provide couchbase cluster node, information.");
            }

            if (configMap.containsKey(CustomConstants.Literal.CLUSTER_ADMIN_USERNAME)) {
                clusterAdminUsername = (String) configMap.get(CustomConstants.Literal.CLUSTER_ADMIN_USERNAME);
            }

            if (configMap.containsKey(CustomConstants.Literal.CLUSTER_ADMIN_PASSWORD)) {
                clusterAdminPassword = (String) configMap.get(CustomConstants.Literal.CLUSTER_ADMIN_PASSWORD);
            }

            if (configMap.containsKey(CustomConstants.Literal.DOCUMENT_TTL)) {
                try {
                    ttl = Integer.parseInt(configMap.get(CustomConstants.Literal.DOCUMENT_TTL).toString());
                } catch (NumberFormatException e) {
                    LOGGER.error("TTL is not an integer. Using default time 5mins as TTL. " + "Record will be removed from couchbase after 5mins.");
                    // Do Nothing use default TTL
                }
            }

            if (configMap.containsKey(CustomConstants.Literal.DOCUMENT_REPLICAS)) {
                try {
                    replicas = Integer.parseInt(configMap.get(CustomConstants.Literal.DOCUMENT_REPLICAS).toString());
                    LOGGER.info("Persisting with replication factor: " + replicas);
                } catch (NumberFormatException e) {
                    // Do Nothing use default Replication factor
                    LOGGER.error("Persisting with default replication factor: " + replicas);
                }
            }

            if (configMap.containsKey(CustomConstants.Literal.RETRY_COUNT)) {
                try {
                    retryCount = Integer.parseInt(configMap.get(CustomConstants.Literal.RETRY_COUNT).toString());
                } catch (NumberFormatException e) {
                    LOGGER.warn("Retry Count is not an integer. Using default retry count as 3.");
                    // Do Nothing use default TTL
                }
            }

            // For Couchbase cluster any one URL is enough to test for server
            // running status.
            String host = clusterNodes[CustomConstants.Numbers.ZERO].split(CustomConstants.Delimiter.COLON)[CustomConstants.Numbers.ZERO];
            int port = Integer
                    .parseInt(clusterNodes[CustomConstants.Numbers.ZERO].split(CustomConstants.Delimiter.COLON)[CustomConstants.Numbers.ONE]);

            if (RetryNTimes.serviceAvailable(host, port, retryCount)) {
                cluster = CouchbaseConnectionManager.getCouchbaseClient(connectTimeout, clusterNodes);
                if (bucket == null) {
                    bucket = CouchbaseConnectionManager.openBucket(cluster, clusterAdminUsername, clusterAdminPassword, bucketName, bucketPassword,
                            createNewBucket, replicas);
                    LOGGER.debug("Bucket Opened ......");
                }
            } else {
                throw new ServiceNotAvailableException("Service not available");
            }
        } catch (Exception e) {
            cleanup();
            throw new IllegalStateException(e);
        }
        LOGGER.debug("Configuration  Map Initialized successfully.");
    }

    /** Cleanup method to disconnect form all open buckets and shuts down the CouchbaseEnvironment if it is the exclusive owner with a custom timeout. */
    public static void cleanup() {
        if (cluster != null) {
            LOGGER.debug("Couchbase connection cleanup..");
            // cluster.disconnect();
        }
    }
}