/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/** Handle Couchbase connections, creating cluster, creating, opening and closing buckets. */
public class CouchbaseConnectionManager {
    private static final Log LOGGER = LogFactory.getLog(CouchbaseConnectionManager.class);

    /** Connect to couchbase server and create cluster.
     * 
     * @param connectTimeout
     *            This parameter is the timeout for bucket connection in ms.
     * @param nodes
     *            This parameter is the Comma separated list of Couchbase cluster nodes
     * @return It returns the CouchbaseCluster instance */
    public static CouchbaseCluster getCouchbaseClient(Long connectTimeout, String... nodes) {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().connectTimeout(connectTimeout)
                .requestBufferSize(CustomConstants.Values.BUFFER_SIZE).build();
        LOGGER.info("Creating cocuhbase cluster with " + nodes + " .");
        CouchbaseCluster cluster = CouchbaseCluster.create(env, nodes);
        return cluster;
    }

    /** Close bucket and release resources when not required.
     * 
     * @param bucket
     *            This parameter is the couchbase bucket object */
    public static void closeBucket(Bucket bucket) {
        if (bucket != null && !bucket.isClosed()) {
            LOGGER.info("Closing bucket " + bucket.name() + " .");
            bucket.async().close();
        }
    }

    /** Opens requested couchbase bucket.
     * 
     * @param cluster
     *            This parameter is the Couchbase Cluster object
     * @param clusterAdminUsername
     *            This parameter is the Couchbase Cluster admin username
     * @param clusterAdminPassword
     *            This parameter is the Couchbase Cluster admin password
     * @param bucketName
     *            This parameter is the bucket name, where data needs to be persist into couchbase cluster
     * @param bucketPassword
     *            This parameter is the password for bucket level data protection
     * @param createNewBucket
     *            It will create a new bucket if value is true else not
     * @param replicas
     *            This parameter is the no of replicas.
     * @return It returns the bucket object. */
    public static Bucket openBucket(Cluster cluster, String clusterAdminUsername, String clusterAdminPassword, String bucketName,
            String bucketPassword, boolean createNewBucket, int replicas) {
        LOGGER.debug("Opening the bucket " + bucketName);
        Bucket bucket = null;
        if (createNewBucket) {
            ClusterManager clusterManager = cluster.clusterManager(clusterAdminUsername, clusterAdminPassword);
            createBucket(bucketName, bucketPassword, createNewBucket, clusterManager, replicas, cluster);
        }
        if (bucketPassword != null) {
            bucket = cluster.openBucket(bucketName, bucketPassword);
        } else {
            bucket = cluster.openBucket(bucketName);
        }
        if (bucket == null) {
            throw new RuntimeException("Unable to open bucket, please check configuration.");
        } else {
            LOGGER.debug("Opened bucket is " + bucket);
        }
        return bucket;
    }

    /** Create a new bucket User want to create new bucket if it is not exists.
     * 
     * @param bucketName
     *            This parameter is the bucket name, where data needs to be persist into couchbase cluster
     * @param bucketPassword
     *            This parameter is the password for bucket level data protection
     * @param createnew
     *            It will create a new bucket if value is true else not
     * @param clusterManager
     *            This parameter is the Couchbase Cluster Manager object
     * @param replicas
     *            This parameter is the no of replicas.
     * @param cluster
     *            It returns the couchbase cluster object. */
    private static void createBucket(String bucketName, String bucketPassword, boolean createnew, ClusterManager clusterManager, int replicas,
            Cluster cluster) {
        if (!clusterManager.hasBucket(bucketName) && createnew) {
            BucketSettings bucketSettings = null;
            if (bucketPassword != null) {
                LOGGER.info("creating new bucket " + bucketName + " with password.");
                bucketSettings = DefaultBucketSettings.builder().type(BucketType.COUCHBASE).name(bucketName).password(bucketPassword)
                        .quota(CustomConstants.Values.BUCKET_QUOTA) // megabytes
                        .replicas(0).indexReplicas(false).enableFlush(false).build();
                clusterManager.insertBucket(bucketSettings);
            } else {
                LOGGER.info("creating new bucket " + bucketName + " without password.");
                bucketSettings = DefaultBucketSettings.builder().type(BucketType.COUCHBASE).name(bucketName)
                        .quota(CustomConstants.Values.BUCKET_QUOTA) // megabytes
                        .replicas(replicas).indexReplicas(false).enableFlush(false).build();
                clusterManager.insertBucket(bucketSettings);
            }
        }
    }
}
