/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.channel;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.mongodb.spark.MongoSpark;
import com.streamanalytix.framework.api.spark.channel.BaseChannel;

/** The Class SampleCustomChannel. */
public class MongoCustomChannel extends BaseChannel implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1234867236823478L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(MongoCustomChannel.class);

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    @Override
    public void init(Map<String, Object> configMap) {

    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.Channel#getDStream(org .apache.spark.streaming.api.java.JavaStreamingContext)
     */
    @Override
    public JavaDStream<Object> getDStream(JavaStreamingContext arg0) {
        return null;
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.BaseChannel#getRDD(org.apache.spark.api.java.JavaSparkContext)
     */
    @Override
    public JavaRDD<Object> getRDD(JavaSparkContext context) {
        return getRDDUsingMongoSpark(context);
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#cleanup()
     */
    @Override
    public void cleanup() {
    }

    /** Gets the RDD using mongo spark.
     *
     * @param context
     *            **this is JavaSparkContext**
     * @return objectRDD */
    public JavaRDD<Object> getRDDUsingMongoSpark(JavaSparkContext context) {

        LOGGER.info("Inside getRDDUsingMongoSpark Method");
        JavaRDD<org.bson.Document> rdd = MongoSpark.load(context);

        JavaRDD<Object> objectRDD = rdd.map(new Function<org.bson.Document, Object>() {
            private static final long serialVersionUID = 335729824771195544L;

            /** call method
             * 
             * @param arg0 */
            @Override
            public Object call(org.bson.Document arg0) throws Exception {
                return arg0.toJson().getBytes();
            }
        });
        return objectRDD;
    }

}
