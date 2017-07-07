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
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.streamanalytix.extensions.utils.SftpConstant;
import com.streamanalytix.extensions.utils.SftpUtilChannel;
import com.streamanalytix.framework.api.spark.channel.BaseChannel;

/**
 * The Class SftpChannel.
 *
 * @author sax The Class SftpChannel is responsible for reading content from SFTP location.
 */
public class SftpChannel extends BaseChannel implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -8657585005598090114L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SftpChannel.class);

    /**  The SFTP => Host IP, userName,Password,location and fileType required to make connection will be provided by end user through UI. */
    private String sftpHost;
    
    /** The user name. */
    private String userName;
    
    /** The password. */
    private String password;
    
    /** The location. */
    private String location;
    
    /** The file type. */
    private String fileType;
    
    /** The delay btw connection retires. */
    private String delayBtwConnectionRetires = SftpConstant.Values.DEFAULT_DELAY_BTW_CONNECTION_RETRIES;
    
    /** The connection retries. */
    private String connectionRetries = SftpConstant.Values.DEFAULT_RETRY_COUNT;;

    /**
     * Inits the.
     *
     * @param configMap            we will get map which contains all the input fields given by user
     */
    public void init(Map<String, Object> configMap) {
        LOGGER.info("entering : SFTP channel : init method");
        LOGGER.debug(" SFTP channel : initializing required fields coming from UI");

        if (configMap.containsKey(SftpConstant.Literal.SFTP_HOST)) {
            sftpHost = (String) configMap.get(SftpConstant.Literal.SFTP_HOST);
        }
        if (configMap.containsKey(SftpConstant.Literal.SFTP_USER)) {
            userName = (String) configMap.get(SftpConstant.Literal.SFTP_USER);
        }
        if (configMap.containsKey(SftpConstant.Literal.PASSOWRD)) {
            password = (String) configMap.get(SftpConstant.Literal.PASSOWRD);
        }
        if (configMap.containsKey(SftpConstant.Literal.LOCATION)) {
            location = (String) configMap.get(SftpConstant.Literal.LOCATION);
        }
        if (configMap.containsKey(SftpConstant.Literal.FILE_TYPE)) {
            fileType = (String) configMap.get(SftpConstant.Literal.FILE_TYPE);
        }
        if (configMap.containsKey(SftpConstant.Literal.CONNECTION_RETRIES)) {
            try {
                connectionRetries = (String) configMap.get(SftpConstant.Literal.CONNECTION_RETRIES);
            } catch (Exception e) {
                connectionRetries = SftpConstant.Values.DEFAULT_RETRY_COUNT;
                LOGGER.warn("Retry Count is not an integer. Using default retry count as 3.");
            }
        }
        if (configMap.containsKey(SftpConstant.Literal.DELAY_BTW_CONNECTION_RETRIES)) {
            try {
                delayBtwConnectionRetires = (String) configMap.get(SftpConstant.Literal.DELAY_BTW_CONNECTION_RETRIES);
            } catch (Exception e) {
                delayBtwConnectionRetires = SftpConstant.Values.DEFAULT_DELAY_BTW_CONNECTION_RETRIES;
                LOGGER.warn("Retry Count is not an integer. Using default retry count as 3.");
            }
        }
        LOGGER.info("exit from init method  : SFTP channel ");
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.BaseChannel#getRDD(org.apache.spark.api.java.JavaSparkContext)
     */
    @SuppressWarnings({ "serial" })
    @Override
    public JavaRDD<Object> getRDD(JavaSparkContext context) {

        LOGGER.info("entering : SFTP channel : getRdd method : This will read data from SFTP and return RDD ");

        JavaRDD<Object> rddReturnObject = null;
        RDD<String> jsonRDD;
        Dataset<Row> jsonDataFrame;
        Dataset<String> jsonStrDataFrame;

        try {
            jsonDataFrame = SftpUtilChannel.createDataFrameFromSftp(userName, password, sftpHost, location, fileType, context,
                    delayBtwConnectionRetires, connectionRetries);
            jsonStrDataFrame = jsonDataFrame.toDF().toJSON();
            LOGGER.info("jsonStrDataFrame::::::::" + jsonStrDataFrame.toString());
            LOGGER.info("jsonStrDataSet::::::::");
            jsonDataFrame.toDF().show();
            rddReturnObject = jsonStrDataFrame.toJavaRDD().map(new Function<String, Object>() {
                @Override
                public Object call(String v1) throws Exception {
                    return v1.getBytes();
                }
            });
        } catch (Exception exception) {
            LOGGER.error(" Error came in getRdd method of class SftpChannel: ", exception);
            throw new RuntimeException("Error came in getRdd method of class SftpChannel", exception);
        }
        LOGGER.info("Successfully exit from getRDD method of SFTPChannel ");
        return rddReturnObject;
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#cleanup()
     */
    @Override
    public void cleanup() {

    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.Channel#getDStream(org.apache.spark.streaming.api.java.JavaStreamingContext)
     */
    @Override
    public JavaDStream<Object> getDStream(JavaStreamingContext arg0) {
        return null;
    }

}