/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.emitter;

import java.util.Map;

import net.minidev.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.streamanalytix.extensions.utils.SftpConstant;
import com.streamanalytix.extensions.utils.SftpUtilEmitter;
import com.streamanalytix.framework.api.spark.processor.RDDProcessor;

/**
 * The Class SftpRddExecutor.
 *
 * @author sax The Class SftpRddExecutor is responsible for writing data to SFTP location in a file.
 */
public class SftpRddExecutor implements RDDProcessor {

    // The Constant serialVersionUID.
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 114034040973486225L;

    // The SFTP => Host IP, userName,Password,location and fileType required to
    // make connection will be provided by end user through UI
    /** The sftp host. */
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
    private String connectionRetries = SftpConstant.Values.DEFAULT_RETRY_COUNT;

    // The Constant LOGGER.
    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SftpRddExecutor.class);

    /**
     * Inits the.
     *
     * @param configMap            we will get map which contains all the input fields given by user
     */
    public void init(Map<String, Object> configMap) {
        LOGGER.info("entering : SftpRddExecutor : init method");
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
                // Do Nothing use default TTL
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

        LOGGER.info("exit from init method  : SftpRddExecutor ");
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.SparkProcessor#process(java.lang.Object, java.util.Map)
     */
    @SuppressWarnings("serial")
    @Override
    public RDD<JSONObject> process(RDD<JSONObject> rdd, Map<String, Object> configMap) {
        try {

            LOGGER.info("entering : SftpRddExecutor : process method");
            LOGGER.debug(" SFTP channel : initializing required fields coming from UI");
            init(configMap);
            LOGGER.debug(" SFTP excecutor : Creating sqlContext from context (  getting context from rdd itself )");
            SQLContext sqlContext = new SQLContext(rdd.context());

            JavaRDD<String> rddString = rdd.toJavaRDD().map(new Function<JSONObject, String>() {
                @Override
                public String call(JSONObject v1) throws Exception {
                    return v1.toJSONString();
                }
            });
            DataFrame dataFrame = sqlContext.read().json(rddString);
            SftpUtilEmitter.writeToSftp(userName, password, sftpHost, location, fileType, dataFrame, delayBtwConnectionRetires, connectionRetries);

        } catch (Exception exception) {
            LOGGER.error("Error came in process method of class SftpRddExecutor ", exception);
            throw new RuntimeException("Error came in process method of class SftpRddExecutor ", exception);
        }
        LOGGER.info("Exit from  : SftpRddExecutor : process method");
        // We are returning same / may changed according to need of business incoming rdd to next processor
        return rdd;
    }
}