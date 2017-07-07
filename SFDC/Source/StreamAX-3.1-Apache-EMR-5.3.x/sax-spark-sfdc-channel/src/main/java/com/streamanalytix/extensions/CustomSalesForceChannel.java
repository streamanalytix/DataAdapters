package com.streamanalytix.extensions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamanalytix.framework.api.spark.channel.BaseChannel;

import net.minidev.json.JSONObject;

/**
 * The Class CustomSalesForceChannel.
 */
public class CustomSalesForceChannel extends BaseChannel {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomSalesForceChannel.class);

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -4823080010372859412L;
    /** The field format map. */
    private Map<String, String> fieldFormatMap = new HashMap<String, String>();
    /** Spark SQL Context *. */
    private SparkSession sparkSession;

    /** The Constant SFDC_USERNAME. */
    private static final String SFDC_USERNAME = "username";
    
    /** The Constant SFDC_PASSWORD. */
    private static final String SFDC_PASSWORD = "password";
    
    /** The Constant SFDC_QUERY. */
    private static final String SFDC_QUERY = "query";
    
    /** The Constant SFDC_VERSION. */
    private static final String SFDC_VERSION = "version";
    
    /** The Constant SFDC_SOQL. */
    private static final String SFDC_SOQL = "soql";
    
    /** The Constant SF_DATAFRAME_FORMAT_OPTIONS. */
    private static final String SF_DATAFRAME_FORMAT_OPTIONS = "com.springml.spark.salesforce";

    /** The username. */
    private String username;
    
    /** The password. */
    private String password;
    
    /** The query. */
    private String query;
    
    /** The version. */
    private String version;

    /* (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    @Override
    public void init(Map<String, Object> conf) {
        username = (String) conf.get(SFDC_USERNAME);
        password = (String) conf.get(SFDC_PASSWORD);
        query = (String) conf.get(SFDC_QUERY);
        version = (String) conf.get(SFDC_VERSION);
    }

    /* (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.Channel#getDStream(org.apache.spark.streaming.api.java.JavaStreamingContext)
     */
    @Override
    public JavaDStream<Object> getDStream(JavaStreamingContext context) {
        return null;
    }

    /* (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.BaseChannel#getRDD(org.apache.spark.api.java.JavaSparkContext)
     */
    @Override
    public JavaRDD<Object> getRDD(JavaSparkContext context) {
        LOGGER.info("Inside ::getRDD method" + context);
        if (this.sparkSession == null) {
            this.sparkSession = SparkSession.builder().sparkContext(context.sc()).getOrCreate();
        }
        Dataset<Row> dataFrame = null;
        dataFrame = this.sparkSession.read().format(SF_DATAFRAME_FORMAT_OPTIONS).option(SFDC_USERNAME, username).option(SFDC_PASSWORD, password)
                .option(SFDC_SOQL, query).option(SFDC_VERSION, version).load();

        return dataFrame.toJavaRDD().map(rowToObjFunction);
    }

    /** The row to obj function. */
    private Function<Row, Object> rowToObjFunction = new Function<Row, Object>() {

        private static final long serialVersionUID = -8640912808479587525L;
        private JSONObject json = new JSONObject();

        @Override
        public Object call(Row row) throws Exception {
            for (StructField field : row.schema().fields()) {
                setValueInJSONObj(row, json, field, field.name());

            }
            return json.toJSONString().getBytes("UTF-8");
        }

    };

    /**
     *  Sets the json value.
     *
     * @param row            the row
     * @param json            the json
     * @param field            the field
     * @param messageFieldName the message field name
     */
    private void setValueInJSONObj(Row row, JSONObject json, StructField field, String messageFieldName) {
        Object value = null;
        String type = field.dataType().typeName();
        if ("date".equalsIgnoreCase(type)) {
            SimpleDateFormat sdf = new SimpleDateFormat(fieldFormatMap.get(messageFieldName));
            value = row.getDate(row.fieldIndex(field.name()));
            json.put(messageFieldName, (value == null) ? null : sdf.format(value));
        } else if ("timestamp".equalsIgnoreCase(type)) {
            SimpleDateFormat sdf = new SimpleDateFormat(fieldFormatMap.get(messageFieldName));
            value = row.getTimestamp(row.fieldIndex(field.name()));
            json.put(messageFieldName, (value == null) ? null : sdf.format(value));
        } else if ("binary".equalsIgnoreCase(type)) {
            byte[] byteValue = (byte[]) row.get(row.fieldIndex(field.name()));
            Object objVal = toObject(byteValue);
            json.put(messageFieldName, (value == null) ? null : objVal.toString());
        } else {
            value = row.get(row.fieldIndex(field.name()));
            json.put(messageFieldName, (value == null) ? null : value);
        }
        LOGGER.info("messageFieldName " + messageFieldName + " Value " + value + " and Type " + type);
    }

    /** Converts the object into byte array.
     * 
     * @param obj
     *            the obj
     * @return the byte[]
     * @throws IOException
     *             Signals that an I/O exception has occurred. */
    public static byte[] toBytes(Object obj) throws IOException {
        LOGGER.info("Inside toBytes method: ");
        ByteArrayOutputStream bos = null;
        ObjectOutput out = null;
        try {
            bos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);
            out.writeObject(obj);
            byte[] serVo = bos.toByteArray();
            LOGGER.info("Exit from toBytes method: ");
            return serVo;
        } finally {
            try {
                if (null != out) {
                    out.close();
                }
                if (null != bos) {
                    bos.close();
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught while writing object into byte array: " + e.getMessage());
            }
        }
    }

    /**
     *  Converts the bytes to Array.
     *
     * @param bytes            the bytes
     * @return the object
     */
    public static Object toObject(byte[] bytes) {
        LOGGER.info("Inside toObject method: ");
        ByteArrayInputStream bis = null;
        ObjectInput in = null;
        try {
            bis = new ByteArrayInputStream(bytes);
            in = new ObjectInputStream(bis);
            return in.readObject();
        } catch (Exception e) {
            return null;
        } finally {
            try {
                if (null != in) {
                    in.close();
                }
                if (null != bis) {
                    bis.close();
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught writing byte array into object: " + e.getMessage());
            }
        }
    }

    /* (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#cleanup()
     */
    @Override
    public void cleanup() {

    }
};
