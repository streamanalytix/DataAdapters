/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.channel;

import static com.streamanalytix.extensions.DynamoDBConstant.AWS_ACCESS_KEY_ID;
import static com.streamanalytix.extensions.DynamoDBConstant.AWS_REGION;
import static com.streamanalytix.extensions.DynamoDBConstant.AWS_SECRET_KEY;
import static com.streamanalytix.extensions.DynamoDBConstant.COLLUMN_VALUE_SEPARATOR;
import static com.streamanalytix.extensions.DynamoDBConstant.FIELD_SEPARATOR;
import static com.streamanalytix.extensions.DynamoDBConstant.FILTER_EXPRESSION;
import static com.streamanalytix.extensions.DynamoDBConstant.INDEX_NAME;
import static com.streamanalytix.extensions.DynamoDBConstant.KEY_CONDITION_EXPRESSION;
import static com.streamanalytix.extensions.DynamoDBConstant.NAME_MAP;
import static com.streamanalytix.extensions.DynamoDBConstant.PROJECTION_EXPRESSION;
import static com.streamanalytix.extensions.DynamoDBConstant.TABLE_NAME;
import static com.streamanalytix.extensions.DynamoDBConstant.VALUE_MAP;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.streamanalytix.framework.api.spark.channel.BaseChannel;

/** This class is used to extract and process data from given table over dynamoDB This also processes different criteria entered by user. */

public class DynamoDBChannel extends BaseChannel implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -3274095661198089353L;

    /** Logger instance */
    private static final Log LOGGER = LogFactory.getLog(DynamoDBChannel.class);

    /** dynamoDB connection pool */
    private static ConcurrentMap<String, DynamoDB> clientConnPool = new ConcurrentHashMap<String, DynamoDB>();

    /** AWS user access key */
    private String accessKey;

    /** AWS user secret key */
    private String secretKey;

    /** AWS user region key */
    private String region;

    /** dynamoDB table name */
    private String tableName;

    /** query projection expression */
    private String projectionExpression;

    /** query filter expression */
    private String filterExpression;

    /** query key condition expression */
    private String keyConditionExpression;

    /** query name map expression */
    private String nameMap;

    /** query value map expression */
    private String valueMap;

    /** query index name */
    private String indexName;

    /** Method to initialize environment and create dynamoDB client.
     * 
     * @param config
     *            input configuration map. */
    @Override
    public void init(Map<String, Object> config) {
        if (config.get(AWS_ACCESS_KEY_ID) != null) {
            accessKey = (String) config.get(AWS_ACCESS_KEY_ID);
        }
        if (config.get(AWS_SECRET_KEY) != null) {
            secretKey = (String) config.get(AWS_SECRET_KEY);
        }
        if (config.get(AWS_REGION) != null) {
            String confRegion = (String) config.get(AWS_REGION);
            Regions regions = Regions.fromName(confRegion);
            region = regions.getName();
        }
        if (config.get(TABLE_NAME) != null) {
            tableName = (String) config.get(TABLE_NAME);
        }
        if (config.get(KEY_CONDITION_EXPRESSION) != null) {
            keyConditionExpression = (String) config.get(KEY_CONDITION_EXPRESSION);
        }
        if (config.get(PROJECTION_EXPRESSION) != null) {
            projectionExpression = (String) config.get(PROJECTION_EXPRESSION);
        }
        if (config.get(FILTER_EXPRESSION) != null) {
            filterExpression = (String) config.get(FILTER_EXPRESSION);
        }
        if (config.get(NAME_MAP) != null) {
            nameMap = (String) config.get(NAME_MAP);
        }
        if (config.get(VALUE_MAP) != null) {
            valueMap = (String) config.get(VALUE_MAP);
        }
        if (config.get(INDEX_NAME) != null) {
            indexName = (String) config.get(INDEX_NAME);
        }

        initializeDynamoDBClient();
    }

    /** Method to initialize dynamoDB client. */
    private void initializeDynamoDBClient() {
        if (clientConnPool.get(accessKey) == null) {
            BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
            AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(creds);
            AmazonDynamoDB client = AmazonDynamoDBClient.builder().withCredentials(credentialsProvider).withRegion(region).build();

            clientConnPool.put(accessKey, new DynamoDB(client));
        }
    }

    @Override
    public JavaDStream<Object> getDStream(JavaStreamingContext arg0) {
        // TODO Auto-generated method stub
        return null;
    }

    /** Method to extract data from dynamodb. It is the first method called from spark to extract data.
     * 
     * @param context
     * @return JavaRDD<Object> */
    @Override
    public JavaRDD<Object> getRDD(JavaSparkContext context) {
        LOGGER.debug("Inside getRDD method");
        List<String> itemsList = extractTableData();
        JavaRDD<Object> rddReturnObject = null;
        if (itemsList != null && itemsList.size() > 0) {
            rddReturnObject = toRDD(context, itemsList);
        }
        return rddReturnObject;
    }

    /** Method used to make decision to call Query or Scan criteria to retrieve data from dynamoDB.
     * 
     * @return List<String> */
    private List<String> extractTableData() {
        Map<String, String> nameDataMap = convertNameExpressionToMap(nameMap);
        Map<String, Object> valueDataMap = convertValueExpressionToMap(valueMap);
        DynamoDB dynamoDBClient = clientConnPool.get(accessKey);
        Table table = dynamoDBClient.getTable(tableName);
        Index index = null;
        if (StringUtils.isNotBlank(indexName)) {
            index = table.getIndex(indexName);
        }
        List<String> itemsList = null;
        if (keyConditionExpression != null) {
            itemsList = queryTableData(table, index, nameDataMap, valueDataMap);
        } else {
            itemsList = scanTableData(table, index, nameDataMap, valueDataMap);
        }
        return itemsList;
    }

    /** Method to convert extracted data into RDD.
     * 
     * @param context
     *            Spark context.
     * @param itemsList
     *            input data to process.
     * @return */
    private JavaRDD<Object> toRDD(JavaSparkContext context, List<String> itemsList) {
        JavaRDD<String> javaRDD = context.parallelize(itemsList);
        JavaRDD<Object> rddReturnObject = javaRDD.map(new Function<String, Object>() {

            @Override
            public Object call(String v1) throws Exception {
                return v1.getBytes();
            }
        });
        return rddReturnObject;
    }

    /** Method to create name map from name map String provided by user.
     * 
     * @param nameString
     *            name map string. Will be null in-case of columns are not having reserved keys.
     * @return Map<String, String> */
    private Map<String, String> convertNameExpressionToMap(String nameString) {
        Map<String, String> nameMap = new HashMap<String, String>();
        if (StringUtils.isBlank(nameString)) {
            return null;
        }
        String[] nameArr = nameString.split(FIELD_SEPARATOR);
        for (String name : nameArr) {
            String[] nameData = name.split(COLLUMN_VALUE_SEPARATOR);
            if (nameData.length > 1) {
                nameMap.put(nameData[0], nameData[1]);
            }
        }
        return nameMap;
    }

    /** Method to create value map from value map String provided by user.
     * 
     * @param valueString
     *            query value String.
     * @return Map<String, Object> */
    private Map<String, Object> convertValueExpressionToMap(String valueString) {
        Map<String, Object> valueMap = new HashMap<String, Object>();
        if (StringUtils.isBlank(valueString)) {
            return null;
        }
        String[] valueArr = valueString.split(FIELD_SEPARATOR);
        for (String value : valueArr) {
            String[] valueData = value.split(COLLUMN_VALUE_SEPARATOR);
            if (valueData.length > 1) {
                if (StringUtils.isNumeric(valueData[1])) {
                    valueMap.put(valueData[0], Integer.valueOf(valueData[1]));
                }
                if (StringUtils.isNumeric(valueData[1])) {
                    valueMap.put(valueData[0], Integer.valueOf(valueData[1]));
                } else {
                    valueMap.put(valueData[0], valueData[1]);
                }
            }
        }
        return valueMap;
    }

    /** Method extract data from dynamoDB as per criteria supplied by user. It fetches information on the basis of table or index.
     * 
     * @param table
     *            Table name from which we need to extract data.
     * @param index
     *            Index name over table.
     * @param nameMap
     *            Map containing key value pair for column name aliases and column name.
     * @param valueMap
     *            map having key/value for query parameter values.
     * @return List<String> return data. */
    private List<String> queryTableData(Table table, Index index, Map<String, String> nameMap, Map<String, Object> valueMap) {

        List<String> itemsList = new ArrayList<String>();
        QuerySpec querySpec = new QuerySpec();
        if (index == null) {
            querySpec.withConsistentRead(true).withScanIndexForward(true).withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        }
        if (StringUtils.isNotBlank(keyConditionExpression)) {
            querySpec.withKeyConditionExpression(keyConditionExpression);
        }
        if (StringUtils.isNotBlank(projectionExpression)) {
            querySpec.withProjectionExpression(projectionExpression);
        }
        if (StringUtils.isNotBlank(filterExpression)) {
            querySpec.withFilterExpression(filterExpression);
        }
        if (nameMap != null) {
            querySpec.withNameMap(nameMap);
        }
        if (valueMap != null) {
            querySpec.withValueMap(valueMap);
        }
        try {
            ItemCollection<QueryOutcome> items;
            if (index == null) {
                LOGGER.debug("Attempting to read data from table " + tableName + " ; please wait...");
                items = table.query(querySpec);
            } else {
                LOGGER.debug("Attempting to read data from index " + indexName + " ; please wait...");
                items = index.query(querySpec);
            }
            Iterator<Item> iterator = items.iterator();
            Item item = null;
            while (iterator.hasNext()) {
                item = iterator.next();
                itemsList.add(item.toJSON());
            }
        } catch (Exception e) {
            LOGGER.error("Unable to scan the table : " + tableName + " or index " + indexName, e);
        }
        return itemsList;
    }

    /** Method scan data from dynamoDB as per criteria supplied by user. It fetches information on the basis of table or index.
     * 
     * @param table
     *            Table name from which we need to extract data.
     * @param index
     *            Index name over table.
     * @param nameMap
     *            Map containing key value pair for column name aliases and column name.
     * @param valueMap
     *            map having key/value for query parameter values.
     * @return List<String> return data. */
    private List<String> scanTableData(Table table, Index index, Map<String, String> nameMap, Map<String, Object> valueMap) {
        LOGGER.debug("Attempting to scan data from table; please wait...");
        List<String> itemsList = new ArrayList<String>();
        ScanSpec scanSpec = new ScanSpec().withConsistentRead(true).withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);
        if (index == null) {
            scanSpec.withConsistentRead(false);
        }
        if (StringUtils.isNotBlank(projectionExpression)) {
            scanSpec.withProjectionExpression(projectionExpression);
        }
        if (StringUtils.isNotBlank(filterExpression)) {
            scanSpec.withFilterExpression(filterExpression);
        }
        if (nameMap != null) {
            scanSpec.withNameMap(nameMap);
        }
        if (valueMap != null) {
            scanSpec.withValueMap(valueMap);
        }
        try {
            ItemCollection<ScanOutcome> items;
            if (index == null) {
                LOGGER.debug("Attempting to read data from table " + tableName + " ; please wait...");
                items = table.scan(scanSpec);
            } else {
                LOGGER.debug("Attempting to read data from index " + indexName + " ; please wait...");
                items = index.scan(scanSpec);
            }
            Iterator<Item> iterator = items.iterator();
            Item item = null;
            while (iterator.hasNext()) {
                item = iterator.next();
                itemsList.add(item.toJSON());
            }
        } catch (Exception e) {
            LOGGER.error("Unable to scan the table : " + tableName + " or index " + indexName, e);
        }
        return itemsList;
    }

    @Override
    public void cleanup() {
        clientConnPool = null;
    }

}