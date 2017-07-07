/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.emitter;

import static com.streamanalytix.extensions.DynamoDBConstant.AWS_ACCESS_KEY_ID;
import static com.streamanalytix.extensions.DynamoDBConstant.AWS_REGION;
import static com.streamanalytix.extensions.DynamoDBConstant.AWS_SECRET_KEY;
import static com.streamanalytix.extensions.DynamoDBConstant.PARTITION_KEY;
import static com.streamanalytix.extensions.DynamoDBConstant.PARTITION_KEY_TYPE;
import static com.streamanalytix.extensions.DynamoDBConstant.READ_CAPACITY_UNITS;
import static com.streamanalytix.extensions.DynamoDBConstant.SORT_KEY;
import static com.streamanalytix.extensions.DynamoDBConstant.SORT_KEY_TYPE;
import static com.streamanalytix.extensions.DynamoDBConstant.TABLE_NAME;
import static com.streamanalytix.extensions.DynamoDBConstant.TIME_OUT;
import static com.streamanalytix.extensions.DynamoDBConstant.WRITE_CAPACITY_UNITS;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.minidev.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.streamanalytix.framework.api.spark.processor.RDDProcessor;

/** The Class DynamodbRDDExecutor. */
public class DynamodbRDDExecutor implements RDDProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(DynamodbRDDExecutor.class);

    /** AWS access key. */
    private String accessKey;

    /** AWS secret key. */
    private String secretKey;

    /** Region where queue exist. */
    private Regions region = Regions.US_WEST_2;

    /** Table where data will be saved/updated. */
    private String tableName;

    /** Partition key for the table. */
    private String partitionKey;

    /** Sort key for the table. */
    private String sortKey;

    /** Data type for Partition key. */
    private String partitionKeyType;

    /** Data type for Sort key. */
    private String sortKeyType;

    /** Read Throughput capacity units . */
    private Long readCapacityUnits = 1L;

    /** Write Throughput capacity units. */
    private Long writeCapacityUnits = 1L;

    /** Time out. */
    private Integer timeOut;

    /** DynaoDB client. */
    private static DynamoDB dynamoDB;

    /** Process.
     *
     * @param rdd
     *            the rdd
     * @param configMap
     *            the config map
     * @return the rdd */
    @Override
    public RDD<JSONObject> process(RDD<JSONObject> rdd, Map<String, Object> configMap) {
        init(configMap);
        createTable();
        return rdd.toJavaRDD().map(new Function<JSONObject, JSONObject>() {
            @Override
            public JSONObject call(JSONObject jsonObject) throws Exception {
                batchWrite(jsonObject);
                return jsonObject;
            }
        }).rdd();
    }

    /** initialization.
     *
     * @param configMap
     *            the config map */
    private void init(Map<String, Object> configMap) {
        LOGGER.debug("Inside init");
        if (configMap.containsKey(AWS_ACCESS_KEY_ID)) {
            this.accessKey = (String) configMap.get(AWS_ACCESS_KEY_ID);
        }
        if (configMap.containsKey(AWS_SECRET_KEY)) {
            this.secretKey = (String) configMap.get(AWS_SECRET_KEY);
        }
        if (configMap.containsKey(TABLE_NAME)) {
            this.tableName = (String) configMap.get(TABLE_NAME);
        }
        if (configMap.containsKey(PARTITION_KEY)) {
            this.partitionKey = (String) configMap.get(PARTITION_KEY);
        }
        if (configMap.containsKey(SORT_KEY)) {
            this.sortKey = (String) configMap.get(SORT_KEY);
        }
        if (configMap.containsKey(PARTITION_KEY_TYPE)) {
            this.partitionKeyType = (String) configMap.get(PARTITION_KEY_TYPE);
        }
        if (configMap.containsKey(SORT_KEY_TYPE)) {
            this.sortKeyType = (String) configMap.get(SORT_KEY_TYPE);
        }
        
        if (configMap.containsKey(AWS_REGION)) {
            String regionName = (String) configMap.get(AWS_REGION);
            try {
                region = Regions.fromName(regionName);
            } catch (IllegalArgumentException e) {
                // use default Region
                LOGGER.info("Invalid Region name was provided in config paramters");

            }
        }
        if (configMap.containsKey(READ_CAPACITY_UNITS)) {
            try {
                this.readCapacityUnits = (Long) configMap.get(READ_CAPACITY_UNITS);
            } catch (ClassCastException e) {
                // use default value
                LOGGER.warn("Read capacity not provided, using default 1, using default");
            }
        }
        if (configMap.containsKey(WRITE_CAPACITY_UNITS)) {
            try {
                this.writeCapacityUnits = (Long) configMap.get(WRITE_CAPACITY_UNITS);
            } catch (ClassCastException e) {
                // use default value
                LOGGER.warn("Write capacity not provided, using default 1");
            }
        }
        if (configMap.containsKey(TIME_OUT)) {
            try {
                this.timeOut = (Integer) configMap.get(TIME_OUT);
            } catch (ClassCastException e) {
                // use default value
                LOGGER.warn("Time out not provided, using default");
            }
        }
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))).withRegion(region).build();
        dynamoDB = new DynamoDB(client);
    }

    /** creates table if it does not exist. */
    private void createTable() {

        try {
            List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
            attributeDefinitions.add(new AttributeDefinition().withAttributeName(partitionKey).withAttributeType(partitionKeyType));
            if (sortKey != null) {
                attributeDefinitions.add(new AttributeDefinition().withAttributeName(sortKey).withAttributeType(sortKeyType));
            }
            ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
            keySchema.add(new KeySchemaElement().withAttributeName(partitionKey).withKeyType(KeyType.HASH));
            if (sortKey != null) {
                keySchema.add(new KeySchemaElement().withAttributeName(sortKey).withKeyType(KeyType.RANGE));
            }
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(keySchema)
                    .withAttributeDefinitions(attributeDefinitions)
                    .withProvisionedThroughput(
                            new ProvisionedThroughput().withReadCapacityUnits(readCapacityUnits).withWriteCapacityUnits(writeCapacityUnits));
            if (timeOut != null) {
                request = request.withSdkRequestTimeout(timeOut);
            }
            Table table = dynamoDB.createTable(request);
            table.waitForActive();
            LOGGER.info("Table " + tableName + " successfully created");
        } catch (Exception e) {
            LOGGER.error("CreateTable request failed for " + tableName + " : " , e);
        }
    }

    /** Batch write.
     *
     * @param jsonObject
     *            the json object */
    private void batchWrite(JSONObject jsonObject) {
        try {
            if (!jsonObject.isEmpty()) {
                PrimaryKey primaryKey = null;
                if (jsonObject.containsKey(partitionKey) && jsonObject.containsKey(sortKey)) {
                    primaryKey = new PrimaryKey(partitionKey, jsonObject.get(partitionKey), sortKey, jsonObject.get(sortKey));
                } else if (jsonObject.containsKey(partitionKey) && !jsonObject.containsKey(sortKey)) {
                    primaryKey = new PrimaryKey(partitionKey, jsonObject.get(partitionKey));
                } else
                    LOGGER.error("Source data is missing primary key");

                Item item = new Item().withPrimaryKey(primaryKey);
                for (String key : jsonObject.keySet()) {
                    item.with(key, jsonObject.get(key));
                }
                TableWriteItems tableWriteItems = new TableWriteItems(tableName).withItemsToPut(item);
                BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(tableWriteItems);
                do {
                    // Check for unprocessed keys which could happen if you
                    // exceed
                    // provisioned throughput
                    Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
                    if (outcome.getUnprocessedItems().size() != 0) {
                        outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                    }
                } while (outcome.getUnprocessedItems().size() > 0);
                LOGGER.debug("Data successfully inserted into table " + tableName);
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while inserting data: ", e);
        }
    }
}
