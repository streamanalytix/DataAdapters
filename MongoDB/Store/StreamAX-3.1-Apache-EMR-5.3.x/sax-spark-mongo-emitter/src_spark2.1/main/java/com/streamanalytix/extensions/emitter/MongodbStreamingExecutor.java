/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.emitter;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.streamanalytix.extensions.utility.MongoConfigConstant;
import com.streamanalytix.extensions.utility.MongoOperationUtilImpl;
import com.streamanalytix.framework.api.spark.processor.DStreamProcessor;
import net.minidev.json.JSONObject;

/** The Class MongoDbStreamingExecutor. */
public class MongodbStreamingExecutor implements DStreamProcessor, MongoConfigConstant {
    
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1L;
    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(MongodbStreamingExecutor.class);

    /** The host. */
    private String host;
    
    /** The port. */
    private int port;
    
    /** The db name. */
    private String dbName;
    
    /** The db collection name. */
    private String dbCollectionName;
    
    /** The out put columns. */
    private String[] outPutColumns;

    /** The is intitialized. */
    private boolean isIntitialized = false;
    
    /** The index created. */
    private boolean indexCreated = false;
    
    /** The shard key. */
    private String shardKey;

    /** The single index. */
    private String singleIndex;
    
    /** The compound index. */
    private String compoundIndex;
    
    /** The text index. */
    private String textIndex;
    
    /** The date type key coulmns. */
    private String dateTypeKeyCoulmns;

    /** The key date format map. */
    private HashMap<String, String> keyDateFormatMap = new HashMap<String, String>();
    
    /** The mongo operation. */
    private MongoOperationUtilImpl mongoOperation;

    /**
     * Inits the.
     *
     * @param configMap            this is for getting the configuration from front - end *
     */
    public void init(Map<String, Object> configMap) {
        try {
            host = configMap.get(HOST).toString();

            port = Integer.parseInt((String) configMap.get(PORTNUMBER));

            dbName = configMap.get(DBNAME).toString();

            dbCollectionName = configMap.get(COLLECTIONNAME).toString();

            outPutColumns = configMap.get(OUTPUTCOLUMN).toString().split(",");

            if (configMap.get(SHARDINGKEY) != null && !("".equals(configMap.get(SHARDINGKEY).toString().trim()))) {
                shardKey = configMap.get(SHARDINGKEY).toString();
            }

            if (configMap.get(SINGLEINDEX) != null && !("".equals(configMap.get(SINGLEINDEX).toString().trim()))) {
                singleIndex = configMap.get(SINGLEINDEX).toString();
            }
            if (configMap.get(COMPOUNDINDEX) != null && !("".equals(configMap.get(COMPOUNDINDEX).toString().trim()))) {
                compoundIndex = configMap.get(COMPOUNDINDEX).toString();
            }

            if (configMap.get(TEXTINDEX) != null && !("".equals(configMap.get(TEXTINDEX).toString().trim()))) {
                textIndex = configMap.get(TEXTINDEX).toString();
            }

            if (configMap.get(DATETYPEKEYCOLUMNS) != null && !("".equals(configMap.get(DATETYPEKEYCOLUMNS).toString().trim()))) {
                dateTypeKeyCoulmns = configMap.get(DATETYPEKEYCOLUMNS).toString();
                buildDateTypeKeyColumnsMap();
            }
        } catch (Exception ex) {
            LOGGER.error("Exception is thrown from init in MongodbStreamingExecutor ", ex);
        }
    }

    /**
     *  The process method is used to write custom implementation.
     *
     * @param dStream the d stream
     * @param configMap            the config map *
     * @return the java d stream
     */
    @Override
    public JavaDStream<JSONObject> process(JavaDStream<JSONObject> dStream, Map<String, Object> configMap) {
        LOGGER.info("Inside process method");
        if (!isIntitialized) {
            init(configMap);
        }
        final Map<String, Object> configMapp = configMap;
        return dStream.mapPartitions(new FlatMapFunction<Iterator<JSONObject>, JSONObject>() {
            private static final long serialVersionUID = 1L;

            @SuppressWarnings("deprecation")
            @Override
            public Iterator<JSONObject> call(Iterator<JSONObject> partition) throws Exception {
                MongoClient mongoClient = null;
                mongoOperation = new MongoOperationUtilImpl();

                try {
                    mongoClient = mongoOperation.getMongoConn(configMapp);

                } catch (Exception retryExcep) {
                    LOGGER.error("getMongoConn() throws the exception   ", retryExcep);
                }

                DB db = mongoClient.getDB(dbName);
                DBCollection collection = db.getCollection(dbCollectionName);

                if (shardKey != null && !("".equals(shardKey.trim()))) {
                    try {
                        mongoOperation.shardProcess(dbName, dbCollectionName, db, shardKey);
                    } catch (Exception ex) {
                        LOGGER.error("Not able to process sharding due to some exception " , ex);
                    }
                }

                if (singleIndex != null && !indexCreated) {
                    try {
                        mongoOperation.setMultiIndexes(collection, singleIndex);
                        indexCreated = true;
                    } catch (Exception ex) {
                        LOGGER.error("Not able to process single indexing due to some exception.** Please check ", ex);
                    }
                }

                if (compoundIndex != null && !indexCreated) {
                    try {
                        LOGGER.info("compoundIndex is -- " + compoundIndex);
                        mongoOperation.setMultiIndexes(collection, compoundIndex);
                        indexCreated = true;
                    } catch (Exception ex) {
                        LOGGER.error("Not able to process compound indexing due to some exception.** Please check " , ex);
                    }
                }

                if (textIndex != null && !indexCreated) {
                    try {
                        mongoOperation.setMultiIndexes(collection, textIndex);
                        indexCreated = true;
                    } catch (Exception ex) {
                        LOGGER.error("Not able to process textIndex indexing due to some exception.** Please check " ,ex);
                    }
                }

                List<JSONObject> outputRdd = new ArrayList<JSONObject>();
                List<BasicDBObject> documentList = new ArrayList<BasicDBObject>();
                BulkWriteOperation streamWrite = collection.initializeOrderedBulkOperation();

                while (partition.hasNext()) {
                    BasicDBObject basicDbObj = new BasicDBObject();
                    JSONObject record = partition.next();
                    for (String key : outPutColumns) {
                        String dateformat = isDateTypeKey(key);
                        if (dateformat != null) {
                            try {
                                Date datatype = new SimpleDateFormat(dateformat).parse(record.get(key).toString());
                                basicDbObj.put(key, datatype);
                            } catch (Exception e) {
                                basicDbObj.put(key, record.get(key));
                                LOGGER.error("Filed is not a date type ");
                            }
                        } else {
                            if (record.get(key) instanceof String) {
                                basicDbObj.put(key, record.get(key));
                            } else {
                                if (record.get(key) instanceof Long) {
                                    try {
                                        int iVal = (int) (long) record.get(key);
                                        basicDbObj.put(key, iVal);
                                    } catch (Exception ex) {
                                        basicDbObj.put(key, record.get(key));
                                    }
                                } else {
                                    basicDbObj.put(key, record.get(key));
                                }
                            }
                        }
                    }
                    outputRdd.add(record);
                    documentList.add(basicDbObj);
                }

                for (BasicDBObject doc : documentList) {
                    try {
                        streamWrite.insert(doc);
                        BulkWriteResult result = streamWrite.execute();
                        streamWrite = collection.initializeOrderedBulkOperation();

                    } catch (Exception e) {
                        LOGGER.error("exception while converting number " , e);
                    }
                }
                return outputRdd.iterator();
            }
        }).cache();
    }

    /*** *this is comment of function. **this is buildDateTypeKeyColumnsMap function call here */
    void buildDateTypeKeyColumnsMap() {
        String[] dateKeysAr = dateTypeKeyCoulmns.split(",");
        for (String dateKey : dateKeysAr) {
            String[] dateKeyValue = dateKey.split("=");
            keyDateFormatMap.put(dateKeyValue[0], dateKeyValue[1]);
        }
    }

    /**
     * Checks if is date type key.
     *
     * @param keyString            for isDateTypeKey
     * @return string
     */
    String isDateTypeKey(String keyString) {
        if (keyDateFormatMap.containsKey(keyString)) {
            return keyDateFormatMap.get(keyString);
        } else {
            return null;
        }
    }
}
