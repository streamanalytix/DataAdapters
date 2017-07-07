/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.emitter;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.minidev.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.streamanalytix.extensions.utility.MongoConfigConstant;
import com.streamanalytix.extensions.utility.MongoOperationUtilImpl;
import com.streamanalytix.framework.api.processor.JSONProcessor;

/** The Class MongoEmitterProcessExecutor. */
public class MongoEmitterProcessExecutor implements JSONProcessor, MongoConfigConstant {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3461208876473240633L;
    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(MongoEmitterProcessExecutor.class);

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

    /** The shard key. */
    private String shardKey;

    /** The single index. */
    private String singleIndex;

    /** The compound index. */
    private String compoundIndex;

    /** The text index. */
    private String textIndex;

    /** The index created. */
    private boolean indexCreated = false;

    /** The is intitialized. */
    private boolean isIntitialized = false;

    /** The date type key coulmns. */
    private String dateTypeKeyCoulmns;

    /** The key date format map. */
    private HashMap<String, String> keyDateFormatMap = new HashMap<String, String>();

    /** The mongo operation. */
    private MongoOperationUtilImpl mongoOperation;

    /** The init method is used to initialize the configuration.
     * 
     * @param configMap
     *            the config map */
    @SuppressWarnings("deprecation")
    @Override
    public void init(Map<String, Object> configMap) {

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

    }

    /** The process method is used to write custom implementation.
     * 
     * @param json
     *            **the json data **
     * @param configMap
     *            **the config map
     * @throws Exception
     *             the Exception ** */
    @Override
    public void process(JSONObject json, Map<String, Object> configMap) throws Exception {
        final Map<String, Object> configMapp = configMap;


        if (!isIntitialized) {
            init(configMap);
        }
        MongoClient mongoClient = null;
        mongoOperation = new MongoOperationUtilImpl();
        try {
            mongoClient = mongoOperation.getMongoConn(configMapp);
        } catch (Exception retryExcep) {
            LOGGER.error("getMongoConn() throws the exception",retryExcep);
        }
        DB db = mongoClient.getDB(dbName);
        DBCollection collection = db.getCollection(dbCollectionName);

        if (shardKey != null && !("".equals(shardKey.trim()))) {
            try {
                mongoOperation.shardProcess(dbName, dbCollectionName, db, shardKey);
            } catch (Exception ex) {
                LOGGER.error("Not able to process sharding due to some exception",ex);
            }
        }

        if (singleIndex != null && !indexCreated) {
            try {
                mongoOperation.setMultiIndexes(collection, singleIndex);
                indexCreated = true;
            } catch (Exception ex) {
                LOGGER.error("Not able to process single indexing due to some exception",ex);
            }
        }

        if (compoundIndex != null && !indexCreated) {
            try {
                mongoOperation.setMultiIndexes(collection, compoundIndex);
                indexCreated = true;
            } catch (Exception ex) {
                LOGGER.error("Not able to process compound indexing due to some exception",ex);
            }
        }

        if (textIndex != null && !indexCreated) {
            try {
                mongoOperation.setMultiIndexes(collection, textIndex);
                indexCreated = true;
            } catch (Exception ex) {
                LOGGER.error("Not able to process textIndex indexing due to some exception",ex);
            }
        }

        List<BasicDBObject> document = documentToInsert(json);

        for (BasicDBObject insertData : document) {
            try {
                WriteResult result = collection.insert(insertData);
            } catch (Exception ex) {

                LOGGER.error("some duplicate data came ,so not inserted when uniq index is craeted " + insertData.toString(),ex);
            }
        }
        LOGGER.info("All process done ");
    }

    /** Document to insert.
     *
     * @param json
     *            ** **json Object Data **
     * @return document */
    private List<BasicDBObject> documentToInsert(JSONObject json) {

        List<BasicDBObject> document = new ArrayList<BasicDBObject>();
        BasicDBObject basicDbObj = new BasicDBObject();
        for (String key : outPutColumns) {

            String dateformat = isDateTypeKey(key);
            if (dateformat != null) {
                try {
                    Date datatype = new SimpleDateFormat(dateformat).parse(json.get(key).toString());
                    basicDbObj.put(key, datatype);
                } catch (Exception e) {
                    basicDbObj.put(key, json.get(key));
                }

            } else {

                if (json.get(key) instanceof String) {
                    basicDbObj.put(key, json.get(key));
                } else {
                    if (json.get(key) instanceof Long) {
                        try {
                            int iVal = (int) (long) json.get(key);
                            basicDbObj.put(key, iVal);
                        } catch (Exception ex) {
                            basicDbObj.put(key, json.get(key));
                        }
                    } else {
                        basicDbObj.put(key, json.get(key));
                    }
                }

            }
            document.add(basicDbObj);
        }
        return document;
    }

    /** this is comment of function. **this is buildDateTypeKeyColumnsMap function call here */
    void buildDateTypeKeyColumnsMap() {
        String[] dateKeysAr = dateTypeKeyCoulmns.split(",");
        for (String dateKey : dateKeysAr) {
            String[] dateKeyValue = dateKey.split("=");
            keyDateFormatMap.put(dateKeyValue[0], dateKeyValue[1]);
        }
    }

    /** Checks if is date type key.
     *
     * @param keyString
     *            for isDateTypeKey
     * @return string */
    String isDateTypeKey(String keyString) {
        if (keyDateFormatMap.containsKey(keyString)) {
            return keyDateFormatMap.get(keyString);
        } else {
            return null;
        }
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.IExecutor#cleanup()
     */
    @Override
    public void cleanup() {

    }

}
