/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.lookup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import com.streamanalytix.extensions.utility.MongoConfigConstant;
import com.streamanalytix.extensions.utility.MongoOperationUtilImpl;
import com.streamanalytix.framework.api.udf.Function;

/** The Class MongoCustomUdf. */
public class MongoCustomUdf implements Function, MongoConfigConstant {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 603358121390027959L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(MongoCustomUdf.class);

    /** The host name. */
    private String hostName;

    /** The port. */
    private int port;

    /** The db name. */
    private String dbName;

    /** The collection name. */
    private String collectionName;

    /** The collec. */
    private MongoCollection<Document> collec;

    /** The mongo client. */
    private MongoClient mongoClient;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    @Override
    public void init(Map<String, Object> configMap) {
        LOGGER.info("Inside the MongoCustomUdf:init.");

        if (configMap.containsKey(HOST)) {
            hostName = (String) configMap.get(HOST);
        }
        if (configMap.containsKey(PORTNUMBER)) {
            port = Integer.parseInt((String) configMap.get(PORTNUMBER));
        }
        if (configMap.containsKey(DBNAME)) {
            dbName = (String) configMap.get(DBNAME);
        }
        if (configMap.containsKey(COLLECTIONNAME)) {
            collectionName = (String) configMap.get(COLLECTIONNAME);
        }
        try {
            MongoOperationUtilImpl mongoOperation = new MongoOperationUtilImpl();
            mongoClient = mongoOperation.getMongoConn(configMap);
            // mongoClient = new MongoClient(hostName, port);
            MongoDatabase db = mongoClient.getDatabase(dbName);
            collec = db.getCollection(collectionName);
        } catch (Exception ex) {
            LOGGER.error("Exception occured inside init()" + ex);
        }
        LOGGER.info("Exit MongoCustomUdf init.");
    }

    /** The process method is used to write custom implementation.
     *
     * @param param
     *            the param
     * @return function value
     * @throws Exception
     *             the exception */
    @Override
    public Object execute(Object... param) throws Exception {
        LOGGER.info("Inside MongoCustomUdf execute. json = " + Arrays.toString(param));
        List<Document> list = new ArrayList<Document>();
        if (null != collec && null != param && param.length > 0) {
            try {
                String query = (String) param[0];
                DBObject dbObject1 = (DBObject) JSON.parse(query);
                MongoCursor<Document> cursor = collec.find((Bson) dbObject1).iterator();
                try {
                    while (cursor.hasNext()) {
                        Document p = cursor.next();
                        list.add(p);
                    }
                } finally {
                    cursor.close();
                }
            } catch (Exception e) {
                LOGGER.error("Error while executing LookupMongo.execute , " + e.getMessage());
            }
        } else {
            LOGGER.info("Either connection or parameter is null");
        }
        LOGGER.info("Exit MongoCustomUdf execute." + list.toString());

        return list.toArray();
    }

    /** The cleanup method to free the resources. */
    @Override
    public void cleanup() {
        LOGGER.info("Inside the LookupMongoUDF:cleanup.");
    }

}
