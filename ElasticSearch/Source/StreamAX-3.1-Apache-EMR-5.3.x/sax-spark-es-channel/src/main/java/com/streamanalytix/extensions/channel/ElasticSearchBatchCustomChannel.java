/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.channel;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder.Operator;
import org.elasticsearch.search.SearchHit;

import com.google.gson.Gson;
import com.streamanalytix.framework.api.spark.channel.BaseChannel;

/** ElasticSearchBatchCustomChannel is custom channel which has functionality to make query on elastic search Support for custom Queries So that i can
 * use that channel to create custom queries. Failure/Retry Mechanism when connection to elastic search can not be established So that attempt to get
 * ES connection can be retried. fetch data based on date & range queries & queries based on time interval So that i can have more filtered data in
 * the response. retrieve data from elastic search with query string as an input so that channel is not limited to accept only JSON Queries.
 * 
 * @author impadmin */
public class ElasticSearchBatchCustomChannel extends BaseChannel implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -8657585005598090114L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(ElasticSearchBatchCustomChannel.class);

    /** The host name. */
    private static String hostName;

    /** The host name. */
    private static String elasticSearchClusterName;

    /** The port. */
    private static Integer port;

    /** Index Name *. */
    private static String indexName;

    /** Elastic Search Query *. */
    private static String eSQuery;

    /** Elastic Search Query Type : Query or collection *. */
    private static String searchType;

    /** Elastic Search Query Result page size *. */
    private static Integer pageSize;

    /** Starting value for range query *. */
    private Object from;

    /** Ending value for range query *. */
    private Object to;

    /** Range field on which Range filter has to be applied *. */
    private static String rangeField;

    /** Custom Query for elastic search in JSON Format Sample Query: {"query":[{"title":"The Green Mile"},{"operator":"OR"},{"director":
     * "Neeraj Pandey"},{"operator":"OR"},{"year":"1999"}]} *. */
    private LinkedHashMap<?, ?> customQuery;

    /** determines if query type is queryString or not *. */
    private boolean isQueryString;

    /** Date format if range field is date type *. */
    private static String dateFormat;

    /** minimum number of connection retries to be made in case of connection failure *. */
    private static int noOfAttempts;

    /** delay between connection retries *. */
    private static long delayBetweenRetries;

    /** The client. */
    private static Client client = null;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    @Override
    public void init(Map<String, Object> configMap) {
        LOGGER.info("ElasticSearchBatchCustomChannel: init:");
        if (configMap.containsKey("hostName")) {
            hostName = (String) configMap.get("hostName");
        }
        if (configMap.containsKey("port")) {
            port = Integer.parseInt((String) configMap.get("port"));
        }
        if (configMap.containsKey("indexName")) {
            indexName = (String) configMap.get("indexName");
        }
        if (configMap.containsKey("searchType")) {
            searchType = (String) configMap.get("searchType");
        }
        if (configMap.containsKey("query")) {
            Object queryStringObj = configMap.get("query");
            if (queryStringObj instanceof LinkedHashMap<?, ?>) {
                Gson gson = new Gson();
                String jsonQuery = gson.toJson(queryStringObj, LinkedHashMap.class);
                eSQuery = jsonQuery;
            } else {
                isQueryString = true;
                eSQuery = (String) queryStringObj;
            }
        }
        if (configMap.containsKey("pageSize")) {
            pageSize = Integer.parseInt((String) configMap.get("pageSize"));
        }

        if (configMap.containsKey("clusterName")) {
            elasticSearchClusterName = (String) configMap.get("clusterName");
        }
        if (configMap.containsKey("To")) {
            to = configMap.get("To");
        }
        if (configMap.containsKey("From")) {
            from = configMap.get("From");
        }
        if (configMap.containsKey("customQuery")) {
            LinkedHashMap<?, ?> queryMap = (LinkedHashMap<?, ?>) configMap.get("customQuery");
            customQuery = queryMap;
        }
        if (configMap.containsKey("rangeField")) {
            rangeField = (String) configMap.get("rangeField");
        }
        if (configMap.containsKey("dateFormat")) {
            dateFormat = (String) configMap.get("dateFormat");
        }
        if (configMap.containsKey("connRetry")) {
            Object connCount = configMap.get("connRetry");
            if (connCount != null) {
                noOfAttempts = Integer.parseInt((String) connCount);
            }
        }
        if (configMap.containsKey("delayBtwConRetry")) {
            Object delay = configMap.get("delayBtwConRetry");
            if (delay != null) {
                delayBetweenRetries = Long.parseLong((String) delay);
            }
        }
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.Channel#getDStream(org .apache.spark.streaming.api.java.JavaStreamingContext)
     */
    @Override
    public JavaDStream<Object> getDStream(JavaStreamingContext context) {
        JavaReceiverInputDStream<String> lines = context.socketTextStream(hostName, port);
        return lines.map(new Function<String, Object>() {
            private static final long serialVersionUID = 7522170517856295596L;

            @Override
            public Object call(String arg0) throws Exception {
                return arg0.getBytes();
            }
        });

    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.BaseChannel#getRDD(org.apache.spark.api.java.JavaSparkContext)
     */
    @Override
    public JavaRDD<Object> getRDD(JavaSparkContext context) {
        LOGGER.debug("ElasticSearchBatchCustomChannel: getRDD: ");
        JavaRDD<Object> rdd = null;
        List<Object> response = null;
        Client client = getClient();
        int totalRetryCount = noOfAttempts;
        while (noOfAttempts > 0) {
            try {
                if ("Collection".equalsIgnoreCase(searchType)) {
                    response = getESDataByCollection(client);
                } else if ("Query".equalsIgnoreCase(searchType) && StringUtils.isNotEmpty(eSQuery)) {
                    response = getESDataByJSONQuery(client, isQueryString);
                } else if ("Custom".equalsIgnoreCase(searchType) && customQuery != null) {
                    response = getESDataByCustomQuery(client);
                }
                rdd = context.parallelize(response);
                break;
            } catch (NoNodeAvailableException e) {
                try {
                    Thread.sleep(delayBetweenRetries);
                } catch (InterruptedException ignored) {
                    LOGGER.error("Failed...! Retrying connection to Elastic Search again.InterruptedException occurred :" + ignored.getMessage());
                }
                LOGGER.error("Failed...! Retrying connection to Elastic Search again. Retry count is :" + (totalRetryCount - noOfAttempts));
                noOfAttempts--;
            }
        }
        return rdd;

    }

    /** returns the elastic search response by building and executing custom Query Standard format of custom Query is below: {"query":[{"title":
     * "The Green Mile"},{"operator":"OR"},{"director":"Neeraj Pandey"},{"operator":"OR"},{"year":"1999"}]} Converts JSON into meaningful Elastic
     * search Query and execute the same.
     * 
     * @param client
     *            name of client
     * @return response of ES Query */
    private List<Object> getESDataByCustomQuery(Client client) {
        LOGGER.debug("Enters into ElasticSearchBatchCustomChannel: getESDataByCustomQuery: ");
        CustomFields field = null;
        List<Object> esResponse = new ArrayList<Object>();
        List<CustomFields> cfields = new ArrayList<CustomFields>();
        ArrayList<?> json = (ArrayList<?>) customQuery.get("query");
        SearchRequestBuilder builder = client.prepareSearch(indexName);

        for (int i = 0; i < json.size(); i++) {
            field = new CustomFields();

            LinkedHashMap<?, ?> fieldMap = (LinkedHashMap<?, ?>) json.get(i);
            Set<?> entrySet = fieldMap.entrySet();

            @SuppressWarnings("unchecked")
            Iterator<Entry<?, ?>> it = (Iterator<Entry<?, ?>>) entrySet.iterator();
            Entry<?, ?> currentEntry = null;

            while (it.hasNext()) {
                currentEntry = it.next();
                if ((i + 1) % 2 == 0) {
                    field.setOperator(currentEntry.getValue().toString());
                } else {
                    field.setKey(currentEntry.getKey().toString());
                    field.setValue(currentEntry.getValue().toString());
                }
            }
            cfields.add(field);
        }

        QueryBuilder customWueryBuilder = buildCustomESQuery(cfields);
        builder.setQuery(customWueryBuilder);
        builder.setSize(pageSize);
        SearchResponse response = builder.execute().actionGet();
        for (SearchHit iterableElement : response.getHits()) {
            esResponse.add(iterableElement.getSourceAsString().getBytes());
        }
        return esResponse;

    }

    /** returns the Querybuilder by iteratively creating Elastic search Query.
     * 
     * @param cfields
     *            name of client
     * @return QueryBuilder for ES Query */
    private static QueryBuilder buildCustomESQuery(List<CustomFields> cfields) {
        LOGGER.debug("Enters into ElasticSearchBatchCustomChannel: buildCustomESQuery: ");
        BoolQueryBuilder query = new BoolQueryBuilder();
        int counter = 1;
        BoolQueryBuilder afterQueryBuider = null;
        BoolQueryBuilder beforeQueryBuilder = null;
        String op = "";
        for (CustomFields customFields : cfields) {
            int rem = counter % 2;
            if (rem == 0) {
                op = (String) customFields.getOperator();
            } else {
                BoolQueryBuilder bq1 = new BoolQueryBuilder();
                if (beforeQueryBuilder == null) {

                    beforeQueryBuilder = bq1.must(QueryBuilders.matchQuery((String) customFields.getKey(), customFields.getValue()).operator(
                            org.elasticsearch.index.query.MatchQueryBuilder.Operator.AND));
                } else {
                    afterQueryBuider = bq1.must(QueryBuilders.matchQuery((String) customFields.getKey(), customFields.getValue()).operator(
                            org.elasticsearch.index.query.MatchQueryBuilder.Operator.AND));
                    if ("OR".equalsIgnoreCase(op)) {

                        beforeQueryBuilder.must(afterQueryBuider);

                    } else if ("AND".equalsIgnoreCase(op)) {
                        beforeQueryBuilder.should(afterQueryBuider);
                    }
                    afterQueryBuider = null;
                }
            }
            counter++;

        }
        query = beforeQueryBuilder;
        return query;
    }

    /** Gets the client.
     *
     * @return Elastic Search Client */
    private Client getClient() {
        LOGGER.debug(" ElasticSearchBatchCustomChannel:Enters into getClient: ");
        Settings settings = Settings.settingsBuilder().put("cluster.name", elasticSearchClusterName).put("client.transport.sniff", true).build();

        client = TransportClient.builder().settings(settings).build();
        ((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(hostName, port)));
        return client;

    }

    /** executes elastic search query to fetch entire data of an index, which can latter be filtered on the basis of range.
     * 
     * @param client
     *            name of client
     * @return Elastic search Query result */
    private List<Object> getESDataByCollection(Client client) {
        LOGGER.debug("ElasticSearchBatchCustomChannel:Enters into  getESDataByCollection: ");
        List<Object> esResponse = new ArrayList<Object>();
        QueryBuilder qbuilder = QueryBuilders.matchAllQuery();
        if (null != client) {

            SearchRequestBuilder builder = client.prepareSearch(indexName);

            if (to != null && from != null) {
                builder.setQuery(qbuilder).setPostFilter(fetchRangeQueryBuilder()).setSize(pageSize);
            } else {
                builder.setQuery(qbuilder).setSize(pageSize);
            }
            SearchResponse response = null;
            response = builder.execute().actionGet();
            for (SearchHit searchHit : response.getHits()) {
                esResponse.add(searchHit.getSourceAsString().getBytes());
            }
        }
        return esResponse;
    }

    /** executes elastic search query to fetch data by input Query, Query Can be a simple JSOn Query or a Qurey String query which can latter be
     * filtered on the basis of range.
     * 
     * @param client
     *            name of client
     * @param isQueryString
     *            check query string
     * @return Elastic search query result */
    private List<Object> getESDataByJSONQuery(Client client, boolean isQueryString) {
        LOGGER.debug("ElasticSearchBatchCustomChannel: getESDataByJSONQuery: ");
        List<Object> esResponse = new ArrayList<Object>();
        SearchRequestBuilder builder = client.prepareSearch(indexName);

        if (isQueryString) {
            QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder(eSQuery);
            queryBuilder.defaultOperator(Operator.AND);
            builder.setQuery(queryBuilder).setSize(pageSize);
        } else {
            if (to != null && from != null) {
                builder.setQuery(eSQuery).setPostFilter(fetchRangeQueryBuilder()).setSize(pageSize);
            } else {
                builder.setQuery(eSQuery).setSize(pageSize);
            }
        }
        SearchResponse response = builder.execute().actionGet();

        for (SearchHit iterableElement : response.getHits()) {
            esResponse.add(iterableElement.getSourceAsString().getBytes());
        }

        return esResponse;
    }

    /** Fetch range query builder.
     *
     * @return rangeQuery Builder */
    private QueryBuilder fetchRangeQueryBuilder() {
        LOGGER.debug("ElasticSearchBatchCustomChannel: fetchRangeQueryBuilder: ");
        int toInt = 0;
        int fromInt = 0;
        Date enddate = null;
        Date startDate = null;
        QueryBuilder builder = null;

        if (StringUtils.isEmpty(dateFormat)) {
            toInt = Integer.parseInt((String) to);
            fromInt = Integer.parseInt((String) from);
            builder = QueryBuilders.rangeQuery(rangeField).from(fromInt).to(toInt);
        } else {
            SimpleDateFormat format = new SimpleDateFormat(dateFormat);
            try {
                startDate = format.parse((String) from);
                enddate = format.parse((String) to);
                java.sql.Date fromDate = new java.sql.Date(startDate.getTime());
                java.sql.Date todate = new java.sql.Date(enddate.getTime());

                Calendar cal1 = Calendar.getInstance();
                cal1.setTime(fromDate);
                Calendar cal2 = Calendar.getInstance();
                cal2.setTime(todate);
                int year1 = cal1.get(Calendar.YEAR);
                int year2 = cal2.get(Calendar.YEAR);

                builder = QueryBuilders.rangeQuery(rangeField).from(year1).to(year2);

            } catch (ParseException e) {
                LOGGER.error("Error occurred in fetchRangeQueryBuilder :" , e);
            }
        }
        return builder;
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#cleanup()
     */
    @Override
    public void cleanup() {
    }
}
