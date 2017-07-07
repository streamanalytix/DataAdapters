/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.channel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.noggit.JSONUtil;

//import com.lucidworks.spark.SolrRDD;
import com.streamanalytix.framework.api.spark.channel.BaseChannel;

/** The Class SampleCustomChannel. */
public class SolrBatchChannel extends BaseChannel implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -8657585005598090114L;
    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SolrBatchChannel.class);
    /** The host name. */
    private String hostName;
    /** The port. */
    private Integer port;
    /** Page number. */
    private Integer pagenumber = new Integer(SolrConstant.DEFAULT_PAGE_NUMBER);
    /** Page Size. */
    private Integer pagesize = new Integer(SolrConstant.DEFAULT_PAGE_SIZE);
    /** Collection Name. */
    private String collection;
    /** Zookeeper host. */
    private String zkHost;
    /** Search type[Query/collection]. */
    private String searchType;
    /** Query String. */
    private String queryString;
    /** Field List. */
    private String fieldList = StringUtils.EMPTY;
    /** Filter Query. */
    private String filterQuery = StringUtils.EMPTY;
    /** Field for sorting. */
    private String sort = StringUtils.EMPTY;
    /** Number of Retried Attempts. */
    private int retriedAttempts = SolrConstant.DEFAULT_RETRY;
    /** Cycle time between two retried attempts. */
    private int cycleTime = SolrConstant.DEFAULT_CYCLE_TIME;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    @Override
    public void init(Map<String, Object> configMap) {
        if (configMap.containsKey(SolrConstant.ZK_HOST)) {
            zkHost = (String) configMap.get(SolrConstant.ZK_HOST);
        }

        if (configMap.containsKey(SolrConstant.COLLECTION)) {
            collection = (String) configMap.get(SolrConstant.COLLECTION);
        }

        if (configMap.containsKey(SolrConstant.PAGE_NUMBER)) {
            pagenumber = Integer.parseInt((String) configMap.get(SolrConstant.PAGE_NUMBER));
        }

        if (configMap.containsKey(SolrConstant.PAGE_SIZE)) {
            pagesize = Integer.parseInt((String) configMap.get(SolrConstant.PAGE_SIZE));
        }

        if (configMap.containsKey(SolrConstant.SEARCH_TYPE)) {
            searchType = (String) configMap.get(SolrConstant.SEARCH_TYPE);
        }

        if (configMap.containsKey(SolrConstant.QUERY_STRING)) {
            queryString = (String) configMap.get(SolrConstant.QUERY_STRING);
        }

        if (configMap.containsKey(SolrConstant.FILTER_QUERY)) {
            filterQuery = (String) configMap.get(SolrConstant.FILTER_QUERY);
        }

        if (configMap.containsKey(SolrConstant.SORT)) {
            sort = (String) configMap.get(SolrConstant.SORT);
        }

        if (configMap.containsKey(SolrConstant.TRIED_ATTEMPTS)) {
            retriedAttempts = Integer.parseInt((String) configMap.get(SolrConstant.TRIED_ATTEMPTS));
        }

        if (configMap.containsKey(SolrConstant.CYCLE_TIME)) {
            cycleTime = Integer.parseInt((String) configMap.get(SolrConstant.CYCLE_TIME));
        }

        CloudSolrClient solrCloudClient = new CloudSolrClient.Builder().withZkHost(zkHost).build();

        solrCloudClient.setDefaultCollection(collection);

        while (retriedAttempts > 0) {
            try {
                solrCloudClient.ping();
                retriedAttempts = 0;

            } catch (Exception e) {
                retriedAttempts--;
                try {
                    Thread.currentThread().sleep(cycleTime);
                } catch (InterruptedException e1) {
                    LOGGER.error("Exception while connection to solr", e1);
                }
            }
        }

        if (retriedAttempts == 0) {
            try {
                solrCloudClient.ping();
            } catch (Exception e) {
                LOGGER.error("Exception while connection to solr", e);
                throw new RuntimeException(e);
            }
        }

    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.Channel#getDStream(org. apache.spark.streaming.api.java.JavaStreamingContext)
     */
    @Override
    public JavaDStream<Object> getDStream(JavaStreamingContext context) {
        JavaReceiverInputDStream<String> lines = context.socketTextStream(hostName, port);

        return lines.map(new Function<String, Object>() {
            private static final long serialVersionUID = 7522170517856295596L;

            @Override
            public Object call(String arg0) throws Exception {
                return arg0.toString().getBytes();
            }
        });
    }

    /** Gets the rdd.
     *
     * @param context
     *            the context
     * @return the rdd */
    @Override
    public JavaRDD<Object> getRDD(JavaSparkContext context) {
        JavaRDD<Object> titleNumbers = null;
        try {

            CloudSolrClient solrCloudClient = new CloudSolrClient.Builder().withZkHost(zkHost).build();

            solrCloudClient.setDefaultCollection(collection);

            SolrQuery query = new SolrQuery();

            if (SolrConstant.QUERY.equalsIgnoreCase(searchType)) {
                query.setQuery(queryString);
            } else {
                query.setQuery("*:*");
            }

            query.setFields(fieldList);
            query.setFilterQueries(filterQuery);
            query.setFacet(true);
            query.setStart(pagenumber);
            query.setRows(pagesize);

            applySortClause(query);
            query.setShowDebugInfo(true);
            QueryResponse rsp = solrCloudClient.query(query);

            SolrDocumentList docs = rsp.getResults();
            LOGGER.debug("Result found :" + docs.toString());

            JavaRDD<SolrDocument> solrJavaRDD = context.parallelize(docs);
            titleNumbers = solrJavaRDD.map(new Function<SolrDocument, Object>() {

                @Override
                public Object call(SolrDocument arg0) throws Exception {
                    String returnValue = JSONUtil.toJSON(arg0);
                    return returnValue.getBytes();
                }
            });
            return titleNumbers;
        } catch (Exception e) {
            LOGGER.error("Exception while query execution", e);
            throw new RuntimeException(e);
        }

    }

    /** Apply sort clause.
     *
     * @param query
     *            solar query. */
    private void applySortClause(SolrQuery query) {
        if (sort.trim().length() > 0) {
            String[] sortValueArr = sort.split(",");
            List<SolrQuery.SortClause> sortList = new ArrayList<>();
            for (String sortValue : sortValueArr) {
                String[] sortValues = sortValue.split(" ");
                if (sortValues.length == 2) {
                    if (SolrConstant.ASC_SORT.equals(sortValues[1]))
                        sortList.add(SortClause.asc(sortValues[0]));
                    else if (SolrConstant.DESC_SORT.equals(sortValues[1]))
                        sortList.add(SortClause.desc(sortValues[0]));
                }

            }
            query.setSorts(sortList);
        }
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#cleanup()
     */
    @Override
    public void cleanup() {
    }
}