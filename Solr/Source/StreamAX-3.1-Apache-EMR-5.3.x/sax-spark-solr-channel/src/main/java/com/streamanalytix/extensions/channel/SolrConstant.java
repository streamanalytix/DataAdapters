/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.channel;

/** Solar constants. */
public interface SolrConstant {

    /** The default page number. */
    int DEFAULT_PAGE_NUMBER = 0;

    /** The default retry. */
    int DEFAULT_RETRY = 3;

    /** The default cycle time. */
    int DEFAULT_CYCLE_TIME = 2000;

    /** The default page size. */
    int DEFAULT_PAGE_SIZE = 100;

    /** The query. */
    String QUERY = "query";

    /** The asc sort. */
    String ASC_SORT = "asc";

    /** The desc sort. */
    String DESC_SORT = "desc";

    /** The zk host. */
    String ZK_HOST = "zkHost";

    /** The collection. */
    String COLLECTION = "collection";

    /** The page number. */
    String PAGE_NUMBER = "pagenumber";

    /** The page size. */
    String PAGE_SIZE = "pagesize";

    /** The search type. */
    String SEARCH_TYPE = "searchType";

    /** The query string. */
    String QUERY_STRING = "queryString";

    /** The filter query. */
    String FILTER_QUERY = "filterQuery";

    /** The sort. */
    String SORT = "sort";

    /** The tried attempts. */
    String TRIED_ATTEMPTS = "triedAttempts";

    /** The cycle time. */
    String CYCLE_TIME = "cycleTime";
}