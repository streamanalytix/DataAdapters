package com.streamanalytix.extensions.lookup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;

import com.streamanalytix.framework.api.udf.Function;

/** The Class SolrLookupFunction. */
@SuppressWarnings("deprecation")
public class SolrLookupFunction implements Function {

  /** The Constant serialVersionUID. */
  private static final long serialVersionUID = 603358121390027959L;

  /** The Constant LOGGER. */
  private static final Log LOGGER = LogFactory.getLog(SolrLookupFunction.class);

  /** The Constant CloudSolrServer. */
  private CloudSolrServer solrServer = null;

  /** The Constant Zookeeper Host. */
  private static final String ZK_HOSTS = "zkHosts";

  /** The Constant index name. */
  private static final String INDEX_NAME = "indexname";

  /** The Constant BLANK. */
  private static final String BLANK = StringUtils.EMPTY;

  /** The Constant ASC. */
  private static final String SORT_ASC = "asc";

  /** The Constant DESC. */
  private static final String SORT_DESC = "desc";

  /** The Constant Solar retry count. */
  private static final String RETRY_COUNT = "Retry Count";

  /** The Constant Solar retry sleep time. */
  private static final String RETRY_SLEEP_TIME = "Retry Sleep Time";

  private static final int SEVEN = 7;

  /** The init method is used to initialize the configuration.
   *
   * @param conf
   *          the conf */
  @Override
  public void init(Map<String, Object> conf) {
    LOGGER.info("Inside LookupSolrSearch init....");
    try {
      String zkPath = String.valueOf(conf.get(ZK_HOSTS));
      String indexName = String.valueOf(conf.get(INDEX_NAME));
      int retryCount = Integer.parseInt(((String) conf.get(RETRY_COUNT)).trim());
      int retrySleepTime = Integer.parseInt(((String) conf.get(RETRY_SLEEP_TIME)).trim());
      solrServer = new CloudSolrServer(zkPath);
      solrServer.setDefaultCollection(indexName);
      int count = 1;
      while (count < retryCount + 1) {
        try {
          if (solrServer.ping().getStatus() == 0) {
            break;
          }
        } catch (Exception e) {
          LOGGER.info("Retrying to connect to Solr....retry count:" + count);
        }
        Thread.sleep(retrySleepTime);
        count++;
        solrServer = new CloudSolrServer(zkPath);
        solrServer.setDefaultCollection(indexName);
      }
      LOGGER.info("Solr connection status :" + solrServer.ping().getStatus());
    } catch (Exception ex) {
      LOGGER.error("Error while connecting Solr Server. " + ex.getMessage());
      throw new RuntimeException("Error while connecting Solr Server. " + ex.getMessage());
    }
    LOGGER.info("Exit LookupSolrSearch init.");
  }

  /** The process method is used to write custom implementation.
   *
   * @param param
   *          the param
   * @return function value
   * @throws Exception
   *           the exception */
  @Override
  public Object execute(Object... param) throws Exception {
    LOGGER.info("Inside LookupSolrSearch execute. " + Arrays.toString(param));
    SolrDocumentList solrQResponse = null;
    List<HashMap<String, String>> responseList = new ArrayList<HashMap<String, String>>();

    if (null != solrServer && null != param && param.length == SEVEN) {
      SolrQuery solrQuery = new SolrQuery();
      int i = 0;
      String query = String.valueOf(param[i++]).trim();
      solrQuery.setQuery(query);

      String fq = String.valueOf(param[i++]).trim();
      solrQuery.setFilterQueries(fq);

      String sortOrder = String.valueOf(param[i++]).trim();
      String sortField = String.valueOf(param[i++]).trim();

      if (!sortField.equals(BLANK)
          && Arrays.asList(SORT_ASC, SORT_DESC).contains(sortOrder.toLowerCase())) {
        ORDER order = sortOrder.equalsIgnoreCase(SORT_ASC) ? ORDER.asc : ORDER.desc;
        solrQuery.setSort(sortField, order);
      }

      int start = Integer.parseInt(((String) param[i++]).trim());
      solrQuery.setStart(start);

      int rows = Integer.parseInt(((String) param[i++]).trim());
      solrQuery.setRows(rows);

      String fields = String.valueOf(param[i++]).trim();
      solrQuery.setFields(fields);
      LOGGER.debug("solr lookup solrQuery " + solrQuery);
      SolrParams params = SolrParams.toSolrParams(solrQuery.toNamedList());
      solrQResponse = solrServer.query(params).getResults();
      if (solrQResponse.getNumFound() > 0) {
        Iterator<SolrDocument> iteratorSolrDoc = solrQResponse.iterator();
        while (iteratorSolrDoc.hasNext()) {
          SolrDocument solrDocument = iteratorSolrDoc.next();
          Iterator<String> names = solrDocument.getFieldNames().iterator();
          HashMap<String, String> map = new HashMap<String, String>();
          while (names.hasNext()) {
            String name = names.next();
            solrDocument.getFieldValue(name).toString();
            map.put(name, solrDocument.getFieldValue(name).toString());
          }
          responseList.add(map);
        }
        LOGGER.debug("solrDocument found are " + responseList.toArray());
      }
    } else {
      LOGGER.debug("Either connection or parameter is null");
    }
    return responseList != null ? responseList.toString() : null;
  }

  /** The cleanup method to free the resources. */
  @Override
  public void cleanup() {
    LOGGER.info("shutdown  the Solr Server.");
    if (null != solrServer) {
      solrServer.shutdown();
    }
  }
}
