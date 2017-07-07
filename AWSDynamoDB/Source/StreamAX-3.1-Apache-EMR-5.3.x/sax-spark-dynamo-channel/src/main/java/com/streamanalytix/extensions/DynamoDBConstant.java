/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions;

/** This interface contains constant used to process data over dynamoDB. */
public interface DynamoDBConstant {

    /** map field value separator. */
    String FIELD_SEPARATOR = ",";

    /** map fields separator. */
    String COLLUMN_VALUE_SEPARATOR = "=";

    /** AWS user access key. */
    String AWS_ACCESS_KEY_ID = "ACCESS_KEY";

    /** AWS user secret key. */
    String AWS_SECRET_KEY = "SECRET_KEY";

    /** AWS user region key. */
    String AWS_REGION = "REGION";

    /** AWS dynamoDB table Name key. */
    String TABLE_NAME = "TABLE_NAME";

    /** key condition expression key. */
    String KEY_CONDITION_EXPRESSION = "KEY_CONDITION_EXPRESSION";

    /** query projection expression key. */
    String PROJECTION_EXPRESSION = "PROJECTION_EXPRESSION";

    /** query filter expression key. */
    String FILTER_EXPRESSION = "FILTER_EXPRESSION";

    /** query name map key. */
    String NAME_MAP = "NAME_MAP";

    /** query value map key. */
    String VALUE_MAP = "VALUE_MAP";

    /** query index name key. */
    String INDEX_NAME = "INDEX_NAME";

    /** partition key. */
    String PARTITION_KEY = "PARTITION_KEY";

    /** sort key. */
    String SORT_KEY = "SORT_KEY";

    /** type of partition key. */
    String PARTITION_KEY_TYPE = "PARTITION_KEY_TYPE";

    /** type of sort key. */
    String SORT_KEY_TYPE = "SORT_KEY_TYPE";

    /** read capacity units. */
    String READ_CAPACITY_UNITS = "READ_CAPACITY_UNITS";

    /** write capacity units. */
    String WRITE_CAPACITY_UNITS = "WRITE_CAPACITY_UNITS";

    /** time out. */
    String TIME_OUT = "TIME_OUT";
}