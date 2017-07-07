/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.utils;

/** This is generic constant class and can be used to define all constants used in custom components. */
public final class CustomConstants {

    /** Instantiates a new custom constants. */
    private CustomConstants() {
    }

    /** Defines all Literal in this inner class. */
    public static class Literal {

        /** The Constant BUCKET_NAME. */
        public static final String BUCKET_NAME = "BUCKET_NAME";

        /** The Constant BUCKET_PASSWORD. */
        public static final String BUCKET_PASSWORD = "BUCKET_PASSWORD";

        /** The Constant CREATE_NEW_BUCKET. */
        public static final String CREATE_NEW_BUCKET = "CREATE_NEW_BUCKET";

        /** The Constant CONNECT_TIMEOUT. */
        public static final String CONNECT_TIMEOUT = "CONNECT_TIMEOUT";

        /** The Constant CLUSTER_NODES. */
        public static final String CLUSTER_NODES = "CLUSTER_NODES";

        /** The Constant CLUSTER_ADMIN_USERNAME. */
        public static final String CLUSTER_ADMIN_USERNAME = "CLUSTER_ADMIN_USERNAME";

        /** The Constant CLUSTER_ADMIN_PASSWORD. */
        public static final String CLUSTER_ADMIN_PASSWORD = "CLUSTER_ADMIN_PASSWORD";

        /** The Constant DOCUMENT_TTL. */
        public static final String DOCUMENT_TTL = "DOCUMENT_TTL";

        /** The Constant DOCUMENT_REPLICAS. */
        public static final String DOCUMENT_REPLICAS = "DOCUMENT_REPLICAS";

        /** The Constant SAX_MANDATORY_FIELD_1. */
        public static final String SAX_MANDATORY_FIELD_1 = "tenantId";

        /** The Constant RETRY_COUNT. */
        public static final String RETRY_COUNT = "RETRY_COUNT";

        /** The Constant PROCESS_IN_BULK. */
        public static final String PROCESS_IN_BULK = "PROCESS_IN_BULK";

        /** The Constant USER_NAME. */
        public static final String USER_NAME = "USER-NAME";

        /** The Constant PASSWORD. */
        public static final String PASSWORD = "PASSWORD";

        /** The Constant HOSTNAME. */
        public static final String HOSTNAME = "HOSTNAME";

        /** The Constant PORT. */
        public static final String PORT = "PORT";

        /** The Constant DELAY_BETWEEN_CONNECTION_RETRIES. */
        public static final String DELAY_BETWEEN_CONNECTION_RETRIES = "DELAY BETWEEN CONNECTION RETRIES";
        
        /** The Constant QUERY. */
        public static final String QUERY = "QUERY";

        /** The Constant CONNECTION_RETRY_COUNT. */
        public static final String CONNECTION_RETRY_COUNT = "CONNECTION_RETRY_COUNT";
    }

    /** Define all delimiters in this inner class. */
    public static class Delimiter {

        /** The Constant COMMA. */
        public static final String COMMA = ",";

        /** The Constant HASH. */
        public static final String HASH = "#";

        /** The Constant SEMI_COLON. */
        public static final String SEMI_COLON = ";";

        /** The Constant PIPE. */
        public static final String PIPE = "|";

        /** The Constant COLON. */
        public static final String COLON = ":";
    }

    /** Define all values here in this class. */
    public static class Values {
        
        /** The Constant DEFAULT_DELAY_BETWEEN_CONNECTION_RETRIES. */
        public static final Integer DEFAULT_DELAY_BETWEEN_CONNECTION_RETRIES = 10000;

        /** The Constant DEFAULT_RETRY_COUNT. */
        public static final int DEFAULT_RETRY_COUNT = 3;

        /** The Constant NO. */
        public static final String NO = "no";

        /** The Constant YES. */
        public static final String YES = "YES";

        /** The Constant BUCKET_QUOTA. */
        public static final int BUCKET_QUOTA = 120;

        /** The Constant DEFAULT_RETRIES. */
        public static final int DEFAULT_RETRIES = 3;

        /** The Constant DEFAULT_WAIT_TIME_IN_MILLI. */
        public static final long DEFAULT_WAIT_TIME_IN_MILLI = 2000;

        /** The Constant DEFAULT_TTL. */
        public static final int DEFAULT_TTL = 3;

        /** The Constant DEFAULT_REPLICAS. */
        public static final int DEFAULT_REPLICAS = 0;
        
        /** The Constant BUFFER_SIZE. */
        public static final int BUFFER_SIZE = 1024;

    }

    /** The Class Numbers. */
    public static class Numbers {

        /** The Constant ZERO. */
        public static final int ZERO = 0;

        /** The Constant ONE. */
        public static final int ONE = 1;

        /** The Constant TWO. */
        public static final int TWO = 2;

        /** The Constant THREE. */
        public static final int THREE = 3;

        /** The Constant FOUR. */
        public static final int FOUR = 4;

        /** The Constant FIVE. */
        public static final int FIVE = 5;

        /** The Constant SIX. */
        public static final int SIX = 6;

        /** The Constant SEVEN. */
        public static final int SEVEN = 7;

        /** The Constant EIGHT. */
        public static final int EIGHT = 8;

        /** The Constant NINE. */
        public static final int NINE = 9;
    }
}
