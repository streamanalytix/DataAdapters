/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.utils;

/** This is generic constant class and can be used to define all constants used in custom components. */

public final class SftpConstant {

    /** Defines all Literal in this inner class. */
    public class Literal {

        /** The Constant SFTP_HOST. */
        public static final String SFTP_HOST = "SFTP_HOST";
        
        /** The Constant SFTP_USER. */
        public static final String SFTP_USER = "SFTP_USER";
        
        /** The Constant PASSOWRD. */
        public static final String PASSOWRD = "PASSWORD";
        
        /** The Constant LOCATION. */
        public static final String LOCATION = "LOCATION";
        
        /** The Constant FILE_TYPE. */
        public static final String FILE_TYPE = "FILE_TYPE";
        
        /** The Constant CONNECTION_RETRIES. */
        public static final String CONNECTION_RETRIES = "CONNECTION_RETRIES";
        
        /** The Constant DELAY_BTW_CONNECTION_RETRIES. */
        public static final String DELAY_BTW_CONNECTION_RETRIES = "DELAY_BTW_CONNECTION_RETRIES";
    }

    /** Define all values here in this class. */
    public class Values {
        
        /** The Constant DEFAULT_RETRY_COUNT. */
        public static final String DEFAULT_RETRY_COUNT = "3";
        
        /** The Constant DEFAULT_DELAY_BTW_CONNECTION_RETRIES. */
        public static final String DEFAULT_DELAY_BTW_CONNECTION_RETRIES = "10000";
        
        /** The Constant HDFS_WRITE_DF_PATH. */
        public static final String HDFS_WRITE_DF_PATH = "/tmp/sftpWriterHdfsLoc";
        
        /** The Constant LOCAL_PATH_TO_WRITE_RDD. */
        public static final String LOCAL_PATH_TO_WRITE_RDD = "sftpWriterLocalLoc";
        
        /** The Constant SFTP_WRITER_LOCAL_LOCATION. */
        public static final String SFTP_WRITER_LOCAL_LOCATION = "sftpWriterHdfsLoc";
        
        /** The Constant CSV. */
        public static final String CSV = "csv";
        
        /** The Constant JSON. */
        public static final String JSON = "json";
        
        /** The Constant AVRO. */
        public static final String AVRO = "avro";
        
        /** The Constant PARQUET. */
        public static final String PARQUET = "parquet";
    }

}
