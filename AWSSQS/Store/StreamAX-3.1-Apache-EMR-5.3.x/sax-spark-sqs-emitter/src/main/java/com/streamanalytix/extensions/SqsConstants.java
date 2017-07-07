/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions;

/** Class for constant values. */
public interface SqsConstants {

    /** AWS user access key */
    String AWS_ACCESS_KEY_ID = "ACCESS_KEY";

    /** AWS user secret key */
    String AWS_SECRET_KEY = "SECRET_KEY";

    /** AWS user region key */
    String AWS_REGION = "REGION";

    /** The max no of msgs to return */
    String MAX_NUMBER_MESSAGES = "MAX_NUMBER_MESSAGES";

    /** Duration for which msg is hidden from subsequent retrieve requests. */
    String VISIBILITY_TIMEOUT = "VISIBILITY_TIMEOUT";

    /** Duration for which call waits for msg to arrive in queue. */
    String WAIT_TIME_SECONDS = "WAIT_TIME_SECONDS";

    /** message groupId */
    String MESSAGE_GROUP_ID = "MESSAGE_GROUP_ID";

    /** delay seconds */
    String DELAY_SECONDS = "DELAY_SECONDS";

    /** Content based Deduplication */
    String IS_CONTENTBASEDDEDUPLICATION = "CONTENTBASEDDEDUPLICATION";

    /** Queue name */
    String QUEUE_NAME = "QUEUE_NAME";

    /** delay seconds */
    String DELAYSECONDS = "DelaySeconds";

    /** Content based Deduplication */
    String CONTENTBASEDDEDUPLICATION = "ContentBasedDeduplication";

    /** Content based Deduplication */
    String FIFO_EXT = ".fifo";

    /** Content based Deduplication */
    String FIFO_QUEUE = "FifoQueue";
}