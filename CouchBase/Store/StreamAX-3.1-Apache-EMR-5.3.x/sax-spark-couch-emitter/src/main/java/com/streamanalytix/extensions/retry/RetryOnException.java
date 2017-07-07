/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.retry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.streamanalytix.extensions.exceptions.ServiceNotAvailableException;
import com.streamanalytix.extensions.utils.CustomConstants;

/** RetryOnException is an exception class for throwing any exception while retrying connection with couchbase server. */
public class RetryOnException {

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(RetryOnException.class);

    /** The number of retries. */
    private int numberOfRetries;

    /** The number of tries left. */
    private int numberOfTriesLeft;

    /** The time to wait. */
    private long timeToWait;

    /** Instantiates a new retry on exception. */
    public RetryOnException() {
        this(CustomConstants.Values.DEFAULT_RETRIES, CustomConstants.Values.DEFAULT_WAIT_TIME_IN_MILLI);
    }

    /** Instantiates a new retry on exception.
     *
     * @param retryCount
     *            This parameter is the integer which indicates the number of times it will try to connect the couchbase server in case couchbase
     *            service is not available. */
    public RetryOnException(int retryCount) {
        this(retryCount, CustomConstants.Values.DEFAULT_WAIT_TIME_IN_MILLI);
    }

    /** Instantiates a new retry on exception.
     *
     * @param numberOfRetries
     *            This parameter is the integer which indicates the number of times it will try to connect the couchbase server in case couchbase
     *            service is not available.
     * @param timeToWait
     *            This parameter indicates the time to wait for retry. */
    public RetryOnException(int numberOfRetries, long timeToWait) {
        this.numberOfRetries = numberOfRetries;
        this.numberOfTriesLeft = numberOfRetries;
        this.timeToWait = timeToWait;
    }

    /** Should retry.
     *
     * @return true if there are tries left */
    public boolean shouldRetry() {
        return numberOfTriesLeft > 0;
    }

    /** Error occured.
     *
     * @throws ServiceNotAvailableException
     *             This exception is thrown when any service is not available */
    public void errorOccured() throws ServiceNotAvailableException {
        numberOfTriesLeft--;
        if (!shouldRetry()) {
            throw new ServiceNotAvailableException("Retry Failed: Total " + numberOfRetries + " attempts made at interval " + timeToWait + "ms");
        }
        try {
            Thread.sleep(timeToWait);
        } catch (InterruptedException ignored) {
            LOGGER.warn("Thread interrupted");
        }
    }
}
