/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.retry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.streamanalytix.extensions.exceptions.ServiceNotAvailableException;
import com.streamanalytix.extensions.utils.CustomConstants;

/** The Class RetryOnException. */
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
     *            the retry count */
    public RetryOnException(int retryCount) {
        this(retryCount, CustomConstants.Values.DEFAULT_WAIT_TIME_IN_MILLI);
    }

    /** Instantiates a new retry on exception.
     *
     * @param numberOfRetries
     *            the number of retries
     * @param timeToWait
     *            the time to wait */
    public RetryOnException(int numberOfRetries, long timeToWait) {
        this.numberOfRetries = numberOfRetries;
        this.numberOfTriesLeft = numberOfRetries;
        this.timeToWait = timeToWait;
    }

    /** Should retry.
     *
     * @return true, if successful */
    public boolean shouldRetry() {
        return numberOfTriesLeft > 0;
    }

    /** Error occured.
     *
     * @throws ServiceNotAvailableException
     *             the service not available exception */
    public void errorOccured() throws ServiceNotAvailableException {
        numberOfTriesLeft--;
        if (!shouldRetry()) {
            throw new ServiceNotAvailableException("Retry Failed: Total " + numberOfRetries + " attempts made at interval " + timeToWait + "ms");
        }
        try {
            Thread.sleep(timeToWait);
        } catch (InterruptedException e) {
            LOGGER.error("Error occured" , e);
        }
    }
}
