/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.utility;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** The Class RetryStrategyMongoDao. */
public class RetryStrategyMongoDao {

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(RetryStrategyMongoDao.class);

    /** The Constant DEFAULT_NUMBER_OF_RETRIES. */
    public static final int DEFAULT_NUMBER_OF_RETRIES = 3;

    /** The Constant DEFAULT_WAIT_TIME. */
    public static final long DEFAULT_WAIT_TIME = 10;

    /** The number of retries. */
    private int numberOfRetries; // total number of tries

    /** The number of tries left. */
    private int numberOfTriesLeft; // number left

    /** The time to wait. */
    private long timeToWait; // wait interval

    /** default one is called . */
    public RetryStrategyMongoDao() {
        this.numberOfRetries = DEFAULT_NUMBER_OF_RETRIES;
        this.numberOfTriesLeft = numberOfRetries;
        this.timeToWait = DEFAULT_WAIT_TIME;
    }

    /** Instantiates a new retry strategy mongo dao.
     *
     * @param numberOfRetries
     *            "number of try"
     * @param timeToWait
     *            "waiting time" */
    public RetryStrategyMongoDao(int numberOfRetries, long timeToWait) {
        this.numberOfRetries = numberOfRetries;
        this.numberOfTriesLeft = numberOfRetries;
        this.timeToWait = timeToWait;
    }

    /** Should retry.
     *
     * @return true if there are tries left */

    public boolean shouldRetry() {
        return numberOfTriesLeft >= 0;
    }

    /** waiting time .
     * 
     * @return timeToWait "waiting time" */
    public long getTimeToWait() {
        return timeToWait;
    }

    /** it will wait for next connection . */
    private void waitUntilNextTry() {
        try {
            Thread.sleep(getTimeToWait());
        } catch (InterruptedException ignored) {
            LOGGER.info("Exception occured in waitUntilNextTry() ", ignored);
        }
    }

    /** retry logic .
     * 
     * @throws RetryException
     *             "the exception" */

    public void errorOccured() throws RetryException {
        numberOfTriesLeft--;
        if (!shouldRetry()) {
            throw new RetryException(numberOfRetries + " " + " attempts to retry failed at " + " " + getTimeToWait() + " " + "ms interval");
        }
        LOGGER.info("errorOccured method is calling and remaining tries" + numberOfTriesLeft);
        waitUntilNextTry();
    }

}
