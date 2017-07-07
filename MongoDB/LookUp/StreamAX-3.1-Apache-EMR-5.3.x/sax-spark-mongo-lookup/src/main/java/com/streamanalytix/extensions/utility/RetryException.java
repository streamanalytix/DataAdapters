package com.streamanalytix.extensions.utility;

/** Custom Exception */
public class RetryException extends Exception {

    /** @param msg
     *            "exception message for throwing exception" */
    public RetryException(String msg) {
        super(msg);
    }

}
