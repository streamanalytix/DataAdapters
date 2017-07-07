/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.retry;

import java.io.IOException;
import java.net.Socket;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.streamanalytix.extensions.exceptions.ServiceNotAvailableException;

/** This is generic class to check whether any service is available or not. Basically, it tries to create socket connection using IP and PORT of any
 * service, if Socket connection create successfully, service is available. If Socket connection failed, it will retry after specified time and will
 * keep retrying retry count times. */
public class RetryNTimes {

    private static final Log LOGGER = LogFactory.getLog(RetryNTimes.class);

    /** Check service's availability.
     * 
     * @param host
     *            This parameter is the hostname of the couchbase server
     * @param port
     *            This parameter is the port of the couchbase server
     * @param retryCount
     *            This parameter is the retryCount of the couchbase server
     * @return returns true if service is available */
    public static boolean serviceAvailable(String host, int port, int retryCount) {
        RetryOnException retry = new RetryOnException(retryCount);
        boolean serviceAvailable = false;
        Socket socket = null;
        int count = 0;
        while (retry.shouldRetry()) {
            try {
                LOGGER.debug("Requested URL:" + host);
                socket = new Socket(host, port);
                if (socket.isConnected()) {
                    socket.close();
                    serviceAvailable = true;
                    break;
                }

            } catch (Exception e) {
                try {
                    LOGGER.warn("Connection failed.....");
                    retry.errorOccured();
                } catch (Exception e1) {
                    throw new ServiceNotAvailableException("Exception while calling URL:" + host, e);
                }
            } finally {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        LOGGER.warn("Error in closing socket");
                    }
                }
            }
        }
        return serviceAvailable;
    }
}
