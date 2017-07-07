/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.channel;

import static com.streamanalytix.extensions.SqsConstants.AWS_ACCESS_KEY_ID;
import static com.streamanalytix.extensions.SqsConstants.AWS_SECRET_KEY;
import static com.streamanalytix.extensions.SqsConstants.QUEUE_NAME;
import static com.streamanalytix.extensions.SqsConstants.AWS_REGION;
import static com.streamanalytix.extensions.SqsConstants.MAX_NUMBER_MESSAGES;
import static com.streamanalytix.extensions.SqsConstants.VISIBILITY_TIMEOUT;
import static com.streamanalytix.extensions.SqsConstants.WAIT_TIME_SECONDS;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.OverLimitException;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.streamanalytix.framework.api.spark.channel.BaseChannel;

/** The Class SQSChannel. */
public class SQSChannel extends BaseChannel implements Serializable {

    /** Constant serialVersionUID. */
    private static final long serialVersionUID = 8657585005598090114L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SQSChannel.class);

    /** queue name. */
    private String queueName;

    /** AWS access key. */
    private String accessKey;

    /** AWS secret key. */
    private String secretKey;

    /** Region where queue exist. */
    private Regions region = Regions.US_WEST_2;

    /** The max no of msgs to return. */
    private final Integer ten = 10;

    /** The max number of messages. */
    private Integer maxNumberOfMessages = ten;

    /** Duration for which msg is hidden from subsequent retrieve requests. */
    private Integer visibilityTimeout;

    /** Duration for which call waits for msg to arrive in queue. */
    private static final Integer TWENTY = 20;

    /** The wait time seconds. */
    private Integer waitTimeSeconds = TWENTY;

    /** Duration for which call waits for msg to arrive in queue. */
    private final Integer thousand = 1000;

    /** SQS client. */
    private static AmazonSQS sqsClient;

    /** Queue url. */
    private String queueUrl;

    /** SQS Receiver. */
    private SQSReceiver sqsReceiver;

    /** Instantiates a new SQS channel helper. */
    public SQSChannel() {
        this.sqsReceiver = new SQSReceiver();
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    /** Inits the.
     *
     * @param configMap
     *            the config map */
    @Override
    public void init(Map<String, Object> configMap) {
        if (configMap.containsKey(AWS_ACCESS_KEY_ID)) {
            accessKey = (String) configMap.get(AWS_ACCESS_KEY_ID);
        }
        if (configMap.containsKey(AWS_SECRET_KEY)) {
            secretKey = (String) configMap.get(AWS_SECRET_KEY);
        }
        if (configMap.containsKey(QUEUE_NAME)) {
            this.queueName = (String) configMap.get(QUEUE_NAME);
        }

        if (configMap.containsKey(AWS_REGION)) {
            String regionName = (String) configMap.get(AWS_REGION);
            try {
                region = Regions.fromName(regionName);
            } catch (IllegalArgumentException e) {
                // use default Region
                LOGGER.warn("Invalid Region name was provided in config paramters");
            }
        }

        if (configMap.containsKey(MAX_NUMBER_MESSAGES)) {
            try {
                this.maxNumberOfMessages = (Integer) configMap.get(MAX_NUMBER_MESSAGES);
            } catch (ClassCastException e) {
                // use default value
                LOGGER.warn("max no. msgs not provided, using default as True");
            }
        }

        if (configMap.containsKey(VISIBILITY_TIMEOUT)) {
            try {
                this.visibilityTimeout = (Integer) configMap.get(VISIBILITY_TIMEOUT);
            } catch (ClassCastException e) {
                // use default value
                LOGGER.warn("visiblity time not provided, using default");
            }
        }

        if (configMap.containsKey(WAIT_TIME_SECONDS)) {
            try {
                this.waitTimeSeconds = (Integer) configMap.get(WAIT_TIME_SECONDS);
            } catch (ClassCastException e) {
                // use default value
                LOGGER.warn("Wait time not provided, using default");

            }
        }
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.channel.Channel#getDStream(org. apache.spark.streaming.api.java.JavaStreamingContext)
     */
    /** Gets the d stream.
     *
     * @param context
     *            the context
     * @return the d stream */
    @Override
    public JavaDStream<Object> getDStream(JavaStreamingContext context) {
        JavaReceiverInputDStream<String> result = context.receiverStream(sqsReceiver);

        return result.map(new Function<String, Object>() {
            private static final long serialVersionUID = 7522170517856295596L;

            @Override
            public Object call(String arg0) throws Exception {
                return arg0.toString().getBytes();
            }
        });
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#cleanup()
     */
    /** Cleanup. */
    @Override
    public void cleanup() {
    }

    /** Class SQSReceiver. */
    class SQSReceiver extends Receiver<String> {

        /** The Constant serialVersionUID. */
        private static final long serialVersionUID = 1L;

        /** constructor. */
        protected SQSReceiver() {
            super(StorageLevel.MEMORY_AND_DISK_2());
        }

        /*
         * (non-Javadoc)
         * @see org.apache.spark.streaming.receiver.Receiver#onStart()
         */
        /** On start. */
        @Override
        public void onStart() {
            // Start the thread that receives data over a connection
            new Thread() {
                @Override
                public void run() {
                    try {
                        sqsClient = AmazonSQSClientBuilder.standard().withRegion(region)
                                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))).build();
                        queueUrl = sqsClient.getQueueUrl(new GetQueueUrlRequest(queueName)).getQueueUrl();
                        receive();
                    } catch (InterruptedException | IllegalArgumentException | QueueDoesNotExistException ex) {
                        LOGGER.error("Exception occurred while creating SQS client: " , ex);
                    }
                }
            }.start();

        }

        /*
         * (non-Javadoc)
         * @see org.apache.spark.streaming.receiver.Receiver#onStop()
         */
        /** On stop. */
        @Override
        public void onStop() {
            // There is nothing much to do as the thread calling poll()
            // is designed to stop by itself isStopped() returns false
        }

        /** fetches messages from SQS Queue.
         *
         * @throws InterruptedException
         *             the interrupted exception */
        private void receive() throws InterruptedException {
            try {
                while (!isStopped()) {
                    ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(waitTimeSeconds);
                    if (maxNumberOfMessages != null) {
                        receiveMessageRequest.setMaxNumberOfMessages(maxNumberOfMessages);
                    }
                    if (visibilityTimeout != null) {
                        receiveMessageRequest.setVisibilityTimeout(visibilityTimeout);
                    }
                    ReceiveMessageResult result = sqsClient.receiveMessage(receiveMessageRequest);
                    List<Message> messages = result.getMessages();
                    if (messages != null && messages.size() > 0) {
                        for (Message message : messages) {
                            store(message.getBody());
                            sqsClient.deleteMessage(queueUrl, message.getReceiptHandle());
                        }
                    }
                    Thread.sleep(thousand);
                }
            } catch (OverLimitException ex) {
                LOGGER.error("Exception occurred while receiving message: " , ex);
                restart(ex.getMessage(), ex);
            }
        }
    }
}
