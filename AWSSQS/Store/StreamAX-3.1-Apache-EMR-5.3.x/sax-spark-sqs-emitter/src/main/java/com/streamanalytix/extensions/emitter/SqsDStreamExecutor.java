/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.emitter;

import static com.streamanalytix.extensions.SqsConstants.AWS_ACCESS_KEY_ID;
import static com.streamanalytix.extensions.SqsConstants.AWS_SECRET_KEY;
import static com.streamanalytix.extensions.SqsConstants.QUEUE_NAME;
import static com.streamanalytix.extensions.SqsConstants.AWS_REGION;
import static com.streamanalytix.extensions.SqsConstants.MESSAGE_GROUP_ID;
import static com.streamanalytix.extensions.SqsConstants.IS_CONTENTBASEDDEDUPLICATION;
import static com.streamanalytix.extensions.SqsConstants.DELAY_SECONDS;
import static com.streamanalytix.extensions.SqsConstants.FIFO_EXT;
import static com.streamanalytix.extensions.SqsConstants.FIFO_QUEUE;
import static com.streamanalytix.extensions.SqsConstants.CONTENTBASEDDEDUPLICATION;
import static com.streamanalytix.extensions.SqsConstants.DELAYSECONDS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.minidev.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.InvalidMessageContentsException;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamanalytix.framework.api.spark.processor.DStreamProcessor;

/** The Class SqsDStreamExecutor. */
public class SqsDStreamExecutor implements DStreamProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3461208876473240633L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SqsProcessingExecutor.class);

    /** The Constant ONE_TWENTY_EIGHT. */
    private static final int ONE_TWENTY_EIGHT = 128;

    /** queue name. */
    private String queueName;

    /** AWS access key. */
    private String accessKey;

    /** AWS secret key. */
    private String secretKey;

    /** Region where queue exist. */
    private Regions region = Regions.US_WEST_2;

    /** Message groupId. */
    private String messageGroupId;

    /** contentBaseDeduplication. */
    private Boolean contentBasedDeduplication = Boolean.TRUE;

    /** Delay in seconds. */
    private Integer delaySeconds;

    /** Queue url. */
    private String queueUrl;

    /** SQS client. */
    private static AmazonSQS sqsClient;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.spark.processor.SparkProcessor#process(java.lang.Object, java.util.Map)
     */
    @Override
    public JavaDStream<JSONObject> process(JavaDStream<JSONObject> dStream, Map<String, Object> configMap) {
        init(configMap);
        return dStream.map(new Function<JSONObject, JSONObject>() {
            @Override
            public JSONObject call(JSONObject jsonObject) throws Exception {
                sendMessage(jsonObject);
                return jsonObject;
            }
        });
    }

    /** initialization.
     *
     * @param configMap
     *            the config map */
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

        if (configMap.containsKey(MESSAGE_GROUP_ID)) {
            this.messageGroupId = (String) configMap.get(MESSAGE_GROUP_ID);
        }
        
        if (configMap.containsKey(IS_CONTENTBASEDDEDUPLICATION)) {
            try {
                this.contentBasedDeduplication = (Boolean) configMap.get(IS_CONTENTBASEDDEDUPLICATION);
            } catch (ClassCastException e) {
                LOGGER.warn("contentBasedDeduplication not provided, using default as True");
            }
        }
        if (configMap.containsKey(DELAY_SECONDS)) {
            try {
                this.delaySeconds = (Integer) configMap.get(DELAY_SECONDS);
            } catch (ClassCastException e) {
                LOGGER.warn("Delay seconds not provided.using default as true");
            }
        }
        getSQSConnection();
    }

    /** Gets the SQS connection. */
    private void getSQSConnection() {
        try {
            sqsClient = AmazonSQSClientBuilder.standard().withRegion(region)
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))).build();
            queueUrl = sqsClient.getQueueUrl(new GetQueueUrlRequest(queueName)).getQueueUrl();
        } catch (IllegalArgumentException e) {
            LOGGER.error("Exception occurred while creating SQS client: " , e);
        } catch (QueueDoesNotExistException e) {
            Map<String, String> attributes = new HashMap<String, String>();
            if (queueName.endsWith(FIFO_EXT)) {
                attributes.put(CONTENTBASEDDEDUPLICATION, String.valueOf(contentBasedDeduplication));
                attributes.put(FIFO_QUEUE, "true");
                // For FIFO queue delaySeconds is set at queue level and not
                // message level
                if (delaySeconds != null) {
                    attributes.put(DELAYSECONDS, String.valueOf(delaySeconds));
                }
            }
            queueUrl = sqsClient.createQueue(new CreateQueueRequest(queueName)).getQueueUrl();
            LOGGER.info("Created Queue with name : " + queueName);
        }
    }

    /** sends message to sqs queue.
     *
     * @param jsonObject
     *            the json object */
    public void sendMessage(JSONObject jsonObject) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String message = objectMapper.writeValueAsString(jsonObject);
            SendMessageRequest messageRequest = new SendMessageRequest(queueUrl, message);
            if (queueName.endsWith(FIFO_EXT)) {
                messageRequest.setMessageGroupId(messageGroupId != null ? messageGroupId : queueName);
                List<String> attributeNames = new ArrayList<String>();
                attributeNames.add(CONTENTBASEDDEDUPLICATION);
                GetQueueAttributesResult result = sqsClient.getQueueAttributes(queueUrl, attributeNames);
                Map<String, String> attributes = result.getAttributes();
                if (attributes == null
                        || (attributes.containsKey(CONTENTBASEDDEDUPLICATION) && "false".equals(attributes.get(CONTENTBASEDDEDUPLICATION)))) {
                    // messageDeduplicationId allows alphanumeric and
                    // punctuation with length 128 char
                    String messageDeduplicationId = message.replaceAll("[^a-zA-Z0-9\\p{Punct}]", "");
                    messageDeduplicationId = messageDeduplicationId.substring(0, Math.min(messageDeduplicationId.length(), ONE_TWENTY_EIGHT));
                    messageRequest.setMessageDeduplicationId(message.replaceAll("[^a-zA-Z0-9\\p{Punct}]", ""));
                }
            } else {
                // delaySeconds can be used at message level for Standard queues
                // only
                if (delaySeconds != null) {
                    messageRequest.setDelaySeconds(delaySeconds);
                }
            }
            if (sqsClient == null)
                getSQSConnection();
            sqsClient.sendMessage(new SendMessageRequest(queueUrl, message));
            LOGGER.debug("Message " + message + " sent to SQS queue " + queueName);
        } catch (JsonProcessingException | InvalidMessageContentsException | UnsupportedOperationException e) {
            LOGGER.error("Exception while sending message: " , e);
        }
    }

}