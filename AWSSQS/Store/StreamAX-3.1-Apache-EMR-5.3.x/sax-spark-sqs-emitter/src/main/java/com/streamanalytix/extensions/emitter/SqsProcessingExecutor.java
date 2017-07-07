/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.emitter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
import com.streamanalytix.framework.api.processor.JSONProcessor;

import net.minidev.json.JSONObject;

/** The Class SQSProcessingExecutor. */
public class SqsProcessingExecutor implements JSONProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3461208876473240633L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(SqsProcessingExecutor.class);

    /** queue name. */
    private String queueName;

    /** AWS access key. */
    private String accessKey;

    /** AWS secret key. */
    private String secretKey;

    /** Region where queue exist. */
    private Regions region = Regions.US_WEST_2;

    /** The message group id. */
    private String messageGroupId;

    /** The content based deduplication. */
    private Boolean contentBasedDeduplication = Boolean.TRUE;

    /** The delay seconds. */
    private Integer delaySeconds;

    /** SQS client. */
    private AmazonSQS sqsClient;

    /** Queue url. */
    private String queueUrl;

    /** integer value. */
    private final Integer onetwentyeight = 128;

    /** The init method is used to initialize the configuration.
     * 
     * @param configMap
     *            the config map */
    @Override
    public void init(Map<String, Object> configMap) {

        if (configMap.containsKey("accessKey")) {
            accessKey = (String) configMap.get("accessKey");
        }

        if (configMap.containsKey("secretKey")) {
            secretKey = (String) configMap.get("secretKey");
        }

        if (configMap.containsKey("queueName")) {
            this.queueName = (String) configMap.get("queueName");
        }

        if (configMap.containsKey("regionName")) {
            String regionName = (String) configMap.get("regionName");
            try {
                region = Regions.fromName(regionName);
            } catch (IllegalArgumentException e) {
                // use default Region
                LOGGER.warn("Invalid Region name was provided in config paramters");
            }
        }

        if (configMap.containsKey("messageGroupId")) {
            this.messageGroupId = (String) configMap.get("messageGroupId");
        }

        if (configMap.containsKey("contentBasedDeduplication")) {
            try {
                this.contentBasedDeduplication = (Boolean) configMap.get("contentBasedDeduplication");
            } catch (ClassCastException e) {
                // use default value
                LOGGER.warn("Input not provided, using default as True");
            }
        }

        if (configMap.containsKey("delaySeconds")) {
            try {
                this.delaySeconds = (Integer) configMap.get("delaySeconds");
            } catch (ClassCastException e) {
                // use default value
                LOGGER.warn("Input not provided, using default as True");
            }
        }

        try {
            sqsClient = AmazonSQSClientBuilder.standard().withRegion(region)
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))).build();
            queueUrl = sqsClient.getQueueUrl(new GetQueueUrlRequest(queueName)).getQueueUrl();
        } catch (IllegalArgumentException e) {
            LOGGER.error("Exception occurred while creating SQS client: " , e);
        } catch (QueueDoesNotExistException e) {
            Map<String, String> attributes = new HashMap<String, String>();
            if (queueName.endsWith(".fifo")) {

                attributes.put("ContentBasedDeduplication", String.valueOf(contentBasedDeduplication));
                attributes.put("FifoQueue", "true");
                // For FIFO queue delaySeconds is set at queue level and not message level
                if (delaySeconds != null) {
                    attributes.put("DelaySeconds", String.valueOf(delaySeconds));
                }
            }
            queueUrl = sqsClient.createQueue(new CreateQueueRequest(queueName)).getQueueUrl();
            LOGGER.info("Created Queue with name : " + queueName);
        }

    }

    /** The process method is used to write custom implementation.
     * 
     * @param json
     *            the json data
     * @param configMap
     *            the config map */
    @Override
    public void process(JSONObject json, Map<String, Object> configMap) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String message = objectMapper.writeValueAsString(json);
            SendMessageRequest messageRequest = new SendMessageRequest(queueUrl, message);
            if (queueName.endsWith(".fifo")) {
                messageRequest.setMessageGroupId(messageGroupId != null ? messageGroupId : queueName);
                List<String> attributeNames = new ArrayList<String>();
                attributeNames.add("ContentBasedDeduplication");
                GetQueueAttributesResult result = sqsClient.getQueueAttributes(queueUrl, attributeNames);
                Map<String, String> attributes = result.getAttributes();
                if (attributes == null
                        || (attributes.containsKey("ContentBasedDeduplication") && "false".equals(attributes.get("ContentBasedDeduplication")))) {
                    // messageDeduplicationId allows alphanumeric and
                    // punctuation with length 128 char
                    String messageDeduplicationId = message.replaceAll("[^a-zA-Z0-9\\p{Punct}]", "");
                    messageDeduplicationId = messageDeduplicationId.substring(0, Math.min(messageDeduplicationId.length(), onetwentyeight));
                    messageRequest.setMessageDeduplicationId(message.replaceAll("[^a-zA-Z0-9\\p{Punct}]", ""));
                }
            } else {
                // delaySeconds can be used at message level for Standard queues only
                if (delaySeconds != null) {
                    messageRequest.setDelaySeconds(delaySeconds);
                }
            }
            sqsClient.sendMessage(new SendMessageRequest(queueUrl, message));
            LOGGER.debug("Message " + message + " sent to SQS queue " + queueName);
        } catch (JsonProcessingException | InvalidMessageContentsException | UnsupportedOperationException e) {
            LOGGER.error("Exception while sending message: " , e);
        }
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.IExecutor#cleanup()
     */
    @Override
    public void cleanup() {
    }

}