/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 ******************************************************************************/
package com.streamanalytix.extensions.executor;

import static com.amazonaws.SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY;
import static com.amazonaws.SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY;
import static com.streamanalytix.extensions.SNSConstant.AWS_ACCESS_KEY_ID;
import static com.streamanalytix.extensions.SNSConstant.AWS_REGION;
import static com.streamanalytix.extensions.SNSConstant.AWS_SECRET_KEY;
import static com.streamanalytix.extensions.SNSConstant.AWS_SQS_ACCESS_KEY;
import static com.streamanalytix.extensions.SNSConstant.AWS_SQS_REGION;
import static com.streamanalytix.extensions.SNSConstant.AWS_SQS_SECRET_KEY;
import static com.streamanalytix.extensions.SNSConstant.AWS_TOPIC_NAME;
import static com.streamanalytix.extensions.SNSConstant.EMAIL_KEY;
import static com.streamanalytix.extensions.SNSConstant.HAVE_SQS_SUBSCRIBER;
import static com.streamanalytix.extensions.SNSConstant.IS_SQS_CRED_DIFF;
import static com.streamanalytix.extensions.SNSConstant.MESSAGE_KEY;
import static com.streamanalytix.extensions.SNSConstant.PROTOCOL_KEY;
import static com.streamanalytix.extensions.SNSConstant.SMS_KEY;
import static com.streamanalytix.extensions.SNSConstant.SQS_KEY;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.streamanalytix.extensions.SNSConstant.Protocol;
import com.streamanalytix.framework.api.spark.processor.DStreamProcessor;

import net.minidev.json.JSONObject;

/** The Class SNSProcessor. */
public class StreamSNSProcessor implements DStreamProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3461208876473240633L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(StreamSNSProcessor.class);

    /** AWS user access key. */
    private String accessKey;

    /** AWS user secret key. */
    private String secretKey;

    /** AWS SNS region key. */
    private String region;

    /** is queue subscribing. */
    private boolean isQueueSubscriber;

    /** is SQS credentials different. */
    private boolean isQueueCredentialsDiff;

    /** AWS user access key. */
    private String sqsAccessKey;

    /** AWS user secret key. */
    private String sqsSecretKey;

    /** AWS SNS region key. */
    private String sqsRegion;

    /** AWS SNS Topic name. */
    private String topicName;

    /** Email notification end point. */
    private String emailEndPoint;

    /** SMS notification end point. */
    private String smsEndPoint;

    /** HTTP/HTTPs end point. */
    private String transferProtocolEndPoint;

    /** SQS end point. */
    private String queueEndPoint;

    /** notification message key. */
    private String notificationMessageKey;

    /** SNS connection pool. */
    private static ConcurrentMap<String, Object> clientConnPool = new ConcurrentHashMap<String, Object>();

    /** The sqs prefix. */
    private final String sqsPrefix = "SQS_";

    /** The init method is used to initialize the SNS configuration.
     * 
     * @param configMap
     *            the config map */
    public void initialize(Map<String, Object> configMap) {
        if (configMap.get(AWS_ACCESS_KEY_ID) != null) {
            accessKey = (String) configMap.get(AWS_ACCESS_KEY_ID);
        }
        if (configMap.get(AWS_SECRET_KEY) != null) {
            secretKey = (String) configMap.get(AWS_SECRET_KEY);
        }
        if (configMap.get(AWS_REGION) != null) {
            String confRegion = (String) configMap.get(AWS_REGION);
            Regions regions = Regions.fromName(confRegion);
            region = regions.getName();
        } else {
            region = Regions.getCurrentRegion().getName();
        }
        if (configMap.get(AWS_TOPIC_NAME) != null) {
            topicName = (String) configMap.get(AWS_TOPIC_NAME);
        }
        if (configMap.get(EMAIL_KEY) != null) {
            emailEndPoint = (String) configMap.get(EMAIL_KEY);
        }
        if (configMap.get(SMS_KEY) != null) {
            smsEndPoint = (String) configMap.get(SMS_KEY);
        }
        if (configMap.get(PROTOCOL_KEY) != null) {
            transferProtocolEndPoint = (String) configMap.get(PROTOCOL_KEY);
        }
        if (configMap.get(SQS_KEY) != null) {
            queueEndPoint = (String) configMap.get(SQS_KEY);
        }
        if (configMap.get(MESSAGE_KEY) != null) {
            notificationMessageKey = (String) configMap.get(MESSAGE_KEY);
        } else {
            LOGGER.info("Message key not found");
        }
        if (configMap.get(HAVE_SQS_SUBSCRIBER) != null) {
            String haveSQSSubscriber = (String) configMap.get(HAVE_SQS_SUBSCRIBER);
            isQueueSubscriber = BooleanUtils.toBoolean(haveSQSSubscriber);
            if (isQueueSubscriber) {
                String isSQSCredentialsDiff = (String) configMap.get(IS_SQS_CRED_DIFF);
                isQueueCredentialsDiff = BooleanUtils.toBoolean(isSQSCredentialsDiff);
                if (isQueueCredentialsDiff) {
                    if (configMap.get(AWS_SQS_ACCESS_KEY) != null) {
                        sqsAccessKey = (String) configMap.get(AWS_SQS_ACCESS_KEY);
                    }
                    if (configMap.get(AWS_SQS_SECRET_KEY) != null) {
                        sqsSecretKey = (String) configMap.get(AWS_SQS_SECRET_KEY);
                    }
                    if (configMap.get(AWS_SQS_REGION) != null) {
                        String confRegion = (String) configMap.get(AWS_SQS_REGION);
                        Regions regions = Regions.fromName(confRegion);
                        sqsRegion = regions.getName();
                    } else {
                        sqsRegion = Regions.getCurrentRegion().getName();
                    }
                } else {
                    sqsRegion = region;
                    sqsAccessKey = accessKey;
                }
                if (configMap.get(SQS_KEY) != null) {
                    queueEndPoint = (String) configMap.get(SQS_KEY);
                }
            }
        }

        initializeSNSClient();
    }

    /** Method used to initialize SQS and SNS client as per given credentials. */
    private void initializeSNSClient() {
        System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, accessKey);
        System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, secretKey);
        AWSCredentials credentials = new SystemPropertiesCredentialsProvider().getCredentials();

        AmazonSNS snsClient = AmazonSNSClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion(region)
                .build();
        if (isQueueSubscriber) {
            if (isQueueCredentialsDiff) {
                if ((StringUtils.isNotBlank(queueEndPoint))
                        && (StringUtils.isBlank(sqsAccessKey) || StringUtils.isBlank(sqsSecretKey) || StringUtils.isBlank(sqsRegion))) {
                    LOGGER.info("SQS Credential is missing. Please review SQS configuration");
                } else {
                    System.setProperty(ACCESS_KEY_SYSTEM_PROPERTY, sqsAccessKey);
                    System.setProperty(SECRET_KEY_SYSTEM_PROPERTY, sqsSecretKey);
                    credentials = new SystemPropertiesCredentialsProvider().getCredentials();
                    AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
                            .withRegion(sqsRegion).build();
                    clientConnPool.put(sqsPrefix + sqsAccessKey, sqsClient);
                }
            }
        }

        clientConnPool.put(accessKey, snsClient);
    }

    /** The process method is used to write SNS implementation.
     * 
     * @param json
     *            the json data
     * @param configMap
     *            the config map */
    public void process(JSONObject json, Map<String, Object> configMap) {
        AmazonSNS snsClient = (AmazonSNS) clientConnPool.get(accessKey);
        if (null != snsClient) {
            String topicArn = extractTopicArn(topicName);
            String mesage = getMessageToPublish(json);
            // Create Topic if it does not Exist
            if (StringUtils.isBlank(topicArn)) {
                topicArn = createTopic(topicName);
            }

            subscribeTopic(topicArn);
            if (StringUtils.isNotBlank(mesage)) {
                publishMessage(snsClient, topicArn, mesage);
            } else {
                LOGGER.info("There is no message to sent in corresponding JSON " + json);
            }
        }
    }

    /** Method to subscribe topic by different End points.
     * 
     * @param topicArn
     *            This is ARN of SNS topic. */
    private void subscribeTopic(String topicArn) {

        if (StringUtils.isNotBlank(emailEndPoint)) {
            subscribeTopic(topicArn, Protocol.EMAIL.getValue(), emailEndPoint);
        }

        if (StringUtils.isNotBlank(smsEndPoint)) {
            subscribeTopic(topicArn, Protocol.SMS.getValue(), smsEndPoint);
        }
        if (StringUtils.isNotBlank(transferProtocolEndPoint)) {
            if (transferProtocolEndPoint.contains(Protocol.HTTPS.getValue())) {
                subscribeTopic(topicArn, Protocol.HTTPS.getValue(), transferProtocolEndPoint);
            } else {
                subscribeTopic(topicArn, Protocol.HTTP.getValue(), transferProtocolEndPoint);
            }
        }
        if (StringUtils.isNotBlank(queueEndPoint)) {
            subscribeTopicForQueue(topicArn, Protocol.QUEUE.getValue(), queueEndPoint);
        }
    }

    /** Gets the message to publish.
     *
     * @param json
     *            input data.
     * @return String */
    private String getMessageToPublish(JSONObject json) {
        String mesage = null;
        if (json != null) {
            mesage = (String) json.get(notificationMessageKey);
        }
        return mesage;
    }

    /** Method to publish message to Topic.
     * 
     * @param snsClient
     *            SNS Client object.
     * @param topicArn
     *            This is ARN of SNS topic.
     * @param mesage
     *            this is the notification message. */
    private void publishMessage(AmazonSNS snsClient, String topicArn, String mesage) {
        PublishRequest publishRequest = new PublishRequest().withTopicArn(topicArn).withMessage(mesage);
        snsClient.publish(publishRequest);
    }

    /** @param topicName
     *            SNS Topic name.
     * @return String */
    private String extractTopicArn(String topicName) {
        Map<String, Topic> topicsMap = getTopics();
        Set<String> keySet = topicsMap.keySet();
        for (String arn : keySet) {
            String[] arnComponent = arn.split(":");
            if (arnComponent[arnComponent.length - 1].equalsIgnoreCase(topicName)) {
                return arn;
            }
        }
        return null;
    }

    /** Method is finding all Topics corresponding to given credentials.
     * 
     * @return Map<String, Topic> */
    public Map<String, Topic> getTopics() {
        Map<String, Topic> topicMap = new HashMap<String, Topic>();
        String nextToken = null;
        AmazonSNS snsClient = (AmazonSNS) clientConnPool.get(accessKey);
        do {
            ListTopicsRequest request = new ListTopicsRequest();
            if (nextToken != null) {
                request = request.withNextToken(nextToken);
            }
            ListTopicsResult result = snsClient.listTopics(request);
            nextToken = result.getNextToken();
            List<Topic> topicList = result.getTopics();
            for (Topic topic : topicList) {
                String arn = topic.getTopicArn();
                topicMap.put(arn, topic);
            }

        } while (nextToken != null);

        return topicMap;
    }

    /** Method is finding all Topics corresponding to given credentials.
     * 
     * @param snsService
     *            SNS Client object.
     * @param topicName
     *            SNS Topic name.
     * @return String */
    public String getTopics(AmazonSNS snsService, String topicName) {
        ListTopicsResult result = snsService.listTopics(topicName);
        List<Topic> allTopics = result.getTopics();
        if (CollectionUtils.isNotEmpty(allTopics)) {
            return allTopics.get(0).getTopicArn();
        }

        return null;
    }

    /** Method used to create Topic over Amazon SNS.
     * 
     * @param topicName
     *            SNS Topic name.
     * @return String */
    public String createTopic(String topicName) {
        AmazonSNS snsClient = (AmazonSNS) clientConnPool.get(accessKey);
        CreateTopicRequest createTopicRequest = new CreateTopicRequest(topicName);
        CreateTopicResult createTopicResult = snsClient.createTopic(createTopicRequest);
        return createTopicResult.getTopicArn();
    }

    /** Method to check whether the endpoint supplied by user already subscribed to Topic or not.
     *
     * @param topicArn
     *            This is ARN of SNS topic.
     * @param protocol
     *            This is type of the connection.
     * @param subscriberEndPoint
     *            subscriber's end point.
     * @param snsClient
     *            SNS Client object.
     * @return boolean */
    private boolean isAlreadySubscribed(String topicArn, String protocol, String subscriberEndPoint, AmazonSNS snsClient) {
        ListSubscriptionsByTopicResult subscribersResults = snsClient.listSubscriptionsByTopic(topicArn);
        List<Subscription> subscribersList = subscribersResults.getSubscriptions();
        if (CollectionUtils.isNotEmpty(subscribersList)) {
            for (Subscription subscription : subscribersList) {
                if (protocol.equalsIgnoreCase(subscription.getProtocol()) && subscriberEndPoint.equalsIgnoreCase(subscription.getEndpoint())) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Method used to subscribe any supplied end point.
     *
     * @param topicArn
     *            This is ARN of SNS topic.
     * @param protocol
     *            This is type of the connection.
     * @param endPoint
     *            This is queEnd point. */
    public void subscribeTopic(String topicArn, String protocol, String endPoint) {
        AmazonSNS snsClient = (AmazonSNS) clientConnPool.get(accessKey);
        if (!isAlreadySubscribed(topicArn, protocol, endPoint, snsClient)) {
            try {
                SubscribeRequest subRequest = new SubscribeRequest(topicArn, protocol, endPoint);
                snsClient.subscribe(subRequest);
                LOGGER.info("Subcriber has been subscribed to Topic with end point: " + endPoint);
            } catch (Exception e) {
                LOGGER.error("Caught Exception while Subscribing Topic with end point: " + endPoint);
            }
        }
    }

    /** This message is used for SQS subscription.
     *
     * @param topicArn
     *            This is ARN of SNS topic.
     * @param protocol
     *            This is type of the connection.
     * @param endPoint
     *            This is queEnd point. */
    public void subscribeTopicForQueue(String topicArn, String protocol, String endPoint) {
        AmazonSNS snsClient = (AmazonSNS) clientConnPool.get(accessKey);
        AmazonSQS sqsClient = (AmazonSQS) clientConnPool.get(sqsPrefix + sqsAccessKey);
        if (sqsClient == null) {
            return;
        }
        if (!isAlreadySubscribed(topicArn, protocol, endPoint, snsClient)) {
            try {
                Topics.subscribeQueue(snsClient, sqsClient, topicArn, endPoint);
                LOGGER.info("Subcriber has been subscribed to Topic with end point: " + endPoint);
            } catch (Exception e) {
                LOGGER.error("Caught Exception while Subscribing Topic with end point: " + endPoint);
            }
        }
    }

    /** Default method called to process message and push notification message to SNS.
     *
     * @param messageStream
     *            the message stream
     * @param configMap
     *            the config map
     * @return JavaDStream<JSONObject> */
    @Override
    public JavaDStream<JSONObject> process(JavaDStream<JSONObject> messageStream, final Map<String, Object> configMap) {
        initialize(configMap);
        return messageStream.map(new Function<JSONObject, JSONObject>() {
            @Override
            public JSONObject call(JSONObject json) throws Exception {
                process(json, configMap);
                return json;
            }
        });
    }

}