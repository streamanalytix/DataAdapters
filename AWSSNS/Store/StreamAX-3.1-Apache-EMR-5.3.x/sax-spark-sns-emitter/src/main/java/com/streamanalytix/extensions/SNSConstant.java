/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 ******************************************************************************/
package com.streamanalytix.extensions;

/** The Interface SNSConstant. */
public interface SNSConstant {
    /* AWS user access key */
    /** The aws access key id. */
    String AWS_ACCESS_KEY_ID = "ACCESS_KEY";

    /* AWS user secret key */
    /** The aws secret key. */
    String AWS_SECRET_KEY = "SECRET_KEY";

    /* AWS user region key */
    /** The aws region. */
    String AWS_REGION = "REGION";

    /* Is SQS credentials different */
    /** The have sqs subscriber. */
    String HAVE_SQS_SUBSCRIBER = "IS_SQS_SUBSCRING_SNS";

    /* Is SQS credentials different */
    /** The is sqs cred diff. */
    String IS_SQS_CRED_DIFF = "IS_SQS_SNS_CRED_DIFF";

    /* AWS SQS user access key */
    /** The aws sqs access key. */
    String AWS_SQS_ACCESS_KEY = "SQS_ACCESS_KEY";

    /* AWS SQS user secret key */
    /** The aws sqs secret key. */
    String AWS_SQS_SECRET_KEY = "SQS_SECRET_KEY";

    /* AWS SQS user region key */
    /** The aws sqs region. */
    String AWS_SQS_REGION = "SQS_REGION";

    /* AWS topic name */
    /** The aws topic name. */
    String AWS_TOPIC_NAME = "TOPIC_NAME";

    /* Email message key */
    /** The message key. */
    String MESSAGE_KEY = "NOTIFY_MSG_KEY";

    /* AWS email subscriber end point */
    /** The email key. */
    String EMAIL_KEY = "EMAIL_ENDPOINT";

    /* AWS SMS subscriber end point */
    /** The sms key. */
    String SMS_KEY = "SMS_ENDPOINT";

    /* AWS HTTP/S subscriber end point */
    /** The protocol key. */
    String PROTOCOL_KEY = "HTTP_S_ENDPOINT";

    /* AWS SQS subscriber end point */
    /** The sqs key. */
    String SQS_KEY = "QUEUE_ENDPOINT";

    /** subscription type enumeration.
     *
     * @author impadmin */
    enum Protocol {

        /** The sms. */
        SMS("sms"),
        /** The email. */
        EMAIL("email"),
        /** The http. */
        HTTP("http"),
        /** The https. */
        HTTPS("https"),
        /** The queue. */
        QUEUE("queue");

        /** The value. */
        private String value;

        /** Enumeration Constructor.
         * 
         * @param protocol
         *            type of subscription. */
        private Protocol(String protocol) {
            value = protocol;
        }

        /** Gets the value.
         *
         * @return the value */
        public String getValue() {
            return value;
        }
    }
}
