package com.streamanalytix.extensions.executor;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.streamanalytix.framework.api.processor.JSONProcessor;

import net.minidev.json.JSONObject;

/** The Class CustomFieldMaskingExecutor. */
public class CustomFieldMaskingExecutor implements JSONProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 7952280235348559499L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(CustomFieldMaskingExecutor.class);

    /** Regex for email. */
    private static final String REGEX_EMAIL = "[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@" + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})";

    /** Regex for SSN or Tin. */
    private static final String REGEX_SSN = "\\d{3}-?\\d{2}-?\\d{4}";

    /** Regex for Credit Card. */
    private static final String REGEX_CREDITCARD = "(?:(?<visa>4[0-9]{12}(?:[0-9]{3})?)|" + "(?<mastercard>5[1-5][0-9]{14})|"
            + "(?<discover>6(?:011|5[0-9]{2})[0-9]{12})|" + "(?<amex>3[47][0-9]{13})|" + "(?<diners>3(?:0[0-5]|[68][0-9])?[0-9]{11})|"
            + "(?<jcb>(?:2131|1800|35[0-9]{3})[0-9]{11}))";

    private static final String CARD = "CARD";
    private static final String TIN = "TIN";
    private static final String SSN = "SSN";
    private static final String EMAIL = "EMAIL";

    private static final int THREE = 3;
    private static final int FOUR = 4;
    private static final int FIVE = 5;
    private static final int SIX = 6;

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.BaseComponent#init(java.util.Map)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void init(Map configMap) {
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.storm.processor.Processor#process(net .minidev.json.JSONObject, java.util.Map)
     */
    @Override
    public void process(JSONObject json, Map<String, Object> configMap) {
        LOGGER.debug("Inside process.");
        String inputField = null;
        String maskedValue = null;
        if (null != configMap.get(CARD)) {
            inputField = configMap.get(CARD).toString();
            maskedValue = processMaskingUsingRegex(json.getAsString(inputField), REGEX_CREDITCARD, CARD);
            json.put(inputField, maskedValue);
        }
        if (null != configMap.get(TIN)) {
            inputField = configMap.get(TIN).toString();
            maskedValue = processMaskingUsingRegex(json.getAsString(inputField), REGEX_SSN, TIN);
            json.put(inputField, maskedValue);
        }
        if (null != configMap.get(SSN)) {
            inputField = configMap.get(SSN).toString();
            maskedValue = processMaskingUsingRegex(json.getAsString(inputField), REGEX_SSN, SSN);
            json.put(inputField, maskedValue);
        }
        if (null != configMap.get(EMAIL)) {
            inputField = configMap.get(EMAIL).toString();
            maskedValue = processMaskingUsingRegex(json.getAsString(inputField), REGEX_EMAIL, EMAIL);
            json.put(inputField, maskedValue);
        }
    }

    /** @processMaskingUsingRegex() Method for pattern Matching using Regex.
     * @return {String}
     * @param inputString
     *            is string for masking.
     * @param regex
     *            for masking.
     * @param type
     *            is type of masking. */
    private static String processMaskingUsingRegex(String inputString, String regex, String type) {
        LOGGER.debug("Enters into CustomFieldMaskingExecutor: processMaskingUsingRegex: ");
        Pattern pattern = Pattern.compile(regex);
        Matcher match = pattern.matcher(inputString);
        while (match.find()) {
            String toBeMaked = match.group();
            String maskedString = "";
            if (CARD.equals(type))
                maskedString = getCardMasked(toBeMaked);
            if (TIN.equals(type))
                maskedString = getTinMasked(toBeMaked);
            if (SSN.equals(type))
                maskedString = getSSNMasked(toBeMaked);
            if (EMAIL.equals(type))
                maskedString = getEmailMasked(toBeMaked);
            return inputString.replaceAll(inputString.substring(match.start(), match.end()), maskedString);

        }
        return null;
    }

    /** @getCardMasked() Method for Masking credit card and Debit card field.
     * @return {String}
     * @param field
     *            on which Card masking would be applied. */
    private static String getCardMasked(String field) {
        LOGGER.debug("Enters into   CustomFieldMaskingExecutor: getCardMasked: ");
        StringBuilder sb = new StringBuilder();
        for (int i = SIX; i < (field.length() - FOUR); i++) {
            sb.append('x');
        }
        return field.substring(0, SIX) + sb.toString() + field.substring(field.length() - FOUR);
    }

    /** @getTinMasked() Method for Masking Tin No field.
     * @return {String}
     * @param field
     *            on which Tin masking would be applied. */
    private static String getTinMasked(String field) {
        LOGGER.debug("Enters into   CustomFieldMaskingExecutor: getTinMasked: ");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < (field.length() - FOUR); i++) {
            sb.append('x');
        }
        return sb.toString() + field.substring(field.length() - FOUR);
    }

    /** @getSSNMasked() Method for Masking SSN field.
     * @return {String}
     * @param field
     *            on which SSN masking would be applied. */
    private static String getSSNMasked(String field) {
        LOGGER.debug("Enters into   CustomFieldMaskingExecutor: getSSNMasked: ");
        StringBuilder sb = new StringBuilder();
        for (int i = FIVE; i < field.length(); i++) {
            sb.append('x');
        }
        return field.substring(0, FIVE) + sb.toString();
    }

    /** @getEmailMasked() Method for Masking email field.
     * @return {String}
     * @param field
     *            on which email masking would be applied. */
    private static String getEmailMasked(String field) {
        LOGGER.debug("Enters into   CustomFieldMaskingExecutor: getEmailMasked: ");
        int unmaskIndex = field.indexOf("@") - THREE;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < unmaskIndex; i++) {
            sb.append('x');
        }
        return sb.toString() + field.substring(unmaskIndex);
    }

    /** @see com.streamanalytix.framework.api.BaseComponent#cleanup(). */
    @Override
    public void cleanup() {

    }

}
