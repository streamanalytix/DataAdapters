/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.executor;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.xml.bind.DatatypeConverter;

import net.minidev.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.streamanalytix.framework.api.processor.JSONProcessor;

/** The Class CryptographyProcessingExecutor. */
public class CryptographyProcessingExecutor implements JSONProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3461208876473240633L;

    /** The Constant LOGGER. */
    private static final Log LOGGER = LogFactory.getLog(CryptographyProcessingExecutor.class);

    /** The Key object. */
    private Key secretKey;

    /** The Cipher object. */
    private Cipher cipher;

    /** The operation - encrypt or decrypt. */
    private CryptoOperation op;

    /** Array to store field name to be encrypt/decrypt. */
    private String[] fieldsToEncrypt;

    /** The fields to encrypt str. */
    private String fieldsToEncryptStr;

    /** The exclude msg fields. */
    private Set<String> excludeMsgFields = new HashSet<String>(Arrays.asList("indexId", "persistId", "saxMessageType"));

    /** The Constant ALL_FIELDS_TOKEN. */
    private static final String ALL_FIELDS_TOKEN = "^A";

    /** The init method is used to initialize the configuration.
     * 
     * @param configMap
     *            the config map */
    @Override
    public void init(Map<String, Object> configMap) {

        Algorithms algo = Algorithms.get((String) configMap.get("ENCRYPTION_ALGO"));
        String encryptionKey = (String) configMap.get("SECRET_KEY");
        op = CryptoOperation.valueOf(StringUtils.upperCase((String) configMap.get("CRYPTO_OPERATION")));
        fieldsToEncryptStr = StringUtils.trimToEmpty((String) configMap.get("FIELDS_TO_ENCRYPT"));

        if (!ALL_FIELDS_TOKEN.equalsIgnoreCase(fieldsToEncryptStr)) {
            fieldsToEncrypt = StringUtils.split(fieldsToEncryptStr, ",");
        }

        try {
            secretKey = (algo.getType() == AlgoType.SYMMETRIC) ? SAXUtil.prepareKey(encryptionKey, algo)
                    : ((algo.getType() == AlgoType.ASYMMETRIC) ? SAXUtil.prepareKey(encryptionKey, algo, op) : null);
            cipher = Cipher.getInstance(algo.getName());
            cipher.init(op.getMode(), secretKey);

        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            LOGGER.error("Error instanciating the Cipher, please check the used Algorithm or its configuration. The algorithm used is '", e);
        } catch (InvalidKeyException | InvalidKeySpecException e) {
            LOGGER.error("Error initializing the Cipher, please check whether the key provided is valid and correct.\n", e);
        }
    }

    /** The process method is used to write Encryption processor implementation.
     * 
     * @param json
     *            the json data
     * @param configMap
     *            the config map */
    @Override
    public void process(JSONObject json, Map<String, Object> configMap) {

        LOGGER.debug("Json Data Before :" + json);

        String jsonTagValue = "";

        Set<String> keySet = new HashSet<>(json.keySet());

        if (!ALL_FIELDS_TOKEN.equalsIgnoreCase(fieldsToEncryptStr) && !(null == fieldsToEncrypt || 0 == fieldsToEncrypt.length)) {

            keySet.clear();
            keySet.addAll(Arrays.asList(fieldsToEncrypt));

        } else if (!ALL_FIELDS_TOKEN.equalsIgnoreCase(fieldsToEncryptStr) && (null == fieldsToEncrypt || 0 == fieldsToEncrypt.length)) {

            keySet.clear();
        } else {

            keySet.removeAll(excludeMsgFields);
        }

        for (String jsonTagName : keySet) {

            jsonTagValue = json.getAsString(jsonTagName);

            if (op == CryptoOperation.ENCRYPT) {

                json.put(jsonTagName, encryptData(jsonTagValue));

            } else if (op == CryptoOperation.DECRYPT) {

                json.put(jsonTagName, decryptData(jsonTagValue));

            }
        }

        LOGGER.debug("Json Data After : " + json);
    }

    /** Method implementing the logic to encrypt a text.
     * 
     * @param plaintext
     *            the plain text
     * @return base 64 encoded cipher text */
    private String encryptData(String plaintext) {

        byte[] cipherText = null;
        try {
            cipherText = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

        } catch (IllegalBlockSizeException | BadPaddingException e) {
            LOGGER.error("Error encrypting the text.\n" , e);
        }

        return (null != cipherText) ? DatatypeConverter.printBase64Binary(cipherText) : null;
    }

    /** Method implementing the logic to decrypt a text.
     * 
     * @param b64EncCipher
     *            base 64 encoded cipher text
     * @return the plain text */
    private String decryptData(String b64EncCipher) {

        String result = null;

        try {
            byte[] plainTextBytes = cipher.doFinal(DatatypeConverter.parseBase64Binary(b64EncCipher));
            result = new String(plainTextBytes, StandardCharsets.UTF_8);

        } catch (IllegalBlockSizeException | BadPaddingException e) {
            LOGGER.error("Error decrypting the cipher.\n" , e);
        }

        return result;
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.IExecutor#cleanup()
     */
    @Override
    public void cleanup() {
    }

}