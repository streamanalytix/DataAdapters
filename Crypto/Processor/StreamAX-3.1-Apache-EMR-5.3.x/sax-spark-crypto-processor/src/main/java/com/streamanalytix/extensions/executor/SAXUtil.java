/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.executor;

import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

/** The class having utility functions like preparing crypto keys. */
public class SAXUtil {

    /** Prepares a key object out of the plaintext key for specified algo.
     * 
     * @param encodedKey
     *            - the base64encoded key
     * @param algo
     *            - the crpto algorithm
     * @return key */
    public static final Key prepareKey(String encodedKey, Algorithms algo) {

        byte[] rawKey = DatatypeConverter.parseBase64Binary(encodedKey);
        Key secretKey = null;

        switch (algo) {
            case AES:
            case DES:
            case BLOWFISH:
            case TRIPLE_DES:
                secretKey = new SecretKeySpec(rawKey, algo.getName());
                break;
            case RSA:
                // Return null as this is asymmetric algo.
                break;
            default:
                break;
        }
        return secretKey;
    }

    /** Prepares a key object out of the plaintext key for specified algo and operation.
     * 
     * @param encodedKey
     *            - the base64encoded key
     * @param algo
     *            - the crpto algorithm
     * @param op
     *            - the crypto operation
     * @return key
     * @throws NoSuchAlgorithmException
     *             - Unknown algo
     * @throws InvalidKeySpecException
     *             - wrong key */
    public static final Key prepareKey(String encodedKey, Algorithms algo, CryptoOperation op) throws NoSuchAlgorithmException,
            InvalidKeySpecException {

        byte[] rawKey = DatatypeConverter.parseBase64Binary(encodedKey);
        Key secretKey = null;

        switch (algo) {
            case RSA:
                KeyFactory keyFactory = KeyFactory.getInstance(Algorithms.RSA.getName());
                switch (op) {
                    case DECRYPT:
                        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(rawKey);
                        secretKey = keyFactory.generatePrivate(keySpec);
                        break;
                    case ENCRYPT:
                        X509EncodedKeySpec keSpec = new X509EncodedKeySpec(rawKey);
                        secretKey = keyFactory.generatePublic(keSpec);
                        break;
                    default:
                        break;
                }
                break;
            case AES:
                // Return null as this is symmetric algo.
                break;
            case DES:
                // Return null as this is symmetric algo.
                break;
            case BLOWFISH:
                // Return null as this is symmetric algo.
                break;
            case TRIPLE_DES:
                // Return null as this is symmetric algo.
                break;
            default:
                break;
        }
        return secretKey;
    }
}
