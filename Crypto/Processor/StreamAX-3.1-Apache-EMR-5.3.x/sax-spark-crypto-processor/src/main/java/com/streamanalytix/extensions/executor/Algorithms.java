/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/

package com.streamanalytix.extensions.executor;

import javax.crypto.Cipher;

import org.apache.commons.lang.StringUtils;

/** The Enum Algorithms. */
public enum Algorithms {

    /** The aes. */
    AES("AES", AlgoType.SYMMETRIC),
    /** The des. */
    DES("DES", AlgoType.SYMMETRIC),
    /** The triple des. */
    TRIPLE_DES("DESede", AlgoType.SYMMETRIC),
    /** The blowfish. */
    BLOWFISH("Blowfish", AlgoType.SYMMETRIC),
    /** The rsa. */
    RSA("RSA", AlgoType.ASYMMETRIC);

    /** The name. */
    private String name;

    /** The type. */
    private AlgoType type;

    /** The constructor.
     * 
     * @param name
     *            - name of the algorithm.
     * @param type
     *            - type of the algorithm. */
    Algorithms(String name, AlgoType type) {
        this.name = name;
        this.type = type;
    }

    /** Getter for algo name.
     * 
     * @return name */
    public String getName() {
        return this.name;
    }

    /** Getter for algo type.
     * 
     * @return algoType */
    public AlgoType getType() {
        return type;
    }

    /** Get the algo enum from name.
     * 
     * @param value
     *            - name of algo
     * @return - algo enum */
    public static Algorithms get(String value) {

        for (Algorithms v : values())
            if (v.getName().equalsIgnoreCase(StringUtils.trimToEmpty(value)))
                return v;
        throw new IllegalArgumentException();
    }
}

/** Algo type enum.
 * 
 * @author IMPETUS */
enum AlgoType {
    SYMMETRIC, ASYMMETRIC
}

/** Crypto operation enum. */
enum CryptoOperation {
    ENCRYPT(Cipher.ENCRYPT_MODE), DECRYPT(Cipher.DECRYPT_MODE);

    private int mode;

    /** The constructor with operation type.
     * 
     * @param mode
     *            - crypto operation type. */
    CryptoOperation(int mode) {
        this.mode = mode;
    }

    /** Getter for operation type/mode.
     * 
     * @return the mode */
    public int getMode() {
        return this.mode;
    }
}