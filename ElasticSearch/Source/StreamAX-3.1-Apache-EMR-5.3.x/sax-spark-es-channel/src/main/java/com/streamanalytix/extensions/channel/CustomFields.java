/*******************************************************************************
 * Copyright StreamAnalytix and Impetus Technologies.
 * All rights reserved
 *******************************************************************************/
package com.streamanalytix.extensions.channel;

/** The Class CustomFields. */
public class CustomFields {

    /** The key. */
    private String key;

    /** The value. */
    private String value;

    /** The operator. */
    private String operator;

    /** Gets the value.
     *
     * @return name */
    public String getValue() {
        return value;
    }

    /** Sets the value.
     *
     * @param value
     *            name */
    public void setValue(String value) {
        this.value = value;
    }

    /** Gets the key.
     *
     * @return name */
    public String getKey() {
        return key;
    }

    /** Sets the key.
     *
     * @param key
     *            name */
    public void setKey(String key) {
        this.key = key;
    }

    /** Gets the operator.
     *
     * @return name */
    public String getOperator() {
        return operator;
    }

    /** Sets the operator.
     *
     * @param operator
     *            name */
    public void setOperator(String operator) {
        this.operator = operator;
    }
}
