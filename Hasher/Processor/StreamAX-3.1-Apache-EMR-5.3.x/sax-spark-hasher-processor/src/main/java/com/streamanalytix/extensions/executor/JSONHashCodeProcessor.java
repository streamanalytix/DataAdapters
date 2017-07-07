package com.streamanalytix.extensions.executor;

import static com.streamanalytix.extensions.HashCodeGeneratorConstant.HASHING_LGORITHM;
import static com.streamanalytix.extensions.HashCodeGeneratorConstant.HASH_CODE_FIELD;

import java.util.Map;

import net.minidev.json.JSONObject;

import com.streamanalytix.framework.api.processor.JSONProcessor;

/** The Class JSONHashCodeProcessor. This class generates hash code corresponding to given field(s) and hashing algorithm. This class is applicable
 * in-case of JSON processing. */
public class JSONHashCodeProcessor implements JSONProcessor {

    /* The Constant serialVersionUID. */
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 3461208876473240633L;

    /* hashing algorithm */
    /** The hashing algorithm. */
    private String hashingAlgorithm;
    /* hash code field */
    /** The hash code field. */
    private String hashCodeField;

    /** The init method is used to initialize the configuration.
     * 
     * @param configMap
     *            the config map */
    @Override
    public void init(Map<String, Object> configMap) {
        if (configMap.get(HASHING_LGORITHM) != null) {
            hashingAlgorithm = (String) configMap.get(HASHING_LGORITHM);
        }
        if (configMap.get(HASH_CODE_FIELD) != null) {
            hashCodeField = (String) configMap.get(HASH_CODE_FIELD);
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

        int hashcode = HashCodeGenerationHelper.calculateHashCode(json, hashCodeField, hashingAlgorithm);
        json.put("hashCode", hashcode);
    }

    /*
     * (non-Javadoc)
     * @see com.streamanalytix.framework.api.IExecutor#cleanup()
     */
    @Override
    public void cleanup() {
    }

}
