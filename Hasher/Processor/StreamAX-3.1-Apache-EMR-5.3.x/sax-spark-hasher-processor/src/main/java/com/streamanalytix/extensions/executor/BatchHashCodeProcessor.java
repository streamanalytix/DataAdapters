package com.streamanalytix.extensions.executor;

import static com.streamanalytix.extensions.HashCodeGeneratorConstant.HASHING_LGORITHM;
import static com.streamanalytix.extensions.HashCodeGeneratorConstant.HASH_CODE_FIELD;

import java.util.Map;

import net.minidev.json.JSONObject;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

import com.streamanalytix.framework.api.spark.processor.RDDProcessor;

/** The Class BatchHashCodeProcessor. This class generates hash code corresponding to given field(s) and hashing algorithm. This class is applicable
 * in-case of batch processing.
 * 
 * @author impadmin */
public class BatchHashCodeProcessor implements RDDProcessor {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -7896304703434818683L;
    /** The hashing algorithm. */
    private String hashingAlgorithm;
    /** The hash code field. */
    private String hashCodeField;

    /** Method is used to generate hash code corresponding to given field(s) and hashing algorithm.
     *
     * @param rdd
     *            the rdd
     * @param configMap
     *            the config map
     * @return JavaDStream<JSONObject> */
    @Override
    public RDD<JSONObject> process(RDD<JSONObject> rdd, final Map<String, Object> configMap) {

        intialize(configMap);

        return rdd.toJavaRDD().map(new Function<JSONObject, JSONObject>() {

            @Override
            public JSONObject call(JSONObject json) throws Exception {

                int hashcode = HashCodeGenerationHelper.calculateHashCode(json, hashCodeField, hashingAlgorithm);
                json.put("hashCode", hashcode);
                return json;
            }
        }).rdd();
    }

    /** Intialize.
     *
     * @param configMap
     *            the config map */
    private void intialize(final Map<String, Object> configMap) {
        if (configMap.get(HASHING_LGORITHM) != null) {
            hashingAlgorithm = (String) configMap.get(HASHING_LGORITHM);
        }
        if (configMap.get(HASH_CODE_FIELD) != null) {
            hashCodeField = (String) configMap.get(HASH_CODE_FIELD);
        }
    }
}