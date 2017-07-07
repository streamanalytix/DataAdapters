package com.streamanalytix.extensions.executor;

import static com.streamanalytix.extensions.HashCodeGeneratorConstant.HASHING_LGORITHM;
import static com.streamanalytix.extensions.HashCodeGeneratorConstant.HASH_CODE_FIELD;

import java.util.Map;

import net.minidev.json.JSONObject;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import com.streamanalytix.framework.api.spark.processor.DStreamProcessor;

/** The Class StreamHashCodeProcessor. This class generates hash code corresponding to given field(s) and hashing algorithm. This class is applicable
 * in-case of Streaming processing. */
public class StreamHashCodeProcessor implements DStreamProcessor {

    /* version id */
    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 91997221968556289L;

    /* hashing algorithm */
    /** The hashing algorithm. */
    private String hashingAlgorithm;
    /* hash code field */
    /** The hash code field. */
    private String hashCodeField;

    /** Method is used to generate hash code corresponding to given field(s) and hashing algorithm.
     *
     * @param dStream
     *            the d stream
     * @param configMap
     *            the config map
     * @return JavaDStream<JSONObject> */
    @Override
    public JavaDStream<JSONObject> process(JavaDStream<JSONObject> dStream, final Map<String, Object> configMap) {

        initialize(configMap);
        return dStream.map(new Function<JSONObject, JSONObject>() {

            @Override
            public JSONObject call(JSONObject json) throws Exception {

                int hashcode = HashCodeGenerationHelper.calculateHashCode(json, hashCodeField, hashingAlgorithm);
                json.put("hashCode", hashcode);
                return json;
            }
        });
    }

    /** Initialize.
     *
     * @param configMap
     *            the config map */
    private void initialize(final Map<String, Object> configMap) {
        if (configMap.get(HASHING_LGORITHM) != null) {
            hashingAlgorithm = (String) configMap.get(HASHING_LGORITHM);
        }
        if (configMap.get(HASH_CODE_FIELD) != null) {
            hashCodeField = (String) configMap.get(HASH_CODE_FIELD);
        }
    }

}