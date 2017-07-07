package com.streamanalytix.extensions.executor;

import static com.streamanalytix.extensions.HashCodeGeneratorConstant.ALL_FIELDS;
import static com.streamanalytix.extensions.HashCodeGeneratorConstant.FIELD_SEPERATOR;

import java.util.Set;

import net.minidev.json.JSONObject;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.streamanalytix.extensions.HashCodeGeneratorConstant.HashType;

/** The Class HashCodeGenerationHelper. */
public final class HashCodeGenerationHelper {

    /** Method hashCode field type and hashing algorithm. It creates the data fields as per hash code field type and pass those for hash code
     * calculation.
     * 
     * @param json
     *            input data.
     * @param hashCodeField
     *            number of fields which need to be included for has code calculation.
     * @param hashingAlgorithm
     *            hash function type.
     * @return int */
    public static int calculateHashCode(JSONObject json, String hashCodeField, String hashingAlgorithm) {
        Set<String> jsonKeys = json.keySet();
        String allJsonValues = StringUtils.EMPTY;
        if (ALL_FIELDS.equalsIgnoreCase(hashCodeField)) {
            for (String key : jsonKeys) {
                String value = json.getAsString(key);
                allJsonValues = allJsonValues.concat(value);
            }
        } else {
            String[] fieldNames = null;
            if (hashCodeField.contains(FIELD_SEPERATOR)) {
                fieldNames = hashCodeField.split(FIELD_SEPERATOR);
                for (String hashField : fieldNames) {
                    for (String key : jsonKeys) {
                        if ((hashField.trim()).equalsIgnoreCase(key)) {
                            String value = json.getAsString(key);
                            allJsonValues = allJsonValues.concat(value);
                            break;
                        }
                    }
                }
            } else {
                for (String key : jsonKeys) {
                    if ((hashCodeField.trim()).equalsIgnoreCase(key)) {
                        String value = json.getAsString(key);
                        allJsonValues = allJsonValues.concat(value);
                        break;
                    }
                }
            }
        }
        hashingAlgorithm = hashingAlgorithm.toUpperCase();
        int hashcode = getHashCode(HashType.valueOf(hashingAlgorithm), allJsonValues);
        return hashcode;
    }

    /** Method calculate hash code by using hashing type and field.
     * 
     * @param hashType
     *            hash function type.
     * @param key
     *            value of which we need to calculate hash code.
     * @return int */
    public static int getHashCode(HashType hashType, Object key) {
        HashCode hashCode;
        HashFunction hashAlgorithm;
        switch (hashType) {
            case MURMUR3_128:
                hashAlgorithm = Hashing.murmur3_128();
                break;
            case MURMUR3_32:
                hashAlgorithm = Hashing.murmur3_32();
                break;
            case MD5:
                hashAlgorithm = Hashing.md5();
                break;
            case SHA1:
                hashAlgorithm = Hashing.sha1();
                break;
            case SHA256:
                hashAlgorithm = Hashing.sha256();
                break;
            case SHA512:
                hashAlgorithm = Hashing.sha512();
                break;
            case ADLER_32:
                hashAlgorithm = Hashing.adler32();
                break;
            case CRC_32:
                hashAlgorithm = Hashing.crc32();
                break;
            default:
                hashAlgorithm = Hashing.md5();
                break;
        }
        if (key instanceof String) {
            hashCode = hashAlgorithm.newHasher().putString((String) key, Charsets.UTF_8).hash();
        } else if (key instanceof Long) {
            hashCode = hashAlgorithm.newHasher().putLong((Long) key).hash();
        } else {
            hashCode = hashAlgorithm.newHasher().hash();
        }
        return hashCode.asInt();
    }
}
