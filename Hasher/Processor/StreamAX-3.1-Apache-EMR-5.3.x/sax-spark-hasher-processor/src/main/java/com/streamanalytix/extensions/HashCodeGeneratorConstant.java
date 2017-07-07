package com.streamanalytix.extensions;

/** This is a constant class defining all constants used in hash code generation process. */
public interface HashCodeGeneratorConstant {
    /** The hashing lgorithm. */
    String HASHING_LGORITHM = "HASHING_LGORITHM";
    /** The hash code field. */
    String HASH_CODE_FIELD = "HASH_CODE_FIELD";
    /** The field seperator. */
    String FIELD_SEPERATOR = ",";
    /** The all fields. */
    String ALL_FIELDS = "ALL";

    /** hashing algorithm type enumaration. */
    enum HashType {

        /** The MURMU r3_128. */
        MURMUR3_128,
        /** The M d5. */
        MD5,
        /** The SH a1. */
        SHA1,
        /** The SH a256. */
        SHA256,
        /** The SH a512. */
        SHA512,
        /** The ADLE r_32. */
        ADLER_32,
        /** The CR c_32. */
        CRC_32,
        /** The MURMU r3_32. */
        MURMUR3_32;
    }
}