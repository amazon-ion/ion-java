// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.*
import com.amazon.ion.SymbolTable.*
import java.util.*

enum class SystemSymbols_1_1(val id: Int, val text: String) {
    // System SID 0 is reserved.
    ION( /*                     */ 1, "\$ion"),
    ION_1_0( /*                 */ 2, "\$ion_1_0"),
    ION_SYMBOL_TABLE( /*        */ 3, "\$ion_symbol_table"),
    NAME( /*                    */ 4, "name"),
    VERSION( /*                 */ 5, "version"),
    IMPORTS( /*                 */ 6, "imports"),
    SYMBOLS( /*                 */ 7, "symbols"),
    MAX_ID( /*                  */ 8, "max_id"),
    ION_SHARED_SYMBOL_TABLE( /* */ 9, "\$ion_shared_symbol_table"),
    ION_ENCODING( /*            */ 10, "\$ion_encoding"),
    ION_LITERAL( /*             */ 11, "\$ion_literal"),
    ION_SHARED_MODULE( /*       */ 12, "\$ion_shared_module"),
    MACRO( /*                   */ 13, "macro"),
    MACRO_TABLE( /*             */ 14, "macro_table"),
    SYMBOL_TABLE( /*            */ 15, "symbol_table"),
    MODULE( /*                  */ 16, "module"),
    RETAIN( /*                  */ 17, "retain"), // https://github.com/amazon-ion/ion-docs/issues/345
    EXPORT( /*                  */ 18, "export"),
    CATALOG_KEY( /*             */ 19, "catalog_key"), // https://github.com/amazon-ion/ion-docs/issues/345
    IMPORT( /*                  */ 20, "import"),
    THE_EMPTY_SYMBOL( /*        */ 21, ""),
    LITERAL( /*                 */ 22, "literal"),
    IF_NONE( /*                 */ 23, "if_none"),
    IF_SOME( /*                 */ 24, "if_some"),
    IF_SINGLE( /*               */ 25, "if_single"),
    IF_MULTI( /*                */ 26, "if_multi"),
    FOR( /*                     */ 27, "for"),
    DEFAULT( /*                 */ 28, "default"),
    VALUES( /*                  */ 29, "values"),
    ANNOTATE( /*                */ 30, "annotate"),
    MAKE_STRING( /*             */ 31, "make_string"),
    MAKE_SYMBOL( /*             */ 32, "make_symbol"),
    MAKE_BLOB( /*               */ 33, "make_blob"),
    MAKE_DECIMAL( /*            */ 34, "make_decimal"),
    MAKE_TIMESTAMP( /*          */ 35, "make_timestamp"),
    MAKE_LIST( /*               */ 36, "make_list"),
    MAKE_SEXP( /*               */ 37, "make_sexp"),
    MAKE_STRUCT( /*             */ 38, "make_struct"),
    PARSE_ION( /*               */ 39, "parse_ion"),
    REPEAT( /*                  */ 40, "repeat"),
    DELTA( /*                   */ 41, "delta"),
    FLATTEN( /*                 */ 42, "flatten"),
    SUM( /*                     */ 43, "sum"),
    SET_SYMBOLS( /*             */ 44, "set_symbols"),
    ADD_SYMBOLS( /*             */ 45, "add_symbols"),
    SET_MACROS( /*              */ 46, "set_macros"),
    ADD_MACROS( /*              */ 47, "add_macros"),
    USE( /*                     */ 48, "use"),
    META( /*                    */ 49, "meta"),
    FLEX_SYMBOL( /*             */ 50, "flex_symbol"),
    FLEX_INT( /*                */ 51, "flex_int"),
    FLEX_UINT( /*               */ 52, "flex_uint"),
    UINT8( /*                   */ 53, "uint8"),
    UINT16( /*                  */ 54, "uint16"),
    UINT32( /*                  */ 55, "uint32"),
    UINT64( /*                  */ 56, "uint64"),
    INT8( /*                    */ 57, "int8"),
    INT16( /*                   */ 58, "int16"),
    INT32( /*                   */ 59, "int32"),
    INT64( /*                   */ 60, "int64"),
    FLOAT16( /*                 */ 61, "float16"),
    FLOAT32( /*                 */ 62, "float32"),
    FLOAT64( /*                 */ 63, "float64"),
    NONE( /*                    */ 64, "none"),
    MAKE_FIELD( /*              */ 65, "make_field"),
    ;

    val utf8Bytes = text.encodeToByteArray()

    val token: SymbolToken = SymbolTokenImpl(text, UNKNOWN_SYMBOL_ID)

    companion object {
        private val ALL_VALUES: Array<SystemSymbols_1_1> = entries.toTypedArray().apply {
            // Put all system symbol enum values into an array, and ensure that they are sorted by ID in that array.
            // This allows us to have O(1) lookup, but it doesn't rely on the enum's ordinal value, which could change.
            Arrays.sort(this) { o1, o2 -> o1.id.compareTo(o2.id) }
        }
        init {
            // Initialization checks to make sure that the system symbols are not misconfigured.
            ALL_VALUES
                .map { it.id }
                .zipWithNext { a, b ->
                    check(b - a > -1) { "System symbols not sorted. Found $a before $b." }
                    check(b - a != 0) { "Duplicate ID $a in system symbols" }
                    check(b - a == 1) { "Gap in system symbols between $a and $b" }
                }
        }

        @JvmStatic
        private val BY_NAME = ALL_VALUES.fold(HashMap<String, SystemSymbols_1_1>(ALL_VALUES.size)) { map, s ->
            check(map.put(s.text, s) == null) { "Duplicate system symbol text: ${s.id}=${s.text}" }
            map
        }

        @JvmStatic
        fun size() = ALL_VALUES.size

        // Private to avoid potential clashes with enum member names.
        @JvmStatic
        private val ALL_SYMBOL_TEXTS = ALL_VALUES.map { it.text }

        @JvmStatic
        fun allSymbolTexts() = ALL_SYMBOL_TEXTS

        /**
         * Returns true if the [id] is a valid system symbol ID.*/
        @JvmStatic
        operator fun contains(id: Int): Boolean {
            return id > 0 && id <= SystemSymbols_1_1.ALL_VALUES.size
        }

        /**
         * Returns the system symbol corresponding to the given system symbol ID,
         * or `null` if not a valid system symbol ID.*/
        @JvmStatic
        operator fun get(id: Int): SystemSymbols_1_1? {
            return if (contains(id)) { SystemSymbols_1_1.ALL_VALUES[id - 1] } else { null }
        }

        /**
         * Returns the system symbol corresponding to the given system symbol text,
         * or `null` if not a valid system symbol text.*/
        @JvmStatic
        operator fun get(text: String): SystemSymbols_1_1? {
            return BY_NAME[text]
        }
    }
}
