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
    ENCODING( /*                */ 10, "encoding"),
    ION_LITERAL( /*             */ 11, "\$ion_literal"),
    ION_SHARED_MODULE( /*       */ 12, "\$ion_shared_module"),
    MACRO( /*                   */ 13, "macro"),
    MACROS( /*                  */ 14, "macros"),
    MODULE( /*                  */ 15, "module"),
    EXPORT( /*                  */ 16, "export"),
    IMPORT( /*                  */ 17, "import"),
    FLEX_SYMBOL( /*             */ 18, "flex_symbol"),
    FLEX_INT( /*                */ 19, "flex_int"),
    FLEX_UINT( /*               */ 20, "flex_uint"),
    UINT8( /*                   */ 21, "uint8"),
    UINT16( /*                  */ 22, "uint16"),
    UINT32( /*                  */ 23, "uint32"),
    UINT64( /*                  */ 24, "uint64"),
    INT8( /*                    */ 25, "int8"),
    INT16( /*                   */ 26, "int16"),
    INT32( /*                   */ 27, "int32"),
    INT64( /*                   */ 28, "int64"),
    FLOAT16( /*                 */ 29, "float16"),
    FLOAT32( /*                 */ 30, "float32"),
    FLOAT64( /*                 */ 31, "float64"),
    EMPTY_TEXT( /*              */ 32, ""),
    FOR( /*                     */ 33, "for"),
    LITERAL( /*                 */ 34, "literal"),
    IF_NONE( /*                 */ 35, "if_none"),
    IF_SOME( /*                 */ 36, "if_some"),
    IF_SINGLE( /*               */ 37, "if_single"),
    IF_MULTI( /*                */ 38, "if_multi"),
    NONE( /*                    */ 39, "none"),
    VALUES( /*                  */ 40, "values"),
    DEFAULT( /*                 */ 41, "default"),
    META( /*                    */ 42, "meta"),
    REPEAT( /*                  */ 43, "repeat"),
    FLATTEN( /*                 */ 44, "flatten"),
    DELTA( /*                   */ 45, "delta"),
    SUM( /*                     */ 46, "sum"),
    ANNOTATE( /*                */ 47, "annotate"),
    MAKE_STRING( /*             */ 48, "make_string"),
    MAKE_SYMBOL( /*             */ 49, "make_symbol"),
    MAKE_DECIMAL( /*            */ 50, "make_decimal"),
    MAKE_TIMESTAMP( /*          */ 51, "make_timestamp"),
    MAKE_BLOB( /*               */ 52, "make_blob"),
    MAKE_LIST( /*               */ 53, "make_list"),
    MAKE_SEXP( /*               */ 54, "make_sexp"),
    MAKE_FIELD( /*              */ 55, "make_field"),
    MAKE_STRUCT( /*             */ 56, "make_struct"),
    PARSE_ION( /*               */ 57, "parse_ion"),
    SET_SYMBOLS( /*             */ 58, "set_symbols"),
    ADD_SYMBOLS( /*             */ 59, "add_symbols"),
    SET_MACROS( /*              */ 60, "set_macros"),
    ADD_MACROS( /*              */ 61, "add_macros"),
    USE( /*                     */ 62, "use"),
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
        private val BY_NAME: Map<String, SystemSymbols_1_1> = Collections.unmodifiableMap(ALL_VALUES.associateBy { it.text })

        @JvmStatic
        fun size() = ALL_VALUES.size

        // Private to avoid potential clashes with enum member names.
        @JvmStatic
        private val ALL_SYMBOL_TEXTS = Collections.unmodifiableList(ALL_VALUES.map { it.text })

        // Private to avoid potential clashes with enum member names.
        @JvmStatic
        private val SYMBOL_IDS_BY_NAME = Collections.unmodifiableMap(ALL_VALUES.associate { it.text to it.id })

        @JvmStatic
        fun allSymbolTexts() = ALL_SYMBOL_TEXTS

        @JvmStatic
        fun symbolIdsByName() = SYMBOL_IDS_BY_NAME

        /** Returns true if the [id] is a valid system symbol ID. */
        @JvmStatic
        operator fun contains(id: Int): Boolean {
            return id > 0 && id <= SystemSymbols_1_1.ALL_VALUES.size
        }

        /** Returns true if the [text] is in the system symbol table. */
        @JvmStatic
        operator fun contains(text: String): Boolean {
            return SystemSymbols_1_1.BY_NAME.containsKey(text)
        }

        /**
         * Returns the system symbol corresponding to the given system symbol ID,
         * or `null` if not a valid system symbol ID.
         */
        @JvmStatic
        operator fun get(id: Int): SystemSymbols_1_1? {
            return if (contains(id)) { SystemSymbols_1_1.ALL_VALUES[id - 1] } else { null }
        }

        /**
         * Returns the system symbol corresponding to the given system symbol text,
         * or `null` if not a valid system symbol text.
         */
        @JvmStatic
        operator fun get(text: String): SystemSymbols_1_1? {
            return BY_NAME[text]
        }
    }
}
