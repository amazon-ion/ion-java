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
    MACRO_TABLE( /*             */ 14, "macro_table"),
    SYMBOL_TABLE( /*            */ 15, "symbol_table"),
    MODULE( /*                  */ 16, "module"),
    EXPORT( /*                  */ 17, "export"),
    IMPORT( /*                  */ 18, "import"),
    FLEX_SYMBOL( /*             */ 19, "flex_symbol"),
    FLEX_INT( /*                */ 20, "flex_int"),
    FLEX_UINT( /*               */ 21, "flex_uint"),
    UINT8( /*                   */ 22, "uint8"),
    UINT16( /*                  */ 23, "uint16"),
    UINT32( /*                  */ 24, "uint32"),
    UINT64( /*                  */ 25, "uint64"),
    INT8( /*                    */ 26, "int8"),
    INT16( /*                   */ 27, "int16"),
    INT32( /*                   */ 28, "int32"),
    INT64( /*                   */ 29, "int64"),
    FLOAT16( /*                 */ 30, "float16"),
    FLOAT32( /*                 */ 31, "float32"),
    FLOAT64( /*                 */ 32, "float64"),
    EMPTY_TEXT( /*              */ 33, ""),
    FOR( /*                     */ 34, "for"),
    LITERAL( /*                 */ 35, "literal"),
    IF_NONE( /*                 */ 36, "if_none"),
    IF_SOME( /*                 */ 37, "if_some"),
    IF_SINGLE( /*               */ 38, "if_single"),
    IF_MULTI( /*                */ 39, "if_multi"),
    NONE( /*                    */ 40, "none"),
    VALUES( /*                  */ 41, "values"),
    DEFAULT( /*                 */ 42, "default"),
    META( /*                    */ 43, "meta"),
    REPEAT( /*                  */ 44, "repeat"),
    FLATTEN( /*                 */ 45, "flatten"),
    DELTA( /*                   */ 46, "delta"),
    SUM( /*                     */ 47, "sum"),
    ANNOTATE( /*                */ 48, "annotate"),
    MAKE_STRING( /*             */ 49, "make_string"),
    MAKE_SYMBOL( /*             */ 50, "make_symbol"),
    MAKE_DECIMAL( /*            */ 51, "make_decimal"),
    MAKE_TIMESTAMP( /*          */ 52, "make_timestamp"),
    MAKE_BLOB( /*               */ 53, "make_blob"),
    MAKE_LIST( /*               */ 54, "make_list"),
    MAKE_SEXP( /*               */ 55, "make_sexp"),
    MAKE_FIELD( /*              */ 56, "make_field"),
    MAKE_STRUCT( /*             */ 57, "make_struct"),
    PARSE_ION( /*               */ 58, "parse_ion"),
    SET_SYMBOLS( /*             */ 59, "set_symbols"),
    ADD_SYMBOLS( /*             */ 60, "add_symbols"),
    SET_MACROS( /*              */ 61, "set_macros"),
    ADD_MACROS( /*              */ 62, "add_macros"),
    USE( /*                     */ 63, "use"),
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
        private val BY_NAME: HashMap<String, SystemSymbols_1_1> = ALL_VALUES.fold(HashMap(ALL_VALUES.size)) { map, s ->
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
