// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import java.util.*

enum class SystemSymbols_1_1(val id: Int, val text: String) {
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
    RETAIN( /*                  */ 17, "retain"),
    EXPORT( /*                  */ 18, "export"),
    CATALOG_KEY( /*             */ 19, "catalog_key"),
    IMPORT( /*                  */ 20, "import"),
    THE_EMPTY_SYMBOL( /*        */ 21, ""),
    LITERAL( /*                 */ 22, "literal"),
    IF_NONE( /*                 */ 23, "if_none"),
    IF_SOME( /*                 */ 24, "if_some"),
    IF_SINGLE( /*               */ 25, "if_single"),
    IF_MULTI( /*                */ 26, "if_multi"),
    FOR( /*                     */ 27, "for"),
    FAIL( /*                    */ 28, "fail"),
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
    ADD_SYMBOLS( /*             */ 44, "add_symbols"),
    ADD_MACROS( /*              */ 45, "add_macros"),
    COMMENT( /*                 */ 46, "comment"),
    FLEX_SYMBOL( /*             */ 47, "flex_symbol"),
    FLEX_INT( /*                */ 48, "flex_int"),
    FLEX_UINT( /*               */ 49, "flex_uint"),
    UINT8( /*                   */ 50, "uint8"),
    UINT16( /*                  */ 51, "uint16"),
    UINT32( /*                  */ 52, "uint32"),
    UINT64( /*                  */ 53, "uint64"),
    INT8( /*                    */ 54, "int8"),
    INT16( /*                   */ 55, "int16"),
    INT32( /*                   */ 56, "int32"),
    INT64( /*                   */ 57, "int64"),
    FLOAT16( /*                 */ 58, "float16"),
    FLOAT32( /*                 */ 59, "float32"),
    FLOAT64( /*                 */ 60, "float64"),
    ;

    val utf8Bytes = text.encodeToByteArray()

    companion object {
        private val ALL_VALUES: Array<SystemSymbols_1_1> = entries.toTypedArray().apply {
            Arrays.sort(this) { o1, o2 -> o1.id.compareTo(o2.id) }
        }
        init {
            ALL_VALUES
                .map { it.id }
                .zipWithNext { a, b ->
                    check(b - a > -1) { "System symbols not sorted. Found $a before $b." }
                    check(b - a != 0) { "Duplicate ID $a in system symbols" }
                    check(b - a == 1) { "Gap in system symbols between $a and $b" }
                }
        }

        /**
         * Returns true if the [id] is a valid system symbol ID.
         */
        @JvmStatic
        operator fun contains(id: Int): Boolean {
            return id > 0 && id <= SystemSymbols_1_1.ALL_VALUES.size
        }

        /**
         * Returns the text of the given system symbol ID, or null if not a valid system symbol ID.
         */
        @JvmStatic
        operator fun get(id: Int): String {
            return SystemSymbols_1_1.ALL_VALUES[id - 1].text
        }
    }
}
