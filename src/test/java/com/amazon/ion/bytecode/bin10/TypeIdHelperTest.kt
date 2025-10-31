// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin10

import com.amazon.ion.IonType
import com.amazon.ion.bytecode.ir.OperationKind
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class TypeIdHelperTest {

    /*
    fun isVariableLength(typeId: Int): Boolean = typeId.and(LOW_NIBBLE_MASK) == VAR_LENGTH_LOW_NIBBLE
     */

    @ParameterizedTest
    @CsvSource(
        "0x10, false",
        "0x21, false",
        "0x32, false",
        "0x43, false",
        "0x54, false",
        "0x65, false",
        "0x8F, true",
        "0x9F, true",
        "0xAF, true",
        "0xBF, true",
    )
    fun testIsNull(tid: Int, expected: Boolean) {
        assertEquals(expected, TypeIdHelper.isNull(tid))
    }

    @ParameterizedTest
    @CsvSource(
        "0x10, false",
        "0x20, true",
        "0x21, true",
        "0x22, true",
        "0x24, true",
        "0x28, true",
        "0x2E, true",
        "0x2F, false",
        "0x30, false",
        "0x32, false",
        "0x34, false",
        "0x43, false",
        "0x54, false",
        "0x65, false",
        "0x8F, false",
        "0x9F, false",
        "0xAF, false",
        "0xBF, false",
    )
    fun testIsNonNullPositiveInt(tid: Int, expected: Boolean) {
        assertEquals(expected, TypeIdHelper.isNonNullPositiveInt(tid))
    }

    @ParameterizedTest
    @CsvSource(
        "0x10, false",
        "0x21, false",
        "0x32, false",
        "0x43, false",
        "0x54, false",
        "0x65, false",
        "0x80, true",
        "0x82, true",
        "0x84, true",
        "0x88, true",
        "0x8E, true",
        "0x8F, false",
        "0x9F, false",
        "0xAF, false",
        "0xBF, false",
    )
    fun testIsNonNullString(tid: Int, expected: Boolean) {
        assertEquals(expected, TypeIdHelper.isNonNullString(tid))
    }

    @ParameterizedTest
    @CsvSource(
        "0x10, false",
        "0x21, false",
        "0x32, false",
        "0x43, false",
        "0x54, false",
        "0x65, false",
        "0x8F, false",
        "0x9F, false",
        "0xAF, false",
        "0xBF, false",
        "0xD0, true",
        "0xD2, true",
        "0xD3, true",
        "0xDE, true",
        "0xDF, false",
    )
    fun testIsNonNullStruct(tid: Int, expected: Boolean) {
        assertEquals(expected, TypeIdHelper.isNonNullStruct(tid))
    }

    @ParameterizedTest
    @CsvSource(
        "0x0E, true",
        "0x10, false",
        "0x21, false",
        "0x2E, true",
        "0x32, false",
        "0x3E, true",
        "0x43, false",
        "0x4E, true",
        "0x54, false",
        "0x5E, true",
        "0x65, false",
        "0xAF, false",
        "0xD0, false",
        "0xDE, true",
        "0xDF, false",
        "0xEE, true",
    )
    fun testIsVariableLength(tid: Int, expected: Boolean) {
        assertEquals(expected, TypeIdHelper.isVariableLength(tid))
    }

    @ParameterizedTest
    @CsvSource(
        // Typed values
        "0x0F, NULL",
        "0x10, BOOL",
        "0x11, BOOL",
        "0x1F, BOOL",
        "0x20, INT",
        "0x2F, INT",
        "0x31, INT",
        "0x3F, INT",
        "0x40, FLOAT",
        "0x48, FLOAT",
        "0x50, DECIMAL",
        "0x5F, DECIMAL",
        "0x63, TIMESTAMP",
        "0x6F, TIMESTAMP",
        "0x70, SYMBOL",
        "0x7F, SYMBOL",
        "0x80, STRING",
        "0x8F, STRING",
        "0x90, CLOB",
        "0x9F, CLOB",
        "0xA0, BLOB",
        "0xAF, BLOB",
        "0xB0, LIST",
        "0xBF, LIST",
        "0xC0, SEXP",
        "0xCF, SEXP",
        "0xD0, STRUCT",
        "0xDF, STRUCT",
        // TypeIds without an IonType
        // NOTE: Empty string for the second value gets converted to Kotlin `null`.
        // NOP
        "0x00, ",
        // IVM
        "0xE0, ",
        // Annotations
        "0xE1, ",
        "0xEF, ",
    )
    fun testIonTypeForTypeId(typeId: Int, expectedType: IonType?) {
        val result = TypeIdHelper.ionTypeForTypeId(typeId)
        assertEquals(expectedType, result)
    }

    @ParameterizedTest
    @CsvSource(
        "0x00, UNSET",
        "0x0E, UNSET",
        "0x0F, NULL",
        "0x10, BOOL",
        "0x11, BOOL",
        "0x1F, BOOL",
        "0x20, INT",
        "0x2E, INT",
        "0x2F, INT",
        "0x30, UNSET",
        "0x31, INT",
        "0x3E, INT",
        "0x3F, INT",
        "0x40, FLOAT",
        "0x48, FLOAT",
        "0x4F, FLOAT",
        "0x50, DECIMAL",
        "0x5E, DECIMAL",
        "0x5F, DECIMAL",
        "0x63, TIMESTAMP",
        "0x6E, TIMESTAMP",
        "0x6F, TIMESTAMP",
        "0x70, SYMBOL",
        "0x7E, SYMBOL",
        "0x7F, SYMBOL",
        "0x80, STRING",
        "0x8E, STRING",
        "0x8F, STRING",
        "0x90, CLOB",
        "0x9E, CLOB",
        "0x9F, CLOB",
        "0xA0, BLOB",
        "0xAE, BLOB",
        "0xAF, BLOB",
        "0xB0, LIST",
        "0xBE, LIST",
        "0xBF, LIST",
        "0xC0, SEXP",
        "0xCE, SEXP",
        "0xCF, SEXP",
        "0xD0, STRUCT",
        "0xD1, UNSET",
        "0xDE, STRUCT",
        "0xDF, STRUCT",
        "0xE0, IVM",
        "0xE3, ANNOTATIONS",
        "0xEE, ANNOTATIONS"
    )
    fun testOperationKindForTypeId(typeId: Int, expectedOperationKind: String) {
        val result = TypeIdHelper.operationKindForTypeId(typeId)
        assertEquals(expectedOperationKind, OperationKind.nameOf(result))
    }

    @ParameterizedTest
    @CsvSource(
        "0x00, 0", // Length 0
        "0x01, 1", // Length 1
        "0x02, 2", // Length 2
        "0x0D, 13", // Length 13
        "0x0E, -1", // VarUInt follows
        "0x0F, 0", // NULL (length 0)
        "0x10, 0", // BOOL false (length 0)
        "0x11, 0", // BOOL true (length 0)
        "0x12, 2", // Length 2
        "0x1E, -1", // VarUInt follows
        "0x1F, 0", // NULL (length 0)
        "0x20, 0", // Length 0
        "0x21, 1", // Length 1
        "0x2E, -1", // VarUInt follows
        "0x2F, 0", // NULL (length 0)
        "0xE0, 3", // IVM (3 bytes after typeId)
        "0xE1, -2", // Reserved (invalid)
        "0xE3, 3", // Annotations (length 3)
        "0xEE, -1", // Annotations (VarUInt follows)
        "0xEF, -2", // Reserved (invalid)
        "0xF0, -2", // Reserved (invalid)
        "0xFF, -2" // Reserved (invalid)
    )
    fun testTypeLengths(typeId: Int, expectedLength: Int) {
        val result = TypeIdHelper.TYPE_LENGTHS[typeId]
        assertEquals(expectedLength, result)
    }
}
