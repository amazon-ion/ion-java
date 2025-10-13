// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.GeneratorTestUtil.assertEqualBytecode
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.unsignedToInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class TypedNullOpcodeHandlerTest {

    @ParameterizedTest
    @CsvSource(
        "8F 01, ${Instructions.I_NULL_BOOL}",
        "8F 02, ${Instructions.I_NULL_INT}",
        "8F 03, ${Instructions.I_NULL_FLOAT}",
        "8F 04, ${Instructions.I_NULL_DECIMAL}",
        "8F 05, ${Instructions.I_NULL_TIMESTAMP}",
        "8F 06, ${Instructions.I_NULL_STRING}",
        "8F 07, ${Instructions.I_NULL_SYMBOL}",
        "8F 08, ${Instructions.I_NULL_BLOB}",
        "8F 09, ${Instructions.I_NULL_CLOB}",
        "8F 0a, ${Instructions.I_NULL_LIST}",
        "8F 0b, ${Instructions.I_NULL_SEXP}",
        "8F 0c, ${Instructions.I_NULL_STRUCT}",
    )
    fun testTypedNull(inputString: String, expectedInstruction: Int) {
        val byteArray: ByteArray = inputString.hexStringToByteArray()
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = byteArray[position++].unsignedToInt()
        position += TypedNullOpcodeHandler.convertOpcodeToBytecode(
            opcode,
            byteArray,
            position,
            buffer
        )

        assertEqualBytecode(intArrayOf(expectedInstruction), buffer.toArray())
        assertEquals(2, position)
    }
}
