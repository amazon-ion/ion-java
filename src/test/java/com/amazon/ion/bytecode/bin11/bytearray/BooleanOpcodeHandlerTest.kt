// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.bin11.OpcodeTestCases.BOOLEAN_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import com.amazon.ion.bytecode.ir.Instructions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.fail
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class BooleanOpcodeHandlerTest {

    @ParameterizedTest
    @MethodSource(BOOLEAN_OPCODE_CASES)
    fun `boolean opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        val buffer = BooleanOpcodeHandler.shouldCompile(input, bytecode)
        val expectedBool = expectedValue.toBoolean()
        val representedBool = when (Instructions.getData(buffer.get(0))) {
            1 -> true
            0 -> false
            else -> fail("Unexpected packed instruction emitted from boolean opcode compiler: ${buffer.get(0)}")
        }
        assertEquals(expectedBool, representedBool)
    }
}
