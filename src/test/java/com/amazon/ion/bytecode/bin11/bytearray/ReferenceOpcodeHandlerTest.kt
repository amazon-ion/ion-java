// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.REFERENCE_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import com.amazon.ion.bytecode.ir.Instructions
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.fail
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class ReferenceOpcodeHandlerTest {

    /**
     * Test that variable-length payload opcodes generate the correct *_REF bytecode.
     * Does not validate the actual payload in any way.
     */
    @OptIn(ExperimentalStdlibApi::class) // for Byte.toHexString()
    @ParameterizedTest
    @MethodSource(REFERENCE_OPCODE_CASES)
    fun `handlers for OP_X_REF opcodes emit correct bytecode`(input: String, bytecode: String) {
        val opcode = input.take(2).toInt(16)
        val handler = when (opcode) {
            OpCode.ANNOTATION_TEXT -> ReferenceOpcodeHandler(Instructions.I_ANNOTATION_REF)
            OpCode.VARIABLE_LENGTH_INTEGER -> ReferenceOpcodeHandler(Instructions.I_INT_REF)
            OpCode.VARIABLE_LENGTH_DECIMAL -> ReferenceOpcodeHandler(Instructions.I_DECIMAL_REF)
            OpCode.VARIABLE_LENGTH_TIMESTAMP -> ReferenceOpcodeHandler(Instructions.I_TIMESTAMP_REF)
            OpCode.VARIABLE_LENGTH_STRING -> ReferenceOpcodeHandler(Instructions.I_STRING_REF)
            OpCode.VARIABLE_LENGTH_SYMBOL -> ReferenceOpcodeHandler(Instructions.I_SYMBOL_REF)
            OpCode.VARIABLE_LENGTH_BLOB -> ReferenceOpcodeHandler(Instructions.I_BLOB_REF)
            OpCode.VARIABLE_LENGTH_CLOB -> ReferenceOpcodeHandler(Instructions.I_CLOB_REF)
            else -> fail("Opcode is not a variable-length reference opcode: 0x${opcode.toByte().toHexString()}")
        }
        handler.shouldCompile(input, bytecode)
    }
}
