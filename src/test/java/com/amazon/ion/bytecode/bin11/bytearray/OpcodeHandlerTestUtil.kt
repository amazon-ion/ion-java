// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.TextToBinaryUtils.decimalStringToIntArray
import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.GeneratorTestUtil.assertEqualBytecode
import com.amazon.ion.bytecode.bin11.OpcodeTestCases.replacePositionTemplates
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import com.amazon.ion.bytecode.util.unsignedToInt
import org.junit.jupiter.api.Assertions.assertEquals

internal object OpcodeHandlerTestUtil {
    /**
     * Asserts that an opcode handler compiles the given input bytes to the given bytecode and that the position
     * returned by the handler points immediately after the last byte in the input.
     *
     * @return The bytecode buffer containing the bytecode compiled by this handler, for convenience of test cases
     * that wish to further validate the compiled bytecode represents a particular value
     */
    fun OpcodeToBytecodeHandler.shouldCompile(inputBytes: ByteArray, expectedBytecode: IntArray): BytecodeBuffer {
        val buffer = BytecodeBuffer()

        var position = 0
        val opcode = inputBytes[position++].unsignedToInt()
        position += this.convertOpcodeToBytecode(
            opcode,
            inputBytes,
            position,
            buffer,
            ConstantPool(0),
            intArrayOf(),
            intArrayOf(),
            arrayOf()
        )

        assertEqualBytecode(expectedBytecode, buffer.toArray())
        assertEquals(inputBytes.size, position)

        return buffer
    }

    /**
     * Asserts that an opcode handler compiles the given input bytes to the given bytecode and that the position
     * returned by the handler points immediately after the last byte in the input.
     *
     * Takes a hex string for the input bytes and a decimal string for the expected bytecode.
     *
     * @return The bytecode buffer containing the bytecode compiled by this handler, for convenience of test cases
     * that wish to further validate the compiled bytecode represents a particular value
     */
    fun OpcodeToBytecodeHandler.shouldCompile(inputBytes: String, expectedBytecode: String): BytecodeBuffer {
        return this.shouldCompile(
            inputBytes.hexStringToByteArray(),
            replacePositionTemplates(expectedBytecode, 0).decimalStringToIntArray()
        )
    }
}
