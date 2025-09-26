// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.ir

import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Simple demonstration tests for the Debugger class, focusing on the renderBytecodeToString method.
 * These tests are not comprehensive but serve to demonstrate the functionality.
 */
class DebuggerTest {

    @Test
    fun `renderBytecodeToString with simple instructions`() {
        // Create a simple bytecode array with a boolean true instruction
        val bytecode = intArrayOf(
            Instructions.I_BOOL.packInstructionData(1), // boolean true
            Instructions.I_INT_I16.packInstructionData(123),
            Instructions.I_SYMBOL_CHAR.packInstructionData('x'.code),
            Instructions.I_NULL_NULL,
            Instructions.I_NULL_BLOB,
            Instructions.I_TIMESTAMP_CP.packInstructionData(7),
            Instructions.I_IVM.packInstructionData(0x0101),
            Instructions.I_REFILL
        )

        val output = StringBuilder()
        Debugger.renderBytecodeToString(bytecode, output::append)

        val result = output.toString()

        val expected =
            """
            L0    BOOL true
            L1    INT_I16 123
            L2    SYMBOL_CHAR x
            L3    NULL_NULL 
            L4    NULL_BLOB 
            L5    TIMESTAMP_CP 7
            L6    IVM 1.1
            L7    REFILL
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString with instructions with operands`() {
        // Create a simple bytecode array with a boolean true instruction
        val bytecode = intArrayOf(
            Instructions.I_FLOAT_F32,
            Float.POSITIVE_INFINITY.toRawBits(),
            Instructions.I_INT_I64,
            1, // High order operand bits
            0, // Low order operand bits
            Instructions.I_STRING_REF.packInstructionData(10),
            23, // Operand (offset)
            Instructions.I_REFILL
        )

        val output = StringBuilder()
        Debugger.renderBytecodeToString(bytecode, output::append)

        val result = output.toString()

        val expected =
            """
            L0    FLOAT_F32 
            L1     └─ <7f800000> Infinity
            L2    INT_I64 
            L3     ├─ <00000001> ─┐
            L4     └─ <00000000> ─┴─ 4294967296
            L5    STRING_REF L=10
            L6     └─ <00000017> offset=23
            L7    REFILL
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString with container structure`() {
        // Create bytecode for a simple list: [42]
        val bytecode = intArrayOf(
            Instructions.I_LIST_START.packInstructionData(2),
            Instructions.I_INT_I16.packInstructionData(42),
            Instructions.I_END_CONTAINER,
            Instructions.I_END_OF_INPUT
        )

        val output = StringBuilder()
        Debugger.renderBytecodeToString(bytecode, output::append)

        val result = output.toString()

        val expected =
            """
            L0    LIST_START L=2
            L1    . INT_I16 42
            L2    END_CONTAINER 
            L3    END_OF_INPUT
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString with directive`() {
        // Create bytecode for a simple list: [42]
        val bytecode = intArrayOf(
            Instructions.I_DIRECTIVE_ADD_SYMBOLS,
            Instructions.I_STRING_CP.packInstructionData(7),
            Instructions.I_STRING_CP.packInstructionData(8),
            Instructions.I_STRING_CP.packInstructionData(9),
            Instructions.I_END_CONTAINER,
            Instructions.I_REFILL
        )

        val output = StringBuilder()
        Debugger.renderBytecodeToString(bytecode, output::append)

        val result = output.toString()

        val expected =
            """
            L0    DIRECTIVE_ADD_SYMBOLS 
            L1    . STRING_CP 7
            L2    . STRING_CP 8
            L3    . STRING_CP 9
            L4    END_CONTAINER 
            L5    REFILL
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString without line numbers and indentation`() {
        val bytecode = intArrayOf(
            Instructions.I_STRUCT_START.packInstructionData(6),
            Instructions.I_FIELD_NAME_SID.packInstructionData(1), // field name
            Instructions.I_LIST_START.packInstructionData(3),
            Instructions.I_INT_I16.packInstructionData(1),
            Instructions.I_INT_I16.packInstructionData(2),
            Instructions.I_END_CONTAINER, // end list
            Instructions.I_END_CONTAINER, // end struct
            Instructions.I_END_OF_INPUT
        )

        val output = StringBuilder()
        Debugger.renderBytecodeToString(
            bytecode,
            output::append,
            useIndent = false,
            useNumbers = false
        )

        val result = output.toString()

        val expected =
            """
            STRUCT_START L=6
            FIELD_NAME_SID $1
            LIST_START L=3
            INT_I16 1
            INT_I16 2
            END_CONTAINER 
            END_CONTAINER 
            END_OF_INPUT
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString with constant pool information`() {
        val bytecode = intArrayOf(
            Instructions.I_STRING_CP.packInstructionData(0), // Reference to constant pool index 0
            Instructions.I_INT_CP.packInstructionData(1), // Reference to constant pool index 1
            Instructions.I_END_OF_INPUT
        )

        val constantPool = arrayOf<Any?>("Hello World", 12345)

        val output = StringBuilder()
        Debugger.renderBytecodeToString(bytecode, output::append, constantPool = constantPool)

        val result = output.toString()

        val expected =
            """
            L0    STRING_CP 0        <Hello World>
            L1    INT_CP 1        <12345>
            L2    END_OF_INPUT
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString with symbol table information`() {
        val bytecode = intArrayOf(
            Instructions.I_SYMBOL_SID.packInstructionData(1), // Symbol ID 1
            Instructions.I_SYMBOL_SID.packInstructionData(2), // Symbol ID 2
            Instructions.I_END_OF_INPUT
        )

        val symbolTable = arrayOf<String?>(null, "name", "version")

        val output = StringBuilder()
        Debugger.renderBytecodeToString(bytecode, output::append, symbolTable = symbolTable)

        val result = output.toString()

        val expected =
            """
            L0    SYMBOL_SID $1        <name>
            L1    SYMBOL_SID $2        <version>
            L2    END_OF_INPUT
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString with nested containers`() {
        // Create bytecode for nested structure: {field: [1, 2]}
        val bytecode = intArrayOf(
            Instructions.I_STRUCT_START.packInstructionData(6),
            Instructions.I_FIELD_NAME_SID.packInstructionData(1), // field name
            Instructions.I_LIST_START.packInstructionData(3),
            Instructions.I_INT_I16.packInstructionData(1),
            Instructions.I_INT_I16.packInstructionData(2),
            Instructions.I_END_CONTAINER, // end list
            Instructions.I_END_CONTAINER, // end struct
            Instructions.I_END_OF_INPUT
        )

        val output = StringBuilder()
        Debugger.renderBytecodeToString(bytecode, output::append)

        val result = output.toString()

        val expected =
            """
            L0    STRUCT_START L=6
            L1    . FIELD_NAME_SID $1
            L2    . LIST_START L=3
            L3    . . INT_I16 1
            L4    . . INT_I16 2
            L5    . END_CONTAINER 
            L6    END_CONTAINER 
            L7    END_OF_INPUT
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString with partial range`() {
        val bytecode = intArrayOf(
            Instructions.I_BOOL.packInstructionData(1), // index 0
            Instructions.I_INT_I16.packInstructionData(42), // index 1
            Instructions.I_STRING_CP.packInstructionData(0), // index 2
            Instructions.I_END_OF_INPUT // index 3
        )

        val output = StringBuilder()
        // Only debug the middle portion (indices 1-2)
        Debugger.renderBytecodeToString(bytecode, output::append, start = 1, end = 3)

        val result = output.toString()

        val expected =
            """
            L1    INT_I16 42
            L2    STRING_CP 0
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString with dangling and unset instructions after REFILL`() {
        val bytecode = intArrayOf(
            Instructions.I_STRUCT_START.packInstructionData(6),
            Instructions.I_FIELD_NAME_SID.packInstructionData(1), // field name
            Instructions.I_LIST_START.packInstructionData(3),
            Instructions.I_INT_I16.packInstructionData(1),
            Instructions.I_INT_I16.packInstructionData(2),
            Instructions.I_END_CONTAINER, // end list
            Instructions.I_END_CONTAINER, // end struct
            Instructions.I_REFILL,
            Instructions.I_ANNOTATION_CP, // Hypothetically, this is leftover from last time the buffer was refilled, but not overwritten.
            0,
            0,
        )

        val output = StringBuilder()
        Debugger.renderBytecodeToString(bytecode, output::append)

        val result = output.toString()

        val expected =
            """
            L0    STRUCT_START L=6
            L1    . FIELD_NAME_SID $1
            L2    . LIST_START L=3
            L3    . . INT_I16 1
            L4    . . INT_I16 2
            L5    . END_CONTAINER 
            L6    END_CONTAINER 
            L7    REFILL
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }

    @Test
    fun `renderBytecodeToString with dangling and unset instructions after END_OF_INPUT`() {
        val bytecode = intArrayOf(
            Instructions.I_STRUCT_START.packInstructionData(6),
            Instructions.I_FIELD_NAME_SID.packInstructionData(1), // field name
            Instructions.I_LIST_START.packInstructionData(3),
            Instructions.I_INT_I16.packInstructionData(1),
            Instructions.I_INT_I16.packInstructionData(2),
            Instructions.I_END_CONTAINER, // end list
            Instructions.I_END_CONTAINER, // end struct
            Instructions.I_END_OF_INPUT,
            Instructions.I_ANNOTATION_CP, // Hypothetically, this is leftover from last time the buffer was refilled, but it was not overwritten.
            0,
            0,
        )

        val output = StringBuilder()
        Debugger.renderBytecodeToString(bytecode, output::append)

        val result = output.toString()

        val expected =
            """
            L0    STRUCT_START L=6
            L1    . FIELD_NAME_SID $1
            L2    . LIST_START L=3
            L3    . . INT_I16 1
            L4    . . INT_I16 2
            L5    . END_CONTAINER 
            L6    END_CONTAINER 
            L7    END_OF_INPUT
            """

        assertEquals(
            expected.trimIndent(),
            result.trim(),
        )
    }
}
