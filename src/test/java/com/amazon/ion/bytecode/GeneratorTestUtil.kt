// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.SystemSymbols
import com.amazon.ion.bytecode.ir.Debugger
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.ConstantPool
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals

object GeneratorTestUtil {

    internal fun BytecodeGenerator.shouldGenerate(
        expectedBytecode: IntArray,
        expectedConstantPool: ConstantPool? = null,
        macroTable: IntArray = EMPTY_MACRO_TABLE,
        symbolTable: Array<String?> = DEFAULT_SYMBOL_TABLE
    ) {

        val generator = this

        val outputBytecode = BytecodeBuffer()
        val constantPool = ConstantPool(32)

        val macroIndices = mutableListOf(0)
        macroTable.forEachIndexed { i, p -> if (p == Instructions.I_END_TEMPLATE) macroIndices.add(i + 1) }

        generator.refill(outputBytecode, constantPool, EMPTY_MACRO_TABLE, macroIndices.toIntArray(), DEFAULT_SYMBOL_TABLE)

        val actualBytecode = outputBytecode.toArray()
        assertEqualBytecode(expectedBytecode, actualBytecode)

        if (expectedConstantPool != null) {
            assertArrayEquals(expectedConstantPool.toArray(), constantPool.toArray())
        }
    }

    internal fun assertEqualBytecode(expectedBytecode: IntArray, actualBytecode: IntArray) {
        if (!expectedBytecode.contentEquals(actualBytecode)) {
            // If they're not equal, we'll use a string-based equality assertion to try to get a friendlier test failure message.
            val expectedBytecodeText = StringBuilder("").apply { Debugger.renderBytecodeToString(expectedBytecode, ::append, useNumbers = false) }.toString()
            val actualBytecodeText = StringBuilder("").apply { Debugger.renderBytecodeToString(actualBytecode, ::append, useNumbers = false) }.toString()
            assertEquals(expectedBytecodeText, actualBytecodeText)
            // But, in case there's a difference that doesn't show up in the debug rendering, we'll follow it with the original check.
            assertArrayEquals(expectedBytecode, actualBytecode)
        }
    }

    val DEFAULT_SYMBOL_TABLE = arrayOf(
        null,
        SystemSymbols.ION,
        SystemSymbols.ION_1_0,
        SystemSymbols.SYMBOLS,
        SystemSymbols.ION_SYMBOL_TABLE,
        SystemSymbols.NAME,
        SystemSymbols.VERSION,
        SystemSymbols.IMPORTS,
        SystemSymbols.MAX_ID,
        SystemSymbols.ION_SHARED_SYMBOL_TABLE,
    )
    val EMPTY_MACRO_TABLE = IntArray(0)
}
