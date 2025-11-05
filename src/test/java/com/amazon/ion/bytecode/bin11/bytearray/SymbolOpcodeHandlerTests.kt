// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.bin11.OpCode
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource

class SymbolOpcodeHandlerTests {
    @Nested
    inner class SingleCharSymbolOpcodeHandlerTest {

        @ParameterizedTest
        @ValueSource(
            strings = [
                "a",
                "Z",
                "5",
                "~",
                " ",
                "\n",
                "\u007F",
                "\u0000",
            ]
        )
        fun `handler compiles single-char symbols`(char: String) {
            val char = char.single()
            val bytes = byteArrayOf(OpCode.SYMBOL_LENGTH_0.or(1).toByte(), char.code.toByte())
            val expectedBytecode = intArrayOf(Instructions.I_SYMBOL_CHAR.packInstructionData(char.code))
            SingleCharSymbolOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }
    }

    @Nested
    inner class SymbolSIDOpcodeHandlerTest {

        @ParameterizedTest
        @CsvSource(
            "50 01,            0",
            "51 01,            1",
            "52 03,           10",
            "53 03,           11",
            "54 03,           12",
            "55 03,           13",
            "56 03,           14",
            "57 03,           15",
            "57 FF,         1023",
            "50 02 02,      1024",
            "57 FE FF,    131071",
            "50 04 00 02, 131072"
        )
        fun `handler compiles symbols with SID`(bytes: String, sid: Int) {
            val bytes = bytes.hexStringToByteArray()
            val expectedBytecode = intArrayOf(Instructions.I_SYMBOL_SID.packInstructionData(sid))
            SymbolSIDOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }
    }
}
