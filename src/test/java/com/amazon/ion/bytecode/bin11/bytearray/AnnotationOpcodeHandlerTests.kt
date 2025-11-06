// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.ir.Instructions.packInstructionData
import org.junit.jupiter.api.Nested
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class AnnotationOpcodeHandlerTests {
    @Nested
    inner class AnnotationSIDOpcodeHandlerTest {

        // NOTE: These test annotations don't actually annotate anything, which is technically invalid.
        //   This however allows us to use shouldCompile().
        @ParameterizedTest
        @CsvSource(
            "58 01,                0",
            "58 03,                1",
            "58 05,                2",
            "58 07,                3",
            "58 FF,              127",
            "58 02 02,           128",
            "58 04 00 02,      16384",
            "58 08 00 00 02, 2097152",
            "58 F8 FF FF 03, 4194303", // Max value that can be packed into data of I_ANNOTATION_SID
        )
        fun `handler compiles annotations with SID`(bytes: String, sid: Int) {
            val bytes = bytes.hexStringToByteArray()
            val expectedBytecode = intArrayOf(Instructions.I_ANNOTATION_SID.packInstructionData(sid))
            AnnotationSIDOpcodeHandler.shouldCompile(bytes, expectedBytecode)
        }
    }
}
