// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.bin11.OpcodeTestCases.SHORT_TIMESTAMP_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import kotlin.String

class ShortTimestampOpcodeHandlerTest {

    @ParameterizedTest
    @MethodSource(SHORT_TIMESTAMP_OPCODE_CASES)
    fun `short timestamp opcode handler emits correct bytecode`(input: String, bytecode: String, expectedValue: String) {
        ShortTimestampOpcodeHandler.shouldCompile(input, bytecode)
    }
}
