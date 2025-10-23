// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.bin11.OpcodeTestCases.NULL_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class NullOpcodeHandlerTest {

    @ParameterizedTest
    @MethodSource(NULL_OPCODE_CASES)
    fun `null opcode handler emits correct bytecode`(input: String, bytecode: String) {
        NullOpcodeHandler.shouldCompile(input, bytecode)
    }
}
