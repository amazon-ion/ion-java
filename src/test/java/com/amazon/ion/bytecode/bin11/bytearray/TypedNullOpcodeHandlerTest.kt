// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.bytecode.bin11.OpcodeTestCases.TYPED_NULL_OPCODE_CASES
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTestUtil.shouldCompile
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class TypedNullOpcodeHandlerTest {

    @ParameterizedTest
    @MethodSource(TYPED_NULL_OPCODE_CASES)
    fun `typed null opcode handler emits correct bytecode`(input: String, bytecode: String) {
        TypedNullOpcodeHandler.shouldCompile(input, bytecode)
    }
}
