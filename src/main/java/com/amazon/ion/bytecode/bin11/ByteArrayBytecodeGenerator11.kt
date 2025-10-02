// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

@SuppressFBWarnings("EI_EXPOSE_REP2", justification = "constructor does not make a defensive copy of source as a performance optimization")
internal class ByteArrayBytecodeGenerator11
@SuppressFBWarnings("URF_UNREAD_FIELD", justification = "field will be read once this class is implemented")
constructor(
    private val source: ByteArray,
    private var i: Int,
) {
    // TODO: This should implement BytecodeGenerator
}
