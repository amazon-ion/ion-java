// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin10

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.io.InputStream

internal class InputStreamBytecodeGenerator10
@SuppressFBWarnings("URF_UNREAD_FIELD", justification = "field will be read once this class is implemented")
constructor(
    private val source: InputStream,
    private var i: Long,
) {
    // TODO: This should implement BytecodeGenerator
}
