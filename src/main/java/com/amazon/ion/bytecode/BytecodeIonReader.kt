// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings

/**
 * TODO: This class should implement [IonReader] for the Bytecode IR.
 */
internal class BytecodeIonReader
@SuppressFBWarnings("URF_UNREAD_FIELD", justification = "field will be read once this class is implemented")
constructor(
    private var bytecodeGenerator: BytecodeGenerator,
)
