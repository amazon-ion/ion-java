// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.ion_1_1

import com.amazon.ion.IonWriter
import com.amazon.ion.Macro

/**
 * Extension of the IonWriter interface that supports Ion 1.1 features.
 */
interface IonWriter_1_1 : IonWriter {

    /**
     * Starts writing a macro invocation, adding it to the macro table, if needed.
     */
    fun startMacro(macro: Macro)

    /**
     * Starts writing a macro invocation, adding it to the macro table, if needed.
     */
    fun startMacro(name: String, macro: Macro)

    /**
     * Ends and steps out of the current macro invocation.
     */
    fun endMacro()

    fun absentArgument()

    fun stepInTaglessElementList(macro: Macro)
    fun stepInTaglessElementList(name: String, macro: Macro)
    fun stepInTaglessElementList(scalar: TaglessScalarType)
    fun stepInTaglessElementSExp(macro: Macro)
    fun stepInTaglessElementSExp(name: String, macro: Macro)
    fun stepInTaglessElementSExp(scalar: TaglessScalarType)
}
