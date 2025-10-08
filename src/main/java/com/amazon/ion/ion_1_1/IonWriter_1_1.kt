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

    /**
     * Writes an "absent argument".
     *
     * @throws com.amazon.ion.IonException if called when not stepped into an E-Expression (macro invocation).
     */
    fun absentArgument()

    /**
     * Starts an Ion List that has homogeneous, macro-shaped children.
     *
     * See [stepIn] for additional information.
     */
    fun stepInTaglessElementList(name: String?, macro: Macro)

    /**
     * Starts an Ion List that has homogeneous, tagless-primitive-encoded children.
     *
     * See [stepIn] for additional information.
     */
    fun stepInTaglessElementList(scalar: TaglessScalarType)

    /**
     * Starts an Ion SExp that has homogeneous, macro-shaped children.
     *
     * See [stepIn] for additional information.
     */
    fun stepInTaglessElementSExp(name: String?, macro: Macro)

    /**
     * Starts an Ion SExp that has homogeneous, tagless-primitive-encoded children.
     *
     * See [stepIn] for additional information.
     */
    fun stepInTaglessElementSExp(scalar: TaglessScalarType)
}
