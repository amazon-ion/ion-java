// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion

import com.amazon.ion.impl.macro.*

/**
 * Extension of the IonWriter interface that supports writing macros.
 *
 * TODO: Consider exposing this as a Facet.
 *
 * TODO: See if we can have some sort of safe reference to a macro.
 */
interface MacroAwareIonWriter : IonWriter {

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
     * Starts writing an expression group. May only be called while the writer is in a macro invocation.
     */
    fun startExpressionGroup()

    /**
     * Ends and steps out of the current expression group.
     */
    fun endExpressionGroup()
}
