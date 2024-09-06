// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion

import com.amazon.ion.impl.macro.*

/**
 * Extension of the IonWriter interface that supports writing macros.
 *
 * TODO: Consider exposing this as a Facet.
 */
interface MacroAwareIonWriter : IonWriter {

    /**
     * Adds a macro to the macro table, returning a MacroRef that can be used to invoke the macro.
     */
    fun addMacro(macro: Macro): MacroRef

    /**
     * Adds a macro to the macro table, returning a MacroRef that can be used to invoke the macro.
     */
    fun addMacro(name: String, macro: Macro): MacroRef

    /**
     * Starts writing a macro invocation, adding it to the macro table, if needed.
     */
    fun startMacro(macro: Macro)

    /**
     * Starts writing a macro using the given [MacroRef].
     */
    fun startMacro(macro: MacroRef)

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
