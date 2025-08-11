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
     * TODO: This should be internal-only, I think.
     *
     * Starts a new encoding segment with an Ion version marker, flushing
     * the previous segment (if any) and resetting the encoding context.
     */
    fun startEncodingSegmentWithIonVersionMarker()

    /**
     *
     * TODO: This should be internal-only, I think.
     *
     * Starts a new encoding segment with an encoding directive, flushing
     * the previous segment (if any).
     * @param macros the macros added in the new segment.
     * @param isMacroTableAppend true if the macros from the previous segment
     *  are to remain available.
     * @param symbols the symbols added in the new segment.
     * @param isSymbolTableAppend true if the macros from the previous
     *  segment are to remain available.
     * @param encodingDirectiveAlreadyWritten true if the encoding directive
     *  that begins the new segment has already been written to this writer.
     *  If false, the writer will write an encoding directive consistent
     *  with the arguments provided to this method, using verbose
     *  s-expression syntax.
     */
    fun startEncodingSegmentWithEncodingDirective(
        macros: Map<MacroRef, Macro>,
        isMacroTableAppend: Boolean,
        symbols: List<String>,
        isSymbolTableAppend: Boolean,
        encodingDirectiveAlreadyWritten: Boolean
    )

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
