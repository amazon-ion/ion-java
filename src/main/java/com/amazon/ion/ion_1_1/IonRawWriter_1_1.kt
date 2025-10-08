// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.ion_1_1

import com.amazon.ion.IonType
import com.amazon.ion.Timestamp
import java.math.BigDecimal
import java.math.BigInteger
import java.util.function.Consumer

/**
 * Writes Ion 1.1 data to an output source.
 *
 * This interface allows the user to write Ion 1.1 data without being concerned about which output format is being used.
 *
 * CAUTION: Using this API requires advanced knowledge of the Ion data format.
 *
 * Implementations are _not_ required to ensure that its methods are called in a valid way—that responsibility falls to
 * the code that is calling the [IonRawWriter_1_1]. For example, implementors are not required to check that parameters
 * match the signature of an E-Expression, that tagless values match the expected type, or that every struct field has
 * exactly one field name.
 *
 * Most users should interact with an [IonWriter] or [IonWriter_1_1] instead of this interface.
 */
interface IonRawWriter_1_1 {

    /**
     * Flushes all buffered data to the output of the raw writer.
     * This method may only be called when not stepped into any top-level values.
     *
     * If the implementation is unbuffered, this should be a no-op.
     *
     * Implementations should allow the application to continue writing further top-level values.
     */
    fun flush()

    /**
     * Closes this stream and releases any system resources associated with it.
     * If the stream is already closed then invoking this method has no effect.
     *
     * If the cursor is between top-level values, this method will [flush] before closing the underlying output stream.
     * If not, the resulting data may be incomplete and invalid Ion.
     */
    fun close()

    /** Returns true if the writer is currently in a struct (indicating that field names are required before every value). */
    fun isInStruct(): Boolean

    /** Returns the current depth the writer is at. This is 0 if the writer is at top-level. The depth _includes_ E-Expressions. */
    fun depth(): Int

    /**
     * Writes the Ion 1.1 IVM. IVMs can only be written at the top level of an Ion stream.
     *
     * This writes the IVM into the raw writer's buffer without flushing the buffer.
     *
     * Caller is responsible for ensuring that this is called at the top level of the stream, and with no annotations.
     */
    fun writeIVM()

    /**
     * Writes one annotation SID for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotation0: Int)

    /**
     * Writes two annotation SIDs for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotation0: Int, annotation1: Int)

    /**
     * Writes any number of annotation SIDs for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotations: IntArray)

    /**
     * Writes one annotation with inline text for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotation0: CharSequence)

    /**
     * Writes two annotations with inline text for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotation0: CharSequence, annotation1: CharSequence)

    /**
     * Writes any number of annotations with inline text for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotations: Array<CharSequence>)

    /**
     * Writes the field name with inline text for the next value. Must be called while in a struct and must be called before [writeAnnotations].
     *
     * Caller is responsible for ensuring that it is legal to write a field name.
     */
    fun writeFieldName(text: CharSequence)

    /**
     * Writes the field name SID for the next value. Must be called while in a struct and must be called before [writeAnnotations].
     *
     * Caller is responsible for ensuring that it is legal to write a field name.
     */
    fun writeFieldName(sid: Int)

    /**
     * Steps into a List.
     *
     * The [usingLengthPrefix] parameter is a suggestion. Implementations may ignore it if it is not relevant for that
     * particular implementation. All implementations must document their specific behavior for this method.
     */
    fun stepInList(usingLengthPrefix: Boolean)

    /**
     * Steps into a SExp.
     *
     * The [usingLengthPrefix] parameter is a suggestion. Implementations may ignore it if it is not relevant for that
     * particular implementation. All implementations must document their specific behavior for this method.
     */
    fun stepInSExp(usingLengthPrefix: Boolean)

    /**
     * Steps into a Struct.
     *
     * The [usingLengthPrefix] parameter is a suggestion. Implementations may ignore it if it is not relevant for that
     * particular implementation. All implementations must document their specific behavior for this method.
     */
    fun stepInStruct(usingLengthPrefix: Boolean)

    /**
     * Writes a macro invocation for the given macro name.
     * A macro is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInEExp(name: CharSequence)

    /**
     * Writes a macro invocation for the given id corresponding to a macro in the macro table.
     * A macro is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInEExp(id: Int, usingLengthPrefix: Boolean)

    /**
     * Writes an "absent argument" token to this writer's buffer.
     *
     * Caller is responsible to ensure that this is only called while stepped into an E-Expression, and that the
     * current parameter is a tagged parameter (thus allowing an absent argument).
     */
    fun writeAbsentArgument()

    /**
     * Steps out of the current container.
     */
    fun stepOut()

    /** Writes a tagged `null.null` value. */
    fun writeNull()
    /** Writes a tagged Ion null value for any [IonType] in the Ion data model. */
    fun writeNull(type: IonType)

    /** Writes a tagged, non-null Ion boolean value */
    fun writeBool(value: Boolean)
    /** Writes a tagged, non-null Ion integer value */
    fun writeInt(value: Long)
    /** Writes a tagged, non-null Ion integer value */
    fun writeInt(value: BigInteger)

    /** Writes a tagged, non-null Ion float value */
    fun writeFloat(value: Float)
    /** Writes a tagged, non-null Ion float value */
    fun writeFloat(value: Double)

    /** Writes a tagged, non-null Ion decimal value */
    fun writeDecimal(value: BigDecimal)

    /**
     * Writes a tagged, non-null Ion timestamp value
     *
     * TODO: Consider adding a function for writing a timestamp that doesn't require creating a [Timestamp] instance, so
     *       that users don't have to allocate an intermediate between their data type and the Ion writer. E.g.:
     * ```
     * fun writeTimestamp(precision: Timestamp.Precision,
     *                    year: Int, month: Int, day: Int,
     *                    hour: Int, minute: Int, second: Int,
     *                    fractionalSeconds: BigDecimal?,
     *                    offsetMinutes: Int, isOffsetKnown: Boolean)
     * ```
     */
    fun writeTimestamp(value: Timestamp)

    /** Writes a tagged, non-null, symbol value as a SID */
    fun writeSymbol(id: Int)
    /** Writes a tagged, non-null, symbol value with inline text */
    fun writeSymbol(text: CharSequence)

    /** Writes a tagged non-null Ion string value */
    fun writeString(value: CharSequence)

    /** Writes a tagged non-null Ion blob value */
    fun writeBlob(value: ByteArray) = writeBlob(value, 0, value.size)
    /** Writes a tagged non-null Ion blob value */
    fun writeBlob(value: ByteArray, start: Int, length: Int)

    /** Writes a tagged non-null Ion clob value */
    fun writeClob(value: ByteArray) = writeClob(value, 0, value.size)
    /** Writes a tagged non-null Ion clob value */
    fun writeClob(value: ByteArray, start: Int, length: Int)

    /**
     * Starts a tagless-element list, using the given opcode for its child elements.
     *
     * Callers are responsible to make sure that [taglessEncodingOpcode] is a valid opcode in [TaglessScalarType].
     */
    fun stepInTaglessElementList(taglessEncodingOpcode: Int)

    /**
     * Starts a tagless-element list, using the given macro for its child elements.
     *
     * If [macroName] is non-null, and the implementation supports invoking macros by name, then the implementation
     * MUST write the macro name rather than the macro id.
     */
    fun stepInTaglessElementList(macroId: Int, macroName: String?)

    /**
     * Starts a tagless-element s-exp, using the given opcode for its child elements.
     *
     * Callers are responsible to make sure that [taglessEncodingOpcode] is a valid opcode in [TaglessScalarType].
     */
    fun stepInTaglessElementSExp(taglessEncodingOpcode: Int)

    /**
     * Starts a tagless-element list, using the given macro for its child elements.
     *
     * If [macroName] is non-null, and the implementation supports invoking macros by name, then the implementation
     * MUST write the macro name rather than the macro id.
     */
    fun stepInTaglessElementSExp(macroId: Int, macroName: String?)

    /**
     * Steps into a tagless E-Expression.
     *
     * Caller is responsible to ensure this is called only when it is valid to write a tagless e-expression.
     */
    fun stepInTaglessEExp()

    /**
     * Writes an integer value without writing the opcode, using the [implicitOpcode] to determine the correct
     * encoding for the value payload.
     *
     * @throws com.amazon.ion.IonException If the [implicitOpcode] is not a valid opcode for a tagless int value.
     */
    fun writeTaglessInt(implicitOpcode: Int, value: Int)

    /**
     * Writes an integer value without writing the opcode, using the [implicitOpcode] to determine the correct
     * encoding for the value payload.
     *
     * @throws com.amazon.ion.IonException If the [implicitOpcode] is not a valid opcode for a tagless int value.
     */
    fun writeTaglessInt(implicitOpcode: Int, value: Long)

    /**
     * Writes an integer value without writing the opcode, using the [implicitOpcode] to determine the correct
     * encoding for the value payload.
     *
     * @throws com.amazon.ion.IonException If the [implicitOpcode] is not a valid opcode for a tagless int value.
     */
    fun writeTaglessInt(implicitOpcode: Int, value: BigInteger)

    /**
     * Writes a float value without writing the opcode, using the [implicitOpcode] to determine the correct
     * encoding for the value payload.
     *
     * @throws com.amazon.ion.IonException If the [implicitOpcode] is not a valid opcode for a tagless float value.
     */
    fun writeTaglessFloat(implicitOpcode: Int, value: Float)

    /**
     * Writes a float value without writing the opcode, using the [implicitOpcode] to determine the correct
     * encoding for the value payload.
     *
     * @throws com.amazon.ion.IonException If the [implicitOpcode] is not a valid opcode for a tagless float value.
     */
    fun writeTaglessFloat(implicitOpcode: Int, value: Double)

    /**
     * Writes a decimal value without writing the opcode, using the [implicitOpcode] to determine the correct
     * encoding for the value payload.
     *
     * @throws com.amazon.ion.IonException If the [implicitOpcode] is not a valid opcode for a tagless decimal value.
     */
    fun writeTaglessDecimal(implicitOpcode: Int, value: BigDecimal)

    /**
     * Writes a timestamp value without writing the opcode, using the [implicitOpcode] to determine the correct
     * encoding for the value payload.
     *
     * @throws com.amazon.ion.IonException If the [implicitOpcode] is not a valid opcode for a tagless timestamp value.
     */
    fun writeTaglessTimestamp(implicitOpcode: Int, value: Timestamp)

    /**
     * Writes a symbol value as SID without writing the opcode, using the [implicitOpcode] to determine the correct
     * encoding for the value payload.
     *
     * @throws com.amazon.ion.IonException If the [implicitOpcode] is not a valid opcode for a tagless symbol value as SID.
     */
    fun writeTaglessSymbol(implicitOpcode: Int, id: Int)

    /**
     * Writes a symbol value with inline text without writing the opcode, using the [implicitOpcode] to determine the correct
     * encoding for the value payload.
     *
     * @throws com.amazon.ion.IonException If the [implicitOpcode] is not a valid opcode for a tagless symbol value with inline text.
     */
    fun writeTaglessSymbol(implicitOpcode: Int, text: CharSequence)

    /**
     * Writes a tagged placeholder to the buffer.
     */
    fun writeTaggedPlaceholder()

    /**
     * Writes a tagged placeholder and its default value to the buffer.
     *
     * Callers are responsible to ensure that exactly one default value is written.
     */
    fun writeTaggedPlaceholderWithDefault(default: Consumer<IonRawWriter_1_1>)

    /**
     * Writes a tagless placeholder along with the [taglessEncodingOpcode].
     *
     * Callers are responsible to make sure that [taglessEncodingOpcode] is a valid opcode in [TaglessScalarType].
     */
    fun writeTaglessPlaceholder(taglessEncodingOpcode: Int)

    /**
     * Steps into an Ion 1.1 directive.
     *
     * Callers are responsible to make sure that [directiveOpcode] is a valid opcode for an Ion 1.1 directive.
     */
    fun stepInDirective(directiveOpcode: Int)
}
