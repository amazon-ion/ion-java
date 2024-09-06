// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.IonType
import com.amazon.ion.Timestamp
import com.amazon.ion.impl.macro.*
import java.math.BigDecimal
import java.math.BigInteger

/**
 * Writes Ion 1.1 data to an output source.
 *
 * This interface allows the user to write Ion data without being concerned about which output format is being used.
 */
interface IonRawWriter_1_1 {

    /**
     * Indicates that writing is completed and all buffered data should be written and flushed as if this were the end
     * of the Ion data stream. For example, an Ion binary writer will finalize any local symbol table, write all
     * top-level values, and then flush.
     *
     * This method may only be called when all top-level values are completely written and (`stepped out`)[stepOut].
     *
     * Implementations should allow the application to continue writing further top-level values following the semantics
     * for concatenating Ion data streams.
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

    /** Returns true if the writer is currently in a struct (indicating that field names are required). */
    fun isInStruct(): Boolean

    /** Returns the current depth of containers the writer is at. This is 0 if the writer is at top-level. */
    fun depth(): Int

    /**
     * Writes the Ion 1.1 IVM. IVMs can only be written at the top level of an Ion stream.
     * @throws com.amazon.ion.IonException if in any container.
     */
    fun writeIVM()

    /**
     * *Attempts* to reset the current annotations. This is not guaranteed to succeed, and will
     * throw an [IllegalStateException] if annotations have eagerly been written to the output
     * buffer.
     *
     * TODO: Decide if this is something that should be public. It seems advantageous for the 1.1
     *       writer if we reserve the ability for it to eagerly write annotations, and if we want
     *       to keep this behavior, then it's best _not_ to expose this method.
     */
    fun _private_clearAnnotations()

    /**
     * Returns true if the reader has at least one annotation set and the first annotation matches the
     * given sid OR text.
     */
    fun _private_hasFirstAnnotation(sid: Int, text: String?): Boolean

    /**
     * Writes one annotation for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotation0: Int)

    /**
     * Writes two annotations for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotation0: Int, annotation1: Int)

    /**
     * Writes any number of annotations for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotations: IntArray)

    /**
     * Writes one annotation for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotation0: CharSequence)

    /**
     * Writes two annotations for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotation0: CharSequence, annotation1: CharSequence)

    /**
     * Writes any number of annotations for the next value.
     * [writeAnnotations] may be called more than once to build up a list of annotations.
     */
    fun writeAnnotations(annotations: Array<CharSequence>)

    /**
     * TODO: Consider making this a public method. It's probably safe to do so.
     */
    fun _private_hasFieldName(): Boolean

    /**
     * Writes the field name for the next value. Must be called while in a struct and must be called before [writeAnnotations].
     * @throws com.amazon.ion.IonException if annotations are already written for the value or if not in a struct.
     */
    fun writeFieldName(text: CharSequence)

    /**
     * Writes the field name for the next value. Must be called while in a struct and must be called before [writeAnnotations].
     * @throws com.amazon.ion.IonException if annotations are already written for the value or if not in a struct.
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
     * Steps into an expression group.
     * An expression group is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInExpressionGroup(usingLengthPrefix: Boolean)

    /**
     * Writes a macro invocation for the given macro name.
     * A macro is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInEExp(name: CharSequence)

    /**
     * Writes a macro invocation for the given id corresponding to a macro in the macro table.
     * A macro is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInEExp(id: Int, usingLengthPrefix: Boolean, macro: Macro)

    /**
     * Steps out of the current container.
     */
    fun stepOut()

    // TODO: Doc comments for the uninteresting functions

    fun writeNull()
    fun writeNull(type: IonType)

    fun writeBool(value: Boolean)

    fun writeInt(value: Long)
    fun writeInt(value: BigInteger)

    fun writeFloat(value: Float)
    fun writeFloat(value: Double)

    fun writeDecimal(value: BigDecimal)

    /**
     * TODO: Consider adding a function for writing a timestamp that doesn't require creating a [Timestamp] instance, so
     *       that users don't have to allocate an intermediate between their data type and the Ion writer. E.g.:
     * ```
     * fun writeTimestamp(precision: Timestamp.Precision,
     *                    year: Int, month: Int?, day: Int?,
     *                    hour: Int?, minute: Int?, second: Int?,
     *                    fractionalSeconds: BigDecimal?,
     *                    offsetMinutes: Int?)
     * ```
     */
    fun writeTimestamp(value: Timestamp)

    fun writeSymbol(id: Int)
    fun writeSymbol(text: CharSequence)

    fun writeString(value: CharSequence)

    fun writeBlob(value: ByteArray) = writeBlob(value, 0, value.size)
    fun writeBlob(value: ByteArray, start: Int, length: Int)

    fun writeClob(value: ByteArray) = writeClob(value, 0, value.size)
    fun writeClob(value: ByteArray, start: Int, length: Int)
}
