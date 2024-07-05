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
    fun finish()

    /**
     * Closes this stream and releases any system resources associated with it.
     * If the stream is already closed then invoking this method has no effect.
     *
     * If the cursor is between top-level values, this method will [finish] before closing the underlying output stream.
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
     * The [delimited] parameter is a suggestion. Implementations may ignore it if it is not relevant for that
     * particular implementation. All implementations must document their specific behavior for this method.
     */
    fun stepInList(delimited: Boolean)

    /**
     * Steps into a SExp.
     *
     * The [delimited] parameter is a suggestion. Implementations may ignore it if it is not relevant for that
     * particular implementation. All implementations must document their specific behavior for this method.
     */
    fun stepInSExp(delimited: Boolean)

    /**
     * Steps into a Struct.
     *
     * The [delimited] parameter is a suggestion. Implementations may ignore it if it is not relevant for that
     * particular implementation. All implementations must document their specific behavior for this method.
     */
    fun stepInStruct(delimited: Boolean)

    /**
     * Steps into an expression group.
     * An expression group is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInExpressionGroup(delimited: Boolean)

    /**
     * Writes a macro invocation for the given macro name.
     * A macro is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInEExp(name: CharSequence)

    /**
     * Writes a macro invocation for the given id corresponding to a macro in the macro table.
     * A macro is not a container in the Ion data model, but it is a container from an encoding perspective.
     *
     * TODO:
     *   There are three approaches here for the third parameter
     *   1. It can be a boolean "requiresPresenceBits", and then the raw writer has to keep track of things
     *      This is tricky because we will need patch points if we don't preallocate enough bytes, but that
     *      will be complicated because it could interfere with the existing patch-point logic.
     *      This is also binary specific.
     *   2. We could accept e.g. ByteArray that is the right size as an argument to stepInEExp().
     *      If it's a null/empty bytearray, then we don't require presence bits.
     *      Benefit is that we always allocate the right size for presence bits.
     *      Is this going to require a lot of allocations? Maybe.
     *   3. We could accept the number of parameters in the macro as an argument to stepInEExp().
     *      I think this is probably the best. It means that the containers can have ByteArrays or LongArrays
     *      that are appropriately sized and live with the ContainerInfo.
     *      However, we don't know if there are any tagless expressions. But presumably, tag-less expressions will
     *      will require different APIs, so we can find out about their existence when they are written. That way,
     *      by the end of the macro, we will have enough information to determine whether presence bits are required.
     *      However, that might be tricky for the 1-parameter-with-empty-expression-group case.
     *      Also, how will macro-shape parameters work?
     *   3b We could have stepInEExp() accept the number of _presence bits_ as an argument.
     *      This has all of the benefits of 3, except that it eliminates the need for the raw writer
     *      to figure out whether presence bits are required, and we don't have any tricky cases such
     *      as 1-parameter-with-empty-expression-group.
     *      Actually, this still doesn't deal with the ! cardinality not requiring presence bits.
     *      How is the raw writer to know which arguments are ! cardinality without seeing the signature.
     *      So... I think the raw writer needs to know the signature. It will probably be simpler to store
     *      the signature than it is to e.g. pass the cardinality with every value we try to write.
     *   4. Pass in the signature. Then we can determine whether the signature requires presence bits,
     *      and we know which arguments require presence bits and which don't.
     *   5. Should we expose a writeRawBytes method and push macro handling into the managed writer?
     *      Maybe... the managed writer has to have knowledge of macros anyway.
     *   5b We could make stepInEExp() accept the number of presenceBytes required. Then, it can reserve
     *      that number, and when stepping out of an EExp, we have a distinct stepOutEExp() that accepts
     *      the values of the presence bits. <-- THIS IS MY CURRENT FAVORITE
     *      However, this does leak "presence bits" (which is an encoding concept) out of the raw writer.
     *   6. Should we create a separate macro writer that manages things?
     *      That doesn't really solve the problem other than encapsulating the logic... which does help
     *      because we can put all of the macro logic into one class, but it also requires possibly more
     *      branching/indirection.
     *
     *
     * TODO:
     *      Macro shape args potentially need presence bits that are detached from a macro container.
     *      Do we need to add a `tagless: Boolean = false` parameter to all of our write functions?
     *      No, not all. However, we probably need to add methods for `writeTagless___` for many types.
     *      We also need to figure out segments for the macro args.
     */
    fun stepInEExp(id: Int, lengthPrefixed: Boolean, macro: Macro)

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
