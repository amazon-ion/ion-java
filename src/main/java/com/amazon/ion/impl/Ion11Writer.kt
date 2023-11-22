package com.amazon.ion.impl

import com.amazon.ion.Decimal
import com.amazon.ion.IonType
import com.amazon.ion.Timestamp
import java.math.BigDecimal
import java.math.BigInteger
import java.time.Instant
import java.time.LocalDate

/**
 * Writes Ion 1.1 data to an output source.
 *
 * This interface allows the user to write Ion data without being concerned about which output format is being used.
 */
interface Ion11Writer {

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

    /** Returns the current depth of containers the writer is at.  This is 0 if the writer is at top-level. */
    fun depth(): Int

    /**
     * Writes the Ion 1.1 IVM. IVMs can only be written at the top level of an Ion stream.
     * @throws com.amazon.ion.IonException if in any container.
     */
    fun writeIVM()

    /** Writes one annotation for the next value. */
    fun writeAnnotations(annotation0: Long)

    /** Writes two annotations for the next value. */
    fun writeAnnotations(annotation0: Long, annotation1: Long)

    /** Writes three or more annotations for the next value. */
    fun writeAnnotations(annotation0: Long, annotation1: Long, vararg annotations: Long)

    /** Writes one annotation for the next value. */
    fun writeAnnotations(annotation0: String)

    /** Writes two annotations for the next value. */
    fun writeAnnotations(annotation0: String, annotation1: String)

    /** Writes three or more annotations for the next value. */
    fun writeAnnotations(annotation0: String, annotation1: String, vararg annotations: String)

    /**
     * Writes the field name for the next value. Must be called while in a struct and must be called before [writeAnnotations].
     * @throws com.amazon.ion.IonException if annotations are already written for the value or if not in a struct.
     */
    fun writeFieldName(text: String)

    /**
     * Writes the field name for the next value. Must be called while in a struct and must be called before [writeAnnotations].
     * @throws com.amazon.ion.IonException if annotations are already written for the value or if not in a struct.
     */
    fun writeFieldName(sid: Long)

    fun stepInList(delimited: Boolean)
    fun stepInSExp(delimited: Boolean)

    /**
     * Delimited struct with FlexSym field names
     * Variable length struct with symbol address field names
     * Variable length struct with FlexSym field names
     *
     * @throws com.amazon.ion.IonException if delimited is true and useFlexSym is false.
     */
    fun stepInStruct(delimited: Boolean, useFlexSym: Boolean)

    /**
     * Steps into a stream.
     * A stream is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInStream()

    /**
     * Writes a macro invocation for the given macro name.
     * A macro is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInEExp(name: String)

    /**
     * Writes a macro invocation for the given id corresponding to a macro in the macro table.
     * A macro is not a container in the Ion data model, but it is a container from an encoding perspective.
     */
    fun stepInEExp(id: Long)

    /**
     * Steps out of the current container.
     */
    fun stepOut()

    // TODO: Doc comments for the uninteresting functions

    fun writeNull()
    fun writeNull(type: IonType)

    fun writeBool(value: Boolean)

    fun writeInt(value: Byte)
    fun writeInt(value: Short)
    fun writeInt(value: Int)
    fun writeInt(value: Long)
    fun writeInt(value: BigInteger)

    fun writeFloat(value: Float)
    fun writeFloat(value: Double)

    fun writeDecimal(value: BigDecimal)
    fun writeDecimal(value: Decimal)

    fun writeTimestamp(value: Timestamp)

    /** Writes an Ion timestamp with Days precision */
    fun writeTimestamp(value: LocalDate)

    /** Writes an Ion timestamp with 0 offset (UTC) and nanosecond precision. */
    fun writeTimestamp(value: Instant)

    fun writeSymbol(id: Long)
    fun writeSymbol(text: CharSequence)

    fun writeString(value: CharSequence)

    fun writeBlob(value: ByteArray) = writeBlob(value, 0, value.size)
    fun writeBlob(value: ByteArray, start: Int, length: Int)

    fun writeClob(value: ByteArray) = writeClob(value, 0, value.size)
    fun writeClob(value: ByteArray, start: Int, length: Int)
}
