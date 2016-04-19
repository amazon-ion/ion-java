/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import software.amazon.ion.system.IonTextWriterBuilder;
import software.amazon.ion.util.IonStreamUtils;

/**
 * Writes Ion data to an output source.
 *
 * This interface allows the user to write Ion data without being concerned
 * about which output format is being used.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * We still have some work to do before this interface is stable.
 * See <a href="https://github.com/amznlabs/ion-java/issues/10">issue amznlabs/ion-java#10</a>
 * <p>
 * A value is written via the set of typed {@code write*()} methods such as
 * {@link #writeBool(boolean)} and {@link #writeInt(long)}.
 * Each of these methods outputs a single Ion value, and afterwards the writer
 * is prepared to receive the data for the next sibling value.
 * <p>
 * Any type annotations must be set before the value is written.
 * Once the value has been written the "pending annotations" are erased,
 * so they are must be set again if they need to be applied to the next value.
 * <p>
 * Similarly the field name must be set before the value is
 * written (assuming the value is a field in a structure).  The field name
 * is also "erased" once used, so it must be set for each field.
 * <p>
 * To write a container, first write any annotations and/or field name
 * applicable to the container itself.
 * Then call {@link #stepIn(IonType)} with the desired container type.
 * Then write each child value in order.
 * Finally, call {@link #stepOut()} to complete the container.
 * <p>
 * Once all the top-level values have been written (and stepped-out back to
 * the starting level), the caller must {@link #close()} the writer
 * (or at least {@link #finish()} it) before accessing
 * the data written to the underlying data sink
 * (for example, via {@link ByteArrayOutputStream#toByteArray()}).
 * The writer may have internal buffers and without closing or finishing it,
 * it may not have written everything to the underlying data sink. In addition,
 * {@link #flush()} isn't guaranteed to be sufficient for all implementations.
 *
 * <h2>Exception Handling</h2>
 * {@code IonWriter} is a generic interface for generating Ion data, and it's
 * not possible to fully specify the set of exceptions that could be thrown
 * from the underlying data sink.  Thus all failures are thrown as instances
 * of {@link IonException}, wrapping the originating cause.  If an application
 * wants to handle (say) {@link IOException}s specially, then it needs to
 * extract that from the wrappers; the documentation of {@link IonException}
 * explains how to do that.
 *
 * @see IonStreamUtils
 * @see IonTextWriterBuilder
 */
public interface IonWriter
    extends Closeable, Flushable
{
    /**
     * Gets the symbol table that is currently in use by the writer.
     * While writing a number of values the symbol table will be
     * populated with any added symbols.
     * <p>
     * Note that the table may be replaced during processing.  For example,
     * the stream may start out with a system table that's later replaced by a
     * local table in order to store newly-encountered symbols.
     * <p>
     * When this method returns a local table, it may be
     * {@linkplain SymbolTable#isReadOnly() mutable}, meaning that additional
     * symbols may be interned until it is
     * {@linkplain SymbolTable#makeReadOnly() made read-only}. Note that
     * manually mutating local symbol tables is a deprecated feature;
     * please instead use
     * {@link IonSystem#newBinaryWriter(java.io.OutputStream, SymbolTable...)}
     * or {@link IonSystem#newTextWriter(java.io.OutputStream, SymbolTable...)}
     * to provide custom symbol table(s) to writers upon construction.
     *
     * @return current symbol table
     */
    public SymbolTable getSymbolTable();


    /**
     * Flushes this writer by writing any buffered output to the underlying
     * output target.
     * <p>
     * For some implementations this may have no effect even when some data is
     * buffered, because it's not always possible to fully write partial data.
     * In particular, when writing binary Ion data, Ion's length-prefixed
     * encoding requires a complete top-level value to be written at once.
     * <p>
     * Furthermore, when writing binary Ion data, <em>nothing</em> can be
     * flushed until the writer knows that no more local symbols can be
     * encountered. This can be accomplished via {@link #finish()} or by
     * making the {@linkplain #getSymbolTable() local symbol table}
     * {@linkplain SymbolTable#makeReadOnly() read-only}.
     *
     * @throws IOException if thrown by the underlying output target.
     *
     * @see #finish()
     */
    public void flush() throws IOException;


    /**
     * Indicates that writing is completed and all buffered data should be
     * written and flushed as if this were the end of the Ion data stream.
     * For example, an Ion binary writer will finalize any local symbol table,
     * write all top-level values, and then flush.
     * <p>
     * This method may only be called when all top-level values are
     * completely written and {@linkplain #stepOut() stepped-out}.
     * <p>
     * Implementations should allow the application to continue writing further
     * top-level values following the semantics for concatenating Ion data
     * streams. If another top-level value is written, the result must behave
     * as if it were preceded by an Ion version marker, resetting the stream
     * context as if this were a new stream. (Whether or not an IVM is written
     * may depend upon the writer's configuration; see
     * {@link software.amazon.ion.system.IonWriterBuilder.IvmMinimizing
     * IvmMinimizing}.)
     * <p>
     * This feature can be used to flush reliably before writing more values.
     * Think about a long-running stream of binary values: without finishing,
     * all the values would continue to buffer since the encoder keeps
     * expecting more local symbols (which must be written into the local
     * symbol table that precedes all top-level values). Such an application
     * can finish occasionally to flush the data out, then continue writing
     * more data using a fresh local symbol table.
     *
     * @throws IOException if thrown by the underlying output target.
     * @throws IllegalStateException when not between top-level values.
     *
     * @see #flush()
     * @see #close()
     */
    public void finish() throws IOException;


    /**
     * Closes this stream and releases any system resources associated with it.
     * If the stream is already closed then invoking this method has no effect.
     * <p>
     * If the cursor is between top-level values, this method will
     * {@link #finish()} before closing the underlying output stream.
     * If not, the resulting data may be incomplete and invalid Ion.
     * <p>
     * In other words: unless you're recovering from a failure condition,
     * <b>don't close the writer until you've
     * {@linkplain #stepOut() stepped-out} back to the starting level.</b>
     *
     * @throws IOException if thrown by the underlying output target.
     *
     * @see #finish()
     */
    public void close() throws IOException;


    /**
     * Sets the pending field name to the given text.
     * <p>
     * The pending field name is cleared when the current value is
     * written via {@link #stepIn(IonType) stepIn()} or one of the
     * {@code write*()} methods.
     *
     * @param name text of the field name
     *
     * @throws IllegalStateException if the current container isn't a struct,
     * that is, if {@link #isInStruct()} is false.
     * @throws NullPointerException if {@code name} is null.
     * @throws EmptySymbolException if {@code name} is empty.
     */
    public void setFieldName(String name);


    /**
     * Sets the pending field name to the given token.
     * <p>
     * The pending field name is cleared when the current value is
     * written via {@link #stepIn(IonType) stepIn()} or one of the
     * {@code write*()} methods.
     *
     * @param name text of the field name
     *
     * @throws IllegalStateException if the current container isn't a struct,
     * that is, if {@link #isInStruct()} is false.
     * @throws NullPointerException if {@code name} is null.
     *
     */
    public void setFieldNameSymbol(SymbolToken name);


    /**
     * Sets the full list of pending annotations to the given text symbols.
     * Any pending annotations are cleared.
     * The contents of the {@code annotations} array are copied into this
     * writer, so the caller does not need to preserve the array.
     * <p>
     * The list of pending annotations is cleared when the current value is
     * written via {@link #stepIn(IonType) stepIn()} or one of the
     * {@code write*()} methods.
     *
     * @param annotations string array with the annotations.
     * If null or empty, any pending annotations are cleared.
     */
    public void setTypeAnnotations(String... annotations);


    /**
     * Sets the full list of pending annotations to the given symbols.
     * Any pending annotations are cleared.
     * The contents of the {@code annotations} array are copied into this
     * writer, so the caller does not need to preserve the array.
     * <p>
     * The list of pending annotations is cleared when the current value is
     * written via {@link #stepIn(IonType) stepIn()} or one of the
     * {@code write*()} methods.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     *
     * @param annotations
     * If null or empty, any pending annotations are cleared.
     *
     */
    public void setTypeAnnotationSymbols(SymbolToken... annotations);


    /**
     * Adds a given string to the list of pending annotations.
     * <p>
     * The list of pending annotations is cleared when the current value is
     * written via {@link #stepIn(IonType) stepIn()} or one of the
     * {@code write*()} methods.
     *
     * @param annotation string annotation to append to the annotation list
     */
    public void addTypeAnnotation(String annotation);


    //=========================================================================
    // Container navigation

    /**
     * Writes the beginning of a non-null container (list, sexp, or struct).
     * This must be matched by a call to {@link #stepOut()} after the last
     * child value.
     * <p>
     * This method is <em>not</em> used to write {@code null.list} et al.
     * To write null values use {@link #writeNull(IonType)}.
     *
     * @param containerType must be one of
     * {@link IonType#LIST}, {@link IonType#SEXP}, or {@link IonType#STRUCT}.
     */
    public void stepIn(IonType containerType) throws IOException;


    /**
     * Writes the end of the current container, returning this writer to the
     * context of parent container.
     * Invocation of this method must match a preceding call to
     * {@link #stepIn(IonType)}.
     */
    public void stepOut() throws IOException;


    /**
     * Determines whether values are being written as fields of a struct.
     * This is especially useful when it is not clear whether field names need
     * to be written or not.
     *
     * @return true when the parent is a struct.
     */
    public boolean isInStruct();


    //=========================================================================
    // Value writing


    /**
     * Writes the current value from a reader.
     * <p>
     * This method also writes annotations and field names (if in a struct),
     * and performs a deep write, including the contents of
     * any containers encountered.
     */
    public void writeValue(IonReader reader) throws IOException;

    /**
     * Writes a reader's current value, and all following values until the end
     * of the current container.  If there's no current value then this method
     * calls {@link IonReader#next()} to get going.
     *  <p>
     * This method iterates until {@link IonReader#next()} returns {@code null}
     * and does not {@linkplain IonReader#stepOut() step out} to the container
     * of the current cursor position.
     * <p>
     * This method also writes annotations and field names (if in a struct),
     * and performs a deep write, including the contents of
     * any containers encountered.
     */
    public void writeValues(IonReader reader) throws IOException;


    /**
     * Writes a value of Ion's null type ({@code null} aka {@code null.null}).
     */
    public void writeNull() throws IOException;

    /**
     * Writes a null value of a specified Ion type.
     *
     * @param type type of the null to be written
     */
    public void writeNull(IonType type) throws IOException;


    /**
     * writes a non-null boolean value (true or false) as an IonBool
     * to output.
     * @param value true or false as desired
     */
    public void writeBool(boolean value) throws IOException;


    /**
     * writes a signed 64 bit value, a Java long, as an IonInt.
     * @param value signed int to write
     */
    public void writeInt(long value) throws IOException;

    /**
     * writes a BigInteger value as an IonInt.  If the
     * BigInteger value is null this writes a null int.
     * @param value BigInteger to write
     */
    public void writeInt(BigInteger value) throws IOException;


    /**
     * writes a 64 bit binary floating point value, a Java double,
     * as an IonFloat.  Currently IonFloat values are output as
     * 64 bit IEEE 754 big endian values.  IonFloat preserves all
     * valid floating point values, including -0.0, Nan and +/-infinity.
     * It does not guarantee preservation of -Nan or other less
     * less "common" values.
     * @param value double to write
     */
    public void writeFloat(double value) throws IOException;

    /**
     * Writes a BigDecimal value as an Ion decimal.  Ion uses an
     * arbitrarily long sign/value and an arbitrarily long signed
     * exponent to write the value. This preserves
     * all of the BigDecimal digits, the number of
     * significant digits.
     * <p>
     * To write a negative zero value, pass this method a
     * {@link Decimal} instance.
     *
     * @param value may be null to represent {@code null.decimal}.
     */
    public void writeDecimal(BigDecimal value) throws IOException;


    /**
     * Writes a timestamp value.
     *
     * @param value may be null to represent {@code null.timestamp}.
     */
    public void writeTimestamp(Timestamp value) throws IOException;

    /**
     * Writes the text of an Ion symbol value.
     *
     * @param content may be null to represent {@code null.symbol}.
     *
     * @throws IllegalArgumentException if the value contains an invalid UTF-16
     * surrogate pair.
     */
    public void writeSymbol(String content) throws IOException;

    /**
     * Writes the content of an Ion symbol value.
     * If the token has text, it is considered canonical and any SID in the
     * token may be ignored or reassigned.
     *
     * @param content may be null to represent {@code null.symbol}.
     *
     * @throws IllegalArgumentException if the value contains an invalid UTF-16
     * surrogate pair.
     *
     */
    public void writeSymbolToken(SymbolToken content) throws IOException;

    /**
     * Writes a {@link java.lang.String} as an Ion string. Since Ion strings are
     * UTF-8 and Java Strings are Unicode 16.  As such the resulting
     * lengths may not match.  In addition some Java strings are not
     * valid as they may contain only one of the two needed surrogate
     * code units necessary to define the Unicode code point to be
     * output, an exception will be raised if this case is encountered.
     *
     * @param value may be null to represent {@code null.string}.
     *
     * @throws IllegalArgumentException if the value contains an invalid UTF-16
     * surrogate pair.
     */
    public void writeString(String value) throws IOException;

    /**
     * write the byte array out as an IonClob value.  This copies
     * the byte array.
     *
     * @param value may be null to represent {@code null.clob}.
     */
    public void writeClob(byte[] value) throws IOException;

    /**
     * Writes a portion of the byte array out as an IonClob value.  This
     * copies the porition of the byte array that is written.
     *
     * @param value bytes to be written.
     * May be {@code null} to represent {@code null.clob}.
     * @param start offset of the first byte in value to write
     * @param len number of bytes to write from value
     */
    public void writeClob(byte[] value, int start, int len)
        throws IOException;

    /**
     * write the byte array out as an IonBlob value.  This copies
     * the byte array.
     *
     * @param value may be null to represent {@code null.blob}.
     */
    public void writeBlob(byte[] value) throws IOException;

    /**
     * Writes a portion of the byte array out as an IonBlob value.  This
     * copies the portion of the byte array that is written.
     *
     * @param value bytes to be written.
     * May be {@code null} to represent {@code null.blob}.
     * @param start offset of the first byte in value to write
     * @param len number of bytes to write from value
     */
    public void writeBlob(byte[] value, int start, int len)
        throws IOException;
}
