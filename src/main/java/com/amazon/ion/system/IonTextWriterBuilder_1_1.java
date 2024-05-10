// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;

import java.nio.charset.Charset;

/**
 * The builder for creating {@link IonWriter}s emitting the 1.1 version of the Ion text format.
 * <p>
 * Builders may be configured once and reused to construct multiple
 * objects.
 * <p>
 * <b>Instances of this class are not not safe for use by multiple threads
 * unless they are {@linkplain #immutable() immutable}.</b>
 *
 */
public interface IonTextWriterBuilder_1_1 extends IonWriterBuilder_1_1<IonTextWriterBuilder_1_1> {
    // TODO add any configuration specific to writing 1.1 text.

    /**
     * Gets the charset denoting the output encoding.
     * Only ASCII and UTF-8 are supported.
     *
     * @return may be null, denoting the default of UTF-8.
     *
     * @see #setCharset(Charset)
     * @see #withCharset(Charset)
     */
    Charset getCharset();

    /**
     * Sets the charset denoting the output encoding.
     * Only ASCII and UTF-8 are supported.
     *
     * @param charset may be null, denoting the default of UTF-8.
     *
     * @see #getCharset()
     * @see #withCharset(Charset)
     *
     * @throws UnsupportedOperationException if this is immutable.
     */
    void setCharset(Charset charset);

    /**
     * Declares the charset denoting the output encoding,
     * returning a new mutable builder if this is immutable.
     *
     * @param charset may be null, denoting the default of UTF-8.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     * @see #getCharset()
     * @see #setCharset(Charset)
     */
    IonTextWriterBuilder_1_1 withCharset(Charset charset);

    /**
     * Declares the output encoding to be {@code US-ASCII}.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     */
    IonTextWriterBuilder_1_1 withCharsetAscii();

    /**
     * Declares that this builder should minimize system-level output
     * (Ion version markers and local symbol tables).
     * <p>
     * This is equivalent to:
     * <ul>
     *   <li>{@link #setIvmMinimizing(IonWriterBuilder.IvmMinimizing)
     *   setIvmMinimizing}{@code (}{@link IonWriterBuilder.IvmMinimizing#DISTANT DISTANT}{@code )}
     *   <li>{@link #setLstMinimizing(IonTextWriterBuilder.LstMinimizing)
     *   setLstMinimizing}{@code (}{@link IonTextWriterBuilder.LstMinimizing#EVERYTHING EVERYTHING}{@code )}.
     *   This requires all macros to be expanded.
     * </ul>
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     */
    IonTextWriterBuilder_1_1 withMinimalSystemData();


    /**
     * Declares that this builder should use basic pretty-printing.
     * Does not alter the handling of system data.
     * Calling this method alters several other configuration properties,
     * so code should call it first, then make any necessary overrides.
     * <p>
     * The specifics of this configuration may change between releases of this
     * library, so automated processes should not depend on the exact output
     * formatting.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     */
    IonTextWriterBuilder_1_1 withPrettyPrinting();

    /**
     * Declares that this builder should downgrade the writers' output to
     * JSON compatibility. This format cannot round-trip back to Ion with full
     * fidelity.
     * <p>
     * The specific conversions are as follows:
     * <ul>
     *   <li>System data is suppressed per {@link #withMinimalSystemData()}.
     *   <li>All annotations are suppressed.
     *   <li>Nulls of any type are printed as JSON {@code null}.
     *   <li>Blobs are printed as strings, containing Base64.
     *   <li>Clobs are printed as strings, containing only Unicode code points
     *       U+00 through U+FF.
     *   <li>Sexps are printed as lists.
     *   <li>Symbols are printed as strings.
     *   <li>Timestamps are printed as strings, using Ion timestamp format.
     * </ul>
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     */
    IonTextWriterBuilder_1_1 withJsonDowngrade();

    /**
     * {@inheritDoc}
     *
     * @return the IVM minimizing strategy.
     * The default value ({@code null}) indicates that no minimization occurs
     * and IVMs are emitted as received by the writer.
     *
     * @see #setIvmMinimizing(IonWriterBuilder.IvmMinimizing)
     * @see #withIvmMinimizing(IonWriterBuilder.IvmMinimizing)
     *

     */
    IonWriterBuilder.IvmMinimizing getIvmMinimizing();

    /**
     * Sets the strategy for reducing or eliminating non-initial Ion version
     * markers. When null, IVMs are emitted as they are written.
     *
     * @param minimizing the IVM minimization strategy.
     * Null indicates that all explicitly-written IVMs will be emitted.
     *
     * @see #getIvmMinimizing()
     * @see #withIvmMinimizing(IonWriterBuilder.IvmMinimizing)
     *
     * @throws UnsupportedOperationException if this is immutable.
     *

     */
    void setIvmMinimizing(IonWriterBuilder.IvmMinimizing minimizing);

    /**
     * Declares the strategy for reducing or eliminating non-initial Ion version
     * markers, returning a new mutable builder if this is immutable.
     * When null, IVMs are emitted as they are written.
     *
     * @param minimizing the IVM minimization strategy.
     * Null indicates that all explicitly-written IVMs will be emitted.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     * @see #setIvmMinimizing(IonWriterBuilder.IvmMinimizing)
     * @see #getIvmMinimizing()
     *

     */
    IonTextWriterBuilder_1_1 withIvmMinimizing(IonWriterBuilder.IvmMinimizing minimizing);

    /**
     * Gets the strategy for reducing or eliminating local symbol tables.
     * By default, LST data is emitted as received or when necessary
     * (for example, binary data will always collect and emit local symbols).
     *
     * @see #setLstMinimizing(IonTextWriterBuilder.LstMinimizing)
     * @see #withLstMinimizing(IonTextWriterBuilder.LstMinimizing)
     *

     */
    IonTextWriterBuilder.LstMinimizing getLstMinimizing();

    /**
     * Sets the strategy for reducing or eliminating local symbol tables.
     * By default, LST data is emitted as received or when necessary
     * (for example, binary data will always collect and emit local symbols).
     *
     * @param minimizing the LST minimization strategy.
     * Null indicates that LSTs will be emitted as received.
     *
     * @see #getLstMinimizing()
     * @see #withLstMinimizing(IonTextWriterBuilder.LstMinimizing)
     *
     * @throws UnsupportedOperationException if this is immutable.
     *

     */
    void setLstMinimizing(IonTextWriterBuilder.LstMinimizing minimizing);

    /**
     * Sets the strategy for reducing or eliminating local symbol tables.
     * By default, LST data is emitted as received or when necessary
     * (for example, binary data will always collect and emit local symbols).
     *
     * @param minimizing the LST minimization strategy.
     * Null indicates that LSTs will be emitted as received.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     *
     * @see #getLstMinimizing()
     * @see #setLstMinimizing(IonTextWriterBuilder.LstMinimizing)
     *
     */
    IonTextWriterBuilder_1_1 withLstMinimizing(IonTextWriterBuilder.LstMinimizing minimizing);

    /**
     * Gets the length beyond which string and clob content will be rendered
     * as triple-quoted "long strings".
     * At present, such content will only line-break on extant newlines.
     *
     * @return the threshold for printing triple-quoted strings and clobs.
     * Zero means no limit.
     *
     * @see #setLongStringThreshold(int)
     * @see #withLongStringThreshold(int)
     */
    int getLongStringThreshold();

    /**
     * Sets the length beyond which string and clob content will be rendered
     * as triple-quoted "long strings".
     * At present, such content will only line-break on extant newlines.
     *
     * @param threshold the new threshold; zero means none.
     *
     * @see #getLongStringThreshold()
     * @see #withLongStringThreshold(int)
     *
     * @throws UnsupportedOperationException if this is immutable.
     */
    void setLongStringThreshold(int threshold);

    /**
     * Declares the length beyond which string and clob content will be rendered
     * as triple-quoted "long strings".
     * At present, such content will only line-break on extant newlines.
     *
     * @param threshold the new threshold; zero means none.
     *
     * @see #getLongStringThreshold()
     * @see #setLongStringThreshold(int)
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     */
    IonTextWriterBuilder_1_1 withLongStringThreshold(int threshold);

    /**
     * Gets the character sequence that will be written as a line separator.
     * The default is {@link IonTextWriterBuilder.NewLineType#PLATFORM_DEPENDENT}
     *
     * @return the character sequence to be written between top-level values; null means the default should be used.
     *
     * @see #setNewLineType(IonTextWriterBuilder.NewLineType)
     * @see #withNewLineType(IonTextWriterBuilder.NewLineType)
     */
    IonTextWriterBuilder.NewLineType getNewLineType();

    /**
     * Sets the character sequence that will be written as a line separator.
     * The default is {@link IonTextWriterBuilder.NewLineType#PLATFORM_DEPENDENT}
     *
     * @param newLineType the character sequence to be written between top-level values; null means the default should be used.
     *
     * @see #getNewLineType()
     * @see #withNewLineType(IonTextWriterBuilder.NewLineType)
     *
     * @throws UnsupportedOperationException if this is immutable.
     */
    void setNewLineType(IonTextWriterBuilder.NewLineType newLineType);

    /**
     * Declares the character sequence that will be written as a line separator.
     * The default is {@link IonTextWriterBuilder.NewLineType#PLATFORM_DEPENDENT}
     *
     * @param newLineType the character sequence to be written between top-level values; null means the default should be used.
     *
     * @see #getNewLineType()
     * @see #setNewLineType(IonTextWriterBuilder.NewLineType)
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     */
    IonTextWriterBuilder_1_1 withNewLineType(IonTextWriterBuilder.NewLineType newLineType);

    /**
     * Gets whether each top level value for standard printing should start on a new line. The default value is {@code false}.
     * When false, the IonTextWriter will insert a single space character (U+0020) between top-level values.
     * When pretty-printing, this setting is ignored; the pretty printer will always start top-level values on a new line.
     *
     * @return value indicating whether standard printing will insert a newline between top-level values
     *
     * @see #setWriteTopLevelValuesOnNewLines(boolean)
     * @see #withWriteTopLevelValuesOnNewLines(boolean)
     */
    boolean getWriteTopLevelValuesOnNewLines();

    /**
     * Sets whether each top level value for standard printing should start on a new line. The default value is {@code false}.
     * When false, the IonTextWriter will insert a single space character (U+0020) between top-level values.
     * When pretty-printing, this setting is ignored; the pretty printer will always start top-level values on a new line.
     *
     * @param writeTopLevelValuesOnNewLines value indicating whether standard printing will insert a newline between top-level values
     *
     * @see #getWriteTopLevelValuesOnNewLines()
     * @see #withWriteTopLevelValuesOnNewLines(boolean)
     */
    void setWriteTopLevelValuesOnNewLines(boolean writeTopLevelValuesOnNewLines);

    /**
     * Declares whether each top level value for standard printing should start on a new line. The default value is {@code false}.
     * When false, the IonTextWriter will insert a single space character (U+0020) between top-level values.
     * When pretty-printing, this setting is ignored; the pretty printer will always start top-level values on a new line.
     *
     * @param writeTopLevelValuesOnNewLines value indicating whether standard printing will insert a newline between top-level values
     *
     * @see #getWriteTopLevelValuesOnNewLines()
     * @see #setWriteTopLevelValuesOnNewLines(boolean)
     */
    IonTextWriterBuilder_1_1 withWriteTopLevelValuesOnNewLines(boolean writeTopLevelValuesOnNewLines);

    /**
     * Gets the maximum number of digits of fractional second precision allowed to be written for timestamp values.
     *
     * @return the currently configured maximum.
     *
     * @see #setMaximumTimestampPrecisionDigits(int)
     * @see #withMaximumTimestampPrecisionDigits(int)
     */
    int getMaximumTimestampPrecisionDigits();

    /**
     * Sets the maximum number of digits of fractional second precision allowed to be written for timestamp values.
     * Default: {@link Timestamp#DEFAULT_MAXIMUM_DIGITS_TEXT}.
     *
     * @see #getMaximumTimestampPrecisionDigits()
     * @see #withMaximumTimestampPrecisionDigits(int)
     */
    void setMaximumTimestampPrecisionDigits(int maximumTimestampPrecisionDigits);

    /**
     * Sets the maximum number of digits of fractional second precision allowed to be written for timestamp values.
     * Default: {@link Timestamp#DEFAULT_MAXIMUM_DIGITS_TEXT}.
     *
     * @return this instance, if mutable; otherwise a mutable copy of this instance.
     *
     * @see #getMaximumTimestampPrecisionDigits()
     * @see #setMaximumTimestampPrecisionDigits(int)
     */
    IonTextWriterBuilder_1_1 withMaximumTimestampPrecisionDigits(int maximumTimestampPrecisionDigits);

    IonWriter build(Appendable out);
}
