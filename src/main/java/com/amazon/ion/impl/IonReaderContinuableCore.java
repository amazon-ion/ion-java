// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonType;
import com.amazon.ion.IvmNotificationConsumer;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnknownSymbolException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.function.Consumer;

/**
 * IonCursor with the core IonReader interface methods. Useful for adapting an IonCursor implementation into a
 * system-level IonReader.
 */
// TODO this is currently public because it is used by MacroCompiler, which exists in a different Java package.
//  consider ways of not exposing this interface, either by moving MacroCompiler into com.amazon.ion.impl, or using
//  the _Private_ naming convention for this interface.
public interface IonReaderContinuableCore extends IonCursor {

    /**
     * Returns the depth into the Ion value that this reader has traversed.
     * At top level the depth is 0.
     */
    int getDepth();

    /**
     * Returns the type of the current value, or null if there is no
     * current value.
     */
    IonType getType();

    /**
     * Returns the type of the current value in the raw encoding, or
     * null if there is no current value.
     */
    IonType getEncodingType();

    /**
     * Returns an {@link IntegerSize} representing the smallest-possible
     * Java type of the Ion {@code int} at the current value.
     *
     * If the current value is {@code null.int} or is not an Ion
     * {@code int}, or if there is no current value, {@code null} will
     * be returned.
     *
     * @see IonInt#getIntegerSize()
     */
    IntegerSize getIntegerSize();

    /**
     * Determines whether the current value is a null Ion value of any type
     * (for example, <code>null</code> or <code>null.int</code>).
     * It should be called before
     * calling getters that return value types (int, long, boolean,
     * double).
     */
    boolean isNullValue();

    /**
     * Determines whether this reader is currently traversing the fields of an
     * Ion struct. It returns false if the iteration
     * is in a list, a sexp, or a datagram.
     */
    boolean isInStruct();

    /**
     * Gets the symbol ID of the field name attached to the current value.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     *
     * @return the symbol ID of the field name, if the current value is a
     * field within a struct.
     * If the current value is not a field, or if the symbol ID cannot be
     * determined, this method returns a value <em>less than zero</em>.
     * If this method returns less than zero and the reader is positioned
     * on a value with a field name, then the text for the field name can be
     * retrieved using {@link #getFieldText()}.
     *
     */
    @Deprecated
    int getFieldId();

    /**
     * @return true if the value on which the reader is currently positioned has field
     *  name text available for reading via {@link #getFieldText()}. If this
     *  method returns false but the reader is positioned on a value with a field name,
     *  then the field name symbol ID can be retrieved using {@link #getFieldId()}.
     */
    boolean hasFieldText();

    /**
     * Reads the text for the current field name. It is the caller's responsibility to
     * ensure {@link #hasFieldText()} returns true before calling this method.
     * @return the field name text.
     */
    String getFieldText();

    /**
     * Gets the current value's field name as a symbol token (text + ID).
     * If the text of the token isn't known, the result's
     * {@link SymbolToken#getText()} will be null.
     * If the symbol ID of the token isn't known, the result's
     * {@link SymbolToken#getSid()} will be
     * {@link SymbolTable#UNKNOWN_SYMBOL_ID}.
     * At least one of the two fields will be defined.
     *
     * @return null if there is no current value or if the current value is
     *  not a field of a struct.
     *
     */
    SymbolToken getFieldNameSymbol();

    /**
     * Consumes SymbolTokens representing the annotations attached to the current value.
     * Each SymbolToken provided will contain *either* a symbol ID, *or* its symbol
     * text, depending on how it was encoded.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     * <p>
     * It is the caller's responsibility to ensure {@link #hasAnnotations()} returns
     * true before calling this method.
     */
    @Deprecated
    void consumeAnnotationTokens(Consumer<SymbolToken> consumer);

    /**
     * Returns the current value as an boolean.
     * This is only valid when {@link #getType()} returns {@link IonType#BOOL}.
     */
    boolean booleanValue();

    /**
     * Returns the current value as an int.  This is only valid if there is
     * an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */
    int intValue();

    /**
     * Returns the current value as a long.  This is only valid if there is
     * an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */
    long longValue();

    /**
     * Returns the current value as a {@link BigInteger}.  This is only valid if there
     * is an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */
    BigInteger bigIntegerValue();

    /**
     * Returns the current value as a double.  This is only valid if there is
     * an underlying value and the value is either float, or decimal.
     */
    double doubleValue();

    /**
     * Returns the current value as a {@link BigDecimal}.
     * This method should not return a {@link Decimal}, so it lacks support for
     * negative zeros.
     * <p>
     * This method is only valid when {@link #getType()} returns
     * {@link IonType#DECIMAL}.
     *
     * @return the current value as a {@link BigDecimal},
     * or {@code null} if the current value is {@code null.decimal}.
     */
    BigDecimal bigDecimalValue();

    /**
     * Returns the current value as a {@link Decimal}, which extends
     * {@link BigDecimal} with support for negative zeros.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#DECIMAL}.
     *
     * @return the current value as a {@link Decimal},
     * or {@code null} if the current value is {@code null.decimal}.
     */
    Decimal decimalValue();


    /**
     * Returns the current value as a {@link java.util.Date}.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#TIMESTAMP}.
     *
     * @return the current value as a {@link Date},
     * or {@code null} if the current value is {@code null.timestamp}.
     */
    Date dateValue();

    /**
     * Returns the current value as a {@link Timestamp}.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#TIMESTAMP}.
     *
     * @return the current value as a {@link Timestamp},
     * or {@code null} if the current value is {@code null.timestamp}.
     */
    Timestamp timestampValue();

    /**
     * Returns the current value as a Java String.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#STRING} or {@link IonType#SYMBOL}.
     *
     * @throws UnknownSymbolException if the current value is a symbol
     * with unknown text.
     *
     * @see IonReaderContinuableApplication#symbolValue()
     */
    String stringValue();

    /**
     * Gets the symbol ID of the current symbol value.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     *
     * @return the symbol ID of the value.
     * If the symbol ID cannot be determined, this method returns a value <em>less than zero</em>.
     * If this is the case and the reader is positioned on a symbol value, then the text for the
     * symbol can be retrieved using {@link #hasSymbolText()}.
     */
    @Deprecated
    int symbolValueId();

    /**
     * @return true if the value on which the reader is currently positioned is a
     *  symbol with text available for reading via {@link #getSymbolText()}. If this
     *  method returns false but the reader is positioned on a symbol value, then
     *  the value's symbol ID can be retrieved using {@link #symbolValueId()}.
     */
    boolean hasSymbolText();

    /**
     * Reads the text for the current symbol value. It is the caller's responsibility to
     * ensure {@link #hasSymbolText()} returns true before calling this method.
     * @return the symbol value text.
     */
    String getSymbolText();

    /**
     * Returns the current value as a symbol token (text + ID).
     * This is only valid when {@link #getType()} returns
     * {@link IonType#SYMBOL}.
     *
     * @return null if {@link #isNullValue()}
     *
     */
    SymbolToken symbolValue();

    /**
     * Gets the size in bytes of the current lob value.
     * This is only valid when {@link #getType()} returns {@link IonType#BLOB}
     * or {@link IonType#CLOB}.
     *
     * @return the lob's size in bytes.
     */
    int byteSize();

    /**
     * Returns the current value as a newly-allocated byte array.
     * This is only valid when {@link #getType()} returns {@link IonType#BLOB}
     * or {@link IonType#CLOB}.
     */
    byte[] newBytes();

    /**
     * Copies the current value into the passed in a byte array.
     * This is only valid when {@link #getType()} returns {@link IonType#BLOB}
     * or {@link IonType#CLOB}.
     *
     * @param buffer destination to copy the value into, this must not be null.
     * @param offset the first position to copy into, this must be non null and
     *  less than the length of buffer.
     * @param len the number of bytes available in the buffer to copy into,
     *  this must be long enough to hold the whole value and not extend outside
     *  of buffer.
     */
    int getBytes(byte[] buffer, int offset, int len);

    /**
     * Returns the major version of the Ion stream at the reader's current position.
     * @return the major version.
     */
    int getIonMajorVersion();

    /**
     * Returns the minor version of the Ion stream at the reader's current position.
     * @return the minor version.
     */
    int getIonMinorVersion();

    /**
     * Register an {@link IvmNotificationConsumer} to be notified whenever the reader
     * encounters an Ion version marker.
     * @param ivmConsumer the consumer to be notified.
     */
    void registerIvmNotificationConsumer(IvmNotificationConsumer ivmConsumer);

    /**
     * Conveys whether the value on which the reader is currently positioned has
     * annotations.
     * @return true if the value has at least one annotation; otherwise, false.
     */
    boolean hasAnnotations();

}
