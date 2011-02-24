// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;

/* One design goal is for the readers and writers to be independent of an
 * IonSystem or ValueFactory and thus independent of particular implementations
 * of the DOM.
 *
 * The issue is that one needs a ValueFactory in order to construct the tree.
 * So one either needs to pass a ValueFactory / IonSystem to the reader, or
 * pass the reader to the system.  I decided that the dependencies were better
 * the latter way.  So we have IonSystem.newValue(IonReader) instead of
 * IonReader.nextValue(IonSystem).
 */

/**
 * Provides stream-based access to Ion data independent of its underlying
 * representation (text, binary, or {@link IonValue} tree).
 * <p>
 * In general, method names are intended to parallel similar methods in the
 * {@link IonValue} hierarchy.  For example, to get the text of a symbol one
 * would use {@link #stringValue()} which mirrors
 * {@link IonSymbol#stringValue()}.
 * <h2>Exception Handling</h2>
 * {@code IonReader} is a generic interface for traversion Ion data, and it's
 * not possible to fully specify the set of exceptions that could be thrown
 * from the underlying data source.  Thus all failures are thrown as instances
 * of {@link IonException}, wrapping the originating cause.  If an application
 * wants to handle (say) {@link IOException}s specially, then it needs to
 * extract that from the wrappers; the documentation of {@link IonException}
 * explains how to do that.
 */
public interface IonReader
    extends Closeable
{

    /**
     * Determines whether there is another value at the current depth;
     * in other words whether there is a sibling value that may be reached
     * using {@link #next()}.
     * This method may be
     * called multiple times, which does not move the current position.
     * <p>
     * <b>WARNING:</b> this method alters the internal state of the reader such
     * that you cannot reliably get values from the "current" element. The only
     * thing you should call after {@code hasNext()} is {@link #next()}!
     *
     * @deprecated Applications should detect the end of the current level by
     * checking for a {@code null} response from {@link #next()}.
     */
    @Deprecated
    public boolean hasNext();

    /**
     * Positions this reader on the next sibling after the current value,
     * returning the type of that value.  Once so positioned the contents of
     * this value can be accessed with the {@code *value()} methods.
     * <p>
     * A sequence of {@code next()} calls traverses the data at a constant
     * depth, within the same container.
     * Use {@link #stepIn()} to traverse down into any containers, and
     * {@link #stepOut()} to traverse up to the parent container.
     *
     * @return the type of the next Ion value (never {@link IonType#DATAGRAM}),
     * or {@code null} when there are no more elements at the current depth in
     * the same container.
     */
    public IonType next();


    /**
     * Positions the iterator in the contents of the current value.  The current
     * value must be a container (sexp, list, or struct).
     * After calling this method, {@link #hasNext()} and {@link #next()} will
     * iterate the child values.
     * There's no current value immediately after stepping in.
     * <p>
     * At any time {@link #stepOut()} may be called to move the cursor back to
     * (just after) the parent value, even if there's more children remaining.
     *
     * @throws IllegalStateException if the current value isn't an Ion container.
     */
    public void stepIn();

    /**
     * Positions the iterator after the current parents value.  Once stepOut()
     * has been called hasNext() must be called to see if a value follows
     * the parent. In other words, there's no current value immediately after
     * stepping out.
     *
     * @throws IllegalStateException if the current value wasn't stepped into.
     */
    public void stepOut();

    /**
     * returns the depth into the Ion value this iterator has traversed. The
     * top level, where it started out is depth 0.
     */
    public int getDepth();


    /**
     * Returns the symbol table that is applicable to the current value.
     * This may be either a system or local symbol table.
     */
    public SymbolTable getSymbolTable();


    /**
     * Returns the type of the current value, or null if there is no valid
     * current value.
     */
    public IonType getType();

    /**
     * Return the annotations of the current value as an array of strings.
     *
     * @return the (ordered) annotations on the current value, or an empty
     * array (not {@code null}) if there are none.
     */
    public String[] getTypeAnnotations();

    /**
     * Return the symbol id's of the annotations on the current value as an
     * array of ints.
     *
     * @return the (ordered) annotations on the current value, or an empty
     * array (not {@code null}) if there are none.
     */
    public int[] getTypeAnnotationIds();

    /**
     * Return the annotations on the curent value as an iterator.  The
     * iterator is empty (hasNext() returns false on the first call) if
     * there are no annotations on the current value.
     */
    public Iterator<String> iterateTypeAnnotations();

    /**
     * Return the symbol table ids of the current values annotation as
     * an iterator.  The iterator is empty (hasNext() returns false on
     * the first call) if there are no annotations on the current value.
     */
    public Iterator<Integer> iterateTypeAnnotationIds();

    /**
     * Gets the symbol ID of the field name attached to the current value.
     *
     * @return the symbol ID of the field name, if the current value is a
     * field within a struct.
     * If the current value is not a field, or if the symbol ID cannot be
     * determined, this method returns a value <em>less than one</em>.
     */
    public int getFieldId();

    /**
     * Return the field name of the current value. Or null if there is no valid
     * current value or if the current value is not a field of a struct.
     */
    public String getFieldName();


    /**
     * Returns the whether or not the current value a null ion value.
     * This is valid on all Ion types.  It should be called before
     * calling getters that return value types (int, long, boolean,
     * double).
     */
    public boolean isNullValue();

    /**
     * returns true if the iterator is currently operating over
     * fields of a structure.  It returns false if the iteration
     * is in a list, a sexp, or a datagram.
     */
    public boolean isInStruct();


    //=========================================================================
    // Value reading

    /**
     * Returns the current value as an boolean.  This is only valid if there is
     * an underlying value and the value is an ion boolean value.
     */
    public boolean booleanValue();

    /**
     * Returns the current value as an int.  This is only valid if there is
     * an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */
    public int intValue();

    /**
     * Returns the current value as a long.  This is only valid if there is
     * an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */
    public long longValue();

    /**
     * Returns the current value as a {@link BigInteger}.  This is only valid if there
     * is an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */
    public BigInteger bigIntegerValue();

    /**
     * Returns the current value as a double.  This is only valid if there is
     * an underlying value and the value is either float, or decimal.
     */
    public double doubleValue();

    /**
     * Returns the current value as a {@link BigDecimal}.
     * This method should not return a {@link Decimal}, so it lacks support for
     * negative zeros.
     * <p>
     * This method is only valid when {@link #getType()} returns
     * {@link IonType#DECIMAL}.
     */
    public BigDecimal bigDecimalValue();
    // TODO do these methods work when isNullValue()?

    /**
     * Returns the current value as a {@link Decimal}, which extends
     * {@link BigDecimal} with support for negative zeros.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#DECIMAL}.
     */
    public Decimal decimalValue();


    /**
     * Returns the current value as a {@link java.util.Date}.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#TIMESTAMP}.
     *
     * @return a new {@link Date} instance,
     * or {@code null} if the current value is {@code null.timestamp}.
     */
    public Date dateValue();

    /**
     * Returns the current value as a {@link Timestamp}.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#TIMESTAMP}.
     *
     * @return the current value as a {@link Timestamp},
     * or {@code null} if the current value is {@code null.timestamp}.
     */
    public Timestamp timestampValue();

    /**
     * Returns the current value as a Java String.  This is only valid if there
     * is an underlying value and the value is either string or symbol.
     */
    public String stringValue();

    /**
     * Returns the current value as an int symbol id.  This is only valid if
     * there is
     * an underlying value and the value is an Ion symbol.
     * <p>
     * If the reader cannot determine the symbol ID, this method returns
     * {@link SymbolTable#UNKNOWN_SYMBOL_ID}.
     *
     * @see #stringValue()
     */
    public int getSymbolId();


    /**
     * Gets the size in bytes of the current lob value.
     * This is only valid when {@link #getType()} returns {@link IonType#BLOB}
     * or {@link IonType#CLOB}.
     *
     * @return the lob's size in bytes.
     */
    public int byteSize();

    /**
     * Returns the current value as a newly-allocated byte array.
     * This is only valid when {@link #getType()} returns {@link IonType#BLOB}
     * or {@link IonType#CLOB}.
     */
    public byte[] newBytes();

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
    public int getBytes(byte[] buffer, int offset, int len);

    /**
     * Returns the current value as a String using the Ion toString() serialization
     * format.  This is only valid if there is an underlying value.  This is
     * logically equivalent to getIonValue().toString() but may be more efficient
     * and does not require an IonSystem context to operate.
     */
    // 2008-10-30 Disabled this because semantics are cloudy.
    // In particular, does this move the cursor beyond the current value?
    // Also, this could be problematic to use since other value-extraction
    // methods are read-once, so one can't look at the value before calling this.
//    public String valueToString();

}
