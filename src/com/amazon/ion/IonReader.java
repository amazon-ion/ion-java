// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Provides stream-based access to Ion data independent of its underlying
 * representation (text, binary, or {@link IonValue} tree).
 */
public interface IonReader
{

    /**
     * Returns true when there is another value at the current iteration level.
     * The iteration takes place at the same "level" in the value it only
     * steps into a child value using stepInto().  So this returns whether
     * or not there is a sibling value that may be visited using next(). This
     * must be called before calling next() or next() may fail.  It may be
     * called multiple times, which does not move the current position.
     *
     */
    public boolean hasNext();

    /**
     * Positions the iterator on the next value.  This returns the underlying
     * IonType of the value that is found.  Once so positioned the contents of
     * this value can be accessed with the get methods.  This traverses the
     * contents at a constant level.
     *
     * @return the type of the next Ion value; never {@link IonType#DATAGRAM}.
     *
     * @throws NoSuchElementException if there are no more elements.
     */
    public IonType next();


    /**
     * Positions the iterator in the contents of the current value.  The current
     * value must be a container (sexp, list, or struct).  Once this method has
     * been called {@link #hasNext()} and {@link #next()} will iterate the child values.
     * At any time {@link #stepOut()} may be called to move the cursor back to
     * (just after) the parent value.
     *
     * @throws IllegalStateException if the current value isn't an Ion container.
     */
    public void stepIn();

    /**
     * Positions the iterator after the current parents value.  Once stepOut()
     * has been called hasNext() must be called to see if a value follows
     * the parent.
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
     * Returns IonType of the current value, or null if there is no valid
     * current value.
     */
    public IonType getType();

    /**
     * Return the annotations of the current value as an array of strings.  The
     * return value is null if there are no annotations on the current value.
     */
    public String[] getTypeAnnotations();

    /**
     * Return the symbol id's of the annotations on the current value as an
     * array of ints.  The return value is null if there are no annotations
     * on the current value.
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
     * Return an symbol table id of the field name of the current value. Or -1 if
     * there is no valid current value or if the current value is not a field
     * of a struct.
     */
    public int getFieldId();

    /**
     * Return the field name of the current value. Or null if there is no valid
     * current value or if the current value is not a field of a struct.
     */
    public String getFieldName();

    /**
     * Return the current value as an IonValue using the passed in IonSystem
     * context. This returns null if there is no valid current value.
     *
     * @param sys ion context for the returned value to be created under.
     * This does not have be the same as the context of the iterators value,
     * if it has one.
     */
     // TODO Probably more appropriate to be system.newIonValue(IonReader)
//    public IonValue getIonValue(IonSystem sys);

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
     * Returns the current value as a BigDecimal.  This is only valid if there
     * is an underlying value and the value is of a numeric type (int, float, or
     * decimal).
     */  // TODO implement bigIntegerValue
//    public BigInteger bigIntegerValue();

    /**
     * Returns the current value as a double.  This is only valid if there is
     * an underlying value and the value is either float, or decimal.
     */
    public double doubleValue();

    /**
     * Returns the current value as a BigDecimal.  This is only valid if there
     * is an underlying value and the value is decimal.
     */
    public BigDecimal bigDecimalValue();

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
     * Returns the current value as a {@link TtTimestamp}.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#TIMESTAMP}.
     *
     * @return the current value as a {@link TtTimestamp},
     * or {@code null} if the current value is {@code null.timestamp}.
     */
    public TtTimestamp timestampValue();

    /**
     * Returns the current value as a Java String.  This is only valid if there
     * is an underlying value and the value is either string or symbol.
     */
    public String stringValue();

    /**
     * Returns the current value as an int symbol id.  This is only valid if
     * there is
     * an underlying value and the value is an Ion symbol.
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
