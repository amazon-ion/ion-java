// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.facet.Faceted;
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
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * We still have some work to do before this interface is stable.
 * See <a href="https://jira2.amazon.com/browse/ION-183">issue ION-183</a>
 * <p>
 * In general, method names are intended to parallel similar methods in the
 * {@link IonValue} hierarchy.  For example, to get the text of a symbol one
 * would use {@link #stringValue()}, mirroring {@link IonSymbol#stringValue()}.
 *
 * <h2>Exception Handling</h2>
 * {@code IonReader} is a generic interface for traversion Ion data, and it's
 * not possible to fully specify the set of exceptions that could be thrown
 * from the underlying data source.  Thus all failures are thrown as instances
 * of {@link IonException}, wrapping the originating cause.  If an application
 * wants to handle (say) {@link IOException}s specially, then it needs to
 * extract that from the wrappers; the documentation of {@link IonException}
 * explains how to do that.
 *
 * <h2>Reader Facets</h2>
 * Readers are {@link Faceted} and implementations may provide additional
 * functionality accessible via the {@link #asFacet(Class)} method.
 *
 * <h3>The {@link SpanProvider} Facet</h3>
 * This facet is available on all readers that directly consume an Ion source.
 * It provides access to the "{@linkplain SpanProvider#currentSpan() current
 * span}" covering the reader's current value.
 * There is <em>not</em> a current span at the start of the source, immediately
 * after a call to {@link #stepIn()} or {@link #stepOut()}, or when the prior
 * call to {@link #next()} returned null (meaning: end of container or end of
 * stream). In such states, {@link SpanProvider#currentSpan()} will fail.
 *
 * <h3>The {@link SeekableReader} Facet</h3>
 * This facet is available on all readers <em>except</em> those created from
 * an {@link java.io.InputStream InputStream}.
 * (See <a href="https://jira2.amazon.com/browse/ION-243">issue ION-243</a>.)
 * It allows the user to reposition the reader to a {@link Span} over the
 * same reader instance or another reader with the same source.
 *
 * <h2>Span Facets</h2>
 * Readers that support the {@link SpanProvider} facet vend {@link Span}s that
 * are also faceted.
 *
 * <h3>The {@link OffsetSpan} Facet</h3>
 * This facet is support by all readers of Ion binary and text data.
 *
 * <h3>The {@link TextSpan} Facet</h3>
 * This facet is supported by all text readers.
 */
public interface IonReader
    extends Closeable, Faceted
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
     * Positions the reader just before the contents of the current value,
     * which must be a container (list, sexp, or struct).
     * There's no current value immediately after stepping in, so the next
     * thing you'll want to do is call {@link #next()} to move onto the first
     * child value (or learn that there's not one).
     * <p>
     * Stepping into a null container ({@code null.list}, {@code null.sexp},
     * or {@code null.struct}) behaves as if the container were empty
     * ({@code []}, {@code ()}, or <code>{}</code>).
     * <p>
     * At any time {@link #stepOut()} may be called to move the cursor back to
     * (just after) the parent value, even if there's more children remaining.
     *
     * @throws IllegalStateException if the current value isn't an Ion container.
     */
    public void stepIn();

    /**
     * Positions the iterator after the current parent's value, moving up one
     * level in the data hierarchy.
     * There's no current value immediately after stepping out, so the next
     * thing you'll want to do is call {@link #next()} to move onto the
     * following value.
     *
     * @throws IllegalStateException if the current value wasn't stepped into.
     */
    public void stepOut();

    /**
     * Returns the depth into the Ion value that this reader has traversed.
     * At top level the depth is 0, and it increases by one on each call to
     * {@link #stepIn()}.
     */
    public int getDepth();


    /**
     * Returns the symbol table that is applicable to the current value.
     * This may be either a system or local symbol table.
     */
    public SymbolTable getSymbolTable();


    /**
     * Returns the type of the current value, or null if there is no
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
     * Gets the current value's annotations as interned symbols (text + ID).
     *
     * @return the (ordered) annotations on the current value, or an empty
     * array (not {@code null}) if there are none.
     */
    public InternedSymbol[] getTypeAnnotationSymbols();

    /**
     * Return the symbol IDs of the annotations on the current value as an
     * array of ints.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     *
     * @return the (ordered) annotations on the current value, or an empty
     * array (not {@code null}) if there are none.
     *
     * @deprecated Use {@link #getTypeAnnotationSymbols()} instead.
     */
    @Deprecated
    public int[] getTypeAnnotationIds();

    /**
     * Return the annotations on the curent value as an iterator.  The
     * iterator is empty (hasNext() returns false on the first call) if
     * there are no annotations on the current value.
     *
     * @return not null.
     */
    public Iterator<String> iterateTypeAnnotations();

    /**
     * Return the symbol table IDs of the current value's annotation as
     * an iterator.  The iterator is empty (hasNext() returns false on
     * the first call) if there are no annotations on the current value.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     *
     * @return not null.
     *
     * @deprecated Use {@link #getTypeAnnotationSymbols()} instead.
     */
    @Deprecated
    public Iterator<Integer> iterateTypeAnnotationIds();

    /**
     * Gets the symbol ID of the field name attached to the current value.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
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
     * Gets the current value's field name as an interned symbol (text + ID).
     *
     * @return null if there is no current value or if the current value is
     *  not a field of a struct.
     */
    public InternedSymbol getFieldNameSymbol();


    /**
     * Determines whether the current value is a null Ion value of any type
     * (for example, <code>null</code> or <code>null.int</code>).
     * It should be called before
     * calling getters that return value types (int, long, boolean,
     * double).
     */
    public boolean isNullValue();

    /**
     * Determines whether this reader is currently traversing the fields of an
     * Ion struct. It returns false if the iteration
     * is in a list, a sexp, or a datagram.
     */
    public boolean isInStruct();


    //=========================================================================
    // Value reading

    /**
     * Returns the current value as an boolean.
     * This is only valid when {@link #getType()} returns {@link IonType#BOOL}.
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
     * Returns the current value as a Java String.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#STRING} or {@link IonType#SYMBOL}.
     */
    public String stringValue();


    /**
     * Returns the current value as an interned symbol (text + ID).
     * This is only valid when {@link #getType()} returns
     * {@link IonType#SYMBOL}.
     *
     * @return null if {@link #isNullValue()}
     */
    public InternedSymbol symbolValue();


    /**
     * Returns the current value as an int symbol ID.
     * This is only valid when {@link #getType()} returns
     * {@link IonType#SYMBOL}.
     * <p>
     * If the reader cannot determine the symbol ID, this method returns
     * {@link SymbolTable#UNKNOWN_SYMBOL_ID}.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
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
