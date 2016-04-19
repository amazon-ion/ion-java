/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;

/**
 * The factory for all {@link IonValue}s.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface ValueFactory
{
    /**
     * Constructs a new <code>null.blob</code> instance.
     */
    public IonBlob newNullBlob();


    /**
     * Constructs a new Ion {@code blob} instance, copying bytes from an array.
     *
     * @param value the data for the new blob, to be <em>copied</em> from the
     * given array into the new instance.
     * May be {@code null} to create a {@code null.blob} value.
     */
    public IonBlob newBlob(byte[] value);


    /**
     * Constructs a new Ion {@code blob}, copying bytes from part of an array.
     * <p>
     * This method copies {@code length} bytes from the given array into the
     * new value, starting at the given offset in the array.
     *
     * @param value the data for the new blob, to be <em>copied</em> from the
     * given array into the new instance.
     * May be {@code null} to create a {@code null.blob} value.
     * @param offset the offset within the array of the first byte to copy;
     * must be non-negative and no larger than {@code bytes.length}.
     * @param length the number of bytes to be copied from the given array;
     * must be non-negative and no larger than {@code bytes.length - offset}.
     *
     * @throws IndexOutOfBoundsException
     * if the preconditions on the {@code offset} and {@code length} parameters
     * are not met.
     */
    public IonBlob newBlob(byte[] value, int offset, int length);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.bool</code> instance.
     */
    public IonBool newNullBool();


    /**
     * Constructs a new <code>bool</code> instance with the given value.
     *
     * @param value the new {@code bool}'s value.
     *
     * @return a bool with
     * <code>{@link IonBool#booleanValue()} == value</code>.
     */
    public IonBool newBool(boolean value);


    /**
     * Constructs a new <code>bool</code> instance with the given value.
     *
     * @param value the new {@code bool}'s value.
     * may be {@code null} to make {@code null.bool}.
     */
    public IonBool newBool(Boolean value);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.clob</code> instance.
     */
    public IonClob newNullClob();


    /**
     * Constructs a new Ion {@code clob} instance from a byte array.
     *
     * @param value the data for the new clob, to be <em>copied</em> from the
     * given array into the new instance.
     * May be {@code null} to create a {@code null.clob} value.
     */
    public IonClob newClob(byte[] value);


    /**
     * Constructs a new Ion {@code clob}, copying bytes from part of an array.
     * <p>
     * This method copies {@code length} bytes from the given array into the
     * new value, starting at the given offset in the array.
     *
     * @param value the data for the new blob, to be <em>copied</em> from the
     * given array into the new instance.
     * May be {@code null} to create a {@code null.clob} value.
     * @param offset the offset within the array of the first byte to copy;
     * must be non-negative an no larger than {@code bytes.length}.
     * @param length the number of bytes to be copied from the given array;
     * must be non-negative an no larger than {@code bytes.length - offset}.
     *
     * @throws IndexOutOfBoundsException
     * if the preconditions on the {@code offset} and {@code length} parameters
     * are not met.
     */
    public IonClob newClob(byte[] value, int offset, int length);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.decimal</code> instance.
     */
    public IonDecimal newNullDecimal();


    /**
     * Constructs a new Ion {@code decimal} instance from a Java
     * {@code long}.
     */
    public IonDecimal newDecimal(long value);


    /**
     * Constructs a new Ion {@code decimal} instance from a Java
     * {@code double}.
     * <p>
     * Note that this does not generate the exact decimal representation of the
     * {@code double}'s binary floating-point value as via
     * {@link BigDecimal#BigDecimal(double)}, but instead uses the more
     * predictable behavior of matching the double's string representation
     * as via {@link BigDecimal#valueOf(double)}.
     */
    public IonDecimal newDecimal(double value);


    /**
     * Constructs a new Ion {@code decimal} instance from a Java
     * {@link BigInteger}.
     */
    public IonDecimal newDecimal(BigInteger value);


    /**
     * Constructs a new Ion {@code decimal} instance from a Java
     * {@link BigDecimal}.
     * To create negative zero values, pass a {@link Decimal}.
     */
    public IonDecimal newDecimal(BigDecimal value);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new {@code null.float} instance.
     */
    public IonFloat newNullFloat();


    /**
     * Constructs a new Ion {@code float} instance from a Java
     * {@code long}.
     */
    public IonFloat newFloat(long value);


    /**
     * Constructs a new Ion {@code float} instance from a Java
     * {@code double}.
     */
    public IonFloat newFloat(double value);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.int</code> instance.
     */
    public IonInt newNullInt();


    /**
     * Constructs a new <code>int</code> instance with the given value.
     *
     * @param value the new int's value.
     */
    public IonInt newInt(int value);


    /**
     * Constructs a new <code>int</code> instance with the given value.
     *
     * @param value the new int's value.
     */
    public IonInt newInt(long value);


    /**
     * Constructs a new <code>int</code> instance with the given value.
     * The integer portion of the number is used, any fractional portion is
     * ignored.
     *
     * @param value the new int's value;
     * may be <code>null</code> to make <code>null.int</code>.
     */
    public IonInt newInt(Number value);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.list</code> instance.
     */
    public IonList newNullList();


    /**
     * Constructs a new empty (not null) <code>list</code> instance.
     */
    public IonList newEmptyList();


    /**
     * Constructs a new {@code list} with the given child.
     * <p>
     * <b>This method is temporary</b> until {@link #newList(Collection)} is
     * removed.  It's sole purpose is to avoid the doomed attempt to add all
     * of the parameter's children to the new list; that will always throw
     * {@link ContainedValueException}.
     *
     * @param child the initial child of the new list.
     *
     * @throws NullPointerException if {@code child} is null.
     * @throws IllegalArgumentException if {@code child} is an {@link IonDatagram}.
     * @throws ContainedValueException
     *  if {@code child}
     *  has <code>{@link IonValue#getContainer()} != null</code>.
     */
    public IonList newList(IonSequence child)
        throws ContainedValueException, NullPointerException;


    /**
     * Constructs a new {@code list} with the given children.
     * <p>
     * Some edge cases are worth examples:
     *<pre>
     *    factory.newList();                     // returns []
     *    factory.newList((IonValue[]) null);    // returns null.list
     *</pre>
     * For clarity, applications should prefer {@link #newEmptyList()} and
     * {@link #newNullList()} instead.
     *
     * @param children
     *  the initial sequence of children.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *
     * @throws NullPointerException if any child is null.
     * @throws IllegalArgumentException if any child is an {@link IonDatagram}.
     * @throws ContainedValueException
     *  if any child has <code>{@link IonValue#getContainer()} != null</code>.
     */
    public IonList newList(IonValue... children)
        throws ContainedValueException, NullPointerException;


    /**
     * Constructs a new <code>list</code> with given <code>int</code> children.
     *
     * @param values
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *  Otherwise, the resulting sequence will contain new {@link IonInt}s with
     *  the given values.
     *
     * @return a new list where each element is an {@link IonInt}.
     */
    public IonList newList(int[] values);


    /**
     * Constructs a new <code>list</code> with given <code>long</code> child
     * elements.
     *
     * @param values
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *  Otherwise, the resulting sequence will contain new {@link IonInt}s with
     *  the given values.
     *
     * @return a new list where each element is an {@link IonInt}.
     */
    public IonList newList(long[] values);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.null</code> instance.
     */
    public IonNull newNull();


    /**
     * Constructs a new Ion null value with the given type.
     *
     * @param type must not be Java null, but it may be {@link IonType#NULL}.
     *
     * @return a new value such that {@link IonValue#isNullValue()} is
     * {@code true}.
     */
    public IonValue newNull(IonType type);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.sexp</code> instance.
     */
    public IonSexp newNullSexp();


    /**
     * Constructs a new empty (not null) <code>sexp</code> instance.
     */
    public IonSexp newEmptySexp();

    /**
     * Constructs a new {@code sexp} with the given child.
     * <p>
     * <b>This method is temporary</b> until {@link #newSexp(Collection)} is
     * removed.  It's sole purpose is to avoid the doomed attempt to add all
     * of the parameter's children to the new sequence; that will always throw
     * {@link ContainedValueException}.
     *
     * @param child the initial child of the new sexp.
     *
     * @throws NullPointerException if {@code child} is null.
     * @throws IllegalArgumentException
     *  if {@code child} is an {@link IonDatagram}.
     * @throws ContainedValueException
     *  if {@code child}
     *  has <code>{@link IonValue#getContainer()} != null</code>.
     */
    public IonSexp newSexp(IonSequence child)
        throws ContainedValueException, NullPointerException;

    /**
     * Constructs a new <code>sexp</code> with given child elements.
     * <p>
     * Some edge cases are worth examples:
     *<pre>
     *    factory.newSexp();                     // returns ()
     *    factory.newSexp((IonValue[]) null);    // returns null.sexp
     *</pre>
     * For clarity, applications should prefer {@link #newEmptySexp()} and
     * {@link #newNullSexp()} instead.
     *
     * @param children
     *  the initial set of children.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *
     * @throws NullPointerException if any child is null.
     * @throws IllegalArgumentException if any child is an {@link IonDatagram}.
     * @throws ContainedValueException
     *  if any child has <code>{@link IonValue#getContainer()} != null</code>.
     */
    public IonSexp newSexp(IonValue... children)
        throws ContainedValueException, NullPointerException;


    /**
     * Constructs a new <code>sexp</code> with given <code>int</code> child
     * values.
     *
     * @param values
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *  Otherwise, the resulting sequence will contain new {@link IonInt}s with
     *  the given values.
     *
     * @return a new sexp where each element is an {@link IonInt}.
     */
    public IonSexp newSexp(int[] values);


    /**
     * Constructs a new <code>sexp</code> with given <code>long</code> child
     * elements.
     *
     * @param values
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *  Otherwise, the resulting sequence will contain new {@link IonInt}s with
     *  the given values.
     *
     * @return a new sexp where each element is an {@link IonInt}.
     */
    public IonSexp newSexp(long[] values);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.string</code> instance.
     */
    public IonString newNullString();


    /**
     * Constructs a new Ion string with the given value.
     *
     * @param value the text of the new string;
     * may be <code>null</code> to make <code>null.string</code>.
     */
    public IonString newString(String value);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.struct</code> instance.
     */
    public IonStruct newNullStruct();


    /**
     * Constructs a new empty (not null) <code>struct</code> instance.
     */
    public IonStruct newEmptyStruct();


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.symbol</code> instance.
     */
    public IonSymbol newNullSymbol();


    /**
     * Constructs a new Ion symbol with the given value.
     *
     * @param value the text of the symbol;
     * may be <code>null</code> to make <code>null.symbol</code>.
     *
     * @throws EmptySymbolException if <code>value</code> is the empty string.
     */
    public IonSymbol newSymbol(String value);


    /**
     * Constructs a new Ion symbol with the given symbol token.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     *
     * @param value the text and/or SID of the symbol;
     * may be <code>null</code> to make <code>null.symbol</code>.
     *
     */
    public IonSymbol newSymbol(SymbolToken value);


    //-------------------------------------------------------------------------


    /**
     * Constructs a new <code>null.timestamp</code> instance.
     */
    public IonTimestamp newNullTimestamp();


    /**
     * Constructs a new {@code timestamp} instance with the given value.
     *
     * @param value may be {@code null} to make {@code null.timestamp}.
     */
    public IonTimestamp newTimestamp(Timestamp value);


    //-------------------------------------------------------------------------


    /**
     * Creates a deep copy of an Ion value.  This method can properly clone
     * {@link IonDatagram}s.
     * <p>
     * The given value can be in the context of any {@code ValueFactory},
     * and the result will be in the context of this one. This allows you to
     * shift data from one factory instance to another.
     *
     * @param value the value to copy.
     *
     * @return a deep copy of value, with no container.
     *
     * @throws NullPointerException if {@code value} is null.
     * @throws IonException if there's a problem creating the clone.
     * @throws UnknownSymbolException
     *          if any part of this value has unknown text but known Sid for
     *          its field name, annotation or symbol.
     *
     * @see IonValue#clone()
     */
    public <T extends IonValue> T clone(T value)
        throws IonException;
}
