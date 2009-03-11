/* Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;



/**
 * Common functionality of Ion <code>list</code> and <code>sexp</code> types.
 */
public interface IonSequence
    extends IonContainer
{
    /**
     * Returns the element at the specified position in this sequence.
     *
     * @param index identifies the element to return.
     * @return the element at the given index; not <code>null</code>.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     * @throws IndexOutOfBoundsException if the index is out of range
     * (<code>index < 0 || index >= size()</code>).
     */
    public IonValue get(int index)
        throws NullValueException, IndexOutOfBoundsException;


    /**
     * Appends a child value to the end of this sequence.
     * If <code>this.isNullValue()</code>, then it becomes a single-element
     * sequence.
     *
     * @param child is the value to be appended to this sequence.
     *
     * @throws NullPointerException
     *   if {@code child} is <code>null</code>.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws IllegalArgumentException
     *   if {@code child} is an {@link IonDatagram}.
     */
    public void add(IonValue child)
        throws ContainedValueException, NullPointerException;


    /**
     * Provides a factory that when invoked constructs a new value and
     * {@code add}s it to this sequence.
     * <p>
     * These two lines are equivalent:
     * <pre>
     *    seq.add().newInt(3);
     *    seq.add(seq.getSystem().newInt(3));
     * </pre>
     */
    public ValueFactory add();


    /**
     * Inserts a child value at the specified position in this sequence.
     * If <code>this.isNullValue()</code>, then it becomes a single-element
     * sequence.
     *
     * @param child is the element to be appended to this sequence.
     *
     * @throws NullPointerException
     *   if {@code child} is <code>null</code>.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws IllegalArgumentException
     *   if {@code child} is an {@link IonDatagram}.
     * @throws IndexOutOfBoundsException if the index is out of range
     * (index < 0 || index > size()).
     */
    public void add(int index, IonValue child)
        throws ContainedValueException, NullPointerException;


    /**
     * Provides a factory that when invoked constructs a new value and
     * {@code add}s it to this sequence at the specified position.
     * <p>
     * These two lines are equivalent:
     * <pre>
     *    seq.add(12).newInt(3);
     *    seq.add(12, seq.getSystem().newInt(3));
     * </pre>
     * <p>
     * The given {@code index} is validated when the factory's creation method
     * is invoked, not when this method is invoked.
     */
    public ValueFactory add(int index);


    /**
     * {@inheritDoc}
     */
    public IonSequence clone();
}
