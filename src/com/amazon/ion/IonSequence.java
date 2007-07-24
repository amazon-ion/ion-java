/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.util.Iterator;


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
     * Creates an iterator that provides the elements of this sequence,
     * in order of their appearance in the Ion representation.
     *
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public Iterator<IonValue> iterator()
        throws NullValueException;


    /**
     * Appends the specified element to the end of this sequence.
     * If <code>this.isNullValue()</code>, then it becomes a single-element
     * sequence.
     *
     * @param element is the element to be appended to this sequence.
     * @throws ContainedValueException if <code>element</code> is already part
     * of a container.
     * @throws NullPointerException if <code>element</code> is <code>null</code>.
     */
    public void add(IonValue element)
        throws ContainedValueException, NullPointerException;

    /**
     * Inserts an element at the specified position in this sequence.
     * If <code>this.isNullValue()</code>, then it becomes a single-element
     * sequence.
     * @param element is the element to be appended to this sequence.
     *
     * @throws ContainedValueException if <code>element</code> is already part
     * of a container.
     * @throws NullPointerException
     * if <code>element</code> is <code>null</code>.
     * @throws IndexOutOfBoundsException if the index is out of range
     * (index < 0 || index > size()).
     */
    public void add(int index, IonValue element)
        throws ContainedValueException, NullPointerException;


    /**
     * Creates a deep copy of an element and appends it to this sequence in the
     * most efficient manner possible.
     *
     * @param element
     *
     * @throws NullPointerException
     * if <code>element</code> is <code>null</code>.
     * @throws IllegalArgumentException if <code>element</code> is an
     * {@link IonDatagram}.
     */
//    public void addEmbedded(IonValue element)
//        throws NullPointerException;


    /**
     * Creates a deep copy of an element and inserts it into this sequence in
     * the most efficient manner possible.
     *
     * @param element
     *
     * @throws NullPointerException
     * if <code>element</code> is <code>null</code>.
     * @throws IllegalArgumentException if <code>element</code> is an
     * {@link IonDatagram}.
     */
//    public void addEmbedded(int index, IonValue element)
//        throws NullPointerException;
}
