/* Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.util.Collection;



/**
 * Common functionality of Ion <code>list</code> and <code>sexp</code> types.
 */
public interface IonSequence
    extends IonContainer, Collection<IonValue>
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
     * @return {@code true} (as per the general contract of the
     * {@link Collection#add} method).
     *
     * @throws NullPointerException
     *   if {@code child} is <code>null</code>.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws IllegalArgumentException
     *   if {@code child} is an {@link IonDatagram}.
     */
    public boolean add(IonValue child)
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
     * Removes a given {@link IonValue} from this sequence, if it is present.
     * <p>
     * <b>Due to the reference-equality-based semantics of Ion sequences,
     * this method does not use {@link Object#equals} as specified by the
     * contract of {@link java.util.Collection}. Instead it uses reference
     * equality ({@code ==} operator) to find the given instance.</b>
     *
     * @returns {@code true} if this sequence changed as a result of the call.
     *
     * @throws NullPointerException if {@code o} is {@code null}.
     * @throws ClassCastException if {@code o} is not an {@link IonValue}.
     */
    public boolean remove(Object o);


    /**
     * Removes all elements from this sequence that are also contained in the
     * specified collection. After this call returns, this sequence will
     * contain no elements in common with the specified collection.
     * <p>
     * <b>Due to the reference-equality-based semantics of Ion sequences,
     * this method does not use {@link Object#equals} as specified by the
     * contract of {@link java.util.Collection}. Instead it uses reference
     * equality ({@code ==} operator) to find the given instance.</b>
     *
     * @returns {@code true} if this sequence changed as a result of the call.
     *
     * @throws NullPointerException if {@code c} is {@code null}.
     * @throws NullPointerException if {@code c} contains one or more
     * {@code null} elements.
     * @throws ClassCastException if {@code c} contains one or more elements
     * that do not implement {@link IonValue}.
     */
    public boolean removeAll(Collection<?> c);


    /**
     * Retains only the elements in this sequence that are also contained in
     * the specified collection. In other words, removes from this sequence
     * all of its elements that are not contained in the specified collection.
     * <p>
     * <b>Due to the reference-equality-based semantics of Ion sequences,
     * this method does not use {@link Object#equals} as specified by the
     * contract of {@link java.util.Collection}. Instead it uses reference
     * equality ({@code ==} operator) to find the given instance.</b>
     *
     * @returns {@code true} if this sequence changed as a result of the call.
     *
     * @throws NullPointerException if {@code c} is {@code null}.
     * @throws NullPointerException if {@code c} contains one or more
     * {@code null} elements.
     * @throws ClassCastException if {@code c} contains one or more elements
     * that do not implement {@link IonValue}.
     */
    public boolean retainAll(Collection<?> c);


    /**
     * Determines whether this sequence contains the given instance.
     * <p>
     * <b>Due to the reference-equality-based semantics of Ion sequences,
     * this method does not use {@link Object#equals} as specified by the
     * contract of {@link java.util.Collection}. Instead it uses reference
     * equality ({@code ==} operator) to find the given instance.</b>
     *
     * @returns {@code true} if {@code o} is an element of this sequence.
     *
     * @throws NullPointerException if {@code o} is {@code null}.
     * @throws ClassCastException if {@code o} is not an {@link IonValue}.
     */
    public boolean contains(Object o);


    /**
     * Determines whether this sequence contains all of the given instances.
     * <p>
     * <b>Due to the reference-equality-based semantics of Ion sequences,
     * this method does not use {@link Object#equals} as specified by the
     * contract of {@link java.util.Collection}. Instead it uses reference
     * equality ({@code ==} operator) to find the given instances.</b>
     *
     * @returns {@code true} if this sequence contains all of the elements of
     * the given collection.
     *
     * @throws NullPointerException if {@code c} is {@code null}.
     * @throws NullPointerException if {@code c} contains one or more
     * {@code null} elements.
     * @throws ClassCastException if {@code c} contains one or more elements
     * that do not implement {@link IonValue}.
     */
    public boolean containsAll(Collection<?> c);

    // Use inherited javadoc, this refines the return type.
    public IonValue[] toArray();


    /**
     * {@inheritDoc}
     */
    public IonSequence clone();
}
