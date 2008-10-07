/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import java.util.Collection;
import java.util.Iterator;

/**
 * Common functionality of Ion <code>struct</code>, <code>list</code>, and
 * <code>sexp</code> types.
 */
public interface IonContainer
    extends IonValue, Iterable<IonValue>
{

    /**
     * Returns the number of elements in this container.
     *
     * @return the number of elements.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public int size()
        throws NullValueException;

    /**
     * Creates an iterator providing the (direct) elements of this container.
     * Note that ordering is not specified in general, although implementations
     * of {@link IonSequence} must return elements
     * in order of their appearance in the Ion representation.
     *
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public Iterator<IonValue> iterator()
        throws NullValueException;


    /**
     * Removes the given element from this container.
     * <p>
     * Note that, unlike {@link Collection#remove(Object)}, this method uses
     * object identity, not {@link Object#equals(Object)}, to find the element.
     * That is, the given instance is removed, not other similarly "equal"
     * instances.
     *
     * @param element the element to be removed from this container,
     * if present.
     * @return <code>true</code> if this container contained the specified
     * element.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     * @throws NullPointerException if the <code>element</code>
     * is <code>null</code>.
     */
    public boolean remove(IonValue element)
        throws NullValueException;

    /**
     * Checks if this container is empty.
     *
     * @return <code>true</code> if this container has no contents
     * @throws NullValueException if this container is an Ion
     * <code>null</code> value
     */
    public boolean isEmpty()
        throws NullValueException;

    /**
     * Clears the contents of this container (if any) and set it to empty.
     * If this container is an Ion <code>null</code> value, set it to
     * empty.
     * <p>
     * Use {@link #makeNull} to make this container null rather than empty.
     */
    public void clear();

    /**
     * Sets the contents of this container to an Ion <code>null</code>
     * value.
     * <p>
     * Use {@link #clear} to make this container empty rather than null.
     */
    public void makeNull();


    public IonContainer clone();
}
