/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.util.Iterator;



/**
 * A datagram is a "top-level" container of Ion values, and the granularity of
 * binary encoding Ion content.
 * <p>
 * Along with the normal user values, datagrams contain system values, notably
 * the symbol table(s) used to atomize all symbols. Most uses of a datagram
 * will not see system values,
 * but applications that need visibility into that data can use the
 * <code>system*()</code> methods.
 * <p>
 * <b>Warning:</b> most modification methods are not yet implemented!
 */
public interface IonDatagram
    extends IonSequence
{

//    public void add(IonValue element)
//        throws ContainedValueException, NullPointerException;

//    public void add(int index, IonValue element)
//        throws ContainedValueException, NullPointerException;


    /**
     * {@inheritDoc}
     * <p>
     * Datagrams always return <code>null</code> from this method, since by
     * definition they have no container.
     */
    public IonContainer getContainer();


    /**
     * Gets the number of elements in the datagram, not counting system
     * elements.
     */
    public int size();

    /**
     * Gets the number of elements in the datagram, including system elements.
     */
    public int systemSize();


    /**
     * Gets a selected non-system element from this datagram.
     *
     * @param index must be less than <code>{@link #size()}</code>.
     * @return the selected element; not <code>null</code>.
     * @throws IndexOutOfBoundsException if the index is bad.
     */
    public IonValue get(int index)
        throws IndexOutOfBoundsException;

    /**
     * Gets a selected element from this datagram, potentially getting a
     * hidden system element (such as a symbol table).
     *
     * @param index must be less than <code>{@link #systemSize()}</code>.
     * @return the selected element; not <code>null</code>.
     * @throws IndexOutOfBoundsException if the index is bad.
     */
    public IonValue systemGet(int index)
        throws IndexOutOfBoundsException;


    /**
     * {@inheritDoc}
     * <p>
     * This iterator returns only user values, ignoring symbol tables and other
     * system values. It does not support the {@link Iterator#remove()}
     * operation.
     * @see #systemIterator()
     */
    public Iterator<IonValue> iterator()
        throws NullValueException;

    /**
     * Iterate all values in the datagram, including the otherwise-hidden
     * system values.
     * This iterator does not support the {@link Iterator#remove()}
     * operation.
     * @return not <code>null</code>.
     */
    public Iterator<IonValue> systemIterator();


    /**
     * Gets the binary-encoded form of this datagram, suitable for storage or
     * transmission.
     *
     * @return a new, non-empty byte array containing the encoded datagram.
     */
    public byte[] toBytes();


    /**
     * This inherited method is not supported by datagrams because there's no
     * single symbol table used across the contents.  Each value contained by
     * the datagram may have its own symbol table.
     *
     * @throws UnsupportedOperationException at every call.
     */
    public LocalSymbolTable getSymbolTable();

    /**
     * This inherited method is not supported by datagrams.
     *
     * @throws UnsupportedOperationException at every call.
     */
    public void addTypeAnnotation(String annotation);

    /**
     * This inherited method is not supported by datagrams.
     *
     * @throws UnsupportedOperationException at every call.
     */
    public void makeNull();
}
