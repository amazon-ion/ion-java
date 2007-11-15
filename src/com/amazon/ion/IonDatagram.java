/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.IOException;
import java.io.OutputStream;
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
     * Returns {@code false} at all times, since datagrams cannot be null.
     *
     * @return <code>false</code>
     */
    public boolean isNullValue();


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
     * Creates an iterator providing the (direct) elements of this datagram.
     * Elements will be returned
     * in order of their appearance in the Ion representation.
     * <p>
     * This iterator returns only user values, ignoring symbol tables and other
     * system values. It does not support the {@link Iterator#remove()}
     * operation.
     *
     * @see #systemIterator()
     */
    public Iterator<IonValue> iterator();

    /**
     * Iterate all values in the datagram, including the otherwise-hidden
     * system values.
     * This iterator does not support the {@link Iterator#remove()}
     * operation.
     * @return not <code>null</code>.
     */
    public Iterator<IonValue> systemIterator();


    /**
     * Gets the number of bytes used to encode this datagram.
     * As a side effect, this method encodes the entire datagram into Ion
     * binary format.

     * @return the number of bytes in the binary encoding of this datagram.
     *
     * @throws IonException if there's an error encoding the data.
     */
    public int byteSize()
        throws IonException;


    /**
     * Copies the binary-encoded form of this datagram into a new byte array.
     *
     * @return a new, non-empty byte array containing the encoded datagram.
     *
     * @throws IonException if there's an error encoding the data.
     */
    public byte[] toBytes()
        throws IonException;


    /**
     * Copies the binary-encoded form of this datagram into a given array.
     * <p>
     * The given array must be large enough to contain all the bytes of this
     * datagram.
     * <p>
     * An invocation of this method of the form {@code dg.get(a)} behaves in
     * exactly the same way as the invocation
     * <pre>
     *     dg.get(a, 0)
     * </pre>
     *
     * @param dst the array into which bytes are to be written.
     *
     * @return the number of bytes copied into {@code dst}.
     *
     * @throws IonException if there's an error encoding the data.
     * @throws IndexOutOfBoundsException if {@code dst.length} is
     * smaller than the result of {@link #byteSize()}.
     *
     * @see #getBytes(byte[],int)
     */
    public int getBytes(byte[] dst)
        throws IonException;


    /**
     * Copies the binary-encoded form of this datagram into a given sub-array.
     * <p>
     * The given subarray must be large enough to contain all the bytes of this
     * datagram.
     *
     * @param dst the array into which bytes are to be written.
     * @param offset the offset within the array of the first byte to be
     *   written; must be non-negative and no larger than {@code dst.length}
     *
     * @return the number of bytes copied into {@code dst}.
     *
     * @throws IonException if there's an error encoding the data.
     * @throws IndexOutOfBoundsException if {@code (dst.length - offset)} is
     * smaller than the result of {@link #byteSize()}.
     */
    public int getBytes(byte[] dst, int offset)
        throws IonException;


    /**
     * Copies the binary-encoded form of this datagram to a specified stream.
     *
     * @param out the output stream to which to write the data.
     *
     * @return the number of bytes written.
     *
     * @throws IonException if there's an error encoding the data.
     * @throws IOException if an error occurs writing the data to the stream.
     */
    public int getBytes(OutputStream out)
        throws IOException, IonException;


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
