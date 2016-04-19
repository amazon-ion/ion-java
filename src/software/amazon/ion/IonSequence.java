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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;



/**
 * Common functionality of Ion <code>list</code> and <code>sexp</code> types.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * Ion sequences implement the standard Java {@link List} interface, behaving
 * generally as expected, with the following exceptions:
 * <ul>
 *   <li>
 *     Due to the reference-equality-based semantics of Ion sequences, methods
 *     like {@link #remove(Object)} do not use {@link Object#equals} as
 *     specified by the contract of {@link java.util.Collection}.
 *     Instead they use reference equality ({@code ==} operator) to find the
 *     given instance.
 *   </li>
 *   <li>
 *     Any given {@link IonValue} instance may be a child of at most one
 *     {@link IonContainer}.  Instances may be children of any number of
 *     non-Ion {@link Collection}s.
 *   </li>
 *   <li>
 *     The method {@link #subList(int, int)} is not implemented at all.
 *     We think it will be quite challenging to get correct, and decided that
 *     it was still valuable to extend {@link List} even with this contractual
 *     violation.
 *   </li>
 * </ul>
 */
public interface IonSequence
    extends IonContainer, List<IonValue>
{
    /**
     * Returns the element at the specified position in this sequence.
     *
     * @param index identifies the element to return.
     * @return the element at the given index; not <code>null</code>.
     * @throws NullValueException if {@link #isNullValue()}.
     * @throws IndexOutOfBoundsException if the index is out of range
     * (<code>index < 0 || index >= size()</code>).
     */
    public IonValue get(int index)
        throws NullValueException, IndexOutOfBoundsException;


    /**
     * Appends a child value to the end of this sequence.
     * If {@link #isNullValue()}, then it becomes a single-element
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
     *<pre>
     *    seq.add().newInt(3);
     *    seq.add(seq.getSystem().newInt(3));
     *</pre>
     */
    public ValueFactory add();


    /**
     * Inserts a child value at the specified position in this sequence.
     * If {@link #isNullValue()}, then it becomes a single-element
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
     *<pre>
     *    seq.add(12).newInt(3);
     *    seq.add(12, seq.getSystem().newInt(3));
     *</pre>
     * <p>
     * The given {@code index} is validated when the factory's creation method
     * is invoked, not when this method is invoked.
     */
    public ValueFactory add(int index);


    /**
     * Replaces the element at the specified position in this list with the
     * specified element.
     *
     * @param index index of the element to replace.
     * @param element element to be stored at the specified position.
     * @return the element previously at the specified index.
     *
     * @throws UnsupportedOperationException
     *   if this is an {@link IonDatagram}.
     * @throws NullPointerException
     *   if the specified element is {@code null}.
     * @throws ContainedValueException
     *   if the specified element is already part of a container.
     * @throws IllegalArgumentException
     *   if the specified element is an {@link IonDatagram}.
     * @throws ReadOnlyValueException
     *   if this value or the specified element {@link #isReadOnly()}.
     * @throws IndexOutOfBoundsException
     *   if the index is out of range ({@code index < 0 || index >= size()}).
     */
    public IonValue set(int index, IonValue element);


    /**
     * Removes the element at the specified position.
     * Shifts any subsequent elements to the left (subtracts one from their
     * indices). Returns the element that was removed from the list.
     *
     * @param index the index of the element to be removed.
     *
     * @return the element previously at the specified position.
     *
     * @throws IndexOutOfBoundsException if the index is out of range
     * (index < 0 || index >= size()).
     */
    public IonValue remove(int index);


    /**
     * Removes a given {@link IonValue} from this sequence, if it is present.
     * <p>
     * <b>Due to the reference-equality-based semantics of Ion sequences,
     * this method does not use {@link Object#equals} as specified by the
     * contract of {@link java.util.Collection}. Instead it uses reference
     * equality ({@code ==} operator) to find the given instance.</b>
     *
     * @return {@code true} if this sequence changed as a result of the call.
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
     * @return {@code true} if this sequence changed as a result of the call.
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
     * @return {@code true} if this sequence changed as a result of the call.
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
     * @return {@code true} if {@code o} is an element of this sequence.
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
     * @return {@code true} if this sequence contains all of the elements of
     * the given collection.
     *
     * @throws NullPointerException if {@code c} is {@code null}.
     * @throws NullPointerException if {@code c} contains one or more
     * {@code null} elements.
     * @throws ClassCastException if {@code c} contains one or more elements
     * that do not implement {@link IonValue}.
     */
    public boolean containsAll(Collection<?> c);

    /**
     * Returns the index in the sequence of the specified element,
     * or -1 if this sequence doesn't contain the element.
     * <p>
     * <b>Due to the reference-equality-based semantics of Ion sequences,
     * this method does not use {@link Object#equals} as specified by the
     * contract of {@link java.util.List}. Instead it uses reference
     * equality ({@code ==} operator) to find the instance.</b>
     *
     * @param o the element to search for.
     * @return the index in this sequence of the element,
     * or -1 if this sequence doesn't contain the element.
     */
    public int indexOf(Object o);

    /**
     * Returns the index in the sequence of the specified element,
     * or -1 if this sequence doesn't contain the element.
     * <p>
     * <b>Due to the reference-equality-based semantics of Ion sequences,
     * this method does not use {@link Object#equals} as specified by the
     * contract of {@link java.util.List}. Instead it uses reference
     * equality ({@code ==} operator) to find the instance.</b>
     *
     * @param o the element to search for.
     * @return the index in this sequence of the element,
     * or -1 if this sequence doesn't contain the element.
     */
    public int lastIndexOf(Object o);


    /**
     * Appends all of the elements in the specified collection to the end of
     * this sequence, in the order that they are returned by the collection's
     * iterator.
     * The behavior of this operation is unspecified if the specified
     * collection is modified while the operation is in progress.
     * (Note that this will occur if the specified collection is this sequence,
     * and it's nonempty.)
     * <p>
     * Since Ion values can only have a single parent, this method will fail if
     * the given collection is a non-empty {@link IonContainer}.
     *
     * @param c
     * elements to be appended to this sequence.
     *
     * @return {@code true} if this sequence changed as a result of the call.
     *
     * @throws UnsupportedOperationException
     * if this is an {@link IonDatagram}.
     * @throws ClassCastException
     * if one of the elements of the collection is not an {@link IonValue}
     * @throws NullPointerException
     * if one of the elements of the collection is {@code null}.
     * @throws ContainedValueException
     * if one of the elements is already contained by an {@link IonContainer}.
     */
    public boolean addAll(Collection<? extends IonValue> c);


    /**
     * Inserts all of the elements in the specified collection into this
     * sequence at the specified position. Shifts the element currently at that
     * position (if any) and any subsequent elements to the right (increases
     * their indices). The new elements will appear in this sequence in the
     * order that they are returned by the specified collection's iterator.
     * The behavior of this operation is unspecified if the specified
     * collection is modified while the operation is in progress.
     * (Note that this will occur if the specified collection is this sequence,
     * and it's nonempty.)
     * <p>
     * Since Ion values can only have a single parent, this method will fail if
     * the given collection is a non-empty {@link IonContainer}.
     *
     * @param index
     * index at which to insert first element from the specified collection.
     * @param c
     * elements to be inserted into this sequence.
     *
     * @return {@code true} if this sequence changed as a result of the call.
     *
     * @throws UnsupportedOperationException
     * if this is an {@link IonDatagram}.
     * @throws ClassCastException
     * if one of the elements of the collection is not an {@link IonValue}
     * @throws NullPointerException
     * if one of the elements of the collection is {@code null}.
     * @throws ContainedValueException
     * if one of the elements is already contained by an {@link IonContainer}.
     * @throws IndexOutOfBoundsException
     * if the index is out of range (index < 0 || index > size()).
     */
    public boolean addAll(int index, Collection<? extends IonValue> c);


    /**
     * Returns a list iterator of the elements in this sequence (in proper
     * order).
     * <p>
     * The result does not support {@link ListIterator#add(Object)} or
     * {@link ListIterator#set(Object)}.
     * If this sequence {@link #isReadOnly()} then it also does not support
     * {@link Iterator#remove()}.
     */
    public ListIterator<IonValue> listIterator();

    /**
     * Returns a list iterator of the elements in this sequence (in proper
     * order), starting at the specified position in this sequence.
     * <p>
     * The result does not support {@link ListIterator#add(Object)} or
     * {@link ListIterator#set(Object)}.
     * If this sequence {@link #isReadOnly()} then it also does not support
     * {@link Iterator#remove()}.
     *
     * @param index
     * index of first element to be returned from the list iterator (by a call
     * to the {@code next} method).
     *
     * @throws IndexOutOfBoundsException
     * if the index is out of range ({@code index < 0 || index > size()}).
     */
    public ListIterator<IonValue> listIterator(int index);


    /**
     * This inherited method is not yet supported.
     * <p>
     * Vote for issue amznlabs/ion-java#52 if you need this.
     *
     * @throws UnsupportedOperationException at every call.
     *
     * @see <a href="https://github.com/amznlabs/ion-java/issues/52">amznlabs/ion-java#52</a>
     */
    public List<IonValue> subList(int fromIndex, int toIndex);


    /**
     * Returns an array containing all of the elements in this sequence in
     * proper order. Obeys the general contract of the
     * {@link Collection#toArray()} method.
     * <p>
     * If this sequence is an {@linkplain #isNullValue() Ion null value}, it
     * will behave like an empty sequence.
     *
     * @return an array containing all of the elements in this sequence in
     *         proper order.
     */
    public IonValue[] toArray();


    /**
     * Returns an array containing all of the elements in this sequence in
     * proper order; the runtime type of the returned array is that of the
     * specified array. Obeys the general contract of the
     * {@link Collection#toArray()} method.
     * <p>
     * If this sequence is an {@linkplain #isNullValue() Ion null value}, it
     * will behave like an empty sequence.
     *
     * @param a the array into which the elements of this sequence are to be
     *        stored, if it is big enough; otherwise, a new array of the same
     *        runtime type is allocated for this purpose.
     *
     * @return an array containing all of the elements in this sequence in
     *         proper order.
     *
     * @throws ArrayStoreException if the runtime type of the specified array
     *         is not a supertype of the runtime type of every element in this
     *         sequence.
     * @throws NullPointerException if the specified array is <code>null</code>.
     */
    public <T> T[] toArray(T[] a);


    /**
     * Removes all children of this sequence, returning them in an array.
     * This is much more efficient than iterating the sequence and removing
     * children one by one.
     *
     * @return a new array with all of the children of {@code s} in order, or
     * {@code null} if {@link #isNullValue()}.
     *
     * @throws NullPointerException if {@code type} is {@code null}.
     * @throws ClassCastException if any value in this sequence does not
     * implement the given type.
     */
    public <T extends IonValue> T[] extract(Class<T> type);


    public IonSequence clone()
        throws UnknownSymbolException;
}
