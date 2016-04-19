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

import java.util.Map;


/**
 * An Ion <code>struct</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * Note that this cannot extend {@link java.util.Map} because that interface
 * requires unique keys, while Ion's structs allow duplicate field names.
 */
public interface IonStruct
    extends IonContainer
{
    /**
     * Gets the number of fields in this struct.  If any field names are
     * repeated, they are counted individually.  For example, the size of
     * <code>{a:1,a:2}</code> is 2.
     *
     * @return the number of fields.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public int size()
        throws NullValueException;


    /**
     * Determines whether this struct contains one or more fields
     * for the specified field name (i.e., key). If this struct is an
     * {@linkplain #isNullValue() Ion null value}, it will behave like an empty
     * struct.
     *
     * @param fieldName field name whose presence in this struct is to be tested
     *
     * @return <code>true</code> if this struct contains a field for the
     *         specified field name
     *
     * @throws ClassCastException if the specified field name is not a
     *         {@link String}
     * @throws NullPointerException if the specified field name is
     *         <code>null</code>
     */
    public boolean containsKey(Object fieldName);


    /**
     * Determines whether this struct contains one or more fields with
     * the specified value. If this struct is an
     * {@linkplain #isNullValue() Ion null value}, it will behave like an empty
     * struct. This uses reference equality to compare the specified value with
     * the value of the struct fields.
     *
     * @param value value whose presence in this struct is to be tested
     *
     * @return <code>true</code> if this struct contains a field for the
     *         specified value
     *
     * @throws ClassCastException if the specified value is not an
     *         {@link IonValue}
     * @throws NullPointerException if the specified value is <code>null</code>
     */
    public boolean containsValue(Object value);


    /**
     * Gets the value of a field in this struct.  If the field name appears
     * more than once, one of the fields will be selected arbitrarily.  For
     * example, calling <code>get("a")</code> on the struct:
     *<pre>
     *    { a:1, b:2, a:3 }
     *</pre>
     * will return either 1 or 3.
     *
     * @param fieldName the desired field.
     * @return the value of the field, or <code>null</code> if it doesn't
     * exist in this struct, or if this is {@code null.struct}.
     * @throws IllegalArgumentException if <code>fieldName</code> is empty.
     * @throws NullPointerException if the <code>fieldName</code>
     * is <code>null</code>.
     */
    public IonValue get(String fieldName);


    /**
     * Puts a new field in this struct, replacing all existing fields
     * with the same name. If {@code child == null} then all existing fields
     * with the given name will be removed.
     * <p>
     * If this is <code>null.struct</code> and {@code child != null} then this
     * becomes a single-field struct.
     * <p>
     * The effect of this method is such that if
     * {@code put(fieldName, child)} succeeds, then
     * {@code get(fieldName) == child} will be true afterwards.
     *
     * @param fieldName the name of the new field.
     * @param child the value of the new field.
     *
     * @throws NullPointerException
     *   if {@code fieldName} is <code>null</code>.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws IllegalArgumentException
     *   if {@code fieldName} is empty or
     *   if {@code element} is an {@link IonDatagram}.
     */
    public void put(String fieldName, IonValue child)
        throws ContainedValueException;


    /**
     * Provides a factory that when invoked constructs a new value and
     * {@link #put(String,IonValue) put}s it into this struct using the given
     * {@code fieldName}.
     * <p>
     * These two lines are equivalent:
     *<pre>
     *    str.put("f").newInt(3);
     *    str.put("f", str.getSystem().newInt(3));
     *</pre>
     *
     * @throws NullPointerException
     *   if {@code fieldName} is <code>null</code>.
     * @throws IllegalArgumentException
     *   if {@code fieldName} is empty.
     *
     * @see #put(String, IonValue)
     */
    public ValueFactory put(String fieldName);


    /**
     * Copies all of the mappings from the specified map to this struct.
     * The effect of this call is equivalent to that of calling
     * {@link #put(String, IonValue) put(k, v)} on this struct once for each
     * mapping from key {@code k} to value {@code v} in the specified map.
     * The behavior of this operation is undefined if the specified map is
     * modified while the operation is in progress.
     * <p>
     * If a key in the map maps to {@code null}, then all fields with that name
     * will be removed from this struct.
     *
     * @throws NullPointerException if the given map is null.
     * @throws ContainedValueException if any values in the map are already
     * part of an {@link IonContainer} (even this one).
     */
    public void putAll(Map<? extends String, ? extends IonValue> m);


    /**
     * Adds a new field to this struct.
     * If this is <code>null.struct</code>, then it becomes a single-field
     * struct.
     * <p>
     * If a field with the given name already exists in this struct,
     * this call will result in repeated fields.
     *
     * @param fieldName the name of the new field.
     * @param child the value of the new field.
     *
     * @throws NullPointerException
     *   if {@code fieldName} or {@code child} is <code>null</code>.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws IllegalArgumentException
     *   if {@code fieldName} is empty or
     *   if {@code element} is an {@link IonDatagram}.
     */
    public void add(String fieldName, IonValue child)
        throws ContainedValueException;


    /**
     * Adds a new field to this struct using a given name and/or SID.
     * If this is <code>null.struct</code>, then it becomes a single-field
     * struct.
     * <p>
     * If a field with the given name already exists in this struct,
     * this call will result in repeated fields.
     * <p>
     * <b>This is an "expert method": correct use requires deep understanding
     * of the Ion binary format. You almost certainly don't want to use it.</b>
     *
     * @param fieldName the name of the new field.
     * @param child the value of the new field.
     *
     * @throws NullPointerException
     *   if {@code fieldName} or {@code child} is <code>null</code>.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws IllegalArgumentException
     *   if {@code fieldName} is empty or
     *   if {@code element} is an {@link IonDatagram}.
     *
     */
    public void add(SymbolToken fieldName, IonValue child)
        throws ContainedValueException;


    /**
     * Provides a factory that when invoked constructs a new value and
     * {@link #add(String,IonValue) add}s it to this struct using the given
     * {@code fieldName}.
     * <p>
     * These two lines are equivalent:
     *<pre>
     *    str.add("f").newInt(3);
     *    str.add("f", str.getSystem().newInt(3));
     *</pre>
     *
     * @throws NullPointerException
     *   if {@code fieldName} is <code>null</code>.
     * @throws IllegalArgumentException
     *   if {@code fieldName} is empty.
     *
     * @see #add(String, IonValue)
     */
    public ValueFactory add(String fieldName);


    /**
     * Removes a field by name, returning a value that was previously
     * associated with the field, or {@code null} if this struct contained no
     * such field.
     * <p>
     * Because Ion structs may have repeated fields, additional fields with the
     * given name may still exist after this method returns.
     * <p>
     * If this struct is an {@linkplain #isNullValue() Ion null value} or empty,
     * then this method returns null and has no effect.
     *
     * @param fieldName must not be null or empty.
     *
     * @return previous value associated with the specified field name, or
     * {@code null} if there was no such field.
     */
    public IonValue remove(String fieldName);


    /**
     * Removes from this struct all fields with names in the given list.
     * If multiple fields with a given name exist in this struct,
     * they will all be removed.
     * <p>
     * If this struct is an {@linkplain #isNullValue() Ion null value} or empty,
     * then this method returns {@code false} and has no effect.
     *
     * @param fieldNames the names of the fields to remove.
     *
     * @return true if this struct changed as a result of this call.
     *
     * @throws NullPointerException
     *   if {@code fieldNames}, or any element within it, is <code>null</code>.
     */
    public boolean removeAll(String... fieldNames);


    /**
     * Retains only the fields in this struct that have one of the given names.
     * In other words, removes all fields with names that are not in
     * {@code fieldNames}.
     * <p>
     * If this struct is an {@linkplain #isNullValue() Ion null value} or empty,
     * then this method returns {@code false} and has no effect.
     *
     * @param fieldNames the names of the fields to retain.
     *
     * @return true if this struct changed as a result of this call.
     *
     * @throws NullPointerException
     *   if {@code fieldNames}, or any element within it, is <code>null</code>.
     */
    public boolean retainAll(String... fieldNames);


    // TODO public Set<K> keySet();
    // TODO public Collection<V> values();
    // TODO public Set<Map.Entry<K,V>> entrySet();

    public IonStruct clone()
        throws UnknownSymbolException;


    /**
     * Clones this struct, excluding certain fields. This can be more
     * efficient than cloning the struct and removing fields later on.
     *
     * @param fieldNames the names of the fields to remove.
     *   A null field name causes removal of fields with unknown names.
     *
     * @throws UnknownSymbolException
     *   if any part of the cloned value would have unknown text but known SID
     *   for its field name, annotation or symbol.
     *
     * @see IonValue#clone()
     */
    public IonStruct cloneAndRemove(String... fieldNames)
        throws UnknownSymbolException;


    /**
     * Clones this struct, including only certain fields. This can be more
     * efficient than cloning the struct and removing fields later on.
     *
     * @param fieldNames the names of the fields to retain.
     *   Nulls are not allowed.
     *
     * @throws NullPointerException
     *   if {@code fieldNames}, or any element within it, is <code>null</code>.
     * @throws UnknownSymbolException
     *   if any part of the cloned value would have unknown text but known SID
     *   for its field name, annotation or symbol.
     *
     * @see IonValue#clone()
     */
    public IonStruct cloneAndRetain(String... fieldNames)
        throws UnknownSymbolException;
}
