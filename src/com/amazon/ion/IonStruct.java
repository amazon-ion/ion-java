/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;


/**
 * An Ion <code>struct</code> value.
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
     * Gets the value of a field in this struct.  If the field name appears
     * more than once, one of the fields will be selected arbitrarily.  For
     * example, calling <code>get("a")</code> on the struct:
     * <pre>
     *   { a:1, b:2, a:3 }
     * </pre>
     * will return either 1 or 3.
     *
     * @param fieldName the desired field.
     * @return the value of the field, or <code>null</code> if it doesn't
     * exist in this struct.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     * @throws IllegalArgumentException if <code>fieldName</code> is empty.
     * @throws NullPointerException if the <code>fieldName</code>
     * is <code>null</code>.
     */
    public IonValue get(String fieldName)
        throws NullValueException;


    /**
     * Puts a new field in this struct, replacing all existing fields
     * with the same name. If {@code child == null} then all existing fields
     * with the given name will be removed.
     * <p>
     * If this is <code>null.struct</code> and {@code child != null} then this
     * becomes a single-field struct.
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
     * Removes from this struct all fields with names in the given list.
     *
     * If multiple fields with a given name exist in this struct,
     * they will all be removed.
     *
     * @param fieldNames the names of the fields to remove.
     *
     * @return true if this struct changed as a result of this call.
     *
     * @throws NullPointerException
     *   if {@code fieldName}, or any element within it, is <code>null</code>.
     */
    public boolean removeAll(String... fieldNames);


    /**
     * Retains only the fields in this struct that have one of the given names.
     * In other words, removes all fields with names that are not in
     * {@code fieldNames}.
     *
     * @param fieldNames the names of the fields to retain.
     *
     * @return true if this struct changed as a result of this call.
     *
     * @throws NullPointerException
     *   if {@code fieldName}, or any element within it, is <code>null</code>.
     */
    public boolean retainAll(String... fieldNames);


    public IonStruct clone();


    /**
     * Creates a deep copy of an element and puts it into this struct,
     * replacing existing fields with the same name.
     * If this is <code>null.struct</code>, then it becomes a single-field
     * struct.
     * <p>
     * Note that multiple field with the given name may already exist in this
     * struct; they will all be removed and replaced by the new one.
     *
     * @param fieldName the name of the new field.
     * @param value the value of the new field, will be deep-copied.
     *
     * @throws IllegalArgumentException if <code>fieldName</code> is empty.
     * @throws NullPointerException if the <code>fieldName</code> or the value
     * is <code>null</code>.
     */
//    public void putEmbedded(String fieldName, IonValue value);
}
