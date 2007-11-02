/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 * Base type for all Ion data types.
 */
public interface IonValue
{
    /**
     * Gets an enumeration value identifying the core Ion data type of this
     * object.
     * <p/>
     * <b>WARNING:</b> this method is not implemented by {@link IonDatagram}.
     *
     * @return a non-<code>null</code> enumeration value.
     */
    public IonType getType();


    /**
     * Determines whether this in an Ion null-value, <em>e.g.</em>,
     * <code>null</code> or <code>null.string</code>.
     * Note that there are unique null values for each Ion type.
     *
     * @return <code>true</code> if this value is one of the Ion null values.
     */
    public boolean isNullValue();


    /**
     * Gets the symbol table used to encode this value.
     *
     * @return the symbol table, or <code>null</code> if this value is not
     * currently backed by binary-encoded data.
     */
    public LocalSymbolTable getSymbolTable();


    /**
     * Gets the member name attached to this value,
     * or <code>null</code> if this is not part of an {@link IonStruct}.
     */
    public String getFieldName();


    /**
     * Gets the container of this value,
     * or <code>null</code> if this is not part of one.
     */
    public IonContainer getContainer();


    /**
     * Gets the user type annotations attached to this value
     * as strings, or <code>null</code> if there are none.
     * @deprecated Use {@link #getTypeAnnotations()} instead
     */
    public String[] getTypeAnnotationStrings();


    /**
     * Gets the user type annotations attached to this value
     * as strings, or <code>null</code> if there are none.
     */
    public String[] getTypeAnnotations();


    /**
     * Determines whether or not the value is annotated with
     * a particular user type annotation.
     * @param annotation as a string value.
     * @return <code>true</code> if this value has the annoation.
     */
    public boolean hasTypeAnnotation(String annotation);


    /**
     * Removes all the user type annotations attached to this value.
     *
     * @throws IonException if <code>annotations</code> is empty.
     */
    public void clearTypeAnnotations();


    /**
     * Adds a user type annotation to the annotations attached to
     * this value.  If the annotation exists the list does not change.
     * @param annotation as a string value.
     */
    public void addTypeAnnotation(String annotation);


    /**
     * Removes a user type annotation from the list of annotations
     * attached to this value.  If the annotation does not exist
     * the list does not change.
     * @param annotation as a string value.
     */
    public void removeTypeAnnotation(String annotation);


    /**
     * Entry point for visitor pattern.  Implementations of this method by
     * concrete classes will simply call the appropriate <code>visit</code>
     * method on the <code>visitor</code>.  For example, instances of
     * {@link IonBool} will invoke {@link ValueVisitor#visit(IonBool)}.
     *
     * @param visitor will have one of its <code>visit</code> methods called.
     * @throws Exception any exception thrown by the visitor is propagated.
     * @throws NullPointerException if <code>visitor</code> is
     * <code>null</code>.
     */
    public void accept(ValueVisitor visitor) throws Exception;


    /**
     * Ensures that this value, and all contained data, is fully materialized
     * into {@link IonValue} instances from any underlying Ion binary buffer.
     */
    public void deepMaterialize();


    /**
     * Returns a canonical text representation of this value.
     * For more configurable rendering, see {@link com.amazon.ion.util.Printer}.
     *
     * @return Ion text data equivalent to this value.
     */
    public String toString();
}
