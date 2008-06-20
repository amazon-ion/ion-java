/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;


/**
 * Base type for all Ion data nodes.
 * <p>
 * The {@code IonValue} hierarchy presents a "tree view" of Ion data;
 * every node in the tree is an instance of this class.  Since the Ion
 * type system is highly orthogonal, most operations use this
 * base type, and applications will need to examine individual instances and
 * "downcast" the value to one of the "real" types (<em>e.g.</em>,
 * {@link IonString}) in order to access the Ion content.
 * <p>
 * Besides the real types, there are other generic interfaces that can be
 * useful:
 * <ul>
 *   <li>
 *     {@link IonText} generalizes {@link IonString} and {@link IonSymbol}
 *   </li>
 *   <li>
 *     {@link IonContainer} generalizes
 *     {@link IonList}, {@link IonSexp}, and {@link IonStruct}
 *   </li>
 *   <li>
 *     {@link IonSequence} generalizes {@link IonList} and {@link IonSexp}
 *   </li>
 *   <li>
 *     {@link IonLob} generalizes {@link IonBlob} and {@link IonClob}
 *   </li>
 * </ul>
 * <p>
 * To determine the real type of a generic {@code IonValue}, there are three
 * main mechanisms:
 * <ul>
 *   <li>
 *     Use {@code instanceof} to look for a desired interface:
 *<pre>
 *    if (v instanceof IonString)
 *    {
 *        useStruct((IonString) v);
 *    }
 *    else if (v instanceof IonStruct)
 *    {
 *        useStruct((IonStruct) v);
 *    }
 *    // ...
 *</pre>
 *   </li>
 *   <li>
 *     Call {@link #getType()} and then {@code switch} over the resulting
 *     {@link IonType}:
 *<pre>
 *    switch (v.getType())
 *    {
 *        case IonType.STRING: useString((IonString) v); break;
 *        case IonType.STRUCT: useStruct((IonStruct) v); break;
 *        // ...
 *    }
 *</pre>
 *   </li>
 *   <li>
 *     Implement {@link ValueVisitor} and call {@link #accept(ValueVisitor)}:
 *<pre>
 *    public class MyVisitor
 *        extends AbstractValueVisitor
 *    {
 *        public void visit(IonString value)
 *        {
 *            useString(v);
 *        }
 *        public void visit(IonStruct value)
 *        {
 *            useStruct(v);
 *        }
 *        // ...
 *    }
 *</pre>
 *   </li>
 * </ul>
 * Use the most appropriate mechanism for your algorithm, depending upon how
 * much validation you've done on the data.
 * <p>
 * <b>Instances of {@code IonValue} are not thread-safe!</b>
 * Your application must perform its own synchronization if you need to access
 * nodes from multiple threads.
 */
public interface IonValue
{
    /**
     * Gets an enumeration value identifying the core Ion data type of this
     * object.
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
     * Gets the field name attached to this value,
     * or <code>null</code> if this is not part of an {@link IonStruct}.
     */
    public String getFieldName();


    /**
     * Gets the field name attached to this value,
     * or <code>null</code> if this is not part of an {@link IonStruct}.
     *
     * @deprecated Renamed to {@link #getFieldId()}.
     */
    @Deprecated
    public int getFieldNameId();


    /**
     * Gets the symbol ID of the field name attached to this value.
     *
     * @return the symbol ID of the field name, if this is part of an
     * {@link IonStruct}. If this is not a field, or if the symbol ID cannot be
     * determined, this method returns a value <em>less than one</em>.
     */
    public int getFieldId();


    /**
     * Gets the container of this value,
     * or <code>null</code> if this is not part of one.
     */
    public IonContainer getContainer();


    /**
     * Gets the user type annotations attached to this value
     * as strings, or <code>null</code> if there are none.
     * @deprecated Use {@link #getTypeAnnotations()} instead.
     */
    @Deprecated
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
     * @return <code>true</code> if this value has the annotation.
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
