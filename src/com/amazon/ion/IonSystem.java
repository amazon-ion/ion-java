/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.Reader;
import java.util.Collection;
import java.util.Date;

/**
 * Entry point to all things Ion.
 * <p>
 * In general, instances returned from one system are not interchangable with
 * those returned by other systems.
 * The intended usage pattern is for an application to construct a single
 * <code>IonSystem</code> instance and use it throughout,
 * rather than constructing multiples and intermingling their use.
 * <p>
 * Implementations of this interface must be safe for use by multiple threads.
 */
public interface IonSystem
{
    /**
     * Gets the default system symbol table.
     */
    public SystemSymbolTable getSystemSymbolTable();


    /**
     * Gets a system symbol table of a specific version.
     *
     * @param systemId must be of the form <code>"$ion_X_Y"</code>.
     * @return the requested system table.
     *
     * @throws UnsupportedSystemVersionException if the requested system
     * version is not supported by this implementation.
     */
    public SystemSymbolTable getSystemSymbolTable(String systemId)
        throws UnsupportedSystemVersionException;


    /**
     * Gets the catalog used by this system.  Unless otherwise noted,
     * all objects derived from this system will use this catalog.
     */
    public IonCatalog getCatalog();


    /**
     * Sets the default catalog used by this system.
     *
     * @param catalog the new system catalog.
     * @throws NullPointerException if <code>catalog</code> is null.
     *
     * @deprecated  Catalog should be immutable.
     */
    @Deprecated
    public void setCatalog(IonCatalog catalog);


    /**
     * Creates a new local symbol table using the default system symbol table.
     * @return not <code>null</code>.
     */
    public LocalSymbolTable newLocalSymbolTable();


    /**
     * Creates a new local symbol table based on a specific system table.
     *
     * @param systemSymbols must not be null.
     * @return a new symbol table.
     */
    public LocalSymbolTable newLocalSymbolTable(SystemSymbolTable systemSymbols);



    /**
     * Creates a new static symbol table from its Ion representation.
     *
     * @param symbolTable must not be an Ion structure defining the static
     * symbol table.
     * @return a new symbol table.
     */
    public StaticSymbolTable newStaticSymbolTable(IonStruct symbolTable);


    /**
     * Creates a new datagram containing one value.  If the given value is
     * contained elsewhere, it is cloned before insertion.
     *
     * @param initialChild becomes the first and only (user) value in the
     * datagram.
     *
     * @return a new datagram.
     *
     * @throws NullPointerException
     *   if {@code initialChild} is null.
     * @throws IllegalArgumentException
     *   if {@code initialChild} is an {@link IonDatagram}.
     */
    public IonDatagram newDatagram(IonValue initialChild)
        throws ContainedValueException;


    /**
     * Constructs a new loader instance.
     */
    public IonLoader newLoader();


    /**
     * Gets the default system loader.  Applications may replace this loader
     * with one configured appropriately, and then access it here.
     *
     * @return not <code>null</code>.
     *
     * @see #setLoader(IonLoader)
     */
    public IonLoader getLoader();


    /**
     * Sets the default system loader.
     *
     * @param loader The new system loader.
     * @throws NullPointerException if loader is null.
     * @throws IllegalArgumentException if <code>loader.getSystem()</code> is
     * not this system.
     *
     * @deprecated Default loader should be immutable.
     */
    @Deprecated
    public void setLoader(IonLoader loader);


    /**
     * Creates a reader for iterating over a stream of Ion text data.
     *
     * @return a new reader instance.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     */
    public IonReader newReader(Reader ionText);


    /**
     * Creates a reader for iterating over a string containing Ion text data.
     *
     * @param ionText must not be null.
     *
     * @return a new reader instance.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     */
    public IonReader newReader(String ionText);


    /**
     * Creates a reader for iterating over a string containing Ion text data.
     *
     * @param ionText must not be null.
     *
     * @return a new reader instance.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     *
     * @deprecated Use {@link #newReader(String)}.
     */
    @Deprecated
    public IonReader newTextReader(String ionText);


    /**
     * Creates a reader for iterating over a stream of Ion text data.
     *
     * @return a new reader instance.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     *
     * @deprecated Use {@link #newReader(Reader)}.
     */
    @Deprecated
    public IonReader newTextReader(Reader ionText);


    /**
     * Creates a reader for iterating over Ion data.
     *
     * @param ionData may be either Ion binary data or (UTF-8) Ion text.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @return a new reader instance.
     *
     * @throws NullPointerException if <code>ionData</code> is null.
     */
    public IonReader newReader(byte[] ionData);



    /**
     * Extracts a single value from Ion text data.
     *
     * @param ionText must not be null.
     *
     * @return the first (and only) user value in the data.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if the data does not contain exactly one user
     * value.
     */
    public IonValue singleValue(String ionText);


    /**
     * Extracts a single value from Ion text or binary data.
     *
     * @param ionData may be either Ion binary data or (UTF-8) Ion text.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @return the first (and only) user value in the data.
     *
     * @throws IonException if the data does not contain exactly one user
     * value.
     */
    public IonValue singleValue(byte[] ionData);


    //-------------------------------------------------------------------------
    // DOM creation

    /**
     * Constructs a new <code>null.blob</code> instance.
     * @deprecated Use {@link #newNullBlob()} instead.
     */
    @Deprecated
    public IonBlob newBlob();


    /**
     * Constructs a new <code>null.blob</code> instance.
     */
    public IonBlob newNullBlob();


    /**
     * Constructs a new <code>null.bool</code> instance.
     * @deprecated Use {@link #newNullBool()} instead
     */
    @Deprecated
    public IonBool newBool();


    /**
     * Constructs a new <code>null.bool</code> instance.
     */
    public IonBool newNullBool();


    /**
     * Constructs a new <code>bool</code> instance with the given content.
     *
     * @param content the initial content of the value.
     *
     * @return a bool with
     * <code>{@link IonBool#booleanValue()} == content</code>.
     */
    public IonBool newBool(boolean content);


    /**
     * Constructs a new <code>null.clob</code> instance.
     * @deprecated Use {@link #newNullClob()} instead
     */
    @Deprecated
    public IonClob newClob();


    /**
     * Constructs a new <code>null.clob</code> instance.
     */
    public IonClob newNullClob();


    /**
     * Constructs a new <code>null.decimal</code> instance.
     * @deprecated Use {@link #newNullDecimal()} instead
     */
    @Deprecated
    public IonDecimal newDecimal();


    /**
     * Constructs a new <code>null.decimal</code> instance.
     */
    public IonDecimal newNullDecimal();


    /**
     * Constructs a new <code>null.float</code> instance.
     * @deprecated Use {@link #newNullFloat()} instead
     */
    @Deprecated
    public IonFloat newFloat();


    /**
     * Constructs a new <code>null.float</code> instance.
     */
    public IonFloat newNullFloat();


    /**
     * Constructs a new <code>null.int</code> instance.
     * @deprecated Use {@link #newNullInt()} instead
     */
    @Deprecated
    public IonInt newInt();


    /**
     * Constructs a new <code>null.int</code> instance.
     */
    public IonInt newNullInt();


    /**
     * Constructs a new <code>int</code> instance with the given content.
     *
     * @param content the new int's content.
     */
    public IonInt newInt(int content);


    /**
     * Constructs a new <code>int</code> instance with the given content.
     *
     * @param content the new int's value.
     */
    public IonInt newInt(long content);


    /**
     * Constructs a new <code>int</code> instance with the given content.
     * The integer portion of the number is used, any fractional portion is
     * ignored.
     *
     * @param content the new int's content;
     * may be <code>null</code> to make <code>null.int</code>.
     */
    public IonInt newInt(Number content);


    /**
     * Constructs a new <code>null.list</code> instance.
     * @deprecated Use {@link #newNullList()} instead
     */
    @Deprecated
    public IonList newList();


    /**
     * Constructs a new <code>null.list</code> instance.
     */
    public IonList newNullList();


    /**
     * Constructs a new empty (not null) <code>list</code> instance.
     */
    public IonList newEmptyList();

    /**
     * Constructs a new <code>list</code> with given child elements.
     *
     * @param elements
     *  the initial set of child elements.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *
     * @throws ContainedValueException
     *  if any value in {@code elements}
     *  has <code>{@link IonValue#getContainer()} != null</code>.
     * @throws NullPointerException
     *   if any value in {@code elements} is null.
     * @throws IllegalArgumentException
     *   if any value in {@code elements} is an {@link IonDatagram}.
     */
    public IonList newList(Collection<? extends IonValue> elements)
        throws ContainedValueException, NullPointerException;


    /**
     * Constructs a new <code>list</code> with given child elements.
     *
     * @param elements
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *  If an element is Java <code>null</code>, its corresponding element in
     *  the result will be an {@link IonNull} value.
     *
     * @throws ContainedValueException
     *  if any value in {@code elements}
     *  has <code>{@link IonValue#getContainer()} != null</code>.
     * @throws NullPointerException
     *   if any value in {@code elements} is null.
     * @throws IllegalArgumentException
     *   if any value in {@code elements} is an {@link IonDatagram}.
     */
    public <T extends IonValue> IonList newList(T... elements)
        throws ContainedValueException, NullPointerException;


    /**
     * Constructs a new <code>list</code> with given <code>int</code> child
     * elements.
     *
     * @param elements
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *  Otherwise, the resulting sequence will contain new {@link IonInt}s with
     *  the given values.
     *
     * @return a new list where each element is an {@link IonInt}.
     */
    public IonList newList(int[] elements);


    /**
     * Constructs a new <code>list</code> with given <code>long</code> child
     * elements.
     *
     * @param elements
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *  Otherwise, the resulting sequence will contain new {@link IonInt}s with
     *  the given values.
     *
     * @return a new list where each element is an {@link IonInt}.
     */
    public IonList newList(long[] elements);


    /**
     * Constructs a new <code>null.null</code> instance.
     */
    public IonNull newNull();


    /**
     * Constructs a new <code>null.sexp</code> instance.
     * @deprecated Use {@link #newNullSexp()} instead
     */
    @Deprecated
    public IonSexp newSexp();


    /**
     * Constructs a new <code>null.sexp</code> instance.
     */
    public IonSexp newNullSexp();


    /**
     * Constructs a new empty (not null) <code>sexp</code> instance.
     */
    public IonSexp newEmptySexp();


    /**
     * Constructs a new <code>sexp</code> with given child elements.
     *
     * @param elements
     *  the initial set of child elements.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *
     * @throws ContainedValueException
     *  if any value in {@code elements}
     *  has <code>{@link IonValue#getContainer()} != null</code>.
     * @throws NullPointerException
     *   if any value in {@code elements} is null.
     * @throws IllegalArgumentException
     *   if any value in {@code elements} is an {@link IonDatagram}.
     */
    public IonSexp newSexp(Collection<? extends IonValue> elements)
        throws ContainedValueException, NullPointerException;


    /**
     * Constructs a new <code>sexp</code> with given child elements.
     *
     * @param elements
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *
     * @throws ContainedValueException
     *  if any value in {@code elements}
     *  has <code>{@link IonValue#getContainer()} != null</code>.
     * @throws NullPointerException
     *   if any value in {@code elements} is null.
     * @throws IllegalArgumentException
     *   if any value in {@code elements} is an {@link IonDatagram}.
     */
    public <T extends IonValue> IonSexp newSexp(T... elements)
        throws ContainedValueException, NullPointerException;


    /**
     * Constructs a new <code>sexp</code> with given <code>int</code> child
     * elements.
     *
     * @param elements
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *  Otherwise, the resulting sequence will contain new {@link IonInt}s with
     *  the given values.
     *
     * @return a new sexp where each element is an {@link IonInt}.
     */
    public IonSexp newSexp(int[] elements);


    /**
     * Constructs a new <code>sexp</code> with given <code>long</code> child
     * elements.
     *
     * @param elements
     *  the initial set of child values.  If <code>null</code>, then the new
     *  instance will have <code>{@link IonValue#isNullValue()} == true</code>.
     *  Otherwise, the resulting sequence will contain new {@link IonInt}s with
     *  the given values.
     *
     * @return a new sexp where each element is an {@link IonInt}.
     */
    public IonSexp newSexp(long[] elements);


    /**
     * Constructs a new <code>null.string</code> instance.
     * @deprecated Use {@link #newNullString()} instead
     */
    @Deprecated
    public IonString newString();


    /**
     * Constructs a new <code>null.string</code> instance.
     */
    public IonString newNullString();


    /**
     * Constructs a new Ion string with the given content.
     *
     * @param content the text of the new string;
     * may be <code>null</code> to make <code>null.string</code>.
     */
    public IonString newString(String content);


    /**
     * Constructs a new <code>null.struct</code> instance.
     * @deprecated Use {@link #newNullStruct()} instead
     */
    @Deprecated
    public IonStruct newStruct();


    /**
     * Constructs a new <code>null.struct</code> instance.
     */
    public IonStruct newNullStruct();


    /**
     * Constructs a new empty (not null) <code>struct</code> instance.
     */
    public IonStruct newEmptyStruct();


    /**
     * Constructs a new <code>null.symbol</code> instance.
     * @deprecated Use {@link #newNullSymbol()} instead
     */
    @Deprecated
    public IonSymbol newSymbol();


    /**
     * Constructs a new <code>null.symbol</code> instance.
     */
    public IonSymbol newNullSymbol();


    /**
     * Constructs a new Ion symbol with the given content.
     *
     * @param name the content of the symbol;
     * may be <code>null</code> to make <code>null.symbol</code>.
     *
     * @throws EmptySymbolException if <code>name</code> is the empty string.
     */
    public IonSymbol newSymbol(String name);


    /**
     * Constructs a new <code>null.timestamp</code> instance.
     * @deprecated Use {@link #newNullTimestamp()} instead
     */
    @Deprecated
    public IonTimestamp newTimestamp();


    /**
     * Constructs a new <code>null.timestamp</code> instance.
     */
    public IonTimestamp newNullTimestamp();


    /**
     * Constructs a new UTC <code>timestamp</code> initialized to represent
     * the specified number of milliseconds since the standard base time known
     * as "the epoch", namely 1970-01-01T00:00:00Z.
     *
     * @param millis the milliseconds since 1970-01-01T00:00:00Z.
     */
    public IonTimestamp newUtcTimestampFromMillis(long millis);


    /**
     * Constructs a new UTC <code>timestamp</code> instance initialized so that
     * it represents the given time.  As per {@link Date} class, this will have
     * millisecond precision.
     * <p>
     * This is equivalent to
     * <code>{@linkplain #newUtcTimestampFromMillis newUtcTimestampFromMillis}(utcDate.getTime())</code>.
     *
     * @param utcDate the time of the new instance;
     * may be <code>null</code> to make <code>null.timestamp</code>.
     */
    public IonTimestamp newUtcTimestamp(Date utcDate);


    /**
     * Constructs a new UTC <code>timestamp</code> instance initialized so that
     * it represents the time at which it was allocated, measured to the nearest
     * millisecond.
     */
    public IonTimestamp newCurrentUtcTimestamp();


    /**
     * Creates a deep copy of an Ion value.  This method can properly clone
     * {@link IonDatagram}s.
     *
     * @param value the value to copy.
     * @return a deep copy of value, with no container.
     * @throws NullPointerException if <code>value</code> is null.
     * @throws IonException if there's a problem creating the clone.
     */
    public <T extends IonValue> T clone(T value)
        throws IonException;
}
