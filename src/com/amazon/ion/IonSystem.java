// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.impl.IonBinaryWriterImpl;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * Entry point to all things Ion.
 * <p>
 * In general, instances returned from one system are not interchangable with
 * those returned by other systems.
 * The intended usage pattern is for an application to construct a single
 * <code>IonSystem</code> instance and use it throughout,
 * rather than constructing multiples and intermingling their use.
 * To create a copy of a value for use by a different system, use
 * {@link #clone(IonValue)}.
 * <p>
 * To create an {@code IonSystem},
 * see {@link com.amazon.ion.system.SystemFactory}.
 * <p>
 * Implementations of this interface must be safe for use by multiple threads.
 */
public interface IonSystem
{
    /**
     * Gets the default system symbol table.
     */
    public SymbolTable getSystemSymbolTable();


    /**
     * Gets a system symbol table of a specific version.
     *
     * @param systemId must be of the form <code>"$ion_X_Y"</code>.
     * @return the requested system table.
     *
     * @throws UnsupportedSystemVersionException if the requested system
     * version is not supported by this implementation.
     */
    public SymbolTable getSystemSymbolTable(String systemId)
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
    public SymbolTable newLocalSymbolTable();


    /**
     * Creates a new local symbol table based on a specific system table.
     *
     * @param systemSymbols must not be null and must have
     * {@link SymbolTable#isSystemTable()} true.
     *
     * @return a new symbol table.
     */
    public SymbolTable newLocalSymbolTable(SymbolTable systemSymbols);


    /**
     * Creates a new shared symbol table containing a given set of symbols.
     * The table will contain symbols in the following order:
     * <ol>
     *   <li>
     *     If {@code version} is larger than 1, the prior version of the
     *     named table is retrieved from the catalog and all of its symbols
     *     are added.
     *   </li>
     *   <li>
     *     For each non-system table in {@code imports}, add all of its
     *     declared symbols.
     *   </li>
     *   <li>
     *     Add all of the symbols provided by {@code newSymbols}.
     *   </li>
     * </ol>
     * Any duplicate symbol texts or null strings are ignored.
     *
     * @param name the symbol table name, a non-empty string.
     * @param version at least one.
     * @param newSymbols provides symbol names; may be null.
     * @param imports other tables from which to import symbols.
     *
     * @return a new shared symbol table with the given name and version.
     *
     * @throws IonException if {@code version > 1} and the prior version does
     * not exist in this system's catalog.
     */
    public SymbolTable newSharedSymbolTable(String name,
                                            int version,
                                            Iterator<String> newSymbols,
                                            SymbolTable... imports);


    /**
     * Creates a new empty datagram.
     *
     * @return a new datagram with no user values.
     */
    public IonDatagram newDatagram();


    /**
     * Creates a new datagram containing one value.  If the given value is
     * contained elsewhere, it is cloned before insertion.
     *
     * @param initialChild becomes the first and only (user) value in the
     * datagram.  The child's {@link IonValue#getSystem() system}
     * must be <em>this</em> system.
     * If {@code null}, then the returned datagram will have no
     * user values.
     *
     * @return a new datagram.
     *
     * @throws IllegalArgumentException
     *   if {@code initialChild} is an {@link IonDatagram}.
     */
    public IonDatagram newDatagram(IonValue initialChild);


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
     * Creates an iterator over a stream of Ion text data.
     * Values returned by the iterator have no container.
     * <p>
     * The iterator will automatically consume Ion system IDs and local symbol
     * tables; they will not be returned by the iterator.
     * <p>
     * This method is suitable for use over unbounded streams with a reasonable
     * schema.
     *
     * @param ionText a stream of Ion text data.  The caller is responsible for
     * closing the Reader after iteration is complete.
     *
     * @return a new iterator instance.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     */
    public Iterator<IonValue> iterate(Reader ionText);


    /**
     * Creates an iterator over a string containing Ion text data.
     * Values returned by the iterator have no container.
     * <p>
     * The iterator will automatically consume Ion system IDs and local symbol
     * tables; they will not be returned by the iterator.
     *
     * @param ionText must not be null.
     *
     * @return a new iterator instance.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     */
    public Iterator<IonValue> iterate(String ionText);


    /**
     * Creates an iterator over Ion data.
     * Values returned by the iterator have no container.
     * <p>
     * The iterator will automatically consume Ion system IDs and local symbol
     * tables; they will not be returned by the iterator.
     *
     * @param ionData may be either Ion binary data or (UTF-8) Ion text.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @return a new iterator instance.
     *
     * @throws NullPointerException if <code>ionData</code> is null.
     */
    public Iterator<IonValue> iterate(byte[] ionData);


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
    // IonReader creation

//  public IonReader newReader(Reader ionText); // TODO add newReader(Reader)


    /**
     * Creates an new {@link IonReader} instance over Ion text data.
     * <p>
     * The text is parsed incrementally by the reader, so any syntax errors
     * will not be detected here.
     *
     * @param ionText must not be null.
     */
    public IonReader newReader(String ionText);

    /**
     * Creates an new {@link IonReader} instance over a block of Ion data,
     * detecting whether it's text or binary data.
     *
     * @param ionData may be either Ion binary data, or UTF-8 Ion text.
     * The reader retains a reference to the array, so its data must not be
     * modified while the reader is active.
     */
    public IonReader newReader(byte[] ionData);

    /**
     * Creates an new {@link IonReader} instance over a block of Ion data,
     * detecting whether it's text or binary data.
     *
     * @param ionData is used only within the range of bytes starting at
     * {@code offset} for {@code len} bytes.
     * The data in that range may be either Ion binary data, or UTF-8 Ion text.
     * The reader retains a reference to the array, so its data must not be
     * modified while the reader is active.
     * @param offset must be non-negative and less than {@code ionData.length}.
     * @param len must be non-negative and {@code offset+len} must not exceed
     * {@code ionData.length}.
     */
    public IonReader newReader(byte[] ionData, int offset, int len);

    /**
     * Creates a new {@link IonReader} instance over a stream of Ion data,
     * detecting whether it's text or binary data.
     * <p>
     * <b>NOTE:</b> The current implementation of this method reads the entire
     * contents of the input stream into memory.
     *
     * @param ionData must not be null.
     *
     * @return a new reader instance.
     */
    public IonReader newReader(InputStream ionData);

    /**
     * Creates an new {@link IonReader} instance over an {@link IonValue} data
     * model. Typically this is used to iterate over a collection, such as an
     * {@link IonStruct}.
     *
     * @param value must not be null.
     */
    public IonReader newReader(IonValue value);


    //-------------------------------------------------------------------------
    // IonWriter creation

    /**
     * Creates a new writer that will add {@link IonValue}s to the given
     * container.
     *
     * @param container a container that will receive new children from the
     * the returned writer.
     * Must not be null.
     *
     * @return a new {@link IonWriter} instance; not {@code null}.
     */
    public IonWriter newWriter(IonContainer container);

    /**
     * Creates a new writer that will write UTF-8 text to the given output
     * stream.
     *
     * @param out the stream that will receive UTF-8 Ion text data.
     * Must not be null.
     *
     * @return a new {@link IonWriter} instance; not {@code null}.
     */
    public IonWriter newTextWriter(OutputStream out);

    /**
     * Creates a new writer that will write UTF-8 text to the given output
     * stream, using the given shared symbol tables as imports.
     * <p>
     * The output stream will be start with an Ion Version Marker and a
     * local symbol table that uses the given {@code imports}.
     *
     * @param out the stream that will receive UTF-8 Ion text data.
     * Must not be null.
     * @param imports a sequence of shared symbol tables
     *
     * @return a new {@link IonWriter} instance; not {@code null}.
     *
     * @throws IOException if its thrown by the output stream.
     */
    public IonWriter newTextWriter(OutputStream out, SymbolTable... imports)
        throws IOException;

    /**
     * Creates a new writer that will encode binary Ion data.
     *
     * @return a new {@link IonBinaryWriter} instance; not {@code null}.
     */
    public IonBinaryWriter newBinaryWriter();

    /**
     * Creates a new writer that will encode binary Ion data,
     * using the given shared symbol tables as imports.
     * <p>
     * The output stream will be start with an Ion Version Marker and a
     * local symbol table that uses the given {@code imports}.
     *
     * @param imports a sequence of shared symbol tables
     *
     * @return a new {@link IonBinaryWriter} instance; not {@code null}.
     */
    public IonBinaryWriterImpl newBinaryWriter(SymbolTable... imports);


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
     * Constructs a new Ion {@code blob} instance, copying bytes from an array.
     *
     * @param value the data for the new blob, to be <em>copied</em> from the
     * given array into the new instance.
     * May be {@code null} to create a {@code null.blob} value.
     */
    public IonBlob newBlob(byte[] value);

    /**
     * Constructs a new Ion {@code blob}, copying bytes from part of an array.
     * <p>
     * This method copies {@code length} bytes from the given array into the
     * new value, starting at the given offset in the array.
     *
     * @param value the data for the new blob, to be <em>copied</em> from the
     * given array into the new instance.
     * May be {@code null} to create a {@code null.blob} value.
     * @param offset the offset within the array of the first byte to copy;
     * must be non-negative an no larger than {@code bytes.length}.
     * @param length the number of bytes to be copied from the given array;
     * must be non-negative an no larger than {@code bytes.length - offset}.
     *
     * @throws IndexOutOfBoundsException
     * if the preconditions on the {@code offset} and {@code length} parameters
     * are not met.
     */
    public IonBlob newBlob(byte[] value, int offset, int length);


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
     * @param value the initial content of the value.
     *
     * @return a bool with
     * <code>{@link IonBool#booleanValue()} == content</code>.
     */
    public IonBool newBool(boolean value);

    /**
     * Constructs a new <code>bool</code> instance with the given content.
     *
     * @param value the initial value of the instance;
     * may be {@code null} to make {@code null.bool}.
     */
    public IonBool newBool(Boolean value);


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
     * Constructs a new Ion {@code clob} instance from a byte array.
     *
     * @param value the data for the new clob, to be <em>copied</em> from the
     * given array into the new instance.
     * May be {@code null} to create a {@code null.clob} value.
     */
    public IonClob newClob(byte[] value);

    /**
     * Constructs a new Ion {@code clob}, copying bytes from part of an array.
     * <p>
     * This method copies {@code length} bytes from the given array into the
     * new value, starting at the given offset in the array.
     *
     * @param value the data for the new blob, to be <em>copied</em> from the
     * given array into the new instance.
     * May be {@code null} to create a {@code null.clob} value.
     * @param offset the offset within the array of the first byte to copy;
     * must be non-negative an no larger than {@code bytes.length}.
     * @param length the number of bytes to be copied from the given array;
     * must be non-negative an no larger than {@code bytes.length - offset}.
     *
     * @throws IndexOutOfBoundsException
     * if the preconditions on the {@code offset} and {@code length} parameters
     * are not met.
     */
    public IonClob newClob(byte[] value, int offset, int length);


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
     * Constructs a new Ion {@code decimal} instance from a Java
     * {@code long}.
     */
    public IonDecimal newDecimal(long value);

    /**
     * Constructs a new Ion {@code decimal} instance from a Java
     * {@code double}.
     */
    public IonDecimal newDecimal(double value);

    /**
     * Constructs a new Ion {@code decimal} instance from a Java
     * {@link BigInteger}.
     */
    public IonDecimal newDecimal(BigInteger value);

    /**
     * Constructs a new Ion {@code decimal} instance from a Java
     * {@link BigDecimal}.
     */
    public IonDecimal newDecimal(BigDecimal value);


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
     * Constructs a new Ion {@code float} instance from a Java
     * {@code long}.
     */
    public IonFloat newFloat(long value);

    /**
     * Constructs a new Ion {@code float} instance from a Java
     * {@code double}.
     */
    public IonFloat newFloat(double value);


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
     * Constructs a new Ion null value with the given type.
     *
     * @param type must not be Java null, but it may be {@link IonType#NULL}.
     *
     * @return a new value such that {@link IonValue#isNullValue()} is
     * {@code true}.
     */
    public IonValue newNull(IonType type);


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
     * <p>
     * The given value can be in the context of any {@code IonSystem} instance,
     * and the result will be in the context of this system. This allows you to
     * shift data from one system instance to another.
     *
     * @param value the value to copy.
     * @return a deep copy of value, with no container.
     * @throws NullPointerException if <code>value</code> is null.
     * @throws IonException if there's a problem creating the clone.
     *
     * @see IonValue#clone()
     */
    public <T extends IonValue> T clone(T value)
        throws IonException;
}
