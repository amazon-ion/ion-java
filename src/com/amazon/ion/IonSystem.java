/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.Reader;
import java.util.Date;

/**
 * Entry point to all things Ion.
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
     * Gets the default catalog used by this system.  Unless otherwise noted,
     * all objects derived from this system will use this catalog.
     */
    public IonCatalog getCatalog();


    /**
     * Sets the default catalog used by this system.
     *
     * @param catalog the new system catalog.
     * @throws NullPointerException if <code>catalog</code> is null.
     */
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
     * @param initialElement
     * @return a new datagram.
     * @throws NullPointerException if <code>initialElement<code> is null.
     */
    public IonDatagram newDatagram(IonValue initialElement);


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
     */
    public void setLoader(IonLoader loader);


//  public IonReader newReader(InputStream stream);
    public IonReader newReader(Reader reader);
    public IonReader newReader(String ionText);


    public IonValue singleValue(String ionText);
    public IonValue singleValue(byte[] ionBinary);


    //-------------------------------------------------------------------------
    // DOM creation

    /**
     * Constructs a new <code>null.blob</code> instance.
     */
    public IonBlob newBlob();

    /**
     * Constructs a new <code>null.bool</code> instance.
     */
    public IonBool newBool();

    public IonBool newBool(boolean value);


    /**
     * Constructs a new <code>null.clob</code> instance.
     */
    public IonClob newClob();

    /**
     * Constructs a new <code>null.decimal</code> instance.
     */
    public IonDecimal newDecimal();

    /**
     * Constructs a new <code>null.float</code> instance.
     */
    public IonFloat newFloat();


    /**
     * Constructs a new <code>null.int</code> instance.
     */
    public IonInt newInt();

    /**
     * Constructs a new <code>int</code> instance with the given content.
     *
     * @param content the new int's value.
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
     * @param content the new int's value;
     * may be <code>null</code> to make <code>null.int</code>.
     */
    public IonInt newInt(Number content);


    /**
     * Constructs a new <code>null.list</code> instance.
     */
    public IonList newList();

    /**
     * Constructs a new empty (not null) <code>list</code> instance.
     */
    public IonList newEmptyList();

    /**
     * Constructs a new <code>null.null</code> instance.
     */
    public IonNull newNull();

    /**
     * Constructs a new <code>null.sexp</code> instance.
     */
    public IonSexp newSexp();

    /**
     * Constructs a new empty (not null) <code>sexp</code> instance.
     */
    public IonSexp newEmptySexp();

    /**
     * Constructs a new <code>null.string</code> instance.
     */
    public IonString newString();

    /**
     * Constructs a new Ion string with the given content.
     *
     * @param content the text of the new string;
     * may be <code>null</code> to make <code>null.string</code>.
     */
    public IonString newString(String content);

    /**
     * Constructs a new <code>null.struct</code> instance.
     */
    public IonStruct newStruct();

    /**
     * Constructs a new empty (not null) <code>struct</code> instance.
     */
    public IonStruct newEmptyStruct();


    /**
     * Constructs a new <code>null.symbol</code> instance.
     */
    public IonSymbol newSymbol();

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
     */
    public IonTimestamp newTimestamp();


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
     * Creates a deep copy of an Ion value.
     *
     * @param value the value to copy.
     * @return a deep copy of value, with no container.
     * @throws NullPointerException if <code>value</code> is null.
     * @throws IonException if there's a problem creating the clone.
     */
    public <T extends IonValue> T clone(T value)
        throws IonException;
}
