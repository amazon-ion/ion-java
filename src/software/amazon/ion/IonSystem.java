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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Date;
import java.util.Iterator;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ion.system.IonTextWriterBuilder;

/**
 * Entry point to all things Ion.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * In general, {@link IonValue} instances returned from one system instance
 * are not interoperable with those returned by other instances.
 * The intended usage pattern is for an application to construct a single
 * <code>IonSystem</code> instance and use it throughout,
 * rather than constructing multiple systems and intermingling their use.
 * To create a copy of a value for use by a different system, use
 * {@link #clone(IonValue)}.
 * <p>
 * To create an {@code IonSystem},
 * see {@link software.amazon.ion.system.IonSystemBuilder}.
 * <p>
 * <b>Implementations of this interface are safe for use by multiple
 * threads.</b>
 *
 * @see IonSystemBuilder
 */
public interface IonSystem
    extends ValueFactory
{
    /**
     * Gets the default system symbol table.
     *
     * @return not null.
     */
    public SymbolTable getSystemSymbolTable();


    /**
     * Gets a system symbol table for a specific version of Ion.
     *
     * @param ionVersionId must be of the form <code>"$ion_X_Y"</code>.
     * @return the requested system table.
     *
     * @throws UnsupportedIonVersionException if the requested version of
     * Ion is not supported by this implementation.
     */
    public SymbolTable getSystemSymbolTable(String ionVersionId)
        throws UnsupportedIonVersionException;


    /**
     * Gets the catalog used by this system.  Unless otherwise noted,
     * all objects derived from this system will use this catalog.
     *
     * @return this system's default catalog; not null.
     */
    public IonCatalog getCatalog();


    /**
     * Creates a new local symbol table based on specific imported tables.
     * If the first imported table is a system table, then the local table will
     * use it appropriately. Otherwise, the local table will use this system's
     * {@linkplain #getSystemSymbolTable() default system symbol table}.
     *
     * @param imports the set of shared symbol tables to import.
     * The first (and only the first) may be a system table.
     *
     * @return a new local symbol table.
     *
     * @throws IllegalArgumentException if any import is a local table,
     * or if any but the first is a system table.
     * @throws NullPointerException if any import is null.
     */
    // TODO amznlabs/ion-java#38 Should we allow substituted imports as valid args?
    public SymbolTable newLocalSymbolTable(SymbolTable... imports);


    //-------------------------------------------------------------------------
    // Shared symtab factory methods


    /*              ===========================
     *              =                         =
     *              =    I M P O R T A N T    =
     *              =                         =
     *              ===========================
     *
     * Any new factory methods MUST be added to the SharedSymtabMaker
     * enumeration to ensure complete test coverage!
     */


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
     * <p>
     * This method is intended for use by utilities that are defining new
     * symbol tables for use by applications. The result will typically be
     * added to an {@link IonCatalog} which is responsible for persistence.
     * Shared symbol tables are serialized via
     * {@link SymbolTable#writeTo(IonWriter)} and materialized via
     * {@link #newSharedSymbolTable(IonReader)}.
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
     * Materializes a shared symbol table from its serialized form.
     * This method expects the reader to be positioned before the struct.
     * Which is to say the reader's next() method has not been called
     * to position the reader on the symbol table struct.
     *
     * @param reader must not be null.
     *
     * @return a new symbol table instance.
     */
    public SymbolTable newSharedSymbolTable(IonReader reader);

    /**
     * Materializes a shared symbol table from its serialized form.
     *
     * @param reader must not be null.
     * @param alreadyOnStruct is true if the caller has aleady next-ed onto the struct, false if a next call is needed
     *
     * @return a new symbol table instance.
     */
    public SymbolTable newSharedSymbolTable(IonReader reader, boolean alreadyOnStruct);


    //-------------------------------------------------------------------------

    /**
     * Constructs a new loader instance using the
     * {@linkplain #getCatalog() default system catalog}.
     */
    public IonLoader newLoader();

    /**
     * Constructs a new loader instance using the given catalog.
     *
     * @param catalog may be null, in which case the loader will use the
     * {@linkplain #getCatalog() default system catalog}.
     *
     * @see #newLoader()
     */
    public IonLoader newLoader(IonCatalog catalog);

    /**
     * Gets the default system loader.  Applications may replace this loader
     * with one configured appropriately, and then access it here.
     *
     * @return not <code>null</code>.
     */
    public IonLoader getLoader();


    /**
     * Creates an iterator over a stream of Ion text data.
     * Values returned by the iterator have no container.
     * <p>
     * The iterator will automatically consume Ion system IDs and local symbol
     * tables; they will not be returned by the iterator.
     * <p>
     * If the input source throws an {@link IOException} during iteration, it
     * will be wrapped in an {@link IonException}. See documentation there for
     * tips on how to recover the cause.
     * <p>
     * This method is suitable for use over unbounded streams with a reasonable
     * schema.
     * <p>
     * Applications should generally use {@link #iterate(InputStream)}
     * whenever possible, since this library has much faster UTF-8 decoding
     * than the Java IO framework.
     * <p>
     * Because this library performs its own buffering, it's recommended that
     * you avoid adding additional buffering to the given stream.
     *
     * @param ionText a stream of Ion text data.  The caller is responsible for
     * closing the Reader after iteration is complete.
     *
     * @return a new iterator instance.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if the source throws {@link IOException}.
     */
    public Iterator<IonValue> iterate(Reader ionText);


    /**
     * Creates an iterator over a stream of Ion data,
     * detecting whether it's text or binary data.
     * Values returned by the iterator have no container.
     * <p>
     * The iterator will automatically consume Ion system IDs and local symbol
     * tables; they will not be returned by the iterator.
     * <p>
     * If the input source throws an {@link IOException} during iteration, it
     * will be wrapped in an {@link IonException}. See documentation there for
     * tips on how to recover the cause.
     * <p>
     * This method is suitable for use over unbounded streams with a reasonable
     * schema.
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
     * <p>
     * Because this library performs its own buffering, it's recommended that
     * you avoid adding additional buffering to the given stream.
     *
     * @param ionData a stream of Ion data.  The caller is responsible for
     * closing the InputStream after iteration is complete.
     *
     * @return a new iterator instance.
     *
     * @throws NullPointerException if <code>ionData</code> is null.
     * @throws IonException if the source throws {@link IOException}.
     */
    public Iterator<IonValue> iterate(InputStream ionData);


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
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
     *
     * @param ionData may be either Ion binary data or (UTF-8) Ion text, or
     * GZIPped Ion data.
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
     * @return the first (and only) user value in the data; not null.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws UnexpectedEofException if the data doesn't contain any user
     * values.
     * @throws IonException if the data does not contain exactly one user
     * value.
     */
    public IonValue singleValue(String ionText);


    /**
     * Extracts a single value from Ion text or binary data.
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
     *
     * @param ionData may be either Ion binary data or (UTF-8) Ion text, or
     * GZIPped Ion data.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @return the first (and only) user value in the data; not null.
     *
     * @throws NullPointerException if {@code ionData} is null.
     * @throws UnexpectedEofException if the data doesn't contain any user
     * values.
     * @throws IonException if the data does not contain exactly one user
     * value.
     */
    public IonValue singleValue(byte[] ionData);


    //-------------------------------------------------------------------------
    // IonReader creation

    /*              ===========================
     *              =                         =
     *              =    I M P O R T A N T    =
     *              =                         =
     *              ===========================
     *
     * Any new factory methods MUST be added to the ReaderMaker
     * enumeration to ensure complete test coverage!
     */

    /**
     * Creates an new {@link IonReader} instance over Ion text data.
     * <p>
     * The text is parsed incrementally by the reader, so any syntax errors
     * will not be detected during this call.
     *
     * @param ionText must not be null.
     */
    public IonReader newReader(String ionText);

    /**
     * Creates an new {@link IonReader} instance over a block of Ion data,
     * detecting whether it's text or binary data.
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
     *
     * @param ionData may be either Ion binary data, or UTF-8 Ion text.
     * The reader retains a reference to the array, so its data must not be
     * modified while the reader is active.
     */
    public IonReader newReader(byte[] ionData);

    /**
     * Creates an new {@link IonReader} instance over a block of Ion data,
     * detecting whether it's text or binary data.
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
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
     * This method will auto-detect and uncompress GZIPped Ion data.
     * <p>
     * Because this library performs its own buffering, it's recommended that
     * you avoid adding additional buffering to the given stream.
     *
     * @param ionData must not be null.
     *
     * @return a new reader instance.
     * Callers must call {@link IonReader#close()} when finished with it.
     *
     * @throws IonException if the source throws {@link IOException}.
     */
    public IonReader newReader(InputStream ionData);

    /**
     * Creates an new {@link IonReader} instance over Ion text data.
     * <p>
     * Applications should generally us {@link #newReader(InputStream)}
     * whenever possible, since this library has much faster UTF-8 decoding
     * than the Java IO framework.
     * <p>
     * Because this library performs its own buffering, it's recommended that
     * you avoid adding additional buffering to the given stream.
     *
     * @throws IonException if the source throws {@link IOException}.
     */
    public IonReader newReader(Reader ionText);

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
     *
     * @see IonTextWriterBuilder
     */
    public IonWriter newTextWriter(OutputStream out);

    /**
     * Creates a new writer that will write text to the given output
     * stream.
     *
     * @param out the stream that will receive Ion text data.
     * Must not be null.
     *
     * @return a new {@link IonWriter} instance; not {@code null}.
     *
     * @see IonTextWriterBuilder
     */
    public IonWriter newTextWriter(Appendable out);

    /**
     * Creates a new writer that will write UTF-8 text to the given output
     * stream, using the given shared symbol tables as imports.
     * <p>
     * The output stream will start with an Ion Version Marker and a
     * local symbol table that uses the given {@code imports}.
     *
     * @param out the stream that will receive UTF-8 Ion text data.
     * Must not be null.
     * @param imports a sequence of shared symbol tables
     *
     * @return a new {@link IonWriter} instance; not {@code null}.
     *
     * @throws IOException if its thrown by the output stream.
     *
     * @see IonTextWriterBuilder
     */
    public IonWriter newTextWriter(OutputStream out, SymbolTable... imports)
        throws IOException;

    /**
     * Creates a new writer that will write text to the given output
     * stream, using the given shared symbol tables as imports.
     * <p>
     * The output stream will start with an Ion Version Marker and a
     * local symbol table that uses the given {@code imports}.
     *
     * @param out the stream that will receive Ion text data.
     * Must not be null.
     * @param imports a sequence of shared symbol tables.
     * The first (and only the first) may be a system table.
     *
     * @return a new {@link IonWriter} instance; not {@code null}.
     *
     * @throws IOException if its thrown by the output stream.
     *
     * @see IonTextWriterBuilder
     */
    public IonWriter newTextWriter(Appendable out, SymbolTable... imports)
        throws IOException;


    /**
     * Creates a new writer that will encode binary Ion data,
     * using the given shared symbol tables as imports.
     * <p>
     * The output stream will start with an Ion Version Marker and a
     * local symbol table that uses the given {@code imports}.
     *
     * @param out the stream to receive binary Ion data; not null.
     * @param imports a sequence of shared symbol tables to import.
     * The first (and only the first) may be a system table.
     *
     * @return a new {@link IonWriter} instance; not null.
     *
     * @throws IllegalArgumentException if any import is a local table,
     * or if any but the first is a system table.
     * @throws NullPointerException if any import is null.
     */
    public IonWriter newBinaryWriter(OutputStream out, SymbolTable... imports);


    //-------------------------------------------------------------------------
    // DOM creation


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
     * Creates a new datagram, bootstrapped with imported symbol tables.
     * Generally an application will use this to aquire a datagram, then adds
     * values to it, then calls {@link IonDatagram#getBytes(byte[])}
     * (or similar) to extract binary data.
     *
     * @param imports the set of shared symbol tables to import.
     * The first (and only the first) may be a system table.
     *
     * @return a new datagram with no user values.
     *
     * @throws IllegalArgumentException if any import is a local table,
     * or if any but the first is a system table.
     *
     * @see #newLocalSymbolTable(SymbolTable...)
     */
    @SuppressWarnings("javadoc")
    public IonDatagram newDatagram(SymbolTable... imports);


    /**
     * Extracts the current value from a reader into an {@link IonValue}.
     * The caller must position the reader on the correct value by calling
     * {@link IonReader#next()} beforehand.
     *
     * @return a new value object, not null.
     */
    public IonValue newValue(IonReader reader);


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
}
