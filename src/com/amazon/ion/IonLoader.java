/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;


/**
 * Loads Ion data in the form of datagrams.  These methods parse the input in
 * its entirety to identify problems immediately.  In contrast, an
 * {@link IonReader} will parse one top-level value at a time, and is better
 * suited for streaming protocols or large inputs.
 * <p>
 * Implementations of this interface must be safe for use by multiple threads.
 *
 * @see IonReader
 */
public interface IonLoader
{
    /**
     * Gets the {@link IonSystem} from which this loader was created.
     *
     * @return the system instance; not <code>null</code>.
     */
    public IonSystem getSystem();


    /**
     * Loads an entire file of Ion data into a single datagram,
     * detecting whether it's text or binary data.
     *
     * @param ionFile a file containing Ion data.
     *
     * @return a datagram containing all the values in the file.
     *
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified file results
     * in an <code>IOException</code>.
     */
    public IonDatagram load(File ionFile)
        throws IonException, IOException;


    /**
     * Loads an Ion text (UTF-8) file.  The file is parsed in its entirety.
     *
     * @param ionFile a file containing UTF-8 encoded Ion text.
     *
     * @return a datagram containing the ordered elements of the file.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if there's a problem reading the file.
     *
     * @deprecated Use {@link #load(File)}.
     */
    @Deprecated
    public IonDatagram loadTextFile(File ionFile)
        throws IonException, IOException;


    /**
     * Loads an entire file of (UTF-8) Ion text data into a single datagram.
     *
     * @param ionFile a file containing UTF-8 encoded Ion text.
     *
     * @return a datagram containing all the values in the file.
     *
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified file results
     * in an <code>IOException</code>.
     *
     * @deprecated Use {@link #load(File)}.
     */
    @Deprecated
    public IonDatagram loadText(File ionFile)
        throws IonException, IOException;


    /**
     * Loads an entire file of Ion binary data into a single datagram.
     *
     * @param ionFile a file containing Ion binary data.
     *
     * @return a datagram containing all the values in the file.
     *
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified file results
     * in an <code>IOException</code>.
     *
     * @deprecated Use {@link #load(File)}.
     */
    @Deprecated
    public IonDatagram loadBinary(File ionFile)
        throws IonException, IOException;


    /**
     * Loads Ion text in its entirety.
     *
     * @param ionText must not be null.
     * @return a datagram containing the input values.
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     */
    public IonDatagram load(String ionText)
        throws IonException;


    /**
     * Loads a string of Ion text into a single datagraam.
     *
     * @param ionText must not be null.
     *
     * @return a datagram containing all the values in the text.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     *
     * @deprecated rUse {@link #load(String)}
     */
    @Deprecated
    public IonDatagram loadText(String ionText)
        throws IonException;


    /**
     * Loads a stream of Ion text into a single datagram.
     * <p/>
     * The specified reader remains open after this method returns.
     *
     * @param ionText the reader from which to read Ion text.
     * @return a datagram containing all the elements on the input stream.
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     */
    public IonDatagram load(Reader ionText)
        throws IonException, IOException;


    /**
     * Loads an entire stream of Ion text data into a single datagram.
     * <p/>
     * The specified reader remains open after this method returns.
     *
     * @param ionText the reader from which to read Ion text.
     *
     * @return a datagram containing all the values on the input stream.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     *
     * @deprecated Use {@link #load(Reader)}
     */
    @Deprecated
    public IonDatagram loadText(Reader ionText)
        throws IonException, IOException;


    /**
     * Loads an entire stream of Ion text data into a single datagram,
     * starting with a given symbolTable for encoding symbols.
     * <p/>
     * The specified reader remains open after this method returns.
     *
     * @param ionText the reader from which to read Ion text.
     * @param symbolTable must be local, not shared or null.
     *
     * @return a datagram containing all the values on the input stream.
     *
     * @throws NullPointerException if either parameter is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     *
     * @deprecated Use {@link #load(Reader)}.
     */
    @Deprecated
    public IonDatagram load(Reader ionText, SymbolTable symbolTable)
        throws IonException, IOException;


    /**
     * Loads an entire stream of Ion text data into a single datagram,
     * starting with a given symbolTable for encoding symbols.
     * <p/>
     * The specified reader remains open after this method returns.
     *
     * @param ionText the reader from which to read Ion text.
     *
     * @return a datagram containing all the values on the input stream.
     *
     * @throws NullPointerException if either parameter is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     *
     * @deprecated Use {@link #load(Reader)}.
     */
    @Deprecated
    public IonDatagram load(Reader ionText, LocalSymbolTable symbolTable)
        throws IonException, IOException;


    /**
     * Loads an entire stream of Ion text data into a single datagram,
     * starting with a given symbolTable for encoding symbols.
     * <p/>
     * The specified reader remains open after this method returns.
     *
     * @param ionText the reader from which to read Ion text.
     *
     * @return a datagram containing all the values on the input stream.
     *
     * @throws NullPointerException if either parameter is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     *
     * @deprecated Use {@link #load(Reader)}.
     */
    @Deprecated
    public IonDatagram loadText(Reader ionText, LocalSymbolTable symbolTable)
        throws IonException, IOException;


    /**
     * Loads a block of Ion data into a single datagram,
     * detecting whether it's text or binary data.
     *
     * @param ionData may be either Ion binary data, or UTF-8 Ion text.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @return a datagram containing all the values on the input stream.
     *
     * @throws NullPointerException if <code>ionData</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     */
    public IonDatagram load(byte[] ionData)
        throws IonException;


    /**
     * Loads an entire stream of Ion data into a single datagram,
     * detecting whether it's text or binary data.
     * <p/>
     * The specified stream remains open after this method returns.
     *
     * @param ionData the stream from which to read Ion data.
     *
     * @return a datagram containing all the values on the input stream.
     *
     * @throws NullPointerException if <code>ionData</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     */
    public IonDatagram load(InputStream ionData)
        throws IonException, IOException;


    /**
     * Loads an entire stream of (UTF-8) Ion text data into a single datagram.
     * <p/>
     * The specified stream remains open after this method returns.
     *
     * @param ionText the stream from which to read UTF-8 encoded Ion text.
     *
     * @return a datagram containing all the values on the input stream.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     *
     * @deprecated Use {@link #load(InputStream)}.
     */
    @Deprecated
    public IonDatagram loadText(InputStream ionText)
        throws IOException;


    /**
     * Loads an entire stream of Ion binary data into a single datagram.
     * <p/>
     * The specified stream remains open after this method returns.
     *
     * @param ionBinary the stream from which to read Ion binary data.
     *
     * @return a datagram containing all the values on the input stream.
     *
     * @throws NullPointerException if <code>ionBinary</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     *
     * @deprecated Use {@link #load(InputStream)}.
     */
    @Deprecated
    public IonDatagram loadBinary(InputStream ionBinary)
        throws IOException;
}
