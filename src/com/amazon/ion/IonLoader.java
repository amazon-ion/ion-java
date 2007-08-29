/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import com.amazon.ion.impl.IonDatagramImpl;
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
     * Loads an Ion text (UTF-8) file.  The file is parsed in its entirety.
     *
     * @param ionFile a file containing UTF-8 encoded Ion text.
     *
     * @return a datagram containing the ordered elements of the file.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if there's a problem reading the file.
     */
    public IonDatagram loadTextFile(File ionFile)
        throws IonException, IOException;


    /**
     * Loads an Ion binary file.  The file is parsed in its entirety.
     *
     * @param ionFile a file containing Ion binary data.
     *
     * @return a datagram containing the ordered elements of the file.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if there's a problem reading the file.
     */
    public IonDatagramImpl loadBinaryFile(File ionFile)
        throws IonException, IOException;


    /**
     * Loads an Ion file, detecting whether it's text or binary data.
     * The file is parsed in its entirety.
     *
     * @param ionFile a file containing Ion data.
     *
     * @return a datagram containing the ordered elements of the file.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if there's a problem reading the file.
     */
    public IonDatagramImpl loadFile(File ionFile)
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
     * Loads Ion text in its entirety.
     *
     * @param ionText will not be closed by this method.
     * @return a datagram containing all the elements on the input stream.
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     */
    public IonDatagram load(Reader ionText)
        throws IonException;


    /**
     * Loads Ion text in its entirety, starting with a given symbolTable for
     * encoding symbols.
     *
     * @param ionText will not be closed by this method.
     * @return a datagram containing all the elements on the input stream.
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     */
    public IonDatagram load(Reader ionText, LocalSymbolTable symbolTable)
        throws IonException;


    /**
     * Loads Ion data in its entirety.
     *
     * @param ionData may be either Ion binary data, or UTF-encoded Ion text.
     * @return a datagram containing all the elements on the input stream.
     * @throws NullPointerException if <code>ionData</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     */
    public IonDatagram load(byte[] ionData);
}
