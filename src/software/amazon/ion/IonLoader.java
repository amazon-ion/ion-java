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
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 * <p>
 * <b>Implementations of this interface are safe for use by multiple
 * threads.</b>
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
     * Gets the {@link IonCatalog} being used by this loader.
     *
     * @return a catalog; not null.
     */
    public IonCatalog getCatalog();


    /**
     * Loads an entire file of Ion data into a single datagram,
     * detecting whether it's text or binary data.
     *
     * @param ionFile a file containing Ion data.
     *
     * @return a datagram containing all the values in the file; not null.
     *
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified file results
     * in an <code>IOException</code>.
     */
    public IonDatagram load(File ionFile)
        throws IonException, IOException;


    /**
     * Loads Ion text in its entirety.
     *
     * @param ionText must not be null.
     *
     * @return a datagram containing the input values; not null.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     */
    public IonDatagram load(String ionText)
        throws IonException;


    /**
     * Loads a stream of Ion text into a single datagram.
     * <p>
     * The specified reader remains open after this method returns.
     * <p>
     * Because this library performs its own buffering, it's recommended that
     * you avoid adding additional buffering to the given stream.
     *
     * @param ionText the reader from which to read Ion text.
     *
     * @return a datagram containing all the elements on the input stream;
     *   not null.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     */
    public IonDatagram load(Reader ionText)
        throws IonException, IOException;


    /**
     * Loads a block of Ion data into a single datagram,
     * detecting whether it's text or binary data.
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
     *
     * @param ionData may be either Ion binary data, or UTF-8 Ion text.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @return a datagram containing all the values on the input stream;
     *   not null.
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
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
     * <p>
     * Because this library performs its own buffering, it's recommended that
     * you avoid adding additional buffering to the given stream.
     *
     * @param ionData the stream from which to read Ion data.
     *
     * @return a datagram containing all the values on the input stream;
     *   not null.
     *
     * @throws NullPointerException if <code>ionData</code> is null.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if reading from the specified input stream results
     * in an <code>IOException</code>.
     */
    public IonDatagram load(InputStream ionData)
        throws IonException, IOException;
}
