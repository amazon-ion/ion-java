// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.util.IonStreamUtils.isIonBinary;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTextReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.Reader;

/**
 * Implementation of the {@link IonLoader} interface.
 * <p>
 * This is an internal implementation class that should not be used directly.
 */
final class LoaderImpl
    implements IonLoader
{
    static final boolean USE_NEW_READERS = true;

    private final IonSystemImpl mySystem;

    /** Not null. */
    private final IonCatalog    myCatalog;

    /**
     * @param system must not be null.
     * @param catalog must not be null.
     */
    public LoaderImpl(IonSystemImpl system, IonCatalog catalog)
    {
        assert system != null;
        assert catalog != null;

        mySystem = system;
        myCatalog = catalog;
    }


    public IonSystemImpl getSystem()
    {
        return mySystem;
    }

    public IonCatalog getCatalog()
    {
        return myCatalog;
    }


    //=========================================================================
    // Loading from File

    public IonDatagramImpl load(File ionFile)
        throws IonException, IOException
    {
        FileInputStream fileStream = new FileInputStream(ionFile);
        try
        {
            return load(fileStream);
        }
        finally
        {
            fileStream.close();
        }
    }


    //=========================================================================
    // Loading from String


    public IonDatagramImpl load(String ionText)
        throws IonException
    {
        IonReader reader = mySystem.newSystemReader(ionText);
        try
        {
            IonDatagramImpl dg =
                new IonDatagramImpl(mySystem, myCatalog, reader);

            return dg;
        }
        catch (IOException e)
        {
            // Wrap this because it shouldn't happen and we don't want to
            // propagate it.
            String message = "Error reading from string: " + e.getMessage();
            throw new IonException(message, e);
        }
    }


    //=========================================================================
    // Loading from Reader

    public IonDatagramImpl load(Reader ionText)
        throws IonException, IOException
    {
        try
        {
            IonReader reader = mySystem.newSystemReader(ionText);
            IonDatagramImpl dg =
                new IonDatagramImpl(mySystem, myCatalog, reader);
            return dg;
        }
        catch (IonException e)
        {
            IOException io = e.causeOfType(IOException.class);
            if (io != null) throw io;
            throw e;
        }
    }


    //=========================================================================
    // Loading from byte[]

    public IonDatagramImpl load(byte[] ionData)
    {
        IonDatagramImpl dg;

        try
        {
            boolean isBinary = isIonBinary(ionData);
            if (!isBinary) {
                IonReader reader = mySystem.newSystemReader(ionData);

                dg = new IonDatagramImpl(mySystem, myCatalog, reader);
            }
            else {
                dg = new IonDatagramImpl(mySystem, myCatalog, ionData);
            }
        }
        catch (IOException e)  // Not expected since we're reading a buffer.
        {
            throw new IonException(e);
        }

        return dg;
    }


    //=========================================================================
    // Loading from InputStream

    public IonDatagramImpl load(InputStream ionData)
        throws IonException, IOException
    {
        // TODO ION-207 gzip support is tricky, don't want multiple pushbacks
        try
        {
            PushbackInputStream pushback = new PushbackInputStream(ionData, 8);
            if (_Private_Utils.streamIsIonBinary(pushback)) {
                if (USE_NEW_READERS)
                {
                    // Nothing special to do. SystemReader works fine to
                    // materialize the top layer of the datagram.
                    // The streaming APIs add no benefit.
                }

                // TODO this can be extended to take the pushback separately
                // thus avoiding the need for the PushbackInputStream entirely
                SystemValueIterator systemReader =
                    mySystem.newBinarySystemIterator(myCatalog, pushback);
                return new IonDatagramImpl(mySystem, systemReader);
            }

            // Input is text
            IonReader reader = mySystem.newSystemReader(pushback);
            assert reader instanceof IonTextReader;

            IonDatagramImpl dg =
                new IonDatagramImpl(mySystem, myCatalog, reader);
            return dg;
        }
        catch (IonException e)
        {
            IOException io = e.causeOfType(IOException.class);
            if (io != null) throw io;
            throw e;
        }
    }
}
