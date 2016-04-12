// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.impl._Private_IonReaderFactory.makeReader;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl._Private_IonWriterFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

final class IonLoaderLite
    implements IonLoader
{
    private final IonSystemLite _system;

    /** Not null. */
    private final IonCatalog    _catalog;

    /**
     * @param system must not be null.
     * @param catalog must not be null.
     */
    public IonLoaderLite(IonSystemLite system, IonCatalog catalog)
    {
        assert system != null;
        assert catalog != null;

        _system = system;
        _catalog = catalog;
    }

    public IonSystem getSystem()
    {
        return _system;
    }

    public IonCatalog getCatalog()
    {
        return _catalog;
    }


    /**
     * This doesn't wrap IOException because some callers need to propagate it.
     *
     * @return a new datagram; not null.
     */
    private IonDatagramLite load_helper(IonReader reader)
    throws IOException
    {
        IonDatagramLite datagram = new IonDatagramLite(_system, _catalog);
        IonWriter writer = _Private_IonWriterFactory.makeWriter(datagram);
        writer.writeValues(reader);
        return datagram;
    }

    public IonDatagram load(File ionFile) throws IonException, IOException
    {
        InputStream ionData = new FileInputStream(ionFile);
        try
        {
            IonDatagram datagram = load(ionData);
            return datagram;
        }
        finally
        {
            ionData.close();
        }
    }

    public IonDatagram load(String ionText) throws IonException
    {
        try {
            IonReader reader = makeReader(_system, _catalog, ionText);
            IonDatagramLite datagram = load_helper(reader);
            return datagram;
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    public IonDatagram load(Reader ionText) throws IonException, IOException
    {
        try {
            IonReader reader = makeReader(_system, _catalog, ionText);
            IonDatagramLite datagram = load_helper(reader);
            return datagram;
        }
        catch (IonException e) {
            IOException io = e.causeOfType(IOException.class);
            if (io != null) throw io;
            throw e;
        }
    }

    public IonDatagram load(byte[] ionData) throws IonException
    {
        try {
            IonReader reader = makeReader(_system, _catalog, ionData, 0, ionData.length);
            IonDatagramLite datagram = load_helper(reader);
            return datagram;
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    public IonDatagram load(InputStream ionData)
        throws IonException, IOException
    {
        try {
            IonReader reader = makeReader(_system, _catalog, ionData);
            IonDatagramLite datagram = load_helper(reader);
            return datagram;
        }
        catch (IonException e) {
            IOException io = e.causeOfType(IOException.class);
            if (io != null) throw io;
            throw e;
        }
    }

}
