// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl.IonReaderFactoryX;
import com.amazon.ion.impl.IonWriterFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 *
 */
public class IonLoaderLite
    implements IonLoader
{
    private final IonSystemLite _system;
    private final IonCatalog    _catalog;

    public IonLoaderLite(IonSystemLite system, IonCatalog catalog)
    {
        _system = system;
        _catalog = catalog;
    }

    public IonSystem getSystem()
    {
        return _system;
    }

    private IonDatagramLite load_helper(IonReader reader)
    {
        IonDatagramLite datagram = new IonDatagramLite(_system, _catalog);
        IonWriter writer = IonWriterFactory.makeWriter(datagram);
        try {
            writer.writeValues(reader);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        datagram.populateSymbolValues(null);
        return datagram;
    }

    public IonDatagram load(File ionFile) throws IonException, IOException
    {
        InputStream ionData = new FileInputStream(ionFile);
        IonDatagram datagram = load(ionData);
        return datagram;
    }

    public IonDatagram load(String ionText) throws IonException
    {
        IonReader reader = IonReaderFactoryX.makeReader(_system, _catalog, ionText);
        IonDatagramLite datagram = load_helper(reader);
        return datagram;
    }

    public IonDatagram load(Reader ionText) throws IonException, IOException
    {
        IonReader reader = IonReaderFactoryX.makeReader(_system, _catalog, ionText);
        IonDatagramLite datagram = load_helper(reader);
        return datagram;
    }

    public IonDatagram load(byte[] ionData) throws IonException
    {
        IonReader reader = IonReaderFactoryX.makeReader(_system, _catalog, ionData, 0, ionData.length);
        IonDatagramLite datagram = load_helper(reader);
        return datagram;
    }

    public IonDatagram load(InputStream ionData)
        throws IonException, IOException
    {
        IonReader reader = IonReaderFactoryX.makeReader(_system, _catalog, ionData);
        IonDatagramLite datagram = load_helper(reader);
        return datagram;
    }

}
