/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.LocalSymbolTable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.io.StringReader;

/**
 * Implementation of the {@link IonLoader} interface.
 * <p>
 * This is an internal implementation class that should not be used directly.
 */
public class LoaderImpl
    implements IonLoader
{
    private final IonSystemImpl mySystem;


    public LoaderImpl(IonSystemImpl system)
    {
        mySystem = system;
    }


    public IonSystemImpl getSystem()
    {
        return mySystem;
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


    /** @deprecated */
    @Deprecated
    public IonDatagramImpl loadTextFile(File ionFile)
        throws IonException, IOException
    {
        return loadText(ionFile);
    }


    public IonDatagramImpl loadText(File ionFile)
        throws IonException, IOException
    {
        FileInputStream fileStream = new FileInputStream(ionFile);
        try
        {
            return loadText(fileStream);
        }
        finally
        {
            fileStream.close();
        }
    }


    public IonDatagramImpl loadBinary(File ionFile)
        throws IonException, IOException
    {
        FileInputStream fileStream = new FileInputStream(ionFile);
        try
        {
            return loadBinary(fileStream);
        }
        finally
        {
            fileStream.close();
        }
    }


    //=========================================================================
    // Loading from String

    public IonDatagramImpl loadText(String ionText)
        throws IonException
    {
        StringReader reader = new StringReader(ionText);
        try
        {
            return loadText(reader);
        }
        catch (IOException e)
        {
            // Wrap this because it shouldn't happen and we don't want to
            // propagate it.
            String message = "Error reading from string: " + e.getMessage();
            throw new IonException(message, e);
        }
        finally
        {
            // This may not be necessary, but for all I know StringReader will
            // release some resources.
            reader.close();
        }
    }


    /** @deprecated */
    @Deprecated
    public IonDatagramImpl load(String ionText)
        throws IonException
    {
        return loadText(ionText);
    }


    //=========================================================================
    // Loading from Reader

    public IonDatagramImpl loadText(Reader ionReader)
        throws IonException, IOException
    {
        return new IonDatagramImpl(mySystem, ionReader);
    }


    public IonDatagramImpl loadText(Reader ionText,
                                    LocalSymbolTable symbolTable)
        throws IonException, IOException
    {
        return new IonDatagramImpl(mySystem, symbolTable, ionText);
    }


    /** @deprecated */
    @Deprecated
    public IonDatagramImpl load(Reader ionReader)
        throws IonException
    {
        try
        {
            return loadText(ionReader);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }


    //=========================================================================
    // Loading from byte[]

    public IonDatagramImpl load(byte[] ionData)
    {
        SystemReader systemReader = mySystem.newSystemReader(ionData);
        return new IonDatagramImpl(systemReader);
    }


    //=========================================================================
    // Loading from InputStream

    public IonDatagramImpl load(InputStream ionData)
        throws IonException, IOException
    {
        PushbackInputStream pushback = new PushbackInputStream(ionData, 8);
        if (isBinary(pushback)) {
            return loadBinary(pushback);
        }

        return loadText(pushback);
    }


    public IonDatagramImpl loadText(InputStream ionText)
        throws IOException
    {
        Reader reader = new InputStreamReader(ionText, "UTF-8");
        return loadText(reader);
    }


    public IonDatagramImpl loadBinary(InputStream ionBinary)
        throws IOException
    {
        SystemReader systemReader = mySystem.newBinarySystemReader(ionBinary);
        return new IonDatagramImpl(systemReader);
    }


    //=========================================================================
    // Other utilities and helpers

    static boolean isBinary(PushbackInputStream pushback)
        throws IonException, IOException
    {
        boolean isBinary = false;
        byte[] cookie = new byte[IonConstants.BINARY_VERSION_MARKER_SIZE];

        int len = pushback.read(cookie);
        if (len == IonConstants.BINARY_VERSION_MARKER_SIZE) {
            isBinary = IonBinary.startsWithBinaryVersionMarker(cookie);
        }
        if (len > 0) {
            pushback.unread(cookie, 0, len);
        }
        return isBinary;
    }
}
