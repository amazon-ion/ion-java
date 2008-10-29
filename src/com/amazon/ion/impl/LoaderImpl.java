/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.SymbolTable;
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
        return load(ionFile);
    }

    @Deprecated
    public IonDatagramImpl loadText(File ionFile)
        throws IonException, IOException
    {
        return load(ionFile);
    }

    @Deprecated
    public IonDatagramImpl loadBinary(File ionFile)
        throws IonException, IOException
    {
        return load(ionFile);
    }


    //=========================================================================
    // Loading from String

    @Deprecated
    public IonDatagramImpl loadText(String ionText)
        throws IonException
    {
        return load(ionText);
    }


    public IonDatagramImpl load(String ionText)
        throws IonException
    {
        StringReader reader = new StringReader(ionText);
        try
        {
            return load(reader);
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


    //=========================================================================
    // Loading from Reader

    @Deprecated
    public IonDatagramImpl loadText(Reader ionReader)
        throws IonException, IOException
    {
        return load(ionReader);
    }


    public IonDatagramImpl load(Reader ionText, SymbolTable symbolTable)
        throws IonException, IOException
    {
        return new IonDatagramImpl(mySystem, symbolTable, ionText);
    }


    @Deprecated
    public IonDatagramImpl load(Reader ionText,
                                LocalSymbolTable symbolTable)
        throws IonException, IOException
    {
        return new IonDatagramImpl(mySystem, symbolTable, ionText);
    }


    @Deprecated
    public IonDatagramImpl loadText(Reader ionText,
                                    LocalSymbolTable symbolTable)
        throws IonException, IOException
    {
        return new IonDatagramImpl(mySystem, symbolTable, ionText);
    }


    public IonDatagramImpl load(Reader ionReader)
        throws IonException, IOException
    {
        return new IonDatagramImpl(mySystem, ionReader);
    }


    //=========================================================================
    // Loading from byte[]

    public IonDatagramImpl load(byte[] ionData)
    {
        SystemReader systemReader = mySystem.newSystemReader(ionData);
        return new IonDatagramImpl(mySystem, systemReader);
    }


    //=========================================================================
    // Loading from InputStream

    public IonDatagramImpl load(InputStream ionData)
        throws IonException, IOException
    {
        PushbackInputStream pushback = new PushbackInputStream(ionData, 8);
        if (IonImplUtils.streamIsIonBinary(pushback)) {
            SystemReader systemReader = mySystem.newBinarySystemReader(pushback);
            return new IonDatagramImpl(mySystem, systemReader);
        }

        Reader reader = new InputStreamReader(pushback, "UTF-8");
        return load(reader);
    }


    @Deprecated
    public IonDatagramImpl loadText(InputStream ionText)
        throws IOException
    {
        Reader reader = new InputStreamReader(ionText, "UTF-8");
        return load(reader);
    }

    @Deprecated
    public IonDatagramImpl loadBinary(InputStream ionBinary)
        throws IOException
    {
        SystemReader systemReader = mySystem.newBinarySystemReader(ionBinary);
        return new IonDatagramImpl(mySystem, systemReader);
    }
}
