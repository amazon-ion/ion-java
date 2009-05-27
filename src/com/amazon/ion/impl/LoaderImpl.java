/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
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
    static final boolean USE_NEW_READERS = false;

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
        if (USE_NEW_READERS)
        {
            IonTextReader textReader = mySystem.newSystemReader(ionText);
            try
            {
                IonDatagramImpl dg = new IonDatagramImpl(mySystem, textReader);
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
    public IonDatagramImpl loadText(Reader ionText)
        throws IonException, IOException
    {
        return load(ionText);
    }

    @Deprecated
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


    public IonDatagramImpl load(Reader ionText)
        throws IonException, IOException
    {
        if (USE_NEW_READERS)
        {
            IonReader reader = mySystem.newSystemReader(ionText);
            try
            {
                IonDatagramImpl dg = new IonDatagramImpl(mySystem, reader);
                return dg;
            }
            catch (IOException e)
            {
                throw new IonException(e);
            }
        }
        return new IonDatagramImpl(mySystem, ionText);
    }


    //=========================================================================
    // Loading from byte[]

    public IonDatagramImpl load(byte[] ionData)
    {
        if (USE_NEW_READERS)
        {
            boolean isBinary = IonBinary.matchBinaryVersionMarker(ionData);
            if (! isBinary)
            {
                IonReader reader = mySystem.newSystemReader(ionData);
                assert reader instanceof IonTextReader;
                try
                {
                    IonDatagramImpl dg = new IonDatagramImpl(mySystem, reader);
                    // Force symtab preparation  FIXME should not be necessary
                    dg.byteSize();
                    return dg;
                }
                catch (IOException e)
                {
                    throw new IonException(e);
                }
            }
            // else fall through, the old implementation is fine
            // TODO refactor this path to eliminate SystemReader
        }

        return new IonDatagramImpl(mySystem, ionData);
    }


    //=========================================================================
    // Loading from InputStream

    public IonDatagramImpl load(InputStream ionData)
        throws IonException, IOException
    {
        PushbackInputStream pushback = new PushbackInputStream(ionData, 8);
        if (IonImplUtils.streamIsIonBinary(pushback)) {
            if (USE_NEW_READERS)
            {
                // Nothing special to do. SystemReader works fine to
                // materialize the top layer of the datagram.
                // The streaming APIs add no benefit.
            }

            SystemReader systemReader =
                mySystem.newBinarySystemReader(pushback);
            return new IonDatagramImpl(mySystem, systemReader);
        }

        // Input is text
        if (USE_NEW_READERS)
        {
            IonReader reader = mySystem.newSystemReader(pushback);
            assert reader instanceof IonTextReader;
            try
            {
                IonDatagramImpl dg = new IonDatagramImpl(mySystem, reader);
                // Force symtab preparation  FIXME should not be necessary
                dg.byteSize();
                return dg;
            }
            catch (IOException e)
            {
                throw new IonException(e);
            }
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
    
    @Deprecated
    public IonDatagramImpl loadPagedBinary(InputStream ionBinary)
        throws IOException
    {
        SystemReader systemReader = mySystem.newPagedBinarySystemReader(ionBinary);
        return new IonDatagramImpl(mySystem, systemReader);
    }
    
}
