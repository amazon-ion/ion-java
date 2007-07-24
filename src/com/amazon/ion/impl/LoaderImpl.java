/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.system.StandardIonSystem;
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
    private final StandardIonSystem mySystem;


    public LoaderImpl(StandardIonSystem system)
    {
        mySystem = system;
    }


    public IonSystem getSystem()
    {
        return mySystem;
    }


    //=========================================================================
    // Loading text


    /**
     * Loads an Ion text (UTF-8) file.  The file is parsed in its entirety.
     *
     * @param ionFile a file containing UTF-8 encoded Ion text.
     *
     * @return an S-expression containing the ordered elements of the file.
     * If the file has no meaningful elements, return an empty sexp.
     * @throws IonException if there's a syntax error in the Ion content.
     * @throws IOException if there's a problem reading the file.
     */
    public IonDatagramImpl loadTextFile(File ionFile)
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

    public IonDatagramImpl loadBinaryFile(File ionFile)
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

    public IonDatagramImpl loadFile(File ionFile)
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


    public IonDatagramImpl load(String ionText)
        throws IonException
    {
        StringReader reader = new StringReader(ionText);
        return load(reader);
    }


    public IonDatagramImpl load(Reader ionReader)
        throws IonException
    {
        return new IonDatagramImpl(mySystem, ionReader);
    }


    public IonDatagramImpl load(Reader ionText, LocalSymbolTable symbolTable)
        throws IonException
    {
        return new IonDatagramImpl(mySystem, symbolTable, ionText);
    }


    //=========================================================================
    // Loading binary


    public IonDatagramImpl load(byte[] ionData)
    {
        SystemReader systemReader = mySystem.newSystemReader(ionData);
        return new IonDatagramImpl(systemReader);
    }


    //=========================================================================
    // Loading binary from a stream

    public IonDatagramImpl load(InputStream ionData)
        throws IonException, IOException
    {
        try
        {
            PushbackInputStream pushback = new PushbackInputStream(ionData, 8);
            if (isBinary(pushback)) {
                return loadBinary(pushback);
            }

            return loadText(pushback);
        }
        finally
        {
            ionData.close();
        }
    }


    public IonDatagramImpl loadText(InputStream ionText)
        throws IOException
    {
        try
        {
            Reader reader = new InputStreamReader(ionText, "UTF-8");
            return load(reader);
        }
        finally
        {
            ionText.close();
        }
    }

    public IonDatagramImpl loadBinary(InputStream ionBinary)
        throws IOException
    {
        try
        {
            SystemReader systemReader = mySystem.newBinarySystemReader(ionBinary);
            return new IonDatagramImpl(systemReader);
        }
        finally
        {
            ionBinary.close();
        }
    }


    //=========================================================================
    // Other utilities and helpers

    static boolean isBinary(PushbackInputStream pushback)
        throws IonException, IOException
    {
        boolean isbinary = false;
        byte[] header = new byte[8];

        int len = pushback.read(header);
        if (len == 8) {
            if (IonBinary.isMagicCookie(header, 4, 4)) {
                return true;
            }
        }
        if (len > 0) {
            pushback.unread(header, 0, len);
        }
        return isbinary;
    }
}
