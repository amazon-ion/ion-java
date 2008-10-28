/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonBinary.Reader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

/**
 * For internal use only!
 */
public final class IonImplUtils
{

    public static byte[] loadFileBytes(File file)
        throws IOException
    {
        int len = (int)file.length();
        byte[] buf = new byte[len];

        FileInputStream in = new FileInputStream(file);
        try {
            // TODO I don't think buffering here is helpful since we are
            // doing a bulk read into our own buffer.
            BufferedInputStream bin = new BufferedInputStream(in);
            try {
                bin.read(buf);
            }
            finally {
                bin.close();
            }
        }
        finally {
            in.close();
        }

        return buf;
    }


    public static byte[] loadStreamBytes(InputStream in)
        throws IOException
    {
        BufferManager buffer = new BufferManager(in);
        Reader bufReader = buffer.reader();
        bufReader.sync();
        bufReader.setPosition(0);
        byte[] bytes = bufReader.getBytes();
        return bytes;
    }


    public static boolean streamIsIonBinary(PushbackInputStream pushback)
        throws IonException, IOException
    {
        boolean isBinary = false;
        byte[] cookie = new byte[IonConstants.BINARY_VERSION_MARKER_SIZE];

        int len = pushback.read(cookie);
        if (len == IonConstants.BINARY_VERSION_MARKER_SIZE) {
            isBinary = IonBinary.matchBinaryVersionMarker(cookie);
        }
        if (len > 0) {
            pushback.unread(cookie, 0, len);
        }
        return isBinary;
    }
}
