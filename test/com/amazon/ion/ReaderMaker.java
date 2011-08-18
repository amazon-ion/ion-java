// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.util.IonStreamUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Abstracts the various ways that {@link IonReader}s can be created, so test
 * cases can cover all the APIs.
 */
public enum ReaderMaker
{
    /**
     * Invokes {@link IonSystem#newReader(String)}.
     */
    FROM_STRING
    {
        @Override
        public IonReader newReader(IonSystem system, String ionText)
        {
            return system.newReader(ionText);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(byte[])} with Ion binary.
     */
    FROM_BYTES_BINARY
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            return system.newReader(ionData);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(byte[])} with Ion text.
     */
    FROM_BYTES_TEXT
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureText(system, ionData);
            return system.newReader(ionData);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(byte[],int,int)} with Ion binary.
     */
    FROM_BYTES_OFFSET_BINARY
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            byte[] padded = new byte[ionData.length + 70];
            System.arraycopy(ionData, 0, padded, 37, ionData.length);
            return system.newReader(padded, 37, ionData.length);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(byte[],int,int)} with Ion text.
     */
    FROM_BYTES_OFFSET_TEXT
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureText(system, ionData);
            byte[] padded = new byte[ionData.length + 70];
            System.arraycopy(ionData, 0, padded, 37, ionData.length);
            return system.newReader(padded, 37, ionData.length);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(InputStream)} with Ion binary.
     */
    FROM_INPUT_STREAM_BINARY
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            InputStream in = new ByteArrayInputStream(ionData);
            return system.newReader(in);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(InputStream)} with Ion text.
     */
    FROM_INPUT_STREAM_TEXT
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureText(system, ionData);
            InputStream in = new ByteArrayInputStream(ionData);
            return system.newReader(in);
        }
    },


    FROM_DOM
    {
        @Override
        public IonReader newReader(IonSystem system, String ionText)
        {
            IonDatagram dg = system.getLoader().load(ionText);
            return system.newReader(dg);
        }

        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            IonDatagram dg = system.getLoader().load(ionData);
            return system.newReader(dg);
        }
    };



    public IonReader newReader(IonSystem system, String ionText)
    {
        byte[] utf8 = IonImplUtils.utf8(ionText);
        return newReader(system, utf8);
    }


    public IonReader newReader(IonSystem system, byte[] ionData)
    {
        IonDatagram dg = system.getLoader().load(ionData);
        String ionText = dg.toString();
        return newReader(system, ionText);
    }


    public static ReaderMaker[] valuesExcluding(ReaderMaker... exclusions)
    {
        ReaderMaker[] all = values();
        ArrayList<ReaderMaker> retained =
            new ArrayList<ReaderMaker>(Arrays.asList(all));
        retained.removeAll(Arrays.asList(exclusions));
        return retained.toArray(new ReaderMaker[retained.size()]);
    }

    private static byte[] ensureBinary(IonSystem system, byte[] ionData)
    {
        if (IonStreamUtils.isIonBinary(ionData)) return ionData;

        IonDatagram dg = system.getLoader().load(ionData);
        return dg.getBytes();
    }

    private static byte[] ensureText(IonSystem system, byte[] ionData)
    {
        if (! IonStreamUtils.isIonBinary(ionData)) return ionData;

        IonReader reader = system.newReader(ionData);

        ByteArrayOutputStream utf8Bytes = new ByteArrayOutputStream();
        IonWriter writer = system.newTextWriter(utf8Bytes);

        try
        {
            writer.writeValues(reader);
            writer.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return utf8Bytes.toByteArray();
    }
}