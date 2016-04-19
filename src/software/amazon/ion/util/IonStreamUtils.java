/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.util;

import static software.amazon.ion.impl.PrivateIonConstants.BINARY_VERSION_MARKER_1_0;
import static software.amazon.ion.util.GzipOrRawInputStream.GZIP_HEADER;

import java.io.IOException;
import java.io.InputStream;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.impl.PrivateListWriter;

/**
 * Utility methods for working with the Ion streaming interfaces,
 * {@link IonReader} and {@link IonWriter}.
 */
public class IonStreamUtils
{
    /**
     * Determines whether a buffer contains Ion binary data by looking for the
     * presence of the Ion Version Marker at its start.
     * A {@code false} result does not imply that the buffer has Ion text,
     * just that it's not Ion binary.
     *
     * @param buffer the data to check.
     *
     * @return {@code true} if the buffer contains Ion binary (starting from
     *  offset zero); {@code false} if the buffer is null or too short.
     *
     * @see #isIonBinary(byte[], int, int)
     */
    public static boolean isIonBinary(byte[] buffer)
    {
        return buffer != null && isIonBinary(buffer, 0, buffer.length);
    }


    /**
     * Determines whether a buffer contains Ion binary data by looking for the
     * presence of the Ion Version Marker at a given offset.
     * A {@code false} result does not imply that the buffer has Ion text,
     * just that it's not Ion binary.
     *
     * @param buffer the data to check.
     * @param offset the position in the buffer at which to start reading.
     * @param length the number of bytes in the buffer that are valid,
     *  starting from {@code offset}.
     *
     * @return {@code true} if the buffer contains Ion binary (starting from
     *  {@code offset}); {@code false} if the buffer is null or if the
     *  {@code length} is too short.
     *
     * @see #isIonBinary(byte[])
     */
    public static boolean isIonBinary(byte[] buffer, int offset, int length)
    {
        return cookieMatches(BINARY_VERSION_MARKER_1_0, buffer, offset, length);
    }


    /**
     * Determines whether a buffer contains GZIPped data.
     *
     * @param buffer the data to check.
     * @param offset the position in the buffer at which to start reading.
     * @param length the number of bytes in the buffer that are valid,
     *  starting from {@code offset}.
     *
     * @return {@code true} if the buffer contains GZIPped data; {@code false}
     * if the buffer is null or if the {@code length} is too short.
     */
    public static boolean isGzip(byte[] buffer, int offset, int length)
    {
        return cookieMatches(GZIP_HEADER, buffer, offset, length);
    }


    private static boolean cookieMatches(byte[] cookie,
                                         byte[] buffer,
                                         int offset,
                                         int length)
    {
        if (buffer == null || length < cookie.length)
        {
            return false;
        }

        for (int i = 0; i < cookie.length; i++)
        {
            if (cookie[i] != buffer[offset + i])
            {
                return false;
            }
        }
        return true;
    }


    /**
     * Returns a stream that decompresses a stream if it contains GZIPped data,
     * otherwise has no effect on the stream (but may wrap it).
     */
    public static InputStream unGzip(InputStream in)
        throws IOException
    {
        return new GzipOrRawInputStream(in);
    }


    //=========================================================================


    /**
     * writes an IonList with a series of IonBool values. This
     * starts a List, writes the values (without any annoations)
     * and closes the list. For text and tree writers this is
     * just a convienience, but for the binary writer it can be
     * optimized internally.
     * @param values boolean values to populate the list with
     */
    public static void writeBoolList(IonWriter writer, boolean[] values)
        throws IOException
    {
        if (writer instanceof PrivateListWriter) {
            ((PrivateListWriter)writer).writeBoolList(values);
            return;
        }

        writer.stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writer.writeBool(values[ii]);
        }
        writer.stepOut();
    }

    /**
     * writes an IonList with a series of IonFloat values. This
     * starts a List, writes the values (without any annoations)
     * and closes the list. For text and tree writers this is
     * just a convienience, but for the binary writer it can be
     * optimized internally.  Note that since, currently, IonFloat
     * is a 64 bit float this is a helper that simply casts
     * the passed in floats to double before writing them.
     * @param values 32 bit float values to populate the lists IonFloat's with
     */
    public static void writeFloatList(IonWriter writer, float[] values)
        throws IOException
    {
        if (writer instanceof PrivateListWriter) {
            ((PrivateListWriter)writer).writeFloatList(values);
            return;
        }

        writer.stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writer.writeFloat(values[ii]);
        }
        writer.stepOut();
    }

    /**
     * writes an IonList with a series of IonFloat values. This
     * starts a List, writes the values (without any annoations)
     * and closes the list. For text and tree writers this is
     * just a convienience, but for the binary writer it can be
     * optimized internally.
     * @param values 64 bit float values to populate the lists IonFloat's with
     */
    public static void writeFloatList(IonWriter writer, double[] values)
        throws IOException
    {
        if (writer instanceof PrivateListWriter) {
            ((PrivateListWriter)writer).writeFloatList(values);
            return;
        }

        writer.stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writer.writeFloat(values[ii]);
        }
        writer.stepOut();
    }


    /**
     * writes an IonList with a series of IonInt values. This
     * starts a List, writes the values (without any annoations)
     * and closes the list. For text and tree writers this is
     * just a convienience, but for the binary writer it can be
     * optimized internally.
     * @param values signed byte values to populate the lists int's with
     */
    public static void writeIntList(IonWriter writer, byte[] values)
        throws IOException
    {
        if (writer instanceof PrivateListWriter) {
            ((PrivateListWriter)writer).writeIntList(values);
            return;
        }

        writer.stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writer.writeInt(values[ii]);
        }
        writer.stepOut();
    }

    /**
     * writes an IonList with a series of IonInt values. This
     * starts a List, writes the values (without any annoations)
     * and closes the list. For text and tree writers this is
     * just a convienience, but for the binary writer it can be
     * optimized internally.
     * @param values signed short values to populate the lists int's with
     */
    public static void writeIntList(IonWriter writer, short[] values)
        throws IOException
    {
        if (writer instanceof PrivateListWriter) {
            ((PrivateListWriter)writer).writeIntList(values);
            return;
        }

        writer.stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writer.writeInt(values[ii]);
        }
        writer.stepOut();
    }

    /**
     * writes an IonList with a series of IonInt values. This
     * starts a List, writes the values (without any annoations)
     * and closes the list. For text and tree writers this is
     * just a convienience, but for the binary writer it can be
     * optimized internally.
     * @param values signed int values to populate the lists int's with
     */
    public static void writeIntList(IonWriter writer, int[] values)
        throws IOException
    {
        if (writer instanceof PrivateListWriter) {
            ((PrivateListWriter)writer).writeIntList(values);
            return;
        }

        writer.stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writer.writeInt(values[ii]);
        }
        writer.stepOut();
    }

    /**
     * writes an IonList with a series of IonInt values. This
     * starts a List, writes the values (without any annoations)
     * and closes the list. For text and tree writers this is
     * just a convienience, but for the binary writer it can be
     * optimized internally.
     * @param values signed long values to populate the lists int's with
     */
    public static void writeIntList(IonWriter writer, long[] values)
        throws IOException
    {
        if (writer instanceof PrivateListWriter) {
            ((PrivateListWriter)writer).writeIntList(values);
            return;
        }

        writer.stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writer.writeInt(values[ii]);
        }
        writer.stepOut();
    }


    /**
     * writes an IonList with a series of IonString values. This
     * starts a List, writes the values (without any annoations)
     * and closes the list. For text and tree writers this is
     * just a convienience, but for the binary writer it can be
     * optimized internally.
     * @param values Java String to populate the lists IonString's from
     */
    public static void writeStringList(IonWriter writer, String[] values)
        throws IOException
    {
        if (writer instanceof PrivateListWriter) {
            ((PrivateListWriter)writer).writeStringList(values);
            return;
        }

        writer.stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writer.writeString(values[ii]);
        }
        writer.stepOut();
    }
}
