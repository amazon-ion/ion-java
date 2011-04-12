// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.impl.IonWriterSystemBinary;
import com.amazon.ion.impl.IonWriterUserBinary;
import java.io.IOException;

/**
 * Utility methods for working with the Ion streaming interfaces,
 * {@link IonReader} and {@link IonWriter}.
 */
public class IonStreamUtils
{

    /**
     * writes an IonList with a series of IonBool values. This
     * starts a List, writes the values (without any annoations)
     * and closes the list. For text and tree writers this is
     * just a convienience, but for the binary writer it can be
     * optimized internally.
     * @param values boolean values to populate the list with
     */
    public void writeBoolList(IonWriter writer, boolean[] values)
        throws IOException
    {
        if (writer instanceof IonWriterUserBinary) {
            ((IonWriterUserBinary)writer).writeBoolList(values);
            return;
        }
        if (writer instanceof IonWriterSystemBinary) {
            ((IonWriterSystemBinary)writer).writeBoolList(values);
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
    public void writeFloatList(IonWriter writer, float[] values)
        throws IOException
    {
        if (writer instanceof IonWriterUserBinary) {
            ((IonWriterUserBinary)writer).writeFloatList(values);
            return;
        }
        if (writer instanceof IonWriterSystemBinary) {
            ((IonWriterSystemBinary)writer).writeFloatList(values);
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
    public void writeFloatList(IonWriter writer, double[] values)
        throws IOException
    {
        if (writer instanceof IonWriterUserBinary) {
            ((IonWriterUserBinary)writer).writeFloatList(values);
            return;
        }
        if (writer instanceof IonWriterSystemBinary) {
            ((IonWriterSystemBinary)writer).writeFloatList(values);
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
    public void writeIntList(IonWriter writer, byte[] values)
        throws IOException
    {
        if (writer instanceof IonWriterUserBinary) {
            ((IonWriterUserBinary)writer).writeIntList(values);
            return;
        }
        if (writer instanceof IonWriterSystemBinary) {
            ((IonWriterSystemBinary)writer).writeIntList(values);
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
    public void writeIntList(IonWriter writer, short[] values)
        throws IOException
    {
        if (writer instanceof IonWriterUserBinary) {
            ((IonWriterUserBinary)writer).writeIntList(values);
            return;
        }
        if (writer instanceof IonWriterSystemBinary) {
            ((IonWriterSystemBinary)writer).writeIntList(values);
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
    public void writeIntList(IonWriter writer, int[] values)
        throws IOException
    {
        if (writer instanceof IonWriterUserBinary) {
            ((IonWriterUserBinary)writer).writeIntList(values);
            return;
        }
        if (writer instanceof IonWriterSystemBinary) {
            ((IonWriterSystemBinary)writer).writeIntList(values);
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
    public void writeIntList(IonWriter writer, long[] values)
        throws IOException
    {
        if (writer instanceof IonWriterUserBinary) {
            ((IonWriterUserBinary)writer).writeIntList(values);
            return;
        }
        if (writer instanceof IonWriterSystemBinary) {
            ((IonWriterSystemBinary)writer).writeIntList(values);
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
    public void writeStringList(IonWriter writer, String[] values)
        throws IOException
    {
        if (writer instanceof IonWriterUserBinary) {
            ((IonWriterUserBinary)writer).writeStringList(values);
            return;
        }
        if (writer instanceof IonWriterSystemBinary) {
            ((IonWriterSystemBinary)writer).writeStringList(values);
            return;
        }

        writer.stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writer.writeString(values[ii]);
        }
        writer.stepOut();
    }
}
