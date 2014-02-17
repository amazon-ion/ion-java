// Copyright (c) 2012-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonWriter;
import java.io.IOException;

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * An IonWriter that has optimized list-writing.
 */
public interface _Private_ListWriter
    extends IonWriter
{
    public void writeBoolList(boolean[] values)throws IOException;
    public void writeFloatList(float[] values) throws IOException;
    public void writeFloatList(double[] values) throws IOException;
    public void writeIntList(byte[] values) throws IOException;
    public void writeIntList(short[] values) throws IOException;
    public void writeIntList(int[] values) throws IOException;
    public void writeIntList(long[] values) throws IOException;
    public void writeStringList(String[] values) throws IOException;
}
