// Copyright (c) 2012-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonWriter;
import java.io.IOException;

/**
 * An IonWriter that has optimized list-writing.
 *
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public interface PrivateListWriter
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
