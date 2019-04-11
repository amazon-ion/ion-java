/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
