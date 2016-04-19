/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl;

import java.io.IOException;
import software.amazon.ion.IonWriter;

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
