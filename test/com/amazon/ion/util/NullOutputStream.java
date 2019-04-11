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

package com.amazon.ion.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An output stream that does not perform any writes, used for testing.
 */
public class NullOutputStream extends OutputStream
{
    @Override
    public void write(int b)
        throws IOException
    {
    }

    @Override
    public void write(byte[] b)
        throws IOException
    {
    }

    @Override
    public void write(byte[] b, int off, int len)
        throws IOException
    {
    }
}
