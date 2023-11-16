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

import java.io.IOException;


/**
 * A destination sink that can be fed bytes.  The typical usage is a {@link _Private_ByteTransferReader} that funnels data
 * to an binary Ion target.
 */
public interface _Private_ByteTransferSink
{
    /**
     * Writes the given data to the sink.
     *
     * @param data      The byte array to write.
     * @param off       The offset in the array to write from.
     * @param len       The length of data to write.
     */
    public void writeBytes(byte[] data, int off, int len) throws IOException;
}
