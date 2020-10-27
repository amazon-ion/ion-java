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

package com.amazon.ion.impl.bin;

import com.amazon.ion.IonWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * NOT FOR APPLICATION USE!
 *
 * Exposes {@link IonRawBinaryWriter} functionality for use when creating Ion hashes.
 */
@Deprecated
public class _PrivateIon_HashTrampoline
{
    private static final PooledBlockAllocatorProvider ALLOCATOR_PROVIDER = new PooledBlockAllocatorProvider();
    
    public static IonWriter newIonWriter(ByteArrayOutputStream baos) throws IOException
    {
        return new IonRawBinaryWriter(
                ALLOCATOR_PROVIDER,
                _Private_IonManagedBinaryWriterBuilder.DEFAULT_BLOCK_SIZE,
                baos,
                AbstractIonWriter.WriteValueOptimization.NONE,
                IonRawBinaryWriter.StreamCloseMode.CLOSE,
                IonRawBinaryWriter.StreamFlushMode.FLUSH,
                IonRawBinaryWriter.PreallocationMode.PREALLOCATE_0,
                false     // force floats to be encoded as binary64
        );
    }
}
