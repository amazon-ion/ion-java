/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl.bin;

/**
 * Constructs {@link BlockAllocator} instances.  Such instances returned by {@link #vendAllocator(int)}
 * should not be shared across threads and must be closed when no longer needed to re-use or clean up
 * any underlying resources.
 * <p>
 * Implementations must be thread-safe.
 */
/*package*/ abstract class BlockAllocatorProvider
{
    /*package*/ BlockAllocatorProvider() {}

    /**
     * Returns a {@link BlockAllocator} that vends blocks of the given size.
     * This method can be used across threads, but the returned {@link BlockAllocator} instances are not
     * thread-safe and must have their {@link BlockAllocator#close()} method called to allow its underlying resources
     * to be released or reused properly.
     */
    public abstract BlockAllocator vendAllocator(int blockSize);
}
