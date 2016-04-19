/*
 * Copyright 2016-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

/**
 * Provides the implementation for the second-generation Ion binary implementation.
 * At this time, this is limited to a binary {@link software.amazon.ion.IonWriter}.
 *
 * <p>
 * This package limits most of its APIs to package-level access, the public API of note is contained within
 * the {@link software.amazon.ion.impl.bin.PrivateIonManagedBinaryWriterBuilder} which builds instances of
 * {@link software.amazon.ion.impl.bin.IonManagedBinaryWriter}.  See the below section for what <i>Managed</i> means
 * in this context.
 *
 * <h2>Block API</h2>
 * A generalized interface for blocks of heap memory are provided via the {@link software.amazon.ion.impl.bin.Block} API.
 * There are two factory type APIs to actually get a {@link software.amazon.ion.impl.bin.Block} instance:
 * {@link software.amazon.ion.impl.bin.BlockAllocator} which vend blocks of a particular fixed size
 * and {@link software.amazon.ion.impl.bin.BlockAllocatorProvider} which creates {@link software.amazon.ion.impl.bin.BlockAllocator}
 * instances.
 * <p>
 * The primary reason for this level of indirection is flexibility for the underlying implementations of {@link software.amazon.ion.impl.bin.Block}
 * and {@link software.amazon.ion.impl.bin.BlockAllocator}.  These APIs are not required to be thread-safe, whereas
 * {@link software.amazon.ion.impl.bin.BlockAllocatorProvider} is required to be thread-safe.
 * <p>
 * The APIs for {@link software.amazon.ion.impl.bin.BlockAllocator} and {@link software.amazon.ion.impl.bin.Block}
 * follow the resource pattern (similar in principle to I/O streams), and should be closed when no longer needed
 * to allow implementation resources to be released or re-used.
 *
 * <h2>Raw Binary Ion Writer</h2>
 * The {@link software.amazon.ion.impl.bin.IonRawBinaryWriter} deals with the low-level encoding considerations of the
 * Ion format.  The {@link software.amazon.ion.impl.bin.WriteBuffer} is used closely with this implementation to
 * deal with the Ion <i>sub-field</i> encodings (e.g. <tt>VarInt</tt>, <tt>VarUInt</tt>, and UTF-8).
 *
 * <h2>Managed Binary Ion Writer</h2>
 * The {@link software.amazon.ion.impl.bin.IonManagedBinaryWriter} is layered on top of the {@link software.amazon.ion.impl.bin.IonRawBinaryWriter}.
 * In particular, it intercepts symbol, annotation, field names and handles the mechanics of symbol table management
 * transparently to the user.
 */
package software.amazon.ion.impl.bin;