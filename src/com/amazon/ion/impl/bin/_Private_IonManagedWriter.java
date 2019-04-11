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
import java.io.IOException;

/**
 * An {@link IonWriter} that manages local symbol tables, while providing access
 * to its underlying IonRawWriter.
 *
 * @deprecated This is a private API subject to change without notice.
 */
@Deprecated
public interface _Private_IonManagedWriter extends IonWriter
{
    /**
     * Get the underlying raw user value writer. This may be used to directly
     * write user values, field names, and annotations, bypassing any symbol
     * table management performed by this IonManagedWriter.
     * @return the {@link _Private_IonRawWriter} responsible for writing this
     *         IonManagedWriter's user values.
     */
    _Private_IonRawWriter getRawWriter();

    /**
     * If a local symbol table has not already been started by this writer since
     * it was constructed or last finished, start writing a local symbol table
     * by opening the struct and declaring the imports. Any local symbols
     * subsequently written using this IonManagedWriter will be appended as they
     * are encountered.
     *
     * When using the raw writer, a call to this API is required unless one of
     * the following applies:
     * <ul>
     *     <li>The user won't be writing any symbols.</li>
     *     <li>The user has manually written the symbol table struct to the
     *         correct raw writer. In this case, making this call would start a
     *         new (and probably redundant) symbol table context, which is
     *         wasteful but not harmful.</li>
     *     <li>The user definitely will be writing more symbols in this context
     *         using the managed writer.</li>
     * </ul>
     * Failing to call this when required will leave the system symbol table as
     * the active symbol table context for the subsequently written values, which
     * will lead to missing symbol ID mappings.
     * @throws IOException
     */
    void requireLocalSymbolTable() throws IOException;

}
