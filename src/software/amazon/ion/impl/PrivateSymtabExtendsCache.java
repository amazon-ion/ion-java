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

package software.amazon.ion.impl;

import static software.amazon.ion.impl.PrivateUtils.symtabExtends;

import software.amazon.ion.SymbolTable;

/**
 * Cache to reduce unnecessary calls to
 * {@link PrivateUtils#symtabExtends(SymbolTable, SymbolTable)}. This is
 * only used if the writer is stream copy optimized.
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public final class PrivateSymtabExtendsCache
{
    private SymbolTable myWriterSymtab;
    private SymbolTable myReaderSymtab;
    private int myWriterSymtabMaxId;
    private int myReaderSymtabMaxId;
    private boolean myResult;

    public boolean symtabsCompat(SymbolTable writerSymtab,
                                 SymbolTable readerSymtab)
    {
        // If the refs. of both writer's and reader's symtab match and are
        // not modified, skip expensive symtab extends check and return
        // cached result.

        assert writerSymtab != null && readerSymtab != null:
            "writer's and reader's current symtab cannot be null";

        if (myWriterSymtab          == writerSymtab &&
            myReaderSymtab          == readerSymtab &&
            myWriterSymtabMaxId     == writerSymtab.getMaxId() &&
            myReaderSymtabMaxId     == readerSymtab.getMaxId())
        {
            // Not modified, return cached result
            return myResult;
        }

        myResult = symtabExtends(writerSymtab, readerSymtab);

        // Track refs.
        myWriterSymtab = writerSymtab;
        myReaderSymtab = readerSymtab;

        // Track modification
        myWriterSymtabMaxId = writerSymtab.getMaxId();
        myReaderSymtabMaxId = readerSymtab.getMaxId();

        return myResult;
    }
}