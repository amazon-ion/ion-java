// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.PrivateUtils.symtabExtends;

import com.amazon.ion.SymbolTable;

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