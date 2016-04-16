// Copyright (c) 2010-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonWriterBuilder.InitialIvmHandling;

/**
 * This is the factory class for constructing writers with various capabilities.
 *
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public final class PrivateIonWriterFactory
{
    /**
     * @param container must not be null.
     */
    public static IonWriter makeWriter(IonContainer container)
    {
        IonSystem sys = container.getSystem();
        IonCatalog cat = sys.getCatalog();
        IonWriter writer = makeWriter(cat, container);
        return writer;
    }

    /**
     * @param container must not be null.
     */
    public static IonWriter makeWriter(IonCatalog catalog,
                                       IonContainer container)
    {
        IonSystem sys = container.getSystem();
        SymbolTable defaultSystemSymtab = sys.getSystemSymbolTable();

        // TODO the SUPPRESS here is a nasty discontinuity with other places
        // that create this kind of reader.  It prevents the Lite DG system
        // iterator from returning two IVMs at the start of the data.
        // The Span tests detect that problem.
        IonWriterSystemTree system_writer =
            new IonWriterSystemTree(defaultSystemSymtab, catalog, container,
                                    InitialIvmHandling.SUPPRESS);

        return new IonWriterUser(catalog, sys, system_writer);
    }


    /**
     * @param container must not be null.
     */
    public static IonWriter makeSystemWriter(IonContainer container)
    {
        IonSystem sys = container.getSystem();
        IonCatalog cat = sys.getCatalog();
        SymbolTable defaultSystemSymtab = sys.getSystemSymbolTable();
        IonWriter writer =
            new IonWriterSystemTree(defaultSystemSymtab, cat, container,
                                    null /* initialIvmHandling */);
        return writer;
    }
}
