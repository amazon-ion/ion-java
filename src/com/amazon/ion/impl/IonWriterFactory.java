// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import java.io.OutputStream;

/**
 *  This is the factory class for constructing writers
 *  with various capabilities.
 */
public class IonWriterFactory
{
    /**
     * static short cut methods to construct IonWriters
     * quickly.
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

        IonWriterSystemTree system_writer =
            new IonWriterSystemTree(defaultSystemSymtab, catalog, container);

        IonWriter writer = new IonWriterUserTree(catalog, sys, system_writer);
        return writer;
    }


    public static IonWriterUserBinary newBinaryWriter(IonSystem system,
                                                      IonCatalog catalog,
                                                      boolean streamCopyOptimized,
                                                      OutputStream output,
                                                      SymbolTable... imports)
    {
        SymbolTable defaultSystemSymtab = system.getSystemSymbolTable();

        IonWriterSystemBinary system_writer =
            new IonWriterSystemBinary(defaultSystemSymtab, output,
                                      /* autoFlush */    false,
                                      /* ensureInitialIvm */ true);

        IonWriterUserBinary writer =
            new IonWriterUserBinary(catalog, system, system_writer,
                                    streamCopyOptimized, imports);
        return writer;
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
            new IonWriterSystemTree(defaultSystemSymtab, cat, container);
        return writer;
    }
}
