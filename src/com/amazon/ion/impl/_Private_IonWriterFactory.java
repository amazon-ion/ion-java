// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_Utils.initialSymtab;

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.BlockedBuffer.BufferedOutputStream;
import com.amazon.ion.system.IonWriterBuilder.InitialIvmHandling;
import java.io.OutputStream;

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * This is the factory class for constructing writers with various capabilities.
 */
public final class _Private_IonWriterFactory
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


    @SuppressWarnings("deprecation")
    public static IonBinaryWriter
    newIonBinaryWriterWithImports(IonSystem system,
                                  IonCatalog catalog,
                                  boolean streamCopyOptimized,
                                  SymbolTable... imports)
    {
        SymbolTable defaultSystemSymtab = system.getSystemSymbolTable();

        IonWriterSystemBinary systemWriter =
            new IonWriterSystemBinary(defaultSystemSymtab,
                                      new BufferedOutputStream(),
                                      false /* autoflush */,
                                      true /* ensureInitialIvm */);

        SymbolTable initialSymtab =
            initialSymtab(system, defaultSystemSymtab, imports);

        return new _Private_IonBinaryWriterImpl(catalog,
                                                defaultSystemSymtab,
                                                system,
                                                systemWriter,
                                                streamCopyOptimized,
                                                initialSymtab);
    }


    public static IonWriterUserBinary
    newBinaryWriterWithImports(IonSystem system,
                               IonCatalog catalog,
                               boolean streamCopyOptimized,
                               OutputStream output,
                               SymbolTable... imports)
    {
        SymbolTable defaultSystemSymtab = system.getSystemSymbolTable();

        SymbolTable initialSymtab =
            initialSymtab(system, defaultSystemSymtab, imports);

        return newBinaryWriterWithInitialSymtab(system,
                                                catalog,
                                                streamCopyOptimized,
                                                output,
                                                defaultSystemSymtab,
                                                initialSymtab);
    }


    public static IonWriterUserBinary
    newBinaryWriterWithInitialSymtab(IonSystem system,
                                     IonCatalog catalog,
                                     boolean streamCopyOptimized,
                                     OutputStream output,
                                     SymbolTable defaultSystemSymtab,
                                     SymbolTable initialSymtab)
    {
        IonWriterSystemBinary systemWriter =
            new IonWriterSystemBinary(defaultSystemSymtab,
                                      output,
                                      false /* autoFlush */,
                                      true /* ensureInitialIvm */);

        IonWriterUserBinary writer =
            new IonWriterUserBinary(catalog, 
                                    system, 
                                    systemWriter,
                                    streamCopyOptimized, 
                                    initialSymtab);
        return writer;
    }


    public static IonWriterUser
    newTextWriterWithImports(IonSystem system,
                             IonCatalog catalog,
                             _Private_IonTextWriterBuilder options,
                             _Private_IonTextAppender output,
                             SymbolTable... imports)
    {
        SymbolTable defaultSystemSymtab = system.getSystemSymbolTable();

        IonWriterSystemText systemWriter =
            new IonWriterSystemText(defaultSystemSymtab, options, output);

        SymbolTable initialSymtab =
            initialSymtab(system, defaultSystemSymtab, imports);

        return new IonWriterUser(catalog, system, systemWriter, initialSymtab);
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
