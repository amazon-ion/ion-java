// Copyright (c) 2013-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SubstituteSymbolTableException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import java.io.OutputStream;

/**
 * NOT FOR APPLICATION USE! Only supported for use by DMSDK libraries!
 *
 * TODO ION-393 Remove this class when these APIs are publicized.
 */
public class _Private_DmsdkUtils
{
    /**
     * Creates a mutable copy of this local symbol table. The cloned table
     * will be created in the context of the same {@link ValueFactory}.
     * <p>
     * Note that the resulting symbol table holds a distinct, deep copy of the
     * given table, adding symbols on either instances will not modify the
     * other.
     *
     * @param symtab
     *
     * @return a new mutable {@link SymbolTable} instance; not null
     *
     * @throws IllegalArgumentException
     *          if the given table is not a local symbol table
     * @throws SubstituteSymbolTableException
     *          if any imported table by the given local symbol table is a
     *          substituted table (whereby no exact match was found in its
     *          catalog)
     */
    // TODO ION-395 We need to think about providing a suitable recovery process
    //      or configuration for users to properly handle the case when the
    //      local symtab has substituted symtabs for imports.
    public static SymbolTable copyLocalSymbolTable(SymbolTable symtab)
        throws SubstituteSymbolTableException
    {
        if (! symtab.isLocalTable())
        {
            String message = "symtab should be a local symtab";
            throw new IllegalArgumentException(message);
        }

        SymbolTable[] imports =
            ((LocalSymbolTable) symtab).getImportedTablesNoCopy();

        // Iterate over each import, we assume that the list of imports
        // rarely exceeds 5.
        for (int i = 0; i < imports.length; i++)
        {
            if (imports[i].isSubstitute())
            {
                String message =
                    "local symtabs with substituted symtabs for imports " +
                    "(indicating no exact match within the catalog) cannot " +
                    "be copied";
                throw new SubstituteSymbolTableException(message);
            }
        }

        return ((LocalSymbolTable) symtab).makeCopy();
    }

    /**
     * Creates a new writer that will encode binary Ion data, using the given
     * local symbol table. While writing a number of values the symbol table
     * may be populated with any newly encountered symbols.
     * <p>
     * Note that the given table will not be replaced during processing.
     *
     * @param system
     * @param out
     * @param localSymtab
     *          the local symbol table to be used for encoding by this writer,
     *          may contain locally declared symbols and/or imports
     *
     * @return a new {@link IonWriter} instance; not null
     *
     * @throws IllegalArgumentException
     *          if the given table is not a local symbol table
     */
    public static IonWriter
    newBinaryWriterWithLocalSymbolTable(IonSystem system,
                                        OutputStream out,
                                        SymbolTable localSymtab)
    {
        if (! localSymtab.isLocalTable())
        {
            String message = "localSymtab should be a local symtab";
            throw new IllegalArgumentException(message);
        }

        // TODO ION-393 Currently, there is no check for substituted imports as
        //      we know that only DMSDK is calling copyLocalSymbolTable() above.
        //      When publicizing this API, the substituted imports check needs
        //      to be included.

        _Private_IonSystem sys = (_Private_IonSystem) system;

        _Private_IonBinaryWriterBuilder b =
            _Private_IonBinaryWriterBuilder.standard();
        b.setCatalog(system.getCatalog());
        b.setStreamCopyOptimized(sys.isStreamCopyOptimized());
        b.setSymtabValueFactory(system);
        b.setInitialSymtab(localSymtab);

        return b.build(out);
    }
}
