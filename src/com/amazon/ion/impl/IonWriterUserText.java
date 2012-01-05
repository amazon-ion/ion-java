// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import java.io.OutputStream;


/**
 *
 */
public class IonWriterUserText
    extends IonWriterUser
{
    @Deprecated
    static public class TextOptions extends _Private_TextOptions
    {
        public TextOptions(boolean prettyPrint, boolean printAscii)
        {
            super(prettyPrint, printAscii);
        }
        public TextOptions(boolean prettyPrint, boolean printAscii,
                           boolean filterOutSymbolTables)
        {
            super(prettyPrint, printAscii, filterOutSymbolTables);
        }
        public TextOptions(boolean prettyPrint, boolean printAscii,
                           boolean filterOutSymbolTables,
                           boolean suppressIonVersionMarker)
        {
            super(prettyPrint, printAscii, filterOutSymbolTables,
                  suppressIonVersionMarker);
        }
    }

    IonWriterUserText(IonCatalog catalog,
                      ValueFactory symtabValueFactory,
                      IonWriterSystemText systemWriter,
                      _Private_TextOptions options,
                      SymbolTable... imports)
    {
        super(catalog, symtabValueFactory, systemWriter,
              options.issuppressIonVersionMarkerOn(), imports);
    }


    /** @deprecated */
    @Deprecated
    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                OutputStream out, _Private_TextOptions options)
    {
        this(catalog, sys,
             new IonWriterSystemText(sys.getSystemSymbolTable(), out, options),
             options);
    }

    /** @deprecated */
    @Deprecated
    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                Appendable out, _Private_TextOptions options)
    {
        this(catalog, sys,
             new IonWriterSystemText(sys.getSystemSymbolTable(), out, options),
             options);
    }
}
