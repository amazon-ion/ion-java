// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.UnifiedSymbolTable.initialSymbolTable;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import java.io.IOException;
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

    protected IonWriterUserText(IonCatalog catalog,
                                IonSystem system,
                                _Private_TextOptions options,
                                IonWriterSystemText systemWriter,
                                SymbolTable... imports)
    {
        super(catalog, system, systemWriter, true /* rootIsDatagram */,
              options.issuppressIonVersionMarkerOn());

        if (imports != null && imports.length != 0)
        {
            SymbolTable initialSymtab = initialSymbolTable(system, imports);

            try {
                setSymbolTable(initialSymtab);
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }
    }


    /** @deprecated */
    @Deprecated
    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                OutputStream out, _Private_TextOptions options)
    {
        super(catalog, sys,
              new IonWriterSystemText(sys.getSystemSymbolTable(),
                                      out, options),
              true /* rootIsDatagram */,
              options.issuppressIonVersionMarkerOn());
    }

    /** @deprecated */
    @Deprecated
    protected IonWriterUserText(IonSystem sys, IonCatalog catalog,
                                Appendable out, _Private_TextOptions options)
    {
        super(catalog, sys,
              new IonWriterSystemText(sys.getSystemSymbolTable(),
                                      out, options),
              true /* rootIsDatagram */,
              options.issuppressIonVersionMarkerOn());
    }
}
