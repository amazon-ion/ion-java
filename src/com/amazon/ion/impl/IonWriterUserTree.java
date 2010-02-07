// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonIterationType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import java.io.IOException;



/**
 *
 */
public class IonWriterUserTree
    extends IonWriterUser
{
    /**
     * really this constructor is here to verify that the
     * user writer is constructed with the right type of
     * system writer - a tree writer.
     *
     * @param systemWriter a System Tree writer to back this
     */
    protected IonWriterUserTree(IonWriterSystemTree systemWriter, IonCatalog catalog, boolean suppressIVM)
    {
        super(systemWriter, catalog, systemWriter.get_root(), suppressIVM);
    }

    @Override
    public IonIterationType getIterationType()
    {
        return IonIterationType.USER_ION_VALUE;
    }

    @Override
    public void set_symbol_table_helper(SymbolTable prev_symbols, SymbolTable new_symbols) throws IOException
    {
        // we do nothing here, the symbol tables will get picked up as
        // tree writer picks up symbol tables on the values as they
        // are appended to the parent value

        // oops, except when they don't come through as values because
        //       we're reading from a non-dom stream and writing what we see.

        // in the case of tree to tree action this call is benign since
        // the value being appended will have a symbol table on it, and
        // that table takes precedence (and should be the same in any event)
        if (_root_is_datagram) {
            assert(_system_writer instanceof IonWriterSystemTree);
            IonValue root = ((IonWriterSystemTree)_system_writer).get_root();
            assert(root instanceof IonValuePrivate);
            assert(root instanceof IonDatagram);
            ((IonValuePrivate)root).setSymbolTable(new_symbols);
        }
    }

    @Override
    UnifiedSymbolTable inject_local_symbol_table() throws IOException
    {
        // no catalog since it doesn't matter as this is a
        // pure local table, with no imports
        // we let the system writer handle this work
        assert(_system_writer instanceof IonWriterSystemBinary);
        UnifiedSymbolTable symbols
            = ((IonWriterSystemTree)_system_writer).inject_local_symbol_table();
        return symbols;
    }
}
