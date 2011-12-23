// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import java.io.IOException;



/**
 *
 */
class IonWriterUserTree
    extends IonWriterUser
{
    /**
     * really this constructor is here to verify that the
     * user writer is constructed with the right type of
     * system writer - a tree writer.
     * @param catalog may be null.
     * @param systemWriter a System Tree writer to back this.
     *   Must not be null.
     */
    protected IonWriterUserTree(IonCatalog catalog,
                                IonWriterSystemTree systemWriter)
    {
        super(catalog,
              systemWriter.get_root().getSystem(),
              systemWriter,
              systemWriter.get_root().getType() == IonType.DATAGRAM);

        // Datagrams have an implicit initial IVM
        _previous_value_was_ivm = true;
        // TODO what if container isn't a datagram?
    }


    @Override
    public void set_symbol_table_helper(SymbolTable new_symbols)
        throws IOException
    {
        // TODO Not always at datagram level here, which is buggy because that
        // means the depth is being computed incorrectly.

        // we do nothing here, the symbol tables will get picked up as
        // tree writer picks up symbol tables on the values as they
        // are appended to the parent value

        // oops, except when they don't come through as values because
        //       we're reading from a non-dom stream and writing what we see.

        // in the case of tree to tree action this call is benign since
        // the value being appended will have a symbol table on it, and
        // that table takes precedence (and should be the same in any event)
        if (_root_is_datagram)
        {
            if (new_symbols.isSystemTable())
            {
                // FIXME this doesn't work in Lite DOM
                // It adds the IVM as a user value.
                _system_writer.writeIonVersionMarker(new_symbols);
                _previous_value_was_ivm = true;
            }
            else
            {
                IonValue root = ((IonWriterSystemTree)_system_writer).get_root();
                ((_Private_IonDatagram)root).appendTrailingSymbolTable(new_symbols);
            }
        }
    }
}
