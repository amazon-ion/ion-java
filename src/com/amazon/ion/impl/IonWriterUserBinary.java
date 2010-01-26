// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonIterationType;
import com.amazon.ion.SymbolTable;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class IonWriterUserBinary
    extends IonWriterUser
{
    // If we wanted to we could keep an extra reference to the
    // system writer which was correctly typed as an
    // IonBinaryWriter and avoid the casting in the 3 "overridden"
    // methods.  However those are sufficiently expensive that
    // the cost of the cast should be lost in the noise.

    protected IonWriterUserBinary(IonSystem system, IonWriterSystemBinary systemWriter)
    {
        super(systemWriter, null);
    }

    @Override
    public IonIterationType getIterationType()
    {
        return IonIterationType.USER_BINARY;
    }

    protected OutputStream getOutputStream()
    {
        assert(_system_writer instanceof IonWriterSystemBinary);
        return ((IonWriterSystemBinary)_system_writer).getOutputStream();
    }

    @Override
    public void setSymbolTable(SymbolTable symbols) throws IOException
    {
        assert(_system_writer instanceof IonWriterSystemBinary);
        IonWriterSystemBinary swriter = ((IonWriterSystemBinary)_system_writer);

        if (!swriter.atDatagramLevel()) {
            throw new IllegalStateException("symbol table can only be set at a top level");
        }
        if (symbols.isSystemTable()) {
            // for the public API this turns into an Item Version Marker
            if (!_after_ion_version_marker) {
                swriter.writeIonVersionMarker();
            }
            swriter.setSymbolTable(symbols);
        }
        else if (symbols.isLocalTable()) {
            swriter.set_symbol_table_and_patch(symbols);
        }
        else {
            throw new IllegalArgumentException("symbol table must be a system or a local table");
        }

    }

}
