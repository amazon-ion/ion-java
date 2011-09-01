// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonImplUtils.symtabExtends;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class IonWriterUserBinary
    extends IonWriterUser
{
    // TODO ION-209 finalize this stuff.
    public static final boolean OUR_FAST_COPY_DEFAULT = true;
    public static volatile boolean ourFastCopyEnabled = OUR_FAST_COPY_DEFAULT;


    // If we wanted to we could keep an extra reference to the
    // system writer which was correctly typed as an
    // IonBinaryWriter and avoid the casting in the 3 "overridden"
    // methods.  However those are sufficiently expensive that
    // the cost of the cast should be lost in the noise.

    protected IonWriterUserBinary(IonSystem system, IonCatalog catalog,
                                  IonWriterSystemBinary systemWriter,
                                  boolean suppressIVM)
    {
        super(system, systemWriter, catalog, suppressIVM);
    }


    protected OutputStream getOutputStream()
    {
        assert(_system_writer instanceof IonWriterSystemBinary);
        return ((IonWriterSystemBinary)_system_writer).getOutputStream();
    }

    @Override
    public void set_symbol_table_helper(SymbolTable prev_symbols,
                                        SymbolTable new_symbols)
        throws IOException
    {
        // for a binary writer we always write out symbol tables
        // writeUserSymbolTable(new_symbols);
        if (new_symbols.isSystemTable()) {
            // TODO: this will suppress multiple attempts to write
            //       a system symbol table - do we want that?  (usually
            //       we do, but always?)
            if (_after_ion_version_marker) {
                return;
            }
            // writing to the system writer keeps us from
            // recursing on the writeIonVersionMarker call
            _system_writer.writeIonVersionMarker();
            _after_ion_version_marker = true;
        }
        else {
            assert(new_symbols.isLocalTable());
            assert(_system_writer instanceof IonWriterSystemBinary);
            ((IonWriterSystemBinary)_system_writer).patchInSymbolTable(new_symbols);
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
            = ((IonWriterSystemBinary)_system_writer).inject_local_symbol_table();
        return symbols;
    }

    @Override
    public void writeValue(IonReader reader) // TODO ION-209 finish fast-copy
        throws IOException
    {
        // TODO check reader state, is it on a value?

        IonType type = reader.getType();
        // TODO Don't bother optimizing trivial scalars (except symbol?)

        // See if we can copy bytes directly from the source. This test should
        // only happen at the outermost call, not recursively down the tree.

        ByteTransferReader transfer = reader.asFacet(ByteTransferReader.class);

        if (ourFastCopyEnabled
            && transfer != null
            && _current_writer instanceof IonWriterSystemBinary
            && symtabExtends(getSymbolTable(), reader.getSymbolTable()))
        {
            IonWriterSystemBinary systemOut =
                (IonWriterSystemBinary) _current_writer;

            // TODO ION-241 Doesn't copy annotations or field names.
            transfer.transferCurrentValue(systemOut);

            return;
        }

        // From here on, we won't call back into this method, so we won't
        // bother doing all those checks again.
        writeValueSlowly(type, reader);
    }
}
