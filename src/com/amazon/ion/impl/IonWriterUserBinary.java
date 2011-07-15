// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class IonWriterUserBinary
    extends IonWriterUser
{
    public static volatile boolean ourFastCopyEntabled = false;


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
    public void writeValue(IonReader reader) throws IOException
    {
        // TODO check reader state, is it on a value?
        // TODO Don't bother optimizing scalars (except symbol?)
        // TODO should only do this at outermost entry to the API,
        //  not recursively down the tree

        // Using positioned subclass so we know there's a contiguous buffer
        // we can copy.
        if (ourFastCopyEntabled
            && reader instanceof IonReaderBinaryUserX
            && _current_writer instanceof IonWriterSystemBinary
            && reader.getTypeAnnotationIds().length == 0   // TODO enable this
            && ! reader.isInStruct())                      // TODO enable this
        {
            IonReaderBinaryUserX binaryReader = (IonReaderBinaryUserX) reader;
            UnifiedInputStreamX binaryInput = binaryReader._input;
            if (binaryInput instanceof UnifiedInputStreamX.FromByteArray)
            {
                SymbolTable inSymbols = binaryReader.getSymbolTable();
                SymbolTable outSymbols = getSymbolTable();
                if (IonImplUtils.symtabExtends(outSymbols, inSymbols))
                {
                    int inOffset = binaryReader._position_start;
                    int inLen    = binaryReader._position_len;

                    // TODO what if there's a pending field name or annots
                    // on this writer?

                    // TODO Use a reader facet to generalize this.
                    IonWriterSystemBinary systemOut =
                        (IonWriterSystemBinary) _current_writer;
                    systemOut._writer.write(binaryInput._bytes, inOffset, inLen);

                    // TODO what happens if the reader has fieldname or annot?

                    systemOut.patch(inLen);

                    return;
                }
            }
        }

        super.writeValue(reader);
    }
}
