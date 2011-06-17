// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.SymbolTable;

import com.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;

import com.amazon.ion.impl.UnifiedInputStreamX.FromByteArray;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;

/**
 * Prototype impl of a relocatable reader.  This version only supports
 * binary Ion from an in memory byte buffer.
 *
 * It expects the position to be before a value (and after the field name
 * if the value was in a struct).
 *
 *
 */
public class IonReaderBinaryWithPosition_test
    extends IonReaderBinaryUserX
{
    public static class IonReaderPosition
    {
        int         _offset;
        int         _limit;
        SymbolTable _symbol_table;

        IonReaderPosition() {
        }
    }

    /**
     * @param system
     * @param catalog
     * @param bytes
     * @param offset
     * @param length
     */
    public IonReaderBinaryWithPosition_test(IonSystem system,
                                            IonCatalog catalog, byte[] bytes,
                                            int offset, int length)
    {
        super(system, catalog, bytes, offset, length);
    }

    public IonReaderPosition getCurrentPosition()
    {
        // check to see that the reader is in a valid position
        // to mark it
        //     - not in the middle of a value


        IonReaderPosition pos = new IonReaderPosition ();

 //       FromByteArray input = (FromByteArray)this._input;

        pos._offset = _position_start;
        pos._limit = _position_len + _position_start;
        pos._symbol_table = _symbols;

        return pos;
    }
    public void seek(IonReaderPosition position)
    {
        // manually reset the input specific type of input stream
        FromByteArray input = (FromByteArray)_input;
        input._pos = position._offset;
        input._limit = position._limit;

        // TODO: these (eof and save points) should be put into
        //       a re-init method on the input stream
        input._eof = false;
        for (;;) {
            SavePoint sp = input._save_points._active_stack;
            if (sp == null) break;
            input._save_points.savePointPopActive(sp);
            sp.free();
        }

        // reset the raw reader
        re_init_raw();

        // reset the system reader
        // - nothing to do

        // reset the user reader
        init_user(this._catalog);

        // now we need to set our symbol table
        _symbols = position._symbol_table;
        _is_in_struct = false;

        return;
    }
}
