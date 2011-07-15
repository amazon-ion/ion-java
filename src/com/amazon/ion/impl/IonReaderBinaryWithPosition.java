// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.UnifiedInputStreamX.FromByteArray;
import com.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;
import java.io.InputStream;

/**
 * Prototype impl of a relocatable reader.  This version only supports
 * binary Ion from an in memory byte buffer.
 *
 * It expects the position to be before a value (and after the field name
 * if the value was in a struct).
 *
 *
 */
public class IonReaderBinaryWithPosition
    extends IonReaderBinaryUserX
    implements IonReaderWithPosition
{
    public static class IonReaderBinaryPosition extends IonReaderPositionBase implements IonReaderOctetPosition
    {
        State       _state;
        int         _offset;
        int         _limit;
        IonType     _value_type;
        boolean     _value_is_null;
        boolean     _value_is_true;
        SymbolTable _symbol_table;

        public long getOffset()
        {
            return _offset;
        }

        public long getLength()
        {
            return _limit - _offset;
        }


    }

    public IonReaderBinaryWithPosition(IonSystem system,
                                       IonCatalog catalog, byte[] bytes,
                                       int offset, int length)
    {
        super(system, catalog, bytes, offset, length);
    }

    public IonReaderBinaryWithPosition(IonSystem system,
                                       IonCatalog catalog,
                                       InputStream input)
    {
        super(system, catalog, input);
    }

    /**
     * Determines the abstract position of the reader, such that one can
     * later {@link #seek} back to it.
     * <p>
     * The current implementation only works when the reader is positioned on
     * a value (not before, between, or after values). In other words, one
     * should only call this method when {@link #getType()} is non-null.
     *
     * @return the current position; not null.
     *
     * @throws IllegalStateException if the reader doesn't have a current
     * value.
     */
    public IonReaderPosition getCurrentPosition()
    {
        // check to see that the reader is in a valid position
        // to mark it
        //     - not in the middle of a value
        //        TODO what does that mean?

        IonReaderBinaryPosition pos = new IonReaderBinaryPosition();

        if (getType() == null)
        {
            String message = "IonReader isn't positioned on a value";
            throw new IllegalStateException(message);
        }

        if (_position_start == -1)
        {
            // special case of the position before the first call to next
            pos._offset = _input._pos;
            pos._limit = _input._limit;
            pos._symbol_table = _symbols;
        }
        else
        {
            pos._offset = _position_start;
            pos._limit = _position_len + _position_start;
            pos._symbol_table = _symbols;
        }

        pos._state         = _state;
        pos._value_type    = _value_type;
        pos._value_is_null = _value_is_null;
        pos._value_is_true = _value_is_true;

        return pos;
    }


    public void seek(IonReaderPosition position)
    {
        IonReaderBinaryPosition pos = position.asFacet(IonReaderBinaryPosition.class);

        if (pos == null)
        {
            throw new IllegalArgumentException("Position invalid for binary reader");
        }
        if (!(_input instanceof FromByteArray))
        {
            throw new UnsupportedOperationException("Binary seek not implemented for non-byte array backed sources");
        }

        // manually reset the input specific type of input stream
        FromByteArray input = (FromByteArray)_input;
        input._pos = pos._offset;
        input._limit = pos._limit;

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
        _symbols = pos._symbol_table;

        // and the other misc state variables we had
        // read past before getPosition gets called
        _state         = pos._state;
        _value_type    = pos._value_type;
        _value_is_null = pos._value_is_null;
        _value_is_true = pos._value_is_true;

        _is_in_struct = false;
    }
}
