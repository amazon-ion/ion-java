/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonSystem;
import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonType;
import com.amazon.ion.OffsetSpan;
import com.amazon.ion.RawValueSpanProvider;
import com.amazon.ion.SeekableReader;
import com.amazon.ion.Span;
import com.amazon.ion.SpanProvider;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.impl.UnifiedInputStreamX.FromByteArray;
import com.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;
import com.amazon.ion.impl._Private_ScalarConversions.AS_TYPE;
import java.io.IOException;
import java.util.Iterator;

final class IonReaderBinaryUserX
    extends IonReaderBinarySystemX
    implements _Private_ReaderWriter
{
    /**
     * This is the physical start-of-stream offset when this reader was created.
     * It must be subtracted from the logical offsets exposed by
     * {@link OffsetSpan}s.
     */
    private final int _physical_start_offset;
    private final _Private_LocalSymbolTableFactory _lstFactory;

    IonCatalog  _catalog;

    private static class IonReaderBinarySpan
        extends DowncastingFaceted
        implements Span, OffsetSpan
    {
        private final boolean _isSeekable;

        public IonReaderBinarySpan(boolean isSeekable)
        {
            _isSeekable = isSeekable;
        }

        State       _state;
        long        _offset;
        long        _limit;
        SymbolTable _symbol_table;

        public long getStartOffset()
        {
            return _offset;
        }

        public long getFinishOffset()
        {
            return _limit;
        }

        public boolean isSeekable()
        {
            return _isSeekable;
        }

    }

    public IonReaderBinaryUserX(IonCatalog catalog,
                                _Private_LocalSymbolTableFactory lstFactory,
                                UnifiedInputStreamX userBytes,
                                int physicalStartOffset)
    {
        super(userBytes);
        _physical_start_offset = physicalStartOffset;
        init_user(catalog);
        _lstFactory = lstFactory;
    }

    //FIXME: PERF_TEST was :private
    final void init_user(IonCatalog catalog)
    {
        // TODO check IVM to determine version: amzn/ion-java#19, amzn/ion-java#24
        _symbols = SharedSymbolTable.getSystemSymbolTable(1);
        _catalog = catalog;
    }

    /**
     * Determines the abstract position of the reader, such that one can
     * later {@link #seek} back to it.
     * <p>
     * The current implementation only works when the reader is positioned on
     * a value (not before, between, or after values). In other words, one
     * should only call this method when {@link #getType()} is non-null.
     *
     * @param beforeTid -
     *          When true, the position returned starts before the
     *          type/length octet.
     *          When false, the position returned starts after the
     *          type/length octet and any optional length octets.
     *
     * @return the current position; not null.
     *
     * @throws IllegalStateException if the reader doesn't have a current
     * value.
     */
    public Span getCurrentPosition(boolean beforeTid)
    {
        if (getType() == null)
        {
            throw new IllegalStateException("IonReader isn't positioned on a value");
        }
        // Only spans that include the TID octet are seekable.
        IonReaderBinarySpan pos = new IonReaderBinarySpan(beforeTid);
        long start = beforeTid ?  _position_start : _value_start;
        long len = beforeTid ? _position_len : _value_len;
        pos._offset = start - _physical_start_offset;
        pos._limit = pos._offset + len;
        pos._symbol_table = _symbols;
        pos._state = _state;
        return pos;
    }

    public byte[] getCurrentBuffer()
    {
        return _input._bytes;
    }


    public void seek(IonReaderBinarySpan position)
    {
        IonReaderBinarySpan pos = position;

        if (pos == null)
        {
            throw new IllegalArgumentException("Position invalid for binary reader");
        }
        if (!(_input instanceof FromByteArray))
        {
            throw new UnsupportedOperationException("Binary seek not implemented for non-byte array backed sources");
        }

        // TODO test that span is within the bounds of the input byte[]

        // manually reset the input specific type of input stream
        FromByteArray input = (FromByteArray)_input;
        input._pos   = (int) (pos._offset + _physical_start_offset);
        input._limit = (int) (pos._limit  + _physical_start_offset);

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
        //           Don't do this, we'll re-read the data from the stream.
        //           Otherwise, this reader will be in the wrong state.
        //           For example, getType() will return non-null but that
        //           shouldn't happen until the user calls next().
//        _state         = pos._state;
//        _value_type    = pos._value_type;
//        _value_is_null = pos._value_is_null;
//        _value_is_true = pos._value_is_true;

//        _is_in_struct = false;
    }


    @Override
    public IonType next()
    {
        IonType t = null;
        if (hasNext()) {
            _has_next_needed = true;
            t = _value_type;
        }
        return t;
    }

    @Override
    public boolean hasNext()
    {
        if (!_eof && _has_next_needed) {
            clear_system_value_stack();
            try {
                while (!_eof && _has_next_needed) {
                    has_next_helper_user();
                }
            }
            catch (IOException e) {
                error(e);
            }
        }
        return !_eof;
    }


    private final void has_next_helper_user() throws IOException
    {
        super.hasNext();
        if (getDepth() == 0 && !_value_is_null) {
            if (_value_tid == _Private_IonConstants.tidSymbol) {
                if (load_annotations() == 0) {
                    // $ion_1_0 is read as an IVM only if it is not annotated
                    load_cached_value(AS_TYPE.int_value);
                    int sid = _v.getInt();
                    if (sid == ION_1_0_SID) {
                        _symbols = SharedSymbolTable.getSystemSymbolTable(1);
                        push_symbol_table(_symbols);
                        _has_next_needed = true;
                    }
                }
            }
            else if (_value_tid == _Private_IonConstants.tidStruct) {
                int count = load_annotations();
                if (count > 0 && _annotation_ids[0] == ION_SYMBOL_TABLE_SID) {
                    _symbols = _lstFactory.newLocalSymtab(_catalog, this, false);
                    push_symbol_table(_symbols);
                    _has_next_needed = true;
                }
            }
            else {
                assert (_value_tid != _Private_IonConstants.tidTypedecl);
            }
        }
    }

    private void validateSymbolToken(SymbolToken symbol) {
        if (symbol != null) {
            if (symbol.getText() == null && symbol.getSid() > getSymbolTable().getMaxId()) {
                throw new UnknownSymbolException(symbol.getSid());
            }
        }
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        SymbolToken[] annotations = super.getTypeAnnotationSymbols();
        for (SymbolToken annotation : annotations) {
            validateSymbolToken(annotation);
        }
        return annotations;
    }

    @Override
    public final SymbolToken getFieldNameSymbol() {
        SymbolToken fieldName = super.getFieldNameSymbol();
        validateSymbolToken(fieldName);
        return fieldName;
    }

    @Override
    public final SymbolToken symbolValue() {
        SymbolToken symbol = super.symbolValue();
        validateSymbolToken(symbol);
        return symbol;
    }

    //  This code handles the skipped symbol table
    //  support - it is cloned in IonReaderTextUserX,
    //  IonReaderBinaryUserX and _Private_IonWriterBase
    //
    //  SO ANY FIXES HERE WILL BE NEEDED IN THOSE
    //  THREE LOCATIONS AS WELL.
    //
    private int _symbol_table_top = 0;
    private SymbolTable[] _symbol_table_stack = new SymbolTable[3]; // 3 is rare, IVM followed by a local sym tab with open content
    private void clear_system_value_stack()
    {
        while (_symbol_table_top > 0) {
            _symbol_table_top--;
            _symbol_table_stack[_symbol_table_top] = null;
        }
    }
    private void push_symbol_table(SymbolTable symbols)
    {
        assert(symbols != null);
        if (_symbol_table_top >= _symbol_table_stack.length) {
            int new_len = _symbol_table_stack.length * 2;
            SymbolTable[] temp = new SymbolTable[new_len];
            System.arraycopy(_symbol_table_stack, 0, temp, 0, _symbol_table_stack.length);
            _symbol_table_stack = temp;
        }
        _symbol_table_stack[_symbol_table_top++] = symbols;
    }
    @Override
    public SymbolTable pop_passed_symbol_table()
    {
        if (_symbol_table_top <= 0) {
            return null;
        }
        _symbol_table_top--;
        SymbolTable symbols = _symbol_table_stack[_symbol_table_top];
        _symbol_table_stack[_symbol_table_top] = null;
        return symbols;
    }


    //========================================================================
    // Facet support


    @Override
    public <T> T asFacet(Class<T> facetType)
    {
        if (facetType == SpanProvider.class)
        {
            return facetType.cast(new SpanProviderFacet());
        }

        // TODO amzn/ion-java/issues/17 support seeking over InputStream
        if (_input instanceof FromByteArray)
        {
            if (facetType == SeekableReader.class)
            {
                return facetType.cast(new SeekableReaderFacet());
            }
            if (facetType == RawValueSpanProvider.class)
            {
                return facetType.cast(new RawValueSpanProviderFacet());
            }
        }

        if (facetType == _Private_ByteTransferReader.class)
        {
            // This is a rather sketchy use of Facets, since the availability
            // of the facet depends upon the current state of this subject,
            // and that can change over time.

            // TODO amzn/ion-java/issues/16 Our {@link #transferCurrentValue} doesn't handle
            //  field names and annotations.

            // Ensure there's a contiguous buffer we can copy.
            if (_input instanceof UnifiedInputStreamX.FromByteArray
                && getTypeAnnotationSymbols().length == 0
                && ! isInStruct())
            {
                return facetType.cast(new ByteTransferReaderFacet());
            }
        }

        return super.asFacet(facetType);
    }


    private class SpanProviderFacet implements SpanProvider
    {
        public Span currentSpan()
        {
            return getCurrentPosition(true);
        }
    }

    private class RawValueSpanProviderFacet implements RawValueSpanProvider
    {

        public Span valueSpan()
        {
            return getCurrentPosition(false);
        }

        public byte[] buffer()
        {
            return getCurrentBuffer();
        }

    }

    private class SeekableReaderFacet
        extends SpanProviderFacet
        implements SeekableReader
    {

        public void hoist(Span span)
        {
            if (! (span instanceof IonReaderBinarySpan) || !((IonReaderBinarySpan)span).isSeekable())
            {
                throw new IllegalArgumentException("Span isn't compatible with this reader.");
            }

            seek((IonReaderBinarySpan) span);
        }

    }


    private class ByteTransferReaderFacet implements _Private_ByteTransferReader
    {
        public void transferCurrentValue(_Private_ByteTransferSink sink)
            throws IOException
        {
            // Ensure there's a contiguous buffer we can copy.
            // TODO Copy from a stream should also be possible.
            if (! (_input instanceof UnifiedInputStreamX.FromByteArray))
            {
                throw new UnsupportedOperationException();
            }

            // TODO amzn/ion-java/issues/16 wrong if current value has a field name or
            //   annotations since the position is in the wrong place.
            // TODO when implementing that, be careful to handle the case where
            //   the writer already holds a pending field name or annotations!
            //   Meaning: the user has set it and then called writeValue().

            int inOffset = (int) _position_start;
            int inLen    = (int) _position_len;

            sink.writeBytes(_input._bytes, inOffset, inLen);
        }
    }
}
