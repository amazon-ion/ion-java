/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl;

import static software.amazon.ion.SystemSymbols.ION_1_0_SID;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static software.amazon.ion.impl.PrivateUtils.newLocalSymtab;

import java.io.IOException;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.OffsetSpan;
import software.amazon.ion.SeekableReader;
import software.amazon.ion.Span;
import software.amazon.ion.SpanProvider;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.impl.PrivateScalarConversions.AS_TYPE;
import software.amazon.ion.impl.UnifiedInputStreamX.FromByteArray;
import software.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;

final class IonReaderBinaryUserX
    extends IonReaderBinarySystemX
    implements PrivateReaderWriter
{
    /**
     * This is the physical start-of-stream offset when this reader was created.
     * It must be subtracted from the logical offsets exposed by
     * {@link OffsetSpan}s.
     */
    private final int _physical_start_offset;

    IonCatalog  _catalog;

    private static final class IonReaderBinarySpan
        extends DowncastingFaceted
        implements Span, OffsetSpan
    {
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
    }

    public IonReaderBinaryUserX(IonSystem system,
                                IonCatalog catalog,
                                UnifiedInputStreamX userBytes,
                                int physicalStartOffset)
    {
        super(system, userBytes);
        _physical_start_offset = physicalStartOffset;
        init_user(catalog);
    }

    public IonReaderBinaryUserX(IonSystem system,
                                IonCatalog catalog,
                                UnifiedInputStreamX userBytes)
    {
        super(system, userBytes);
        _physical_start_offset = 0;
        init_user(catalog);
    }

    //FIXME: PERF_TEST was :private
    final void init_user(IonCatalog catalog)
    {
        _symbols = _system.getSystemSymbolTable();
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
     * @return the current position; not null.
     *
     * @throws IllegalStateException if the reader doesn't have a current
     * value.
     */
    public Span getCurrentPosition()
    {
        IonReaderBinarySpan pos = new IonReaderBinarySpan();

        if (getType() == null)
        {
            String message = "IonReader isn't positioned on a value";
            throw new IllegalStateException(message);
        }

        if (_position_start == -1)  // TODO remove? should be unreachable.
        {
            // special case of the position before the first call to next
            pos._offset = _input._pos;
            pos._limit = _input._limit;
            pos._symbol_table = _symbols;
        }
        else
        {
            pos._offset = _position_start - _physical_start_offset;
            pos._limit = pos._offset + _position_len;
            pos._symbol_table = _symbols;
        }

        return pos;
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
    boolean hasNext()
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
            if (_value_tid == PrivateIonConstants.tidSymbol) {
                if (load_annotations() == 0) {
                    // $ion_1_0 is read as an IVM only if it is not annotated
                    load_cached_value(AS_TYPE.int_value);
                    int sid = _v.getInt();
                    if (sid == ION_1_0_SID) {
                        _symbols = _system.getSystemSymbolTable();
                        push_symbol_table(_symbols);
                        _has_next_needed = true;
                    }
                }
            }
            else if (_value_tid == PrivateIonConstants.tidStruct) {
                int count = load_annotations();
                for(int ii=0; ii<count; ii++) {
                    if (_annotation_ids[ii] == ION_SYMBOL_TABLE_SID) {
                        _symbols =
                            newLocalSymtab(_system,
                                           // TODO should be current symtab:
                                           _system.getSystemSymbolTable(),
                                           _catalog,
                                           this,
                                           false);
                        push_symbol_table(_symbols);
                        _has_next_needed = true;
                        break;
                    }
                }
            }
            else {
                assert (_value_tid != PrivateIonConstants.tidTypedecl);
            }
        }
    }

    //
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

        // TODO amznlabs/ion-java#17 support seeking over InputStream
        if (_input instanceof FromByteArray)
        {
            if (facetType == SeekableReader.class)
            {
                return facetType.cast(new SeekableReaderFacet());
            }
        }

        if (facetType == PrivateByteTransferReader.class)
        {
            // This is a rather sketchy use of Facets, since the availability
            // of the facet depends upon the current state of this subject,
            // and that can change over time.

            // TODO amznlabs/ion-java#16 Our {@link #transferCurrentValue} doesn't handle
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
            return getCurrentPosition();
        }
    }


    private class SeekableReaderFacet
        extends SpanProviderFacet
        implements SeekableReader
    {
        public void hoist(Span span)
        {
            if (! (span instanceof IonReaderBinarySpan))
            {
                throw new IllegalArgumentException("Span isn't compatible with this reader.");
            }

            seek((IonReaderBinarySpan) span);
        }
    }


    private class ByteTransferReaderFacet implements PrivateByteTransferReader
    {
        public void transferCurrentValue(PrivateByteTransferSink sink)
            throws IOException
        {
            // Ensure there's a contiguous buffer we can copy.
            // TODO Copy from a stream should also be possible.
            if (! (_input instanceof UnifiedInputStreamX.FromByteArray))
            {
                throw new UnsupportedOperationException();
            }

            // TODO amznlabs/ion-java#16 wrong if current value has a field name or
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
