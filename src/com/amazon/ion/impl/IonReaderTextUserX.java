// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;

import com.amazon.ion.InternedSymbol;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.OffsetSpan;
import com.amazon.ion.SeekableReader;
import com.amazon.ion.Span;
import com.amazon.ion.SpanProvider;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.TextSpan;
import java.util.Iterator;

/**
 *    The text user reader add support for symbols and recognizes,
 *    and consumes (and processes), the system values $ion_1_0 and
 *    local symbol tables (tagged with $ion_symbol_table).
 *
 *    Should this materialize and "symbolate" all the symbol
 *    values as they come through?  - No.
 *
 *    Probably if we want the symbol id's to be the same for this
 *    reader as it is for other variants.  Hmmm, that's expensive
 *    when you don't need it (which is most of the time).
 *
 *    This will not auto-populate a symbol table.  In the event
 *    a symbol is a '$<digits>' symbol id symbol this will return
 *    that value.  If the string is present in the current symbol
 *    table it will return the id, which would be true if the
 *    symbol is a system symbol or if there is a local symbol
 *    table in the input stream.  Otherwise it return the
 *    undefined symbol value.
 *
 */
public class IonReaderTextUserX
    extends IonReaderTextSystemX
    implements IonReaderWriterPrivate, IonReaderWithPosition
{
    /**
     * This is the physical start-of-stream offset when this reader was created.
     * It must be subtracted from the logical offsets exposed by
     * {@link OffsetSpan}s.
     */
    private final int _physical_start_offset;

    // IonSystem   _system; now in IonReaderTextSystemX where it could be null
    IonCatalog  _catalog;
    SymbolTable _symbols;


    protected IonReaderTextUserX(IonSystem system, IonCatalog catalog,
                                 UnifiedInputStreamX uis,
                                 int physicalStartOffset)
    {
        super(system, uis);
        _physical_start_offset = physicalStartOffset;
        initUserReader(system, catalog);
    }

    protected IonReaderTextUserX(IonSystem system, IonCatalog catalog,
                                 UnifiedInputStreamX uis) {
        super(system, uis);
        _physical_start_offset = 0;
        initUserReader(system, catalog);
    }

    private void initUserReader(IonSystem system, IonCatalog catalog) {
        if (system == null) {
            throw new IllegalArgumentException();
        }
        _system = system;
        if (catalog != null) {
            _catalog = catalog;
        }
        else {
            _catalog = system.getCatalog();
        }
        // not needed, getSymbolTable will force this when necessary
        //  _symbols = system.getSystemSymbolTable();
    }

    @Override
    public IonSystem getSystem()
    {
        return _system;
    }

    /**
     * this looks forward to see if there is an upcoming value
     * and if there is it returns true.  It may have to clean up
     * any value that's partially complete (for example a
     * collection whose annotation has been read and loaded
     * but the use has chosen not to step into the collection).
     *
     * The user reader variant of hasNext also looks for system
     * values to process.  System values are the Ion version
     * marker ($ion_1_0) and local symbol tables.  If either of
     * these is encountered the symbol table processing will be
     * handled and the value will be "skipped".
     *
     * @return true if more data remains, false on eof
     */
    @Override
    public boolean hasNext()
    {
        boolean has_next = has_next_user_value();
        return has_next;
    }
    private final boolean has_next_user_value()
    {
        // clear out our previous value
        clear_system_value_stack();

        // changed to 'while' since consumed
        // values will not be counted
        while (!_has_next_called)
        {
            // first move to the next value regardless of whether
            // it's a system value or a user value
            has_next_raw_value();

            // system values are only at the datagram level
            // we don't care about them if they're buried
            // down in some other value - note that _value_type
            // will be null at eof and on as yet undetermined
            // numeric types (which are never system values)
            if (_value_type != null && !isNullValue() && IonType.DATAGRAM.equals(getContainerType())) {
                switch (_value_type) {
                case STRUCT:
                    if (_annotation_count > 0) {
                        for (int ii=0; ii<_annotation_count; ii++) {
                            String a = _annotations[ii];
                            if (ION_SYMBOL_TABLE.equals(a)) {
                                _symbols = makeNewLocalSymbolTable(_system,
                                                                   _system.getSystemSymbolTable(),
                                                                   _catalog,
                                                                   this,
                                                                   true);
                                push_symbol_table(_symbols);
                                _has_next_called = false;
                                break;
                            }
                        }
                    }
                    break;
                case SYMBOL:
                    InternedSymbol is = symbolValue();
                    String sym = is.getText();
                    if (ION_1_0.equals(sym)) {
                        symbol_table_reset();
                        push_symbol_table(_system.getSystemSymbolTable());
                        _has_next_called = false;
                    }
                    break;
                default:
                    break;
                }
            }
        }
        return (_eof != true);
    }
    private final void symbol_table_reset()
    {
        IonType t = next();
        assert( IonType.SYMBOL.equals(t) );
        _symbols = null; // was: _symbols.getSystemSymbolTable(); - the null is fixed in getSymbolTable()
        return;
    }

    @Override
    public int getFieldId()
    {
        // Superclass handles hoisting logic
        int id = super.getFieldId();
        if (id == SymbolTable.UNKNOWN_SYMBOL_ID)
        {
            String fieldname = getRawFieldName();
            if (fieldname != null)
            {
                SymbolTable symbols = getSymbolTable();
                id = symbols.findSymbol(fieldname);
            }
        }
        return id;
    }


    @Override
    public SymbolTable getSymbolTable()
    {
        if (_symbols == null) {
            SymbolTable system_symbols = _system.getSystemSymbolTable();
            _symbols = makeNewLocalSymbolTable(_system, system_symbols);
        }
        return _symbols;
    }

    @Override
    public int[] getTypeAnnotationIds()
    {
        int[]    ids;
        String[] syms = getTypeAnnotations();
        int      len  = syms.length;

        if (len == 0) {
            ids = IonImplUtils.EMPTY_INT_ARRAY;
        }
        else {
            SymbolTable symbols = getSymbolTable();
            ids  = new int[len];
            for (int ii=0; ii<len; ii++) {
                String sym = syms[ii];
                ids[ii] = symbols.findSymbol(sym);
            }
        }
        return ids;
    }

    @Override
    public Iterator<Integer> iterateTypeAnnotationIds()
    {
        int[] ids = getTypeAnnotationIds();
        return IonImplUtils.intIterator(ids);
    }


    //
    //  This code handles the skipped symbol table
    //  support - it is cloned in IonReaderTreeUserX
    //  and IonReaderBinaryUserX
    //
    //  SO ANY FIXES HERE WILL BE NEEDED IN THOSE
    //  TWO LOCATIONS AS WELL.
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


    static final class IonReaderTextPosition
        extends IonReaderPositionBase
        implements TextSpan, OffsetSpan
    {
        private final UnifiedDataPageX _data_page;
        private final IonType          _container_type;

        private final long             _start_offset;
        private final long             _start_line;
        private final long             _start_column;

        IonReaderTextPosition(IonReaderTextUserX reader)
        {
            // TODO: convert _start_char_offset from a long and data page
            //       to be an abstract reference into the Unified* data source

            UnifiedInputStreamX current_stream = reader._scanner.getSourceStream();
            //
            // TODO: this page isn't safe, except where we have only a single
            //       page of buffered input Which is the case for the time
            //       being.  Later, when this is stream aware, this needs to change.
            _data_page = current_stream._buffer.getCurrentPage();
            _container_type = reader.getContainerType();

            _start_offset = reader._value_start_offset - reader._physical_start_offset;
            _start_line   = reader._value_start_line;
            _start_column = reader._value_start_column;
        }

        public long getStartLine()
        {
            if (_start_line < 1) {
                throw new IllegalStateException("not positioned on a reader");
            }
            return _start_line;
        }

        public long getStartColumn()
        {
            if (_start_column < 0) {
                throw new IllegalStateException("not positioned on a reader");
            }
            return _start_column;
        }

        public long getFinishLine()
        {
            return -1;
        }

        public long getFinishColumn()
        {
            return -1;
        }

        public long getStartOffset()
        {
            return _start_offset;
        }

        public long getFinishOffset()
        {
            return -1;
        }

        IonType getContainerType() {
            return _container_type;
        }

        UnifiedDataPageX getDataPage() {
            return _data_page;
        }
    }


    public IonReaderPosition getCurrentPosition()
    {
        if (getType() == null) {
            throw new IllegalStateException("must be on a value");
        }
        IonReaderTextPosition pos = new IonReaderTextPosition(this);
        return pos;
    }

    private void hoistImpl(Span span)
    {
        if (!(span instanceof IonReaderTextPosition)) {
            throw new IllegalArgumentException("position must match the reader");
        }
        IonReaderTextPosition text_span = (IonReaderTextPosition)span;

        UnifiedInputStreamX current_stream = _scanner.getSourceStream();
        UnifiedDataPageX    curr_page      = text_span.getDataPage();
        int                 array_offset   = (int)text_span._start_offset + _physical_start_offset;
        int                 page_limit     = curr_page._page_limit;
        int                 array_length   = page_limit - array_offset;

        // we're going to cast this value down.  Since we only support
        // in memory single buffered chars here this is ok.
        assert(text_span.getStartOffset() <= Integer.MAX_VALUE);

        // Now - create a new stream
        // TODO: this is a pretty expensive way to do this. UnifiedInputStreamX
        //       needs to have a reset method added that can reset the position
        //       and length of the input to be some subset of the original source.
        //       This would avoid a lot of object creation (and wasted destruction.
        //       But this is a time-to-market solution here.  The change can be
        //       made as support for streams is added.
        UnifiedInputStreamX iis;
        if (current_stream._is_byte_data) {
            byte[] bytes = current_stream.getByteArray();
            assert(bytes != null);
            iis = UnifiedInputStreamX.makeStream(
                                            bytes
                                          , array_offset
                                          , array_length
                                      );
        }
        else {
            char[] chars = current_stream.getCharArray();
            assert(chars != null);
            iis = UnifiedInputStreamX.makeStream(
                                            chars
                                          , array_offset
                                          , array_length
                                      );
        }
        IonType container = text_span.getContainerType();
        re_init(iis, container, text_span._start_line, text_span._start_column);
    }

    public void seek(IonReaderPosition position)
    {
        hoistImpl(position);
    }


    //========================================================================


    @Override
    public <T> T asFacet(Class<T> facetType)
    {
        if (facetType == IonReaderWithPosition.class)
        {
            return facetType.cast(this);
        }

        if (facetType == SpanProvider.class)
        {
            return facetType.cast(new SpanProviderFacet());
        }

        if (facetType == SeekableReader.class && _scanner.isBufferedInput())
        {
            return facetType.cast(new SeekableReaderFacet());
        }

        return super.asFacet(facetType);
    }


    private class SpanProviderFacet
        implements SpanProvider
    {
        public Span currentSpan()
        {
            return getCurrentPosition();
        }
    }


    private final class SeekableReaderFacet
        extends SpanProviderFacet
        implements SeekableReader
    {
        public void hoist(Span span)
        {
            hoistImpl(span);
        }
    }
}
