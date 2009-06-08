/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonImplUtils.EMPTY_ITERATOR;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.Timestamp;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * implements an Ion iterator over a Ion binary buffer.
 */
public final class IonBinaryReader
    implements IonReader
{
    // TODO------------------------------------------------------------------------------------------
    static final boolean _verbose_debug = false;
    static String getTidAsString(int tid) {
        IonType event = null;
        switch (tid) {
            case IonConstants.tidNull:      // 0
                event = IonType.NULL;
                break;
            case IonConstants.tidBoolean:   // 1
                event = IonType.BOOL;
                break;
            case IonConstants.tidPosInt:    // 2
            case IonConstants.tidNegInt:    // 3
                event = IonType.INT;
                break;
            case IonConstants.tidFloat:     // 4
                event = IonType.FLOAT;
                break;
            case IonConstants.tidDecimal:   // 5
                event = IonType.DECIMAL;
                break;
            case IonConstants.tidTimestamp: // 6
                event = IonType.TIMESTAMP;
                break;
            case IonConstants.tidSymbol:    // 7
                event = IonType.SYMBOL;
                break;
            case IonConstants.tidString:    // 8
                event = IonType.STRING;
                break;
            case IonConstants.tidClob:      // 9
                event = IonType.CLOB;
                break;
            case IonConstants.tidBlob:      // 10 A
                event = IonType.BLOB;
                break;
            case IonConstants.tidList:      // 11 B
                event = IonType.LIST;
                break;
            case IonConstants.tidSexp:      // 12 C
                event = IonType.SEXP;
                break;
            case IonConstants.tidStruct:    // 13 D
                event = IonType.STRUCT;
                break;
        }
        return event+"";
    }


    SimpleByteBuffer    _buffer;
    ByteReader          _reader;
    IonCatalog          _catalog;
    SymbolTable         _current_symtab;

    /**
     * When we encounter a local symtab while returning system values, we first
     * load the symtab, then back up and allow the user to read the symtab's
     * data. In the meanwhile, we stash the loaded symtab here then move it
     * into {@link #_current_symtab} after the data has been scanned again.
     */
    private SymbolTable _pending_symtab;

    int                 _parent_tid;  // using -1 for eof (or bof aka undefined) and 16 for datagram

    final boolean _is_returning_system_values;
    boolean _eof;
    int     _local_end;

    // static final int TID_DATAGRAM       = IonConstants.tidDATAGRAM; // 16;

    static final int S_INVALID          = 0;
    static final int S_BEFORE_TID       = 1;
    static final int S_AFTER_TID        = 2;
    static final int S_BEFORE_CONTENTS  = 3;

    int     _state; // 0=before tid, 1=after tid, 2=before contents

    int     _annotation_start;
    IonType _value_type;
    int     _value_field_id;
    int     _value_tid;
    int     _value_len;

    // local stack for stepInto() and stepOut()
    int         _top;
    int[]       _next_position_stack;
    int[]       _parent_tid_stack;
    int[]       _local_end_stack;
    SymbolTable[] _symbol_stack;



    public IonBinaryReader(byte[] buf, int start, int len,
                           IonCatalog catalog,
                           boolean returnSystemValues)
    {
        this(new SimpleByteBuffer(buf, start, len, true /*isReadOnly*/),
             IonType.DATAGRAM,
             catalog,
             returnSystemValues);
    }

    public IonBinaryReader(byte[] buf, IonCatalog catalog)
    {
        this ( buf, 0, buf.length, catalog );
    }
    public IonBinaryReader(byte[] buf, int start, int len, IonCatalog catalog)
    {
        this(new SimpleByteBuffer(buf, start, len, true /*isReadOnly*/),
             IonType.DATAGRAM,
             catalog,
             false);
    }
    //
    // sometime (in the far distant future) we might want to allow the caller to
    // pass in the parent of this value - but not now
    //
    //IonBinaryIterator(IonType parent, UnifiedCatalog catalog, byte[] buf, int start, int len)
    //{
    //    this( new SimpleByteBuffer(buf, start, len)
    //      , parent
    //        , catalog
    //    );
    //}


    private IonBinaryReader(SimpleByteBuffer ssb,
                            IonType parent,
                            IonCatalog catalog,
                            boolean returnSystemValues)
    {
        _is_returning_system_values = returnSystemValues;
        _buffer = ssb;
        _reader = _buffer.getReader();
        _local_end = _buffer._eob;
        _state = S_BEFORE_TID;
        _catalog = catalog;
        _current_symtab = UnifiedSymbolTable.getSystemSymbolTableInstance();
        _parent_tid = (parent == IonType.DATAGRAM
                           ? IonConstants.tidDATAGRAM
                           : get_tid_from_ion_type(parent));
    }

    final boolean is_at_top_level() {
        boolean expected = (_parent_tid == IonConstants.tidDATAGRAM);
        return expected;
    }

    final boolean is_in_struct() {
        boolean is_struct = (_parent_tid == IonConstants.tidStruct);
        return is_struct;
    }

    public boolean hasNext()
    {
        if (_eof) return false;
        if (_state == S_AFTER_TID) return true;

        if (_state == S_BEFORE_CONTENTS) {
            // if we stepped into the value with next() and then
            // decided never read the value itself we have to skip
            // over the value contents here
            _reader.skip(_value_len);  // FIXME _state = S_BEFORE_TID
        }

        int value_start = _reader.position();
        if (value_start >= this._local_end) {
            _eof = true;
            return false;
        }

        int td = -1;
        _value_field_id = -1;
        try {
            if (is_in_struct()) {
                _value_field_id = _reader.readVarUInt();
            }
            else if (is_at_top_level() && _pending_symtab != null) {
                _current_symtab = _pending_symtab;
                _pending_symtab = null;
            }

            // the "Possible" routines return -1 if they
            // found, and consumed, interesting data. And
            // if they did we need to read another byte
            while (td == -1) {
                td = _reader.read();
                if (td == ByteReader.EOF) {
                    _eof = true;
                    return false;
                }
                if (is_at_top_level()) {
                    // first check for the magic cookie - especially since the first byte
                    // says this is an annotation (with an, otherwise, invalid length of zero)
                    final int marker_byte_1 = (IonConstants.BINARY_VERSION_MARKER_1_0[0] & 0xff);
                    if (td == marker_byte_1) {
                        td = processPossibleMagicCookie(td);
                    }
                    else if (((td >>> 4) & 0xf) == IonConstants.tidTypedecl) {
                        td = processPossibleSymbolTable(td);
                    }
                }
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        // mark where we are
        _value_tid = td;
        _state = IonBinaryReader.S_AFTER_TID;
        return true;
    }

    public IonType next()
    {
        // get actual type id, this also handle the hasNext & eof logic as necessary
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        int tid = this.nextTid();
        _value_type = get_iontype_from_tid(tid);
        return _value_type;
    }

    private int nextTid()
    {
        if (_state != IonBinaryReader.S_AFTER_TID && !this.hasNext()) {
                throw new NoSuchElementException();
        }

        // get actual type id
        int tid = (_value_tid & 0xf0) >>> 4;

        try {
            _value_len = read_length(_value_tid);

            if (tid == IonConstants.tidTypedecl) {

                //      skip forward annotation length

                //      first we skip the value length and then
                //      read the local annotation length
                if (_value_len == 0) {
                    // This is a version marker (or broken data)
                    if (! is_at_top_level()) {
                        // TODO test this
                        throw new IonException("invalid type descriptor byte encountered at "+_reader.position());
                    }

                    // If we're seeing $E0 at this point, we've already matched
                    // the next three bytes
                    assert _is_returning_system_values;

                    // This isn't a real annotation.
                    _annotation_start = -1;
                }
                else {
                    //      set annotation start to the position of
                    //      the first type desc byte
                    this._annotation_start = _reader.position();
                    int annotation_len = _reader.readVarUInt();
                    _reader.skip(annotation_len);

                    // Read the "real" tid nested inside the annotation wrapper
                    _value_tid = _reader.read();
                    tid = (_value_tid & 0xf0) >>> 4;
                    if (tid == IonConstants.tidTypedecl) {
                        // TODO test this
                        throw new IonException("invalid nested annotations encountered at "+_reader.position());
                    }
                    _value_len = read_length(_value_tid);
                }
            }
            else {
                // clear the annotation marker that's left over from our previous value
                _annotation_start = -1;
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        // set the state forward
        _state = IonBinaryReader.S_BEFORE_CONTENTS;
        return tid;
    }

    private final int get_tid_from_ion_type(IonType t) {
        int tid;
        switch (t) {
        case NULL:
            tid = IonConstants.tidNull;
            break;
        case BOOL:
            tid = IonConstants.tidBoolean;
            break;
        case INT:
            tid = IonConstants.tidPosInt; // IonConstants.tidNegInt
            break;
        case FLOAT:
            tid = IonConstants.tidFloat;
            break;
        case DECIMAL:
            tid = IonConstants.tidDecimal;
            break;
        case TIMESTAMP:
            tid = IonConstants.tidTimestamp;
            break;
        case SYMBOL:
            tid = IonConstants.tidSymbol;
            break;
        case STRING:
            tid = IonConstants.tidString;
            break;
        case CLOB:
            tid = IonConstants.tidClob;
            break;
        case BLOB:
            tid = IonConstants.tidBlob;
            break;
        case LIST:
            tid = IonConstants.tidList;
            break;
        case SEXP:
            tid = IonConstants.tidSexp;
            break;
        case STRUCT:
            tid = IonConstants.tidStruct;
            break;
        default:
            tid = -1;
        }
        return tid;
    }

    private final IonType get_iontype_from_tid(int tid)
    {
        IonType t = null;
        switch (tid) {
        case IonConstants.tidNull:      // 0
            t = IonType.NULL;
            if ((_value_tid & 0xf) != IonConstants.lnIsNull) {
                throw new IonException("invalid type descriptor byte encountered at "+_reader.position());
            }
            break;
        case IonConstants.tidBoolean:   // 1
            t = IonType.BOOL;
            break;
        case IonConstants.tidPosInt:    // 2
        case IonConstants.tidNegInt:    // 3
            t = IonType.INT;
            break;
        case IonConstants.tidFloat:     // 4
            t = IonType.FLOAT;
            break;
        case IonConstants.tidDecimal:   // 5
            t = IonType.DECIMAL;
            break;
        case IonConstants.tidTimestamp: // 6
            t = IonType.TIMESTAMP;
            break;
        case IonConstants.tidSymbol:    // 7
            t = IonType.SYMBOL;
            break;
        case IonConstants.tidString:    // 8
            t = IonType.STRING;
            break;
        case IonConstants.tidClob:      // 9
            t = IonType.CLOB;
            break;
        case IonConstants.tidBlob:      // 10 A
            t = IonType.BLOB;
            break;
        case IonConstants.tidList:      // 11 B
            t = IonType.LIST;
            break;
        case IonConstants.tidSexp:      // 12 C
            t = IonType.SEXP;
            break;
        case IonConstants.tidStruct:    // 13 D
            t = IonType.STRUCT;
            break;
        case IonConstants.tidTypedecl:  // 14 E
            if (is_at_top_level()) {
                assert (_value_tid == 0xE0);
                t = IonType.SYMBOL;
                break;
            }
            // else fall through to fail...
        default:
            throw new IonException("unrecognized value type encountered: "+tid);
        }
        return t;
    }

    int read_length(int tid) throws IOException {
        switch ((tid >>> 4) & 0xf) {
        case IonConstants.tidNull:      // 0
        case IonConstants.tidBoolean:   // 1
            return 0;
        case IonConstants.tidStruct:    // 13 D
            if ((tid & 0xf) == 1) {
                // if this is an ordered struct (with the magic length
                // of 1) then fake it as a var length (which is actually is)
                tid = (tid & 0xf0) | 14;
            }
        case IonConstants.tidPosInt:    // 2
        case IonConstants.tidNegInt:    // 3
        case IonConstants.tidFloat:     // 4
        case IonConstants.tidDecimal:   // 5
        case IonConstants.tidTimestamp: // 6
        case IonConstants.tidSymbol:    // 7
        case IonConstants.tidString:    // 8
        case IonConstants.tidClob:      // 9
        case IonConstants.tidBlob:      // 10 A
        case IonConstants.tidList:      // 11 B
        case IonConstants.tidSexp:      // 12 C
        case IonConstants.tidTypedecl:  // 14 E
            int len = tid & 0xf;
            if (len == 14) {
                len = _reader.readVarUInt();
            }
            else if (len == 15) {  // FIXME This is actually invalid
                len = 0;
            }
            return len;
        default:
            throw new IonException("unrecognized type encountered");
        }
    }

    int processPossibleMagicCookie(int td) {
        int original_position = _reader.position();
        byte[] cookie = new byte[IonConstants.BINARY_VERSION_MARKER_SIZE - 1];

        try {
            int vlen = _reader.read(cookie, 0, cookie.length);
            if (vlen == cookie.length) {
                boolean is_magic = true;
                // note the "off by 1" difference in the indices, we already read
                // the first byte of the cookie before calling this method, so we
                // only have to check the remaining bytes
                if      ((cookie[0] & 0xff) != (IonConstants.BINARY_VERSION_MARKER_1_0[1] & 0xff)) {
                    is_magic = false;
                }
                else if ((cookie[1] & 0xff) != (IonConstants.BINARY_VERSION_MARKER_1_0[2] & 0xff)) {
                    is_magic = false;
                }
                else if ((cookie[2] & 0xff) != (IonConstants.BINARY_VERSION_MARKER_1_0[3] & 0xff)) {
                    is_magic = false;
                }
                if (is_magic) {
                    // there's magic here!  start over with
                    // a fresh new symbol table!
                    this.resetSymbolTable();
                    _state = IonBinaryReader.S_BEFORE_TID;
                }

                if (is_magic && !_is_returning_system_values) {
                    // Caller should skip this value and return the next one.
                    td = -1;
                }
                else {
                    // Caller should re-read this value and return it.
                    _reader.position(original_position);
                }
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return td;
    }

    /**
     * Looks at the current value, checks to see if it has the
     * $ion_symbol_table annotation and if it does load the symbol table and
     * move forward, otherwise just read the actual values td and
     * return that instead.  If it's not a symbol table, then the 14
     * (user type annotation) will be handled during "next()"
     *
     * @param typedesc
     *
     * @return -1 if we've skipped a local symtab, so the caller should read
     * the next typedesc.
     */

    int processPossibleSymbolTable(int typedesc) {
        int original_position = _reader.position();
        boolean is_symbol_table = false;

        try {
            @SuppressWarnings("unused")
            int vlen = read_length(typedesc);   // we have to read past the overall value length first
            int alen = _reader.readVarUInt();   // now we have the length of the annotations themselves
            int aend = _reader.position() + alen;
            // this is reading the annotations the preceed this value
            // until it finds a symbol table annotation or runs out of
            // annotations - while there should be only 1 annotation
            // that isn't a requirement.
            // loadLocalSymbolTableIfStruct will check to see if the
            // value is a struct or not
            while (_reader.position() < aend && !is_symbol_table) {
                int a = _reader.readVarUInt();
                if (a == SystemSymbolTable.ION_SYMBOL_TABLE_SID) {
                    is_symbol_table = loadLocalSymbolTableIfStruct(aend);
                }
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        if (is_symbol_table && !_is_returning_system_values) {
            // Caller should hide this value and continue with the next.
            // Make our caller read the upcoming byte
            typedesc = -1;
        }
        else {
            // Caller should return this value to the user.
            _reader.position(original_position);
        }

        return typedesc;
    }

    boolean loadLocalSymbolTableIfStruct(int contents_start)
        throws IOException
    {
        boolean is_symbol_table = false;

        _reader.position(contents_start);

        int td = _reader.read();

        if (((td >>> 4) & 0xf) == IonConstants.tidStruct) {
            //boolean was_struct = this._in_struct;
            int     prev_parent_tid = this._parent_tid;  // TODO Always DATAGRAM
            int     prev_end = this._local_end;

            this._value_tid = td;
            //this._in_struct = true;
            this._parent_tid = IonConstants.tidStruct;

            int len = this.read_length(td);
            this._local_end = this._reader.position() + len;

            if (this._local_end >= prev_end) {
                _state = IonBinaryReader.S_INVALID;
                throw new IonException("invalid binary format");
            }

            SymbolTable systemSymbols = _current_symtab.getSystemSymbolTable();
            UnifiedSymbolTable local =
                new UnifiedSymbolTable(systemSymbols, this, _catalog);

            _eof = false; // set by hasNext() on the last field in the symtab

            // we've read it, it must be a symbol table
            is_symbol_table = true;

            assert local.isLocalTable();

            if (_is_returning_system_values) {
                this._pending_symtab = local;
            }
            else {
                this._current_symtab = local;
            }

            this._parent_tid = prev_parent_tid;
            this._local_end  = prev_end;

            int value_start = _reader.position();
            if (value_start >= this._local_end) {
                _eof = true;
            }
        }
        return is_symbol_table;
    }

    public int getDepth() {
        return _top;
    }

    public void stepIn()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidSexp
         && tid != IonConstants.tidList
         && tid != IonConstants.tidStruct
        ) {
            throw new IllegalStateException();
        }

        // first make room, if we need to
        if (_next_position_stack == null || _next_position_stack.length <= _top) {
            grow();
        }

        // here we can push the stack and start at the
        // beginning of the collections values

        // when we step back out we'll be just before our
        // siblings type desc byte
        int next_start = _reader.position() + _value_len;
        _next_position_stack[_top] = next_start;
        _parent_tid_stack[_top] = _parent_tid;
        // _in_struct_stack[_top] = _in_struct;
        _local_end_stack[_top] = _local_end;
        _symbol_stack[_top] = _current_symtab;
        _top++;

        // now we set up for this collections contents
        // _in_struct = (tid == IonConstants.tidStruct);
        // _expect_symbol_table = (tid == IonConstants.tidSexp);
        _local_end = next_start;
        _state = S_BEFORE_TID;
        _parent_tid = (_value_tid >> 4) & 0xf;
    }
    private void grow() {
        int newlen = (_next_position_stack == null) ? 10 : (_next_position_stack.length * 2);
        int [] temp1 = new int[newlen];
        int [] temp2 = new int[newlen];
        int [] temp3 = new int[newlen];
        SymbolTable [] temp4 = new SymbolTable[newlen];
        if (_top > 1) {
            System.arraycopy(_next_position_stack, 0, temp1, 0, _top);
            System.arraycopy(_parent_tid_stack, 0, temp2, 0, _top);
            //System.arraycopy(_in_struct_stack, 0, temp2, 0, _top);
            System.arraycopy(_local_end_stack, 0, temp3, 0, _top);
            System.arraycopy(_symbol_stack, 0, temp4, 0, _top);
        }
        _next_position_stack = temp1;
        //_in_struct_stack = temp2;
        _parent_tid_stack = temp2;
        _local_end_stack = temp3;
        _symbol_stack = temp4;
    }

    public void stepOut()
    {
        if (_top < 1) {
            // if we didn't step in, we can't step out
            throw new EmptyStackException();
        }

        int next_start;

        _top--;
        next_start = _next_position_stack[_top];
        //_in_struct = _in_struct_stack[_top];
        _parent_tid = _parent_tid_stack[_top];
        _local_end = _local_end_stack[_top];
        _current_symtab   = _symbol_stack[_top];
        // _expect_symbol_table = _;

        _reader.position(next_start);
        _state = S_BEFORE_TID;
        _eof = false;
    }

    public SymbolTable getSymbolTable() {
        return _current_symtab;
    }

    public void resetSymbolTable()
    {
        // TODO can this just use isSystemTable() ?
        if ( _current_symtab == null
        || !_current_symtab.isSharedTable()
        ||  _current_symtab.getMaxId() != UnifiedSymbolTable.getSystemSymbolTableInstance().getMaxId()
        ) {
            // we only need to "reset" the symbol table if it isn't
            // the system symbol table already
            _current_symtab = UnifiedSymbolTable.getSystemSymbolTableInstance();
        }
    }


    public IonType getType()
    {
        if (_eof || _state == IonBinaryReader.S_BEFORE_TID) {
            throw new IllegalStateException();
        }
        return _value_type;
    }

    public int[] getTypeAnnotationIds()
    {
        int[] ids = null;
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        if (_annotation_start == -1) return null;

        int pos = _reader.position();

        try {
            // first count
            _reader.position(_annotation_start);
            int len = _reader.readVarUInt();
            int annotation_end = _reader.position() + len;
            int count = 0;
            while (annotation_end > _reader.position()) {
                _reader.readVarUInt();
                count++;
            }

            // now, again, to save those values
            ids = new int[count];
            _reader.position(_annotation_start);
            _reader.readVarUInt();
            count = 0;
            while (annotation_end > _reader.position()) {
                ids[count++] = _reader.readVarUInt();
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        // restore out cursor position before we go back to our previous tasks
        _reader.position(pos);
        return ids;
    }

    public String[] getTypeAnnotations()
    {
        int[] ids = getTypeAnnotationIds();
        if (ids == null) return null;
        if (_current_symtab == null) {
            throw new IllegalStateException();
        }

        String[] annotations = new String[ids.length];
        for (int ii=0; ii<ids.length; ii++) {
            annotations[ii] = _current_symtab.findSymbol(ids[ii]);
        }

        return annotations;
    }

    @SuppressWarnings("unchecked")
    public Iterator<Integer> iterateTypeAnnotationIds()
    {
        int[] ids = getTypeAnnotationIds();
        if (ids == null) return (Iterator<Integer>) EMPTY_ITERATOR;
        return new IonTreeReader.IdIterator(ids);
    }

    @SuppressWarnings("unchecked")
    public Iterator<String> iterateTypeAnnotations()
    {
        String[] ids = getTypeAnnotations();
        if (ids == null) return (Iterator<String>) EMPTY_ITERATOR;
        return new IonTreeReader.StringIterator(ids);
    }

    public boolean isInStruct() {
        return is_in_struct();
    }

    public int getFieldId()
    {
        if (_value_field_id == -1) {
            throw new IllegalStateException();
        }
        return _value_field_id;
    }

    public String getFieldName()
    {
        if (_value_field_id == -1 || _current_symtab == null) {
            throw new IllegalStateException();
        }
        return _current_symtab.findSymbol(_value_field_id);
    }

    @SuppressWarnings("deprecation")
    public IonValue getIonValue(IonSystem sys)
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }

        int tid = (_value_tid >>> 4);
        if (isNullValue()) {
            switch ((tid >>> 4) & 0xf) {
            case IonConstants.tidNull:      return sys.newNull();
            case IonConstants.tidBoolean:   return sys.newNullBool();
            case IonConstants.tidPosInt:
            case IonConstants.tidNegInt:    return sys.newNullInt();
            case IonConstants.tidFloat:     return sys.newNullFloat();
            case IonConstants.tidDecimal:   return sys.newNullDecimal();
            case IonConstants.tidTimestamp: return sys.newNullTimestamp();
            case IonConstants.tidSymbol:    return sys.newNullSymbol();
            case IonConstants.tidString:    return sys.newNullString();
            case IonConstants.tidClob:      return sys.newNullClob();
            case IonConstants.tidBlob:      return sys.newNullBlob();
            case IonConstants.tidList:      return sys.newNullList();
            case IonConstants.tidSexp:      return sys.newNullSexp();
            case IonConstants.tidStruct:    return sys.newNullString();
            default:
                throw new IonException("unrecognized type encountered");
            }
        }

        switch ((tid >>> 4) & 0xf) {
        case IonConstants.tidNull:      return sys.newNull();
        case IonConstants.tidBoolean:   return sys.newBool(booleanValue());
        case IonConstants.tidPosInt:
        case IonConstants.tidNegInt:    return sys.newInt(longValue());
        case IonConstants.tidFloat:     return sys.newFloat(doubleValue());
        case IonConstants.tidDecimal:   return sys.newDecimal(bigDecimalValue());
        case IonConstants.tidTimestamp:
            IonTimestamp t = sys.newTimestamp();
            Timestamp ti = timestampValue();
            t.setValue(ti);
            return t;
        case IonConstants.tidSymbol:    return sys.newSymbol(stringValue());
        case IonConstants.tidString:    return sys.newString(stringValue());
        case IonConstants.tidClob:
            IonClob clob = sys.newNullClob();
            // FIXME inefficient: both newBytes and setBytes copy the data
            clob.setBytes(newBytes());
            return clob;
        case IonConstants.tidBlob:
            IonBlob blob = sys.newNullBlob();
            // FIXME inefficient: both newBytes and setBytes copy the data
            blob.setBytes(newBytes());
            return blob;
        case IonConstants.tidList:
            IonList list = sys.newNullList();
            fillContainer(sys, list);
            return list;
        case IonConstants.tidSexp:
            IonSexp sexp = sys.newNullSexp();
            fillContainer(sys, sexp);
            return sexp;
        case IonConstants.tidStruct:
            IonStruct struct = sys.newNullStruct();
            fillContainer(sys, struct);
            return struct;
        default:
            throw new IonException("unrecognized type encountered");
        }
    }

    /**
     * @param list
     */
    private void fillContainer(IonSystem sys, IonSequence list)
    {
        stepIn();
        while(hasNext()) {
            next();
            list.add(getIonValue(sys));
        }
        stepOut();
    }
    private void fillContainer(IonSystem sys, IonStruct struct)
    {
        stepIn();
        while(hasNext()) {
            next();
            String fieldname = getFieldName();
            struct.add(fieldname, getIonValue(sys));
        }
        stepOut();
    }

    public boolean isNullValue()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid == IonConstants.tidNull) {
            return true;
        }
        return ((_value_tid & 0xf) == IonConstants.lnIsNullAtom);
    }

    public boolean booleanValue()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidBoolean) {
            throw new IllegalStateException();
        }
        switch (_value_tid & 0xf) {
        case IonConstants.lnIsNullAtom:
            throw new NullPointerException();
        case IonConstants.lnBooleanFalse:
            _state = S_BEFORE_TID; // now we (should be) just in front of the next value
            return false;
        case IonConstants.lnBooleanTrue:
            _state = S_BEFORE_TID; // now we (should be) just in front of the next value
            return true;
        default:
            throw new IllegalStateException();
        }
    }

    public int intValue()
    {
        return (int)longValue();
    }

    public long longValue()
    {
        long value;

        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidNegInt && tid != IonConstants.tidPosInt) {
            throw new IllegalStateException();
        }

        try {
            value = _reader.readULong(_value_len);
            if (tid == IonConstants.tidNegInt) {
                value = -value;
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in front of the next value
        return value;
    }

    public double doubleValue()
    {
        double value;

        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        try {
            if (tid == IonConstants.tidDecimal) {
                BigDecimal dec = _reader.readDecimal(_value_len);
                value = dec.doubleValue();
            }
            else if (tid == IonConstants.tidFloat) {
                value = _reader.readFloat(_value_len);
            }
            else {
            throw new IllegalStateException();
        }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in front of the next value
        return value;
    }

    public BigDecimal bigDecimalValue()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidDecimal) {
            throw new IllegalStateException();
        }

        BigDecimal value;
        try {
            value = _reader.readDecimal(_value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID;
        return value;
    }

    public int getSymbolId()
    {
        long value;

        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidSymbol) {
            throw new IllegalStateException();
        }

        try {
            value = _reader.readULong(_value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in front of the next value
        return (int)value;
    }

    public Date dateValue()
    {
        return timestampValue().dateValue();
    }

    public Timestamp timestampValue()
    {
        Timestamp ti;

        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidTimestamp) {
            throw new IllegalStateException();
        }

        try {
            ti = _reader.readTimestamp(_value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in front of the next value
        return ti;
    }

    public String stringValue()
    {
        String string_value;

        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }

        try {
            int tid = (_value_tid >>> 4);
            if (tid != IonConstants.tidSymbol && tid != IonConstants.tidString)
            {
                // Is it a version marker?
                if (is_at_top_level() && tid == IonConstants.tidTypedecl) {
                    // Skip the remaining 3 bytes of the BVM
                    _reader.read();
                    _reader.read();
                    _reader.read();

                    return SystemSymbolTable.ION_1_0;
                }
                throw new IllegalStateException();
            }

            if ((_value_tid & 0xf) == IonConstants.lnIsNullAtom) {
                return null;
            }

            if (tid == IonConstants.tidSymbol) {
                long sid = _reader.readULong(_value_len);
                if (_current_symtab == null) {

                }
                string_value = _current_symtab.findSymbol((int)sid);
            }
            else {
                string_value = _reader.readString(_value_len);
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in front of the next value
        return string_value;
    }

    public int byteSize()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidBlob && tid != IonConstants.tidClob) {
            throw new IllegalStateException();
        }

        return _value_len;
    }

    public byte[] newBytes()
    {
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidBlob && tid != IonConstants.tidClob) {
            throw new IllegalStateException();
        }

        byte[] value;
        try {
            value = new byte[_value_len];
            _reader.read(value, 0, _value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in front of the next value
        return value;
    }

    public int getBytes(byte[] buffer, int offset, int len)
    {
        int readlen = -1;
        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidBlob && tid != IonConstants.tidClob) {
            throw new IllegalStateException();
        }
        if (len < _value_len) {
            throw new IllegalArgumentException("buffer is too short for value");
        }

        try {
            readlen = _reader.read(buffer, offset, _value_len);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        _state = S_BEFORE_TID; // now we (should be) just in front of the next value
        return readlen;
    }
}

