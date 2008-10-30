/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import static com.amazon.ion.impl.IonImplUtils.EMPTY_ITERATOR;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.TtTimestamp;
import com.amazon.ion.impl.IonConstants;
import com.amazon.ion.system.SimpleCatalog;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.EmptyStackException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * implements an Ion iterator over a Ion binary buffer.
 */
public final class IonBinaryIterator
    implements IonReader
{
    static final boolean _debug_return_system_values = false;


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
    UnifiedSymbolTable  _current_symtab;
    int                 _parent_tid;  // using -1 for eof (or bof aka undefined) and 16 for datagram

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
    UnifiedSymbolTable[] _symbol_stack;

    IonBinaryIterator() {}

    // public to allow someone to DECLARE this buf to be binary
    // irrespective of the presense or absense of an Ion Version
    // Marker (aka magic cookie)
    public IonBinaryIterator(byte[] buf)
    {
        this ( buf, 0, buf.length );
    }

    public IonBinaryIterator(byte[] buf, int start, int len)
    {
        this(  new SimpleByteBuffer(buf, start, len, true /*isReadOnly*/)
             , null
             , new SimpleCatalog()
        );
    }
    public IonBinaryIterator(byte[] buf, IonCatalog catalog)
    {
        this ( buf, 0, buf.length, catalog );
    }
    public IonBinaryIterator(byte[] buf, int start, int len, IonCatalog catalog)
    {
        this(  new SimpleByteBuffer(buf, start, len, true /*isReadOnly*/)
             , null
             , catalog
        );
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
    IonBinaryIterator(SimpleByteBuffer ssb, IonType parent, IonCatalog catalog)
    {
        _buffer = ssb;
        _reader = _buffer.getReader();
        _local_end = _buffer._eob;
        _state = S_BEFORE_TID;
        _catalog = catalog;
        _current_symtab = UnifiedSymbolTable.getSystemSymbolTableInstance();
        _parent_tid = (parent == null) ? IonConstants.tidDATAGRAM : get_tid_from_ion_type(parent);
    }

    final boolean expect_symbol_table() {
        // FIXME - this should be looking at tidSexp, it should be looking at
        //         is embedded value or is datagram instead !!
        boolean expected =  (  _parent_tid == IonConstants.tidSexp
                            || _parent_tid == IonConstants.tidDATAGRAM
                            );
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
            _reader.skip(_value_len);
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

            // the "Possible" routines return -1 if they
            // found, and consumed, interesting data. And
            // if they did we need to read another byte
            while (td == -1) {
                td = _reader.read();
                if (td == ByteReader.EOF) {
                    _eof = true;
                    return false;
                }
                if (expect_symbol_table()) {
                    // first check for the magic cookie - especially since the first byte
                    // says this is an annotation (with an, otherwise, invalid length of zero)
                    final int marker_byte_1 = (IonConstants.BINARY_VERSION_MARKER_1_0[0] & 0xff);
                    if (td == marker_byte_1) {
                        td = processPossibleMagicCookie(td);
                    }
                    else if (((td >>> 4) & 0xf) == IonConstants.tidTypedecl) {
                        // this looks at the current value, checks to see if it has the
                        // $ion_1_0 annoation and if it does load the symbol table and
                        // move forward, otherwise just read the actual values td and
                        // return that instead.  If it's not a symbol table, then the 14
                        // (user type annotation) will be handled during "next()"
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
        _state = IonBinaryIterator.S_AFTER_TID;
        return true;
    }

    public IonType next()
    {
        // get actual type id, this also handle the hasNext & eof logic as necessary
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        int ion_type = this.nextTid();
        _value_type = get_iontype_from_tid(ion_type);
        return _value_type;
    }
    int nextTid()
    {
        if (_state != IonBinaryIterator.S_AFTER_TID && !this.hasNext()) {
                throw new NoSuchElementException();
        }

        // get actual type id
        int ion_type = (_value_tid & 0xf0) >>> 4;

        try {
            // if 14 (annotation)
            if (ion_type == IonConstants.tidTypedecl) {
                //      skip forward annotation length

                //      first we skip the value length and then
                //      read the local annotation length
                read_length(_value_tid);

                //      set annotation start to the position of
                //      the first type desc byte
                this._annotation_start = _reader.position();
                int annotation_len = _reader.readVarUInt();
                _reader.skip(annotation_len);

                //      read tid again
                _value_tid = _reader.read();
                ion_type = (_value_tid & 0xf0) >>> 4;
            }
            else {
                // clear the annotation marker that's left over from our previous value
                _annotation_start = -1;
            }

            // read length (if necessary)
            _value_len = read_length(_value_tid);
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        // set the state forward
        _state = IonBinaryIterator.S_BEFORE_CONTENTS;
        return ion_type;
    }
    final int get_tid_from_ion_type(IonType t) {
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
    final IonType get_iontype_from_tid(int tid)
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
            else if (len == 15) {
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
                    _state = IonBinaryIterator.S_BEFORE_TID;
                    td = -1; // this says we used up the value and the caller can skip it
                }
                else {
                    // the magic was lost, go back to your regularly
                    // scheduled activity
                    _reader.position(original_position);
                }
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return td;
    }

    int processPossibleSymbolTable(int typedesc) {
        int original_position = _reader.position();
        boolean is_symbol_table = false;

        try {
            int vlen = read_length(typedesc);   // we have to read past the overall value length first
            assert vlen == vlen;                // shut eclipse up about not reading vlen
            int alen = _reader.readVarUInt();   // now we have the length of the annotations themselves
            int aend = _reader.position() + alen;

            while (_reader.position() < aend && !is_symbol_table) {
                int a = _reader.readVarUInt();
                switch (a) {
                case SystemSymbolTable.ION_SYMBOL_TABLE_SID:
                // cas 25 apr 2008: removed case SystemSymbolTable.ION_1_0_SID:
                    is_symbol_table = loadSymbolTable(a, original_position, aend);
                    break;
                case SystemSymbolTable.ION_EMBEDDED_VALUE_SID:
                    // TODO - we should do embedded value processing here
                    // like: is_embedded_value = true;
                    break;
                default:
                    break;
                }
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }

// FIXME: return everything ...
        if (!_debug_return_system_values) {
            if (is_symbol_table) {
                // make our caller read the upcoming byte
                typedesc = -1;
            }
            else {
                // we changed our minds, everything is as is should be
                _reader.position(original_position);
           }
        }
        return typedesc;
    }

    boolean loadSymbolTable(int annotationid, int original_start, int contents_start) throws IOException
    {
        boolean is_symbol_table = false;

        _reader.position(contents_start);

        int td = _reader.read();

        if (((td >>> 4) & 0xf) == IonConstants.tidStruct) {
            //boolean was_struct = this._in_struct;
            int     prev_parent_tid = this._parent_tid;
            int     prev_end = this._local_end;

            this._value_tid = td;
            //this._in_struct = true;
            this._parent_tid = IonConstants.tidStruct;

            int len = this.read_length(td);
            this._local_end = this._reader.position() + len;

            if (this._local_end >= prev_end) {
                _state = IonBinaryIterator.S_INVALID;
                throw new IonException("invalid binary format");
            }
            // TODO: this should get it's system symbol table somewhere else
            // like passed in from the user or deduced from the version stamp
            UnifiedSymbolTable systemSymbols = UnifiedSymbolTable.getSystemSymbolTableInstance();
            UnifiedSymbolTable local =
                new UnifiedSymbolTable(systemSymbols, this, _catalog);

            _eof = false; // the hasNext() on the last field in the symbol table sets this

            // we've read it, it must be a symbol table
            is_symbol_table = true;

            if (local.isLocalTable()) {
                this._current_symtab = local;
            }
            else {
                this._catalog.putTable(local);
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

    public int getContainerSize()
    {
        int size = 0;
        boolean counting_a_struct = false;

        if (_state != S_BEFORE_CONTENTS) {
            throw new IllegalStateException();
        }
        int tid = (_value_tid >>> 4);
        switch (tid) {
            case IonConstants.tidSexp:
            case IonConstants.tidList:
                counting_a_struct = false;
                break;
            case IonConstants.tidStruct:
                counting_a_struct = true;
                break;
            default:
                throw new IllegalStateException();
        }

        int original_position = _reader.position();

        try {
            int aend = _value_len + original_position;

            while (_reader.position() < aend) {
                if (counting_a_struct) {
                    int fldid = _reader.readVarUInt();
                    assert fldid == fldid;
                }
                int td = _reader.read();
                int vlen = this.read_length(td);
                _reader.skip(vlen);
                size++;
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        _reader.position(original_position);
        return size;
    }


    public void stepInto()
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
        UnifiedSymbolTable [] temp4 = new UnifiedSymbolTable[newlen];
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

    public UnifiedSymbolTable getSymbolTable() {
        return _current_symtab;
    }

    public void resetSymbolTable()
    {
    	if ( _current_symtab == null
    	 || !_current_symtab._is_locked
    	 ||  _current_symtab.getMaxId() != UnifiedSymbolTable.getSystemSymbolTableInstance().getMaxId()
    	) {
    		// we only need to "reset" the symbol table if it isn't
    		// the system symbol table already
    		_current_symtab = UnifiedSymbolTable.getSystemSymbolTableInstance();
    	}
    }


    public IonType getType()
    {
        if (_eof || _state == IonBinaryIterator.S_BEFORE_TID) {
            throw new IllegalStateException();
        }
        return _value_type;
    }

    public int getTypeId()
    {
        if (_eof || _state == IonBinaryIterator.S_BEFORE_TID) {
            throw new IllegalStateException();
        }
        return ((_value_tid >>> 4) & 0xf);
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
        return new IonTreeIterator.IdIterator(ids);
    }

    @SuppressWarnings("unchecked")
    public Iterator<String> iterateTypeAnnotations()
    {
        String[] ids = getTypeAnnotations();
        if (ids == null) return (Iterator<String>) EMPTY_ITERATOR;
        return new IonTreeIterator.StringIterator(ids);
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
        case IonConstants.tidFloat:
            IonFloat f = sys.newFloat();
            f.setValue(doubleValue());
            return f;
        case IonConstants.tidDecimal:
            IonDecimal dec = sys.newDecimal();
            dec.setValue(bigDecimalValue());
            return dec;
        case IonConstants.tidTimestamp:
            IonTimestamp t = sys.newTimestamp();
            TtTimestamp ti = timestampValue();
            t.setValue(ti);
            return t;
        case IonConstants.tidSymbol:    return sys.newSymbol(stringValue());
        case IonConstants.tidString:    return sys.newString(stringValue());
        case IonConstants.tidClob:
            IonClob clob = sys.newNullClob();
            clob.setBytes(newBytes());
            return clob;
        case IonConstants.tidBlob:
            IonBlob blob = sys.newNullBlob();
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
        stepInto();
        while(hasNext()) {
            next();
            list.add(getIonValue(sys));
        }
        stepOut();
    }
    private void fillContainer(IonSystem sys, IonStruct struct)
    {
        stepInto();
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
        if (tid != IonConstants.tidFloat) {
            throw new IllegalStateException();
        }

        try {
            value = _reader.readFloat(_value_len);
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
        if (tid == IonConstants.tidNull) {
            throw new NullPointerException();
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

    public TtTimestamp timestampValue()
    {
        TtTimestamp ti;

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
        int tid = (_value_tid >>> 4);
        if (tid != IonConstants.tidSymbol && tid != IonConstants.tidString) {
            throw new IllegalStateException();
        }

        if ((_value_tid & 0xf) == IonConstants.lnIsNullAtom) {
            return null;
        }

        try {
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
