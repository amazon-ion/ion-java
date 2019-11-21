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

import static com.amazon.ion.SystemSymbols.ION_1_0_SID;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import com.amazon.ion.Timestamp.Precision;
import com.amazon.ion.impl.UnifiedSavePointManagerX.SavePoint;
import com.amazon.ion.impl._Private_ScalarConversions.AS_TYPE;
import com.amazon.ion.impl._Private_ScalarConversions.ValueVariant;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;


/**
 *  low level reader, base class, for reading Ion binary
 *  input sources.  This using the UnifiedInputStream just
 *  as the updated (july 2009) text reader does.  The
 *  routines in this impl only include those needed to handle
 *  field id's, annotation ids', and access to the value
 *  headers.  In particular hasNext, next, stepIn and stepOut
 *  are handled here.
 *
 *  scalar values are handled by IonReaderBinarySystem and
 *  symbol tables (as well as field names and annotations as
 *  strings) are handled by IonBinaryReaderUser.
 */
abstract class IonReaderBinaryRawX
    implements IonReader
{
    static final int DEFAULT_CONTAINER_STACK_SIZE = 12; // a multiple of 3
    static final int DEFAULT_ANNOTATION_SIZE = 10;
    static final int NO_LIMIT = Integer.MIN_VALUE;
    static final int UTF8_BUFFER_SIZE_IN_BYTES = 4 * 1024;

    protected enum State {
        S_INVALID,
        S_BEFORE_FIELD, // only true in structs
        S_BEFORE_TID,
        S_BEFORE_VALUE,
        S_AFTER_VALUE,
        S_EOF
    }
    State               _state;
    UnifiedInputStreamX _input;
    int                 _local_remaining;
    boolean             _eof;
    boolean             _has_next_needed;
    ValueVariant        _v;
    IonType             _value_type;
    boolean             _value_is_null;
    boolean             _value_is_true;   // cached boolean value (since we step on the length)

    /**
     * {@link SymbolTable#UNKNOWN_SYMBOL_ID} means "not on a struct field"
     * since otherwise we always know the SID.
     */
    int                 _value_field_id;
    int                 _value_tid;
    int                 _value_len;
    long                _value_start;
    int                 _value_lob_remaining;
    boolean             _value_lob_is_ready;

    long                _position_start;
    long                _position_len;


    SavePoint           _annotations;
    int[]               _annotation_ids;
    int                 _annotation_count;

    // local stack for stepInto() and stepOut()
    boolean             _is_in_struct;
    boolean             _struct_is_ordered;
    int                 _parent_tid;
    int                 _container_top;
    long[]              _container_stack; // triples of: position, type, local_end


    // `StandardCharsets.UTF_8` wasn't introduced until Java 7, so we have to use Charset#forName(String) instead.
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private CharsetDecoder utf8CharsetDecoder = UTF8.newDecoder();

    // Calling read() to pull in the next byte of a string requires an EOF check to be performed for each byte.
    // This reusable buffer allows us to call read(utf8InputBuffer) instead, letting us can pay the cost of an EOF check
    // once per buffer rather than once per byte.
    private ByteBuffer utf8InputBuffer = ByteBuffer.allocate(UTF8_BUFFER_SIZE_IN_BYTES);

    // A reusable scratch space to hold the decoded bytes as they're read from the utf8InputBuffer.
    private CharBuffer utf8DecodingBuffer = CharBuffer.allocate(UTF8_BUFFER_SIZE_IN_BYTES);

    protected IonReaderBinaryRawX() {
    }

    /**
     * @return This implementation always returns null.
     */
    public <T> T asFacet(Class<T> facetType)
    {
        return null;
    }

    protected final void init_raw(UnifiedInputStreamX uis) {
        _input = uis;
        _container_stack = new long[DEFAULT_CONTAINER_STACK_SIZE];
        _annotations = uis.savePointAllocate();
        _v = new ValueVariant();
        _annotation_ids = new int[DEFAULT_ANNOTATION_SIZE];

        re_init_raw();

        _position_start = -1;
    }

    final void re_init_raw() {
        _local_remaining = NO_LIMIT;
        _parent_tid = _Private_IonConstants.tidDATAGRAM;
        _value_field_id = SymbolTable.UNKNOWN_SYMBOL_ID;
        _state = State.S_BEFORE_TID; // this is where we always start
        _has_next_needed = true;

        _eof = false;
        _value_type = null;
        _value_is_null = false;
        _value_is_true = false;

        _value_len = 0;
        _value_start = 0;
        _value_lob_remaining = 0;
        _value_lob_is_ready = false;

        _annotation_count = 0;

        _is_in_struct = false;
        _struct_is_ordered = false;
        _parent_tid = 0;
        _container_top = 0;
    }

    public void close()
        throws IOException
    {
        _input.close();
    }

    static private final int  POS_OFFSET        = 0;
    static private final int  TYPE_LIMIT_OFFSET = 1;
    static private final long TYPE_MASK         = 0xffffffff;
    static private final int  LIMIT_SHIFT       = 32;
    static private final int  POS_STACK_STEP    = 2;

    private final void push(int type, long position, int local_remaining)
    {
        int oldlen = _container_stack.length;
        if ((_container_top + POS_STACK_STEP) >= oldlen) {
            int newlen = oldlen * 2;
            long[] temp = new long[newlen];
            System.arraycopy(_container_stack, 0, temp, 0, oldlen);
            _container_stack = temp;
        }
        _container_stack[_container_top + POS_OFFSET]  = position;
        long type_limit = local_remaining;
        type_limit <<= LIMIT_SHIFT;
        type_limit  |= (type & TYPE_MASK);
        _container_stack[_container_top + TYPE_LIMIT_OFFSET] = type_limit;
        _container_top += POS_STACK_STEP;
    }
    private final long get_top_position() {
        assert(_container_top > 0);
        long pos = _container_stack[(_container_top - POS_STACK_STEP) + POS_OFFSET];
        return pos;
    }
    private final int get_top_type() {
        assert(_container_top > 0);
        long type_limit = _container_stack[(_container_top - POS_STACK_STEP) + TYPE_LIMIT_OFFSET];
        int type = (int)(type_limit & TYPE_MASK);
        if (type < 0 || type > _Private_IonConstants.tidDATAGRAM) {
            throwErrorAt("invalid type id in parent stack");
        }
        return type;
    }
    private final int get_top_local_remaining() {
        assert(_container_top > 0);
        long type_limit = _container_stack[_container_top - POS_STACK_STEP + TYPE_LIMIT_OFFSET];
        int  local_remaining = (int)((type_limit >> LIMIT_SHIFT) & TYPE_MASK);
        return local_remaining;
    }
    private final void pop() {
        assert(_container_top > 0);
        _container_top -= POS_STACK_STEP;
    }
    public boolean hasNext()
    {
        if (!_eof && _has_next_needed) {
            try {
                has_next_helper_raw();
            }
            catch (IOException e) {
                error(e);
            }
        }
        return !_eof;
    }
    public IonType next()
    {
        if (_eof) {
            return null;
        }
        if (_has_next_needed) {
            try {
                has_next_helper_raw();
            }
            catch (IOException e) {
                error(e);
            }
        }
        _has_next_needed = true;
        // this should only be null here if we're at eof
        assert( _value_type != null || _eof == true);
        return _value_type;
    }
    //from IonConstants
    //public static final byte[] BINARY_VERSION_MARKER_1_0 =
    //    { (byte) 0xE0,
    //      (byte) 0x01,
    //      (byte) 0x00,
    //      (byte) 0xEA };
    private static final int BINARY_VERSION_MARKER_TID = _Private_IonConstants.getTypeCode(_Private_IonConstants.BINARY_VERSION_MARKER_1_0[0] & 0xff);
    private static final int BINARY_VERSION_MARKER_LEN = _Private_IonConstants.getLowNibble(_Private_IonConstants.BINARY_VERSION_MARKER_1_0[0] & 0xff);
    private final void has_next_helper_raw() throws IOException
    {
        clear_value();
        while (_value_tid == -1 && !_eof) {
            switch (_state) {
            case S_BEFORE_FIELD:
                assert _value_field_id == SymbolTable.UNKNOWN_SYMBOL_ID;
                _value_field_id = read_field_id();
                if (_value_field_id == UnifiedInputStreamX.EOF) {
                    // FIXME why is EOF ever okay in the middle of a struct?
                    assert UnifiedInputStreamX.EOF == SymbolTable.UNKNOWN_SYMBOL_ID;
                    _eof = true;
                    break;
                }
                // fall through to try to read the type id right now
            case S_BEFORE_TID:
                _state = State.S_BEFORE_VALUE; // read_type_id may change this for null and bool values
                _value_tid = read_type_id();
                if (_value_tid == UnifiedInputStreamX.EOF) {
                    _state = State.S_EOF;
                    _eof = true;
                    break;
                }
                if (_value_tid == _Private_IonConstants.tidNopPad) {
                    // skips size of pad and resets State machine
                    skip(_value_len);
                    clear_value();
                    break;
                }
                else if (_value_tid == _Private_IonConstants.tidTypedecl) {
                    assert (_value_tid == (BINARY_VERSION_MARKER_TID & 0xff)); // the bvm tid happens to be type decl
                    if (_value_len == BINARY_VERSION_MARKER_LEN ) {
                        // this isn't valid for any type descriptor except the first byte
                        // of a 4 byte version marker - so lets read the rest
                        load_version_marker();
                        _value_type = IonType.SYMBOL;
                    }
                    else {
                        // if it's not a bvm then it's an ordinary annotated value

                        // The next call changes our positions to that of the
                        // wrapped value, but we need to remember the overall
                        // wrapper position.
                        long wrapperStart = _position_start;
                        long wrapperLen   = _position_len;

                        _value_type = load_annotation_start_with_value_type();

                        // Wrapper and wrapped value should finish together!
                        long wrapperFinish = wrapperStart + wrapperLen;
                        long wrappedValueFinish = _position_start + _position_len;
                        if (wrapperFinish != wrappedValueFinish) {
                            throw newErrorAt(String.format("Wrapper length mismatch: wrapper %s wrapped value %s", wrapperFinish, wrappedValueFinish));
                        }

                        _position_start = wrapperStart;
                        _position_len   = wrapperLen;
                    }
                }
                else {
                    // if it's not a typedesc then we just get the IonType and we're done
                    _value_type = get_iontype_from_tid(_value_tid);
                }
                break;
            case S_BEFORE_VALUE:
                skip(_value_len);
                // fall through to "after value"
            case S_AFTER_VALUE:
                if (isInStruct()) {
                    _state = State.S_BEFORE_FIELD;
                }
                else {
                    _state = State.S_BEFORE_TID;
                }
                break;
            case S_EOF:
                break;
            default:
                error("internal error: raw binary reader in invalid state!");
            }
        }
        // we always want to exit here
        _has_next_needed = false;
        return;
    }
    private final void load_version_marker() throws IOException
    {
        for (int ii=1; ii<_Private_IonConstants.BINARY_VERSION_MARKER_1_0.length; ii++) {
            int b = read();
            if (b != (_Private_IonConstants.BINARY_VERSION_MARKER_1_0[ii] & 0xff)) {
                throwErrorAt("invalid binary image");
            }
        }
        // so it's a 4 byte version marker - make it look like
        // the symbol $ion_1_0 ...
        _value_tid = _Private_IonConstants.tidSymbol;
        _value_len = 0; // so skip will go the right place - here
        _value_start = 0;
        _v.setValue(ION_1_0_SID);
        _v.setAuthoritativeType(AS_TYPE.int_value);
        // _value_type = IonType.SYMBOL;  we do this in the caller so it's easier to see
        _value_is_null = false;
        _value_lob_is_ready = false;
        _annotations.clear();
        _value_field_id = SymbolTable.UNKNOWN_SYMBOL_ID;
        _state = State.S_AFTER_VALUE;
    }
    private final IonType load_annotation_start_with_value_type() throws IOException
    {
        IonType value_type;

        // we need to skip over the annotations to read
        // the actual type id byte for the value.  We'll
        // save the annotations using a save point, which
        // will pin the input buffers until we free this,
        // not later than the next call to hasNext().

        int alen = readVarUInt();
        _annotations.start(getPosition(), 0);
        skip(alen);
        _annotations.markEnd();

        // this will both get the type id and it will reset the
        // length as well (over-writing the len + annotations value
        // that is there now, before the call)
        _value_tid = read_type_id();
        if (_value_tid == _Private_IonConstants.tidNopPad) {
            throwErrorAt("NOP padding is not allowed within annotation wrappers.");
        }
        if (_value_tid == UnifiedInputStreamX.EOF) {
            throwErrorAt("unexpected EOF encountered where a type descriptor byte was expected");
        }
        if (_value_tid == _Private_IonConstants.tidTypedecl) {
            throwErrorAt("An annotation wrapper may not contain another annotation wrapper.");
        }

        value_type = get_iontype_from_tid(_value_tid);
        assert( value_type != null );

        return value_type;
    }

    protected final int load_annotations() {
        switch (_state) {
        case S_BEFORE_VALUE:
        case S_AFTER_VALUE:
            if (_annotations.isDefined()) {
                int local_remaining_save = _local_remaining;
                _input._save_points.savePointPushActive(_annotations, getPosition(), 0);
                _local_remaining =  NO_LIMIT; // limit will be handled by the save point
                _annotation_count = 0;

                try {
                    do {
                        int a = readVarUIntOrEOF();
                        if (a == UnifiedInputStreamX.EOF) {
                            break;
                        }
                        load_annotation_append(a);
                    } while (!isEOF());
                }
                catch (IOException e) {
                    error(e);
                }

                _input._save_points.savePointPopActive(_annotations);
                _local_remaining = local_remaining_save;
                _annotations.clear();
            }
            // else the count stays zero (or it was previously set)
            break;
        default:
            throw new IllegalStateException("annotations require the value to be ready");
        }
        return _annotation_count;
    }

    private final void load_annotation_append(int a)
    {
        int oldlen = _annotation_ids.length;
        if (_annotation_count >= oldlen) {
            int newlen = oldlen * 2;
            int[] temp = new int[newlen];
            System.arraycopy(_annotation_ids, 0, temp, 0, oldlen);
            _annotation_ids = temp;
        }
        _annotation_ids[_annotation_count++] =  a;
    }
    private final void clear_value()
    {
        _value_type = null;
        _value_tid  = -1;
        _value_is_null = false;
        _value_lob_is_ready = false;
        _annotations.clear();
        _v.clear();
        _annotation_count = 0;
        _value_field_id = SymbolTable.UNKNOWN_SYMBOL_ID;
    }

    /**
     * @return the field SID, or -1 if at EOF.
     */
    private final int read_field_id() throws IOException
    {
        int field_id = readVarUIntOrEOF();
        return field_id;
    }
    private final int read_type_id() throws IOException
    {
        long start_of_tid   = _input.getPosition();
        long start_of_value = start_of_tid + 1;

        int td = read();
        if (td < 0) {
            return UnifiedInputStreamX.EOF;
        }
        int tid = _Private_IonConstants.getTypeCode(td);
        int len = _Private_IonConstants.getLowNibble(td);

        // NOP Padding
        if (tid == _Private_IonConstants.tidNull && len != _Private_IonConstants.lnIsNull) {
            if (len == _Private_IonConstants.lnIsVarLen) {
                len = readVarUInt();
            }
            _state = _is_in_struct ? State.S_BEFORE_FIELD : State.S_BEFORE_TID;
            tid = _Private_IonConstants.tidNopPad; // override typeId to use Pad marker
        }
        else if (len == _Private_IonConstants.lnIsVarLen) {
            len = readVarUInt();
            start_of_value = _input.getPosition();
        }
        else if (tid == _Private_IonConstants.tidNull) {
            _value_is_null = true;
            len = 0;
            _state = State.S_AFTER_VALUE;
        }
        else if (len == _Private_IonConstants.lnIsNull) {
            _value_is_null = true;
            len = 0;
            _state = State.S_AFTER_VALUE;
        }
        else if (tid == _Private_IonConstants.tidBoolean) {
            switch (len) {
                case _Private_IonConstants.lnBooleanFalse:
                    _value_is_true = false;
                    break;
                case _Private_IonConstants.lnBooleanTrue:
                    _value_is_true = true;
                    break;
                default:
                    throwErrorAt("invalid length nibble in boolean value: "+len);
                    break;
            }
            len = 0;
            _state = State.S_AFTER_VALUE;
        }
        else if (tid == _Private_IonConstants.tidStruct) {
            if ((_struct_is_ordered = (len == 1))) {
                // special case of an ordered struct, it gets the
                // otherwise impossible to have length of 1
                len = readVarUInt();
                if (len == 0) {
                    throwErrorAt("Structs flagged as having ordered keys must contain at least one key/value pair.");
                }
                start_of_value = _input.getPosition();
            }
        }
        _value_tid = tid;
        _value_len = len;
        // TODO Keeping track of _value_start is only necessary because top-level
        // symbol values are treated differently than all other values: they
        // are read during has_next_helper_user in order to compare them
        // against the IVM (symbol 2). This behavior is actually contrary to
        // the spec, and leads to the reader position being advanced PAST
        // the value BEFORE a *Value() method is even called. Once that is fixed,
        // _value_start can be removed, and _input._pos can be used to find the
        // start of the value at the current valid position.
        // amzn/ion-java/issues/88 tracks the fix for bringing IVM handling up to
        // spec.
        _value_start = start_of_value;
        _position_len = len + (start_of_value - start_of_tid);
        _position_start = start_of_tid;
        return tid;
    }
    private final IonType get_iontype_from_tid(int tid)
    {
        IonType t = null;
        switch (tid) {
        case _Private_IonConstants.tidNull:      // 0
            t = IonType.NULL;
            break;
        case _Private_IonConstants.tidBoolean:   // 1
            t = IonType.BOOL;
            break;
        case _Private_IonConstants.tidPosInt:    // 2
        case _Private_IonConstants.tidNegInt:    // 3
            t = IonType.INT;
            break;
        case _Private_IonConstants.tidFloat:     // 4
            t = IonType.FLOAT;
            break;
        case _Private_IonConstants.tidDecimal:   // 5
            t = IonType.DECIMAL;
            break;
        case _Private_IonConstants.tidTimestamp: // 6
            t = IonType.TIMESTAMP;
            break;
        case _Private_IonConstants.tidSymbol:    // 7
            t = IonType.SYMBOL;
            break;
        case _Private_IonConstants.tidString:    // 8
            t = IonType.STRING;
            break;
        case _Private_IonConstants.tidClob:      // 9
            t = IonType.CLOB;
            break;
        case _Private_IonConstants.tidBlob:      // 10 A
            t = IonType.BLOB;
            break;
        case _Private_IonConstants.tidList:      // 11 B
            t = IonType.LIST;
            break;
        case _Private_IonConstants.tidSexp:      // 12 C
            t = IonType.SEXP;
            break;
        case _Private_IonConstants.tidStruct:    // 13 D
            t = IonType.STRUCT;
            break;
        case _Private_IonConstants.tidTypedecl:  // 14 E
            t = null;  // we don't know yet
            break;
        default:
            throw newErrorAt("unrecognized value type encountered: "+tid);
        }
        return t;
    }
    public void stepIn()
    {
        if (_value_type == null || _eof) {
            throw new IllegalStateException();
        }
        switch (_value_type) {
        case STRUCT:
        case LIST:
        case SEXP:
            break;
        default:
            throw new IllegalStateException();
        }
        if (_value_is_null) {
            if (_state != State.S_AFTER_VALUE) {
                assert( _state == State.S_AFTER_VALUE );
            }
        }
        else {
            if (_state != State.S_BEFORE_VALUE) {
                assert( _state == State.S_BEFORE_VALUE );
            }
        }
        // first push place where we'll take up our next
        // value processing when we step out
        long curr_position = getPosition();
        long next_position = curr_position + _value_len;
        int  next_remaining = _local_remaining;
        if (next_remaining != NO_LIMIT) {
            next_remaining -= _value_len;
            if (next_remaining < 0) {
                next_remaining = 0; // we'll see and EOF down the road  TODO: should we error now?
            }
        }
        push(_parent_tid, next_position, next_remaining);
        _is_in_struct = (_value_tid == _Private_IonConstants.tidStruct);
        _local_remaining = _value_len;
        _state = _is_in_struct ? State.S_BEFORE_FIELD : State.S_BEFORE_TID;
        _parent_tid = _value_tid;
        clear_value();
        _has_next_needed = true;
    }
    public void stepOut()
    {
        if (getDepth() < 1) {
            throw new IllegalStateException(IonMessages.CANNOT_STEP_OUT);
        }
        // first we get the top values, then we
        // pop them all off in one fell swoop.
        long next_position   = get_top_position();
        int  local_remaining = get_top_local_remaining();
        int  parent_tid      = get_top_type();
        pop();
        _eof = false;
        _parent_tid = parent_tid;
        // later, only after we've skipped to our new location: _local_remaining = local_remaining;
        if (_parent_tid == _Private_IonConstants.tidStruct) {
            _is_in_struct = true;
            _state = State.S_BEFORE_FIELD;
        }
        else {
            _is_in_struct = false;
            _state = State.S_BEFORE_TID;
        }
        _has_next_needed = true;

        clear_value();

        long curr_position = getPosition();
        if (next_position > curr_position) {
            try {
                long distance = next_position - curr_position;
                int  max_skip = Integer.MAX_VALUE - 1; // -1 just in case
                while (distance > max_skip) {
                    skip(max_skip);
                    distance -= max_skip;
                }
                if (distance > 0) {
                    assert( distance < Integer.MAX_VALUE );
                    skip((int)distance);
                }
            }
            catch (IOException e) {
                error(e);
            }
        }
        else if (next_position < curr_position) {
            String message = "invalid position during stepOut, current position "
                           + curr_position
                           + " next value at "
                           + next_position;
            error(message);
        }
        assert(next_position == getPosition());
        _local_remaining = local_remaining;
    }
    public int byteSize()
    {
        int len;
        switch (_value_type) {
        case BLOB:
        case CLOB:
            break;
        default:
            throw new IllegalStateException("only valid for LOB values");
        }
        if (!_value_lob_is_ready) {
            if (_value_is_null) {
                len = 0;
            }
            else {
                len = _value_len;
            }
            _value_lob_remaining = len;
            _value_lob_is_ready = true;
        }
        return _value_lob_remaining;
    }
    public byte[] newBytes()
    {
        int len = byteSize(); // does out validation for us
        byte[] bytes;
        if (_value_is_null) {
            bytes = null;
        }
        else {
            bytes = new byte[len];
            getBytes(bytes, 0, len);
        }
        return bytes;
    }
    public int getBytes(byte[] buffer, int offset, int len)
    {
        int value_len = byteSize(); // again validation
        if (value_len > len) {
            value_len = len;
        }
        int read_len = readBytes(buffer, offset, value_len);
        return read_len;
    }
    public int readBytes(byte[] buffer, int offset, int len)
    {
        if (offset < 0 || len < 0) {
            throw new IllegalArgumentException();
        }
        int value_len = byteSize(); // again validation
        if (_value_lob_remaining > len) {
            len = _value_lob_remaining;
        }
        if (len < 1) {
            return 0;
        }
        int read_len;
        try {
            read_len = read(buffer, offset, value_len);
            _value_lob_remaining -= read_len;
        }
        catch (IOException e) {
            read_len = -1;
            error(e);
        }
        if (_value_lob_remaining == 0) {
            _state = State.S_AFTER_VALUE;
        }
        else {
            _value_len = _value_lob_remaining;
        }
        return read_len;
    }
    public int getDepth()
    {
        return (_container_top / POS_STACK_STEP);
    }
    public IonType getType()
    {
        //if (_has_next_needed) {
        //    throw new IllegalStateException("getType() isn't valid until you have called next()");
        //}
        return _value_type;
    }
    public boolean isInStruct()
    {
        return _is_in_struct;
    }
    public boolean isNullValue()
    {
        return _value_is_null;
    }
    //
    //  helper read routines - these were lifted
    //  from SimpleByteBuffer.SimpleByteReader
    //
    private final int read() throws IOException
    {
        if (_local_remaining != NO_LIMIT) {
            if (_local_remaining < 1) {
                return UnifiedInputStreamX.EOF;
            }
            _local_remaining--;
        }
        return _input.read();
    }
    private final int read(byte[] dst, int start, int len) throws IOException
    {
        if (dst == null || start < 0 || len < 0 || start + len > dst.length) {
            // no need to test this start >= dst.length ||
            // since we test start+len > dst.length which is the correct test
            throw new IllegalArgumentException();
        }
        int read;
        if (_local_remaining == NO_LIMIT) {
            read = _input.read(dst, start, len);
        }
        else {
            if (len > _local_remaining) {
                if (_local_remaining < 1) {
                    throwUnexpectedEOFException();
                }
                len = _local_remaining;
            }
            read = _input.read(dst, start, len);
            _local_remaining -= read;
        }
        return read;
    }
    /**
     * Uses {@link #read(byte[], int, int)} until the entire length is read.
     * This method will block until the request is satisfied.
     *
     * @param buf       The buffer to read to.
     * @param offset    The offset of the buffer to read from.
     * @param len       The length of the data to read.
     */
    public void readAll(byte[] buf, int offset, int len) throws IOException
    {
        int rem = len;
        while (rem > 0)
        {
            int amount = read(buf, offset, rem);
            if (amount <= 0)
            {
                throwUnexpectedEOFException();
            }
            rem -= amount;
            offset += amount;
        }
    }
    private final boolean isEOF() {
        if (_local_remaining > 0) return false;
        if (_local_remaining == NO_LIMIT) {
            return _input.isEOF();
        }
        return true;
    }
    private final long getPosition() {
        long pos = _input.getPosition();
        return pos;
    }
    private final void skip(int len) throws IOException
    {
        if (len < 0) {
            // no need to test this start >= dst.length ||
            // since we test start+len > dst.length which is the correct test
            throw new IllegalArgumentException();
        }
        if (_local_remaining == NO_LIMIT) {
            _input.skip(len);
        }
        else {
            if (len > _local_remaining) {
                if (_local_remaining < 1) {
                    throwUnexpectedEOFException();
                }
                len = _local_remaining;
            }
            _input.skip(len);
            _local_remaining -= len;
        }
        return;
    }
    protected final long readULong(int len) throws IOException
    {
        long    retvalue = 0;
        int b;
        switch (len) {
        default:
            throw new IonException("value too large for Java long");
        case 8:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 7:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 6:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 5:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 4:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 3:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 2:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 1:
            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 8) | b;
        case 0:
            // do nothing, it's just a 0 length is a 0 value
        }
        return retvalue;
    }
    protected final BigInteger readBigInteger(int len, boolean is_negative) throws IOException
    {
        BigInteger value;
        if (len > 0) {
            byte[] bits = new byte[len];
            readAll(bits, 0, len);
            int signum = is_negative ? -1 : 1;
            value = new BigInteger(signum, bits);
        }
        else {
            value = BigInteger.ZERO;
        }
        return value;
    }
    protected final int readVarInt() throws IOException
    {
        return readVarInt(read());
    }
    /**
     * Reads an integer value, returning null to mean -0.
     * @throws IOException
     */
    protected final Integer readVarInteger() throws IOException
    {
        int firstByte = read();

        // if byte represents -0 returns null
        if (firstByte == 0xC0) {
            return null;
        }

        return readVarInt(firstByte);
    }
    /**
     * reads a varInt after the first byte was read. The first byte is used to specify the sign and -0 has different
     * representation on the protected API that was called
     *
     * @param firstByte last varInt octet
     */
    private int readVarInt(int firstByte) throws IOException {
        // VarInt uses the high-order bit of the last octet as a marker; some (but not all) 5-byte VarInts can fit
        // into a Java int.
        // To validate overflows we accumulate the VarInt in a long and then check if it can be represented by an int
        //
        // see http://amzn.github.io/ion-docs/docs/binary.html#varuint-and-varint-fields

        long retValue = 0;
        int b = firstByte;
        boolean isNegative = false;

        for (;;) {
            if (b < 0) throwUnexpectedEOFException();
            if ((b & 0x40) != 0) {
                isNegative = true;
            }

            retValue = (b & 0x3F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retValue = (retValue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;
            // for the rest, they're all the same


            if ((b = read()) < 0) throwUnexpectedEOFException();
            retValue = (retValue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;
            // for the rest, they're all the same

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retValue = (retValue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;
            // for the rest, they're all the same

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retValue = (retValue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            // Don't support anything above a 5-byte VarInt for now, see https://github.com/amzn/ion-java/issues/146
            throwVarIntOverflowException();
        }

        if (isNegative) {
            retValue = -retValue;
        }

        int retValueAsInt = (int) retValue;
        if (retValue != ((long) retValueAsInt)) {
            throwVarIntOverflowException();
        }

        return retValueAsInt;
    }

    protected final int readVarUIntOrEOF() throws IOException
    {
// VarUInt uses the high-order bit of the last octet as a marker; some (but not all) 5-byte VarUInt can fit
        // into a Java int.
        // To validate overflows we accumulate the VarInt in a long and then check if it can be represented by an int
        //
        // see http://amzn.github.io/ion-docs/docs/binary.html#varuint-and-varint-fields

        long retvalue = 0;
        int  b;
        for (;;) { // fake loop to create a "goto done"
            if ((b = read()) < 0) {
                return UnifiedInputStreamX.EOF;
            }
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            if ((b = read()) < 0) throwUnexpectedEOFException();
            retvalue = (retvalue << 7) | (b & 0x7F);
            if ((b & 0x80) != 0) break;

            // Don't support anything above a 5-byte VarUInt for now, see https://github.com/amzn/ion-java/issues/146
            throwVarIntOverflowException();
        }

        int retValueAsInt = (int) retvalue;
        if (retvalue != ((long) retValueAsInt)) {
            throwVarIntOverflowException();
        }

        return retValueAsInt;
    }
    protected final int readVarUInt() throws IOException
    {
        int varUInt = readVarUIntOrEOF();
        if (varUInt == UnifiedInputStreamX.EOF) {
            throwUnexpectedEOFException();
        }

        return varUInt;
    }
    protected final double readFloat(int len) throws IOException
    {
        if (len == 0)
        {
            // special case, return pos zero
            return 0.0d;
        }
        if (len != 4 && len != 8)
        {
            throw new IOException("Length of float read must be 0, 4, or 8");
        }

        long dBits = this.readULong(len);

        return len == 4
            ? (double) Float.intBitsToFloat((int) (dBits & 0xffffffffL))
            : Double.longBitsToDouble(dBits);
    }

    protected final Decimal readDecimal(int len) throws IOException
    {
        MathContext mathContext = MathContext.UNLIMITED;
        Decimal bd;
        // we only write out the '0' value as the nibble 0
        if (len == 0) {
            bd = Decimal.valueOf(0, mathContext);
        }
        else {
            // otherwise we to it the hard way ....
            int save_limit = NO_LIMIT;
            if (_local_remaining != NO_LIMIT) {
                save_limit = _local_remaining - len;
            }
            _local_remaining = len;
            int  exponent = readVarInt();
            BigInteger value;
            int signum;
            if (_local_remaining > 0)
            {
                byte[] bits = new byte[_local_remaining];
                readAll(bits, 0, _local_remaining);
                signum = 1;
                if (bits[0] < 0)
                {
                    // value is negative, clear the sign
                    bits[0] &= 0x7F;
                    signum = -1;
                }
                value = new BigInteger(signum, bits);
            }
            else {
                signum = 0;
                value = BigInteger.ZERO;
            }
            // Ion stores exponent, BigDecimal uses the negation "scale"
            int scale = -exponent;
            if (value.signum() == 0 && signum == -1)
            {
                assert value.equals(BigInteger.ZERO);
                bd = Decimal.negativeZero(scale, mathContext);
            }
            else
            {
                bd = Decimal.valueOf(value, scale, mathContext);
            }
            _local_remaining = save_limit;
        }
        return bd;
    }

    /**
     * @see IonBinary.Reader#readTimestampValue
     */
    protected final Timestamp readTimestamp(int len) throws IOException
    {
        if (len < 1) {
            // nothing to do here - and the timestamp will be NULL
            return null;
        }

        int         year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
        BigDecimal  frac = null;
        int         save_limit = NO_LIMIT;
        if (_local_remaining != NO_LIMIT) {
            save_limit = _local_remaining - len;
        }
        _local_remaining = len;  // > 0

        // first up is the offset, which requires a special int reader
        // to return the -0 as a null Integer
        Integer offset = readVarInteger();
        // now we'll read the struct values from the input stream

        // year is from 0001 to 9999
        // or 0x1 to 0x270F or 14 bits - 1 or 2 bytes
        year  = readVarUInt();
        Precision p = Precision.YEAR; // our lowest significant option

        // now we look for months
        if (_local_remaining > 0) {
            month = readVarUInt();
            p = Precision.MONTH;

            // now we look for days
            if (_local_remaining > 0) {
                day   = readVarUInt();
                p = Precision.DAY; // our lowest significant option

                // now we look for hours and minutes
                if (_local_remaining > 0) {
                    hour   = readVarUInt();
                    minute = readVarUInt();
                    p = Precision.MINUTE;
                    if (_local_remaining > 0) {
                        second = readVarUInt();
                        p = Precision.SECOND;
                        if (_local_remaining > 0) {
                            // now we read in our actual "milliseconds since the epoch"
                            frac = readDecimal(_local_remaining);
                            if (frac.compareTo(BigDecimal.ZERO) < 0 || frac.compareTo(BigDecimal.ONE) >= 0) {
                                throwErrorAt(
                                        "The fractional seconds value in a timestamp must be greater than or "
                                              + "equal to zero and less than one."
                                );
                            }
                        }
                    }
                }
            }
        }
        // restore out outer limit(s)
        _local_remaining  = save_limit;
        // now we let timestamp put it all together
        try {
            Timestamp val =
                Timestamp.createFromUtcFields(p, year, month, day, hour,
                                              minute, second, frac, offset);
            return val;
        }
        catch (IllegalArgumentException e)
        {
            // Rewrap to the expected type.
            throw newErrorAt("Invalid timestamp encoding: " + e.getMessage());
        }
    }

    protected final String readString(int numberOfBytes) throws IOException
    {
        // If the string we're reading is small enough to fit in our reusable buffer, we can avoid the overhead
        // of looping and bounds checking.
        if (numberOfBytes <= utf8InputBuffer.capacity()) {
            return readStringWithReusableBuffer(numberOfBytes);
        }

        // Otherwise, allocate a one-off decoding buffer that's large enough to hold the string
        // and prepare to decode the string in chunks.
        CharBuffer decodingBuffer = CharBuffer.allocate(numberOfBytes);
        utf8CharsetDecoder.reset();

        int save_limit = NO_LIMIT;
        if (_local_remaining != NO_LIMIT) {
            save_limit = _local_remaining - numberOfBytes;
        }
        _local_remaining = numberOfBytes;

        // The following loop will:
        // * Fill the input buffer with utf8 bytes
        // * Write decoded chars to the decoding buffer
        // * Move any remaining partial character bytes to the front of the buffer
        // * Repeat until the requested number of bytes have been decoded.
        // * Create a new String object from the contents of the decoding buffer.
        int totalBytesRead = 0;
        int carryoverBytes = 0;
        while (totalBytesRead < numberOfBytes) {

            int bytesRemaining = numberOfBytes - totalBytesRead;
            // When decoding, it's possible to have 'carryover' bytes left in the buffer which represent
            // a partial unicode character. We need to leave these in the buffer so that we can complete the
            // partial character in the next call to read().
            int capacityRemaining = utf8InputBuffer.array().length - carryoverBytes;
            int bytesToRead = Math.min(bytesRemaining, capacityRemaining);

            int bytesRead = read(utf8InputBuffer.array(), carryoverBytes, bytesToRead);
            if (bytesRead <= 0) {
                // UnifiedInputStreamX doesn't adhere to InputStream's API. See the comments on
                // UnifiedInputStreamX#read() for more information.
                throwUnexpectedEOFException();
            }

            totalBytesRead += bytesRead;

            utf8InputBuffer.position(0);
            utf8InputBuffer.limit(carryoverBytes + bytesRead);

            CoderResult coderResult = utf8CharsetDecoder.decode(
                    utf8InputBuffer,
                    decodingBuffer,
                    totalBytesRead >= numberOfBytes
            );
            if (coderResult.isError()) {
                throw new IonException("Illegal value encountered while validating UTF-8 data in input stream. " + coderResult.toString());
            }

            // Shift leftover partial character bytes (if any) to the beginning of the buffer
            carryoverBytes = utf8InputBuffer.remaining();
            if (carryoverBytes > 0) {
                System.arraycopy(
                        utf8InputBuffer.array(),
                        utf8InputBuffer.position(),
                        utf8InputBuffer.array(),
                        0,
                        carryoverBytes
                );
            }
        }

        _local_remaining = save_limit;

        decodingBuffer.flip();
        return decodingBuffer.toString();
    }

    private String readStringWithReusableBuffer(int numberOfBytes) throws IOException {
        int save_limit = NO_LIMIT;
        if (_local_remaining != NO_LIMIT) {
            save_limit = _local_remaining - numberOfBytes;
        }
        _local_remaining = numberOfBytes;
        readAll(utf8InputBuffer.array(), 0, numberOfBytes);
        _local_remaining = save_limit;

        utf8InputBuffer.position(0);
        utf8InputBuffer.limit(numberOfBytes);

        utf8DecodingBuffer.position(0);
        utf8DecodingBuffer.limit(utf8DecodingBuffer.capacity());

        utf8CharsetDecoder.reset();
        CoderResult coderResult = utf8CharsetDecoder.decode(utf8InputBuffer, utf8DecodingBuffer, true);
        if (coderResult.isError()) {
            throw new IonException("Illegal value encountered while validating UTF-8 data in input stream. " + coderResult.toString());
        }
        utf8DecodingBuffer.flip();
        return utf8DecodingBuffer.toString();
    }

    private final void throwUnexpectedEOFException() throws IOException {
        throwErrorAt("unexpected EOF in value");
    }
    private final void throwVarIntOverflowException() throws IOException {
        throwErrorAt("int in stream is too long for a Java int 32");
    }

    protected IonException newErrorAt(String msg) {
        String msg2 = msg + " at position " + getPosition();
        return new IonException(msg2);
    }
    protected void throwErrorAt(String msg) {
        throw newErrorAt(msg);
    }
    protected void error(String msg) {
        throw new IonException(msg);
    }
    protected void error(Exception e) {
        throw new IonException(e);
    }
}
