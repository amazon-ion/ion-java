// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonBlob;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.IonReaderTextRawTokensX.IonReaderTextTokenException;
import com.amazon.ion.impl.IonScalarConversionsX.AS_TYPE;
import com.amazon.ion.impl.IonScalarConversionsX.CantConvertException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;

/**
 * This reader calls the IonTextReaderRaw for low level events.
 * It surfaces the reader functions that construct instances
 * of various sorts (numbers, java strings, etc). It also
 * caches the fieldname and annotations of the current value.
 *
 *  It does not understand symbol tables nor care about them
 *  the IonTextUserReader is responsible for that.
 *
 */
public class IonReaderTextSystemX
    extends IonReaderTextRawX
{

    Iterator<String> EMPTY_ITERATOR = new StringIterator(null);

    public IonReaderTextSystemX(char[] chars) {
        UnifiedInputStreamX iis;
        iis = UnifiedInputStreamX.makeStream(chars);
        init(iis);
    }
    public IonReaderTextSystemX(char[] chars, int offset, int length) {
        UnifiedInputStreamX iis;
        iis = UnifiedInputStreamX.makeStream(chars, offset, length);
        init(iis);
    }
    public IonReaderTextSystemX(CharSequence chars) {
        UnifiedInputStreamX iis;
        iis = UnifiedInputStreamX.makeStream(chars);
        init(iis);
    }
    public IonReaderTextSystemX(CharSequence chars, int offset, int length) {
        UnifiedInputStreamX iis;
        iis = UnifiedInputStreamX.makeStream(chars, offset, length);
        init(iis);
    }
    public IonReaderTextSystemX(Reader userChars) {
        UnifiedInputStreamX iis;
        try {
            iis = UnifiedInputStreamX.makeStream(userChars);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        init(iis);
    }
    public IonReaderTextSystemX(byte[] bytes) {
        UnifiedInputStreamX iis;
        iis = UnifiedInputStreamX.makeStream(bytes);
        init(iis);
    }
    public IonReaderTextSystemX(byte[] bytes, int offset, int length) {
        UnifiedInputStreamX iis;
        iis = UnifiedInputStreamX.makeStream(bytes, offset, length);
        init(iis);
    }
    public IonReaderTextSystemX(InputStream userBytes) {
        UnifiedInputStreamX iis;
        try {
            iis = UnifiedInputStreamX.makeStream(userBytes);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        init(iis);
    }
    public IonReaderTextSystemX(File file) {
        UnifiedInputStreamX iis;
        try {
            InputStream userBytes = new FileInputStream(file);
            iis = UnifiedInputStreamX.makeStream(userBytes);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        init(iis);
    }

    public IonReaderTextSystemX(UnifiedInputStreamX iis) {
        init(iis);
    }

    /**
     * this checks the state of the raw reader to make sure
     * this is valid.  It also checks for an existing cached
     * value of the correct type.  It will either cast the
     * current value from an existing type to the type desired
     * or it will construct the desired type from the raw
     * input in the raw reader
     *
     * @param value_type desired value type (in local type terms)
     * @throws IOException
     */
    private final void load_or_cast_cached_value(int value_type) {
        if (_v.isEmpty()) {
            try {
                load_scalar_value();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }
        if (value_type != 0 && !_v.hasValueOfType(value_type)) {
            cast_cached_value(value_type);
        }
    }

    static final int MAX_IMAGE_LENGTH_INT = Integer.toString(Integer.MAX_VALUE).length()-1;
    static final int MAX_IMAGE_LENGTH_LONG = Long.toString(Long.MAX_VALUE).length()-1;
    static final int MAX_IMAGE_LENGTH_HEX_INT = Integer.toHexString(Integer.MAX_VALUE).length()-1;
    static final int MAX_IMAGE_LENGTH_HEX_LONG = Long.toHexString(Long.MAX_VALUE).length()-1;

    private final void load_scalar_value() throws IOException {
        // make sure we're trying to load a scalar value here
        switch(_value_type) {
        case NULL:
            _v.setValueToNull(_null_type);
            _v.setAuthoritativeType(AS_TYPE.null_value);
            return;
        case BOOL:
        case INT:
        case FLOAT:
        case DECIMAL:
        case TIMESTAMP:
        case SYMBOL:
        case STRING:
            break;
        default:
            return;
        }

        StringBuilder cs = token_contents_load(_scanner.getToken());

        int token_type = _scanner.getToken();

        if (_value_type == IonType.DECIMAL) {
            // we do this here (instead of in the case below
            // so that we can modify the value while it's not
            // a string, but still in the StringBuilder
            for (int ii=0; ii<cs.length(); ii++) {
                int c = cs.charAt(ii);
                if (c == 'd' || c == 'D') {
                    cs.setCharAt(ii, 'e');
                    break;
                }
            }
        }
        else if (token_type == IonTokenConstsX.TOKEN_HEX) {
            boolean is_negative = (cs.charAt(0) == '-');
            // prefix = is_negative ? "-0x" : "0x";
            int pos;
            if (is_negative) {
                pos = 1;
            }
            else {
                pos = 0;
            }
            assert((cs.length() > 2) && (cs.charAt(pos) == '0') && (cs.charAt(pos+1) == 'x' || cs.charAt(pos+1) == 'X'));
            cs.deleteCharAt(pos);
            cs.deleteCharAt(pos);
        }


        int          len = cs.length();
        String       s  = cs.toString();

        clear_current_value_buffer();

        switch (token_type) {
        case IonTokenConstsX.TOKEN_UNKNOWN_NUMERIC:
            switch (_value_type) {
            case INT:
                if (len <= MAX_IMAGE_LENGTH_INT) {
                    _v.setValue(Integer.parseInt(s));
                }
                else if (len <= MAX_IMAGE_LENGTH_LONG) {
                    _v.setValue(Long.parseLong(s));
                }
                else {
                    _v.setValue(new BigInteger(s));
                }
                break;
            case DECIMAL:
                // note that the string was modified above when it was a charsequence
                _v.setValue(Decimal.valueOf(s));
                break;
            case FLOAT:
                _v.setValue(Double.parseDouble(s));
                break;
            case TIMESTAMP:
                _v.setValue(Timestamp.valueOf(s));
                break;
            default:
                String message = "unexpected prefectched value type "
                               + getType().toString()
                               + " encountered handling an unquoted symbol";
                parse_error(message);
            }
            break;
        case IonTokenConstsX.TOKEN_INT:
            if (len <= MAX_IMAGE_LENGTH_INT) {
                _v.setValue(Integer.parseInt(s));
            }
            else if (len <= MAX_IMAGE_LENGTH_LONG) {
                _v.setValue(Long.parseLong(s));
            }
            else {
                _v.setValue(new BigInteger(s));
            }
            break;
        case IonTokenConstsX.TOKEN_HEX:
            if (len <= MAX_IMAGE_LENGTH_HEX_INT) {
                int v_int = Integer.parseInt(s, 16);
                _v.setValue(v_int);
            }
            else if (len <= MAX_IMAGE_LENGTH_HEX_LONG) {
                long v_long = Long.parseLong(s, 16);
                _v.setValue(v_long);
            }
            else {
                BigInteger v_big_int = new BigInteger(s, 16);
                _v.setValue(v_big_int);
            }
            break;
        case IonTokenConstsX.TOKEN_DECIMAL:
            _v.setValue(Decimal.valueOf(s));
            break;
        case IonTokenConstsX.TOKEN_FLOAT:
            _v.setValue(Double.parseDouble(s));
            break;
        case IonTokenConstsX.TOKEN_TIMESTAMP:
            _v.setValue(Timestamp.valueOf(s));
            break;
        case IonTokenConstsX.TOKEN_SYMBOL_BASIC:
            // this includes the various value keywords like true
            // and nan, in addition to "normal" unquoted symbols

            // we check to make sure it's not null first
            // since it could be null.symbol (which would
            // have the getType() of SYMBOL and would confuse
            // us as to what the saved type is)
            if (isNullValue()) {
                // the raw parser set _null_type when it
                // detected the unquoted symbol null in the
                // input (which is what isNullValue looks at)
                _v.setValueToNull(_null_type);
            }
            else {
                switch(getType()) {
                case SYMBOL:
                    _v.setValue(s);
                    break;
                case FLOAT:
                    switch (_value_keyword) {
                    case IonTokenConstsX.KEYWORD_NAN:
                        _v.setValue(Double.NaN);
                        break;
                    case IonTokenConstsX.KEYWORD_INF:
                    case IonTokenConstsX.KEYWORD_PLUS_INF:
                        _v.setValue(Double.POSITIVE_INFINITY);
                        break;
                    case IonTokenConstsX.KEYWORD_MINUS_INF:
                        _v.setValue(Double.NEGATIVE_INFINITY);
                        break;
                    default:
                        String message = "unexpected keyword "
                                       + s
                                       + " identified as a FLOAT";
                        parse_error(message);
                    }
                    break;
                case BOOL:
                    switch (_value_keyword) {
                    case IonTokenConstsX.KEYWORD_TRUE:
                        _v.setValue(true);
                        break;
                    case IonTokenConstsX.KEYWORD_FALSE:
                        _v.setValue(false);
                        break;
                    default:
                        String message = "unexpected keyword "
                            + s
                            + " identified as a BOOL";
                        parse_error(message);
                    }
                    break;
                default:
                    String message = "unexpected prefectched value type "
                                   + getType().toString()
                                   + " encountered handling an unquoted symbol";
                    parse_error(message);
                }
            }
            break;
        case IonTokenConstsX.TOKEN_SYMBOL_QUOTED:
        case IonTokenConstsX.TOKEN_SYMBOL_OPERATOR:
        case IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE:
            _v.setValue(s);
            break;
        case IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE:
            // long strings (triple quoted strings) are never
            // finished by the raw parser.  At most it reads
            // the first triple quoted string.
            _v.setValue(s);
            break;
        default:
            parse_error("scalar token "+IonTokenConstsX.getTokenName(_scanner.getToken())+"isn't a recognized type");
        }
    }
    private final void cast_cached_value(int value_type)
    {
        // this should only be called when it actually has to do some work
        assert !_v.hasValueOfType(value_type);

        if (_v.isNull()) {
            return;
        }

        if (IonType.SYMBOL.equals(_value_type)) {
            switch(value_type) {
                case AS_TYPE.int_value:
                    int sid = _v.getInt();
                    String sym = getSymbolTable().findSymbol(sid);
                    _v.setValue(sym);
                    break;
                case AS_TYPE.string_value:
                    sym = _v.getString();
                    sid = getSymbolTable().findSymbol(sym);
                    _v.setValue(sid);
                    break;
                default:
                {   String message = "can't cast symbol from "
                        +IonScalarConversionsX.getValueTypeName(_v.getAuthoritativeType())
                        +" to "
                        +IonScalarConversionsX.getValueTypeName(value_type);
                    throw new CantConvertException(message);
                }
            }
        }
        else {
            if (!_v.can_convert(value_type)) {
                String message = "can't cast from "
                    +IonScalarConversionsX.getValueTypeName(_v.getAuthoritativeType())
                    +" to "
                    +IonScalarConversionsX.getValueTypeName(value_type);
                throw new CantConvertException(message);
            }
            int fnid = _v.get_conversion_fnid(value_type);
            _v.cast(fnid);
        }
    }

    //
    // public value routines
    //
    @Override
    public boolean isNullValue()
    {
        return _v.isNull();
    }
    @Override
    public boolean booleanValue()
    {
        load_or_cast_cached_value(AS_TYPE.boolean_value);
        return _v.getBoolean();
    }
    @Override
    public double doubleValue()
    {
        load_or_cast_cached_value(AS_TYPE.double_value);
        return _v.getDouble();
    }
    @Override
    public int intValue()
    {
        load_or_cast_cached_value(AS_TYPE.int_value);
        return _v.getInt();
    }
    @Override
    public long longValue()
    {
        load_or_cast_cached_value(AS_TYPE.long_value);
        return _v.getLong();
    }
    @Override
    public BigInteger bigIntegerValue()
    {
        load_or_cast_cached_value(AS_TYPE.bigInteger_value);
        return _v.getBigInteger();
    }
    @Override
    public BigDecimal bigDecimalValue()
    {
        load_or_cast_cached_value(AS_TYPE.decimal_value);
        return _v.getBigDecimal();
    }
    public Decimal decimalValue()
    {
        load_or_cast_cached_value(AS_TYPE.decimal_value);
        return _v.getDecimal();
    }
    @Override
    public Date dateValue()
    {
        load_or_cast_cached_value(AS_TYPE.date_value);
        return _v.getDate();
    }
    @Override
    public Timestamp timestampValue()
    {
        load_or_cast_cached_value(AS_TYPE.timestamp_value);
        return _v.getTimestamp();
    }
    @Override
    public String stringValue()
    {
        load_or_cast_cached_value(AS_TYPE.string_value);
        return _v.getString();
    }

    //
    // blob and clob support routines
    //
    @Override
    public int byteSize()
    {
        long len;
        try {
            len = load_lob_contents();
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        if (len < 0 || len > Integer.MAX_VALUE) {
            load_lob_length_overflow_error(len);
        }
        return (int)len;
    }
    private final void load_lob_length_overflow_error(long len) {
        String message = "Size overflow: "
            + _value_type.toString()
            + " size ("
            + Long.toString(len)
            + ") exceeds int ";
        throw new IonException(message);
    }

    private final long load_lob_save_point() throws IOException
    {
        if (LOB_STATE.EMPTY.equals(_lob_loaded)) {
            assert(!_current_value_save_point_loaded && _current_value_save_point.isClear());
            _scanner.save_point_start(_current_value_save_point);
            _scanner.skip_over_lob(_lob_token, _current_value_save_point);
            _scanner.skip_lob_close_punctuation(_lob_token);
            _current_value_save_point_loaded = true;
            tokenValueIsFinished();
            _lob_loaded = LOB_STATE.READ;
        }

        long size = _current_value_save_point.length();
        return size;
    }
    private int load_lob_contents() throws IOException
    {
        if (LOB_STATE.EMPTY.equals(_lob_loaded)) {
            load_lob_save_point();
        }
        if (LOB_STATE.READ.equals(_lob_loaded)) {
            long raw_size =  _current_value_save_point.length();
            if (raw_size < 0 || raw_size > Integer.MAX_VALUE) {
                load_lob_length_overflow_error(raw_size);
            }
            _lob_bytes = new byte[(int)raw_size];

            try {
                assert(_current_value_save_point_loaded && _current_value_save_point.isDefined());
                _scanner.save_point_activate(_current_value_save_point);
                _lob_actual_len = readBytes(_lob_bytes, 0, (int)raw_size);
                _scanner.save_point_deactivate(_current_value_save_point);
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            assert(_lob_actual_len <= raw_size);

            _lob_loaded = LOB_STATE.FINISHED;
        }
        assert( _lob_loaded == LOB_STATE.FINISHED);
        return _lob_actual_len;
    }

    @Override
    public byte[] newBytes()
    {
        byte[] bytes;
        int    len;

        try {
            len = load_lob_contents();
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        bytes = new byte[len];
        System.arraycopy(_lob_bytes, 0, bytes, 0, len);

        return bytes;
    }
    @Override
    public int getBytes(byte[] buffer, int offset, int len)
    {
        int len_read;

        switch (_value_type) {
        case CLOB:
        case BLOB:
            break;
        default:
            throw new IllegalStateException("getBytes is only valid if the reader is on a lob value, not a "+_value_type+" value");
        }

        if (LOB_STATE.READ.equals(_lob_loaded)) {
            // if we've already read through the lob
            // (and therefore have it's length and the
            // bytes cached in our input buffer) anyway
            // just finish the job of converting the
            // data to something useful
            try {
                load_lob_contents();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }

        if (LOB_STATE.FINISHED.equals(_lob_loaded)) {
            // if we have loaded data, just copy it
            len_read = len;
            if (len_read > _lob_actual_len) {
                len_read = _lob_actual_len;
            }
            System.arraycopy(_lob_bytes, 0, buffer, offset, len_read);
        }
        else {
            // if we haven't loaded it, the we also haven't
            // even read over the data - so we'll read if from
            // the input source
            try {
                if (_current_value_save_point_loaded && _lob_value_position > 0) {
                    if (_current_value_save_point.isActive()) {
                        _scanner.save_point_deactivate(_current_value_save_point);
                    }
                    _scanner.save_point_activate(_current_value_save_point);
                    _lob_value_position = 0;
                }

                assert(_current_value_save_point_loaded && _current_value_save_point.isDefined());
                _scanner.save_point_activate(_current_value_save_point);

                len_read = readBytes(buffer, offset, len);
                _scanner.save_point_deactivate(_current_value_save_point);

            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }
        return len_read;
    }
    public int readBytes(byte[] buffer, int offset, int len) throws IOException
    {
        switch (_value_type) {
        case CLOB:
        case BLOB:
            break;
        default:
            throw new IllegalStateException("readBytes is only valid if the reader is on a lob value, not a "+_value_type+" value");
        }

        int starting_offset = offset;
        int c = -1;

        switch (_lob_token) {
        case IonTokenConstsX.TOKEN_OPEN_DOUBLE_BRACE:
            while (len-- > 0) {
                c = _scanner.read_base64_byte();
                if (c < 0) break;
                buffer[offset++] = (byte)c;
            }
            break;
        case IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE:
            while (len-- > 0) {
                c = _scanner.read_double_quoted_char(true);
                if (c < 0) {
                    if (c == IonTokenConstsX.EMPTY_ESCAPE_SEQUENCE) {
                        continue;
                    }
                    break;
                }
                assert(c >= 0 && c <= Byte.MAX_VALUE);
                buffer[offset++] = (byte)c;
            }
            break;
        case IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE:
            while (len-- > 0) {
                c = _scanner.read_triple_quoted_char(true);
                if (c < 0) {
                    if (c == IonTokenConstsX.EMPTY_ESCAPE_SEQUENCE) {
                        continue;
                    }
                    break;
                }
                assert(c >= 0 && c <= Byte.MAX_VALUE);
                buffer[offset++] = (byte)c;
            }
            break;
        default:
            String message = "invalid type ["+_value_type.toString()+"] for lob handling";
            throw new IonReaderTextTokenException(message);
        }
        if (c == -1) {
            _scanner.tokenIsFinished();
        }
        int read = offset - starting_offset;
        _lob_value_position += read;   // TODO: is _lob_value_position really needed?
        return read;
    }

    public IonValue getIonValue(IonSystem sys)
    {
        if (isNullValue()) {
            switch (_value_type) {
            case NULL:      return sys.newNull();
            case BOOL:      return sys.newNullBool();
            case INT:       return sys.newNullInt();
            case FLOAT:     return sys.newNullFloat();
            case DECIMAL:   return sys.newNullDecimal();
            case TIMESTAMP: return sys.newNullTimestamp();
            case SYMBOL:    return sys.newNullSymbol();
            case STRING:    return sys.newNullString();
            case CLOB:      return sys.newNullClob();
            case BLOB:      return sys.newNullBlob();
            case LIST:      return sys.newNullList();
            case SEXP:      return sys.newNullSexp();
            case STRUCT:    return sys.newNullString();
            default:
                throw new IonException("unrecognized type encountered");
            }
        }

        switch (_value_type) {
        case NULL:      return sys.newNull();
        case BOOL:      return sys.newBool(booleanValue());
        case INT:       return sys.newInt(longValue());
        case FLOAT:     return sys.newFloat(doubleValue());
        case DECIMAL:   return sys.newDecimal(decimalValue());
        case TIMESTAMP:
            IonTimestamp t = sys.newNullTimestamp();
            Timestamp ti = timestampValue();
            t.setValue(ti);
            return t;
        case SYMBOL:    return sys.newSymbol(stringValue());
        case STRING:    return sys.newString(stringValue());
        case CLOB:
            IonClob clob = sys.newNullClob();
            // FIXME inefficient: both newBytes and setBytes copy the data
            clob.setBytes(newBytes());
            return clob;
        case BLOB:
            IonBlob blob = sys.newNullBlob();
            // FIXME inefficient: both newBytes and setBytes copy the data
            blob.setBytes(newBytes());
            return blob;
        case LIST:
            IonList list = sys.newNullList();
            fillContainerList(sys, list);
            return list;
        case SEXP:
            IonSexp sexp = sys.newNullSexp();
            fillContainerList(sys, sexp);
            return sexp;
        case STRUCT:
            IonStruct struct = sys.newNullStruct();
            fillContainerStruct(sys, struct);
            return struct;
        default:
            throw new IonException("unrecognized type encountered");
        }
    }
    private final void fillContainerList(IonSystem sys, IonSequence list) {
        this.stepIn();
        while (this.next() != null) {
            IonValue v = this.getIonValue(sys);
            list.add(v);
        }
        this.stepOut();
    }
    private final void fillContainerStruct(IonSystem sys, IonStruct struct) {
        this.stepIn();
        while (this.next() != null) {
            String name = this.getFieldName();
            IonValue v = this.getIonValue(sys);
            struct.add(name, v);
        }
        this.stepOut();
    }
}
