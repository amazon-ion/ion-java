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

import static software.amazon.ion.IonType.SYMBOL;
import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import software.amazon.ion.Decimal;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.NullValueException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;
import software.amazon.ion.UnknownSymbolException;
import software.amazon.ion.impl.PrivateScalarConversions.AS_TYPE;
import software.amazon.ion.impl.PrivateScalarConversions.ValueVariant;

class IonReaderBinarySystemX
    extends IonReaderBinaryRawX
    implements PrivateReaderWriter
{
    IonSystem _system;
    SymbolTable _symbols;
    // ValueVariant _v; actually owned by the raw reader so it can be cleared at appropriate times

    IonReaderBinarySystemX(IonSystem system, UnifiedInputStreamX in)
    {
        super();
        init_raw(in);
        _system = system;
        _symbols = system.getSystemSymbolTable();
    }


    //
    // public methods that typically user level methods
    // these are filled in by either the system reader
    // or the user reader.  Here they just fail.
    //

    public SymbolToken[] getTypeAnnotationSymbols()
    {
        load_annotations();

        int count = _annotation_count;
        if (count == 0) return SymbolToken.EMPTY_ARRAY;

        SymbolTable symtab = getSymbolTable();

        SymbolToken[] result = new SymbolToken[count];
        for (int i = 0; i < count; i++)
        {
            int sid = _annotation_ids[i];
            String text = symtab.findKnownSymbol(sid);
            result[i] = new SymbolTokenImpl(text, sid);
        }

        return result;
    }

    private void load_once()
    {
        if (_v.isEmpty()) {
            try {
                load_scalar_value();
            }
            catch (IOException e) {
                error(e);
            }
        }
    }

    //
    //  basic scalar value getters (for actual content)
    //
    protected final void prepare_value(int as_type) {
        load_once();
        if (as_type != 0 && !_v.hasValueOfType(as_type)) {
            // we should never get here with a symbol asking for anything other
            // than a numeric cast (from some other numeric already loaded)
            if (IonType.SYMBOL.equals(_value_type) && !ValueVariant.isNumericType(as_type)) {
                assert(IonType.SYMBOL.equals(_value_type) && !ValueVariant.isNumericType(as_type));
            }

            if (!_v.can_convert(as_type)) {
                String message = "can't cast from "
                    +PrivateScalarConversions.getValueTypeName(_v.getAuthoritativeType())
                    +" to "
                    +PrivateScalarConversions.getValueTypeName(as_type);
                throw new IllegalStateException(message);
            }
            int fnid = _v.get_conversion_fnid(as_type);
            _v.cast(fnid);
        }
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
    protected final void load_cached_value(int value_type) throws IOException
    {
        if (_v.isEmpty()) {
            load_scalar_value();
        }
    }

    /** Utility method to convert an unsigned magnitude stored as a long to a {@link BigInteger}. */
    private static BigInteger unsignedLongToBigInteger(int signum, long val)
    {
        byte[] magnitude = {
            (byte) ((val >> 56) & 0xFF),
            (byte) ((val >> 48) & 0xFF),
            (byte) ((val >> 40) & 0xFF),
            (byte) ((val >> 32) & 0xFF),
            (byte) ((val >> 24) & 0xFF),
            (byte) ((val >> 16) & 0xFF),
            (byte) ((val >>  8) & 0xFF),
            (byte) (val & 0xFF),
        };
        return new BigInteger(signum, magnitude);
    }

    static final BigInteger MIN_LONG_VALUE = BigInteger.valueOf(Long.MIN_VALUE);
    static final BigInteger MAX_LONG_VALUE = BigInteger.valueOf(Long.MAX_VALUE);

    private final void load_scalar_value() throws IOException
    {
        // make sure we're trying to load a scalar value here
        switch(_value_type) {
        case NULL:
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

        // this will be true when the value_type is null as
        // well as when we encounter a null of any other type
        if (_value_is_null) {
            _v.setValueToNull(_value_type);
            _v.setAuthoritativeType(AS_TYPE.null_value);
            return;
        }

        switch (_value_type) {
        default:
            return;
        case BOOL:
            _v.setValue(_value_is_true);
            _v.setAuthoritativeType(AS_TYPE.boolean_value);
            break;
        case INT:
            if (_value_len == 0) {
                int v = 0;
                _v.setValue(v);
                _v.setAuthoritativeType(AS_TYPE.int_value);
            }
            else if (_value_len <= Long.BYTES) {
                long v = readULong(_value_len);
                boolean is_negative = _value_tid == PrivateIonConstants.tidNegInt;

                if (v < 0) {
                    // we probably can't fit this magnitude properly into a Java long
                    int signum = !is_negative ? 1 : -1;
                    BigInteger big = unsignedLongToBigInteger(signum, v);
                    _v.setValue(big);
                    // boundary condition
                    if (big.compareTo(MIN_LONG_VALUE) < 0 || big.compareTo(MAX_LONG_VALUE) > 0) {
                        _v.setAuthoritativeType(AS_TYPE.bigInteger_value);
                    } else {
                        // fits in long
                        _v.addValue(big.longValue()); // keep the BigInteger value set in case the user wants to resurrect it as such
                        _v.setAuthoritativeType(AS_TYPE.long_value);
                    }
                }
                else {
                    if (is_negative) {
                        v = -v;
                    }
                    if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                        _v.setValue(v);
                        _v.setAuthoritativeType(AS_TYPE.long_value);
                    } else {
                        _v.setValue((int)v);
                        _v.setAuthoritativeType(AS_TYPE.int_value);
                    }
                }
            }
            else {
                boolean is_negative = (_value_tid == PrivateIonConstants.tidNegInt);
                BigInteger v = readBigInteger(_value_len, is_negative);
                _v.setValue(v);
                _v.setAuthoritativeType(AS_TYPE.bigInteger_value);
            }
            break;
        case FLOAT:
            double d;
            if (_value_len == 0) {
                d = 0.0;
            }
            else {
                d = readFloat(_value_len);
            }
            _v.setValue(d);
            _v.setAuthoritativeType(AS_TYPE.double_value);
            break;
        case DECIMAL:
            Decimal dec = readDecimal(_value_len);
            _v.setValue(dec);
            _v.setAuthoritativeType(AS_TYPE.decimal_value);
            break;
        case TIMESTAMP:
            // TODO: it looks like a 0 length return a null timestamp - is that right?
            Timestamp t = readTimestamp(_value_len);
            _v.setValue(t);
            _v.setAuthoritativeType(AS_TYPE.timestamp_value);
            break;
        case SYMBOL:
            long sid = readULong(_value_len);
            if (sid < 1 || sid > Integer.MAX_VALUE) {
                String message = "symbol id ["
                               + sid
                               + "] out of range "
                               + "(1-"
                               + Integer.MAX_VALUE
                               + ")";
                throwErrorAt(message);
            }
            // TODO: is treating this as an int too misleading?
            _v.setValue((int)sid);
            _v.setAuthoritativeType(AS_TYPE.int_value);
            break;
        case STRING:
            String s = readString(_value_len);
            _v.setValue(s);
            _v.setAuthoritativeType(AS_TYPE.string_value);
            break;
        }
        _state = State.S_AFTER_VALUE;
    }

    //
    // public value routines
    //

    @Override
    public boolean isNullValue()
    {
        return _value_is_null;
    }

    public boolean booleanValue()
    {
        prepare_value(AS_TYPE.boolean_value);
        return _v.getBoolean();
    }

    public double doubleValue()
    {
        prepare_value(AS_TYPE.double_value);
        return _v.getDouble();
    }

    public int intValue()
    {
        if (_value_type != IonType.INT &&
            _value_type != IonType.DECIMAL &&
            _value_type != IonType.FLOAT)
        {
            throw new IllegalStateException();
        }

        prepare_value(AS_TYPE.int_value);
        return _v.getInt();
    }

    public long longValue()
    {
        if (_value_type != IonType.INT &&
            _value_type != IonType.DECIMAL &&
            _value_type != IonType.FLOAT)
        {
            throw new IllegalStateException();
        }

        prepare_value(AS_TYPE.long_value);
        return _v.getLong();
    }

    public BigInteger bigIntegerValue()
    {
        if (_value_type != IonType.INT &&
            _value_type != IonType.DECIMAL &&
            _value_type != IonType.FLOAT)
        {
            throw new IllegalStateException();
        }

        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.bigInteger_value);
        return _v.getBigInteger();
    }

    public BigDecimal bigDecimalValue()
    {
        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.decimal_value);
        return _v.getBigDecimal();
    }

    public Decimal decimalValue()
    {
        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.decimal_value);
        return _v.getDecimal();
    }

    public Date dateValue()
    {
        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.date_value);
        return _v.getDate();
    }

    public Timestamp timestampValue()
    {
        if (_value_is_null) {
            return null;
        }
        prepare_value(AS_TYPE.timestamp_value);
        return _v.getTimestamp();
    }

    @Override
    public IntegerSize getIntegerSize()
    {
        load_once();
        if (_value_type != IonType.INT || _v.isNull())
        {
            return null;
        }
        return PrivateScalarConversions.getIntegerSize(_v.getAuthoritativeType());
    }

    // TODO amznlabs/ion-java#63 this needs to use the appropriate symbol table for user values

    public final String stringValue()
    {
        if (! IonType.isText(_value_type)) throw new IllegalStateException();
        if (_value_is_null) return null;

        if (_value_type == SYMBOL) {
            if (!_v.hasValueOfType(AS_TYPE.string_value)) {
                int sid = getSymbolId();
                String name = _symbols.findKnownSymbol(sid);
                if (name == null) {
                    throw new UnknownSymbolException(sid);
                }
                _v.addValue(name);
            }
        }
        else {
            prepare_value(AS_TYPE.string_value);
        }
        return _v.getString();
    }

    public final SymbolToken symbolValue()
    {
        if (_value_type != SYMBOL) throw new IllegalStateException();
        if (_value_is_null) return null;

        int sid = getSymbolId();
        assert sid != UNKNOWN_SYMBOL_ID;
        String text = _symbols.findKnownSymbol(sid);

        return new SymbolTokenImpl(text, sid);
    }

    int getSymbolId()
    {
        if (_value_type != SYMBOL) throw new IllegalStateException();
        if (_value_is_null) throw new NullValueException();

        prepare_value(AS_TYPE.int_value);
        return _v.getInt();
    }

    public final String getFieldName()
    {
        String name;
        if (_value_field_id == SymbolTable.UNKNOWN_SYMBOL_ID) {
            name = null;
        }
        else {
            name = _symbols.findKnownSymbol(_value_field_id);
            if (name == null) {
                throw new UnknownSymbolException(_value_field_id);
            }
        }
        return name;
    }

    public final SymbolToken getFieldNameSymbol()
    {
        if (_value_field_id == SymbolTable.UNKNOWN_SYMBOL_ID) return null;
        int sid = _value_field_id;
        String text = _symbols.findKnownSymbol(sid);
        return new SymbolTokenImpl(text, sid);
    }

    public final Iterator<String> iterateTypeAnnotations()
    {
        String[] annotations = getTypeAnnotations();
        return PrivateUtils.stringIterator(annotations);
    }

    public final String[] getTypeAnnotations()
    {
        load_annotations();
        String[] anns;
        if (_annotation_count < 1) {
            anns = PrivateUtils.EMPTY_STRING_ARRAY;
        }
        else {
            anns = new String[_annotation_count];
            for (int ii=0; ii<_annotation_count; ii++) {
                anns[ii] = _symbols.findKnownSymbol(_annotation_ids[ii]);
                if (anns[ii] == null) {
                    throw new UnknownSymbolException(_annotation_ids[ii]);
                }
            }
        }
        return anns;
    }

    public final SymbolTable getSymbolTable()
    {
        return _symbols;
    }

    // system readers don't skip any symbol tables
    public SymbolTable pop_passed_symbol_table()
    {
        return null;
    }
}
