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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import software.amazon.ion.Decimal;
import software.amazon.ion.IntegerSize;
import software.amazon.ion.IonException;
import software.amazon.ion.IonType;
import software.amazon.ion.Timestamp;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public class PrivateScalarConversions
{
    public static final class AS_TYPE {

        public static final int null_value         =  1;
        public static final int boolean_value      =  2;
        public static final int int_value          =  3;
        public static final int long_value         =  4;
        public static final int bigInteger_value   =  5;
        public static final int decimal_value      =  6;
        public static final int double_value       =  7;
        public static final int string_value       =  8;
        public static final int date_value         =  9;
        public static final int timestamp_value    = 10;

        static final int idx_to_bit_mask(int idx) {
            return 1 << (idx - 1);
        }

        public static final int numeric_types      = idx_to_bit_mask(int_value)
                                                   | idx_to_bit_mask(long_value)
                                                   | idx_to_bit_mask(bigInteger_value)
                                                   | idx_to_bit_mask(decimal_value)
                                                   | idx_to_bit_mask(double_value);

        public static final int datetime_types     = idx_to_bit_mask(date_value)
                                                   | idx_to_bit_mask(timestamp_value);

        public static final int convertable_type   = idx_to_bit_mask(numeric_types)
                                                   | idx_to_bit_mask(datetime_types)
                                                   | idx_to_bit_mask(boolean_value)
                                                   | idx_to_bit_mask(string_value);
    }

    public static IntegerSize getIntegerSize(int authoritative_type) {
        switch (authoritative_type)
        {
            case AS_TYPE.int_value:
                return IntegerSize.INT;
            case AS_TYPE.long_value:
                return IntegerSize.LONG;
            case AS_TYPE.bigInteger_value:
                return IntegerSize.BIG_INTEGER;
            default:
                return null;
        }
    }

    public static String getValueTypeName(int value_type) {
        switch (value_type) {
        case AS_TYPE.null_value:       return "null";
        case AS_TYPE.boolean_value:    return "boolean";
        case AS_TYPE.int_value:        return "int";
        case AS_TYPE.long_value:       return "long";
        case AS_TYPE.bigInteger_value: return "bigInteger";
        case AS_TYPE.decimal_value:    return "decimal";
        case AS_TYPE.double_value:     return "double";
        case AS_TYPE.string_value:     return "string";
        case AS_TYPE.date_value:       return "date";
        case AS_TYPE.timestamp_value:  return "timestamp";
        default:                       return "<unrecognized conversion value type: "+Integer.toString(value_type)+">";
        }
    }
    public static String get_value_type_name(int value_type) {
        switch (value_type) {
        case AS_TYPE.null_value:
        case AS_TYPE.boolean_value:
        case AS_TYPE.int_value:
        case AS_TYPE.long_value:
        case AS_TYPE.bigInteger_value:
        case AS_TYPE.decimal_value:
        case AS_TYPE.double_value:
        case AS_TYPE.string_value:
        case AS_TYPE.date_value:
        case AS_TYPE.timestamp_value:
            return getValueTypeName(value_type)+"_value";
        default:
            return "<unrecognized conversion value type: "
                   +Integer.toString(value_type)
                   + ">";
        }
    }

    protected static int FNID_no_conversion               = -1;
    protected static int FNID_identity                    =  0;

    //from_string_conversion
    protected final static int FNID_FROM_STRING_TO_NULL         =  1;
    protected final static int FNID_FROM_STRING_TO_BOOLEAN      =  2;
    protected final static int FNID_FROM_STRING_TO_INT          =  3;
    protected final static int FNID_FROM_STRING_TO_LONG         =  4;
    protected final static int FNID_FROM_STRING_TO_BIGINTEGER   =  5;
    protected final static int FNID_FROM_STRING_TO_DECIMAL      =  6;
    protected final static int FNID_FROM_STRING_TO_DOUBLE       =  7;
    protected final static int FNID_FROM_STRING_TO_DATE         =  8;
    protected final static int FNID_FROM_STRING_TO_TIMESTAMP    =  9;
    static int [] from_string_conversion = {
                        FNID_no_conversion,  // fake entry to switch from 0 based to 1 based
                        FNID_FROM_STRING_TO_NULL,
                        FNID_FROM_STRING_TO_BOOLEAN,
                        FNID_FROM_STRING_TO_INT,
                        FNID_FROM_STRING_TO_LONG,
                        FNID_FROM_STRING_TO_BIGINTEGER,
                        FNID_FROM_STRING_TO_DECIMAL,
                        FNID_FROM_STRING_TO_DOUBLE,
                        FNID_identity,
                        FNID_FROM_STRING_TO_DATE,
                        FNID_FROM_STRING_TO_TIMESTAMP,
                        FNID_no_conversion,
                        FNID_no_conversion
                    };

    //to_string_conversions;
    protected final static int FNID_FROM_NULL_TO_STRING         = 10;
    protected final static int FNID_FROM_BOOLEAN_TO_STRING      = 11;
    protected final static int FNID_FROM_INT_TO_STRING          = 12;
    protected final static int FNID_FROM_LONG_TO_STRING         = 13;
    protected final static int FNID_FROM_BIGINTEGER_TO_STRING   = 14;
    protected final static int FNID_FROM_DECIMAL_TO_STRING      = 15;
    protected final static int FNID_FROM_DOUBLE_TO_STRING       = 16;
    protected final static int FNID_FROM_DATE_TO_STRING         = 17;
    protected final static int FNID_FROM_TIMESTAMP_TO_STRING    = 18;
    static int [] to_string_conversions = {
                        FNID_no_conversion,  // fake entry to switch from 0 based to 1 based
                        FNID_FROM_NULL_TO_STRING,
                        FNID_FROM_BOOLEAN_TO_STRING,
                        FNID_FROM_INT_TO_STRING,
                        FNID_FROM_LONG_TO_STRING,
                        FNID_FROM_BIGINTEGER_TO_STRING,
                        FNID_FROM_DECIMAL_TO_STRING,
                        FNID_FROM_DOUBLE_TO_STRING,
                        FNID_identity,
                        FNID_FROM_DATE_TO_STRING,
                        FNID_FROM_TIMESTAMP_TO_STRING,
                        FNID_no_conversion,
                        FNID_no_conversion
                    };


    //to_int_conversion;
    protected final static int FNID_FROM_LONG_TO_INT         = 19;
    protected final static int FNID_FROM_BIGINTEGER_TO_INT   = 20;
    protected final static int FNID_FROM_DECIMAL_TO_INT      = 21;
    protected final static int FNID_FROM_DOUBLE_TO_INT       = 22;
    static int [] to_int_conversion = {
                        FNID_no_conversion,  // fake entry to switch from 0 based to 1 based
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_identity,
                        FNID_FROM_LONG_TO_INT,
                        FNID_FROM_BIGINTEGER_TO_INT,
                        FNID_FROM_DECIMAL_TO_INT,
                        FNID_FROM_DOUBLE_TO_INT,
                        FNID_FROM_STRING_TO_INT,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion
                };

    //to_long_conversion;
    protected final static int FNID_FROM_INT_TO_LONG          = 23;
    protected final static int FNID_FROM_BIGINTEGER_TO_LONG   = 24;
    protected final static int FNID_FROM_DECIMAL_TO_LONG      = 25;
    protected final static int FNID_FROM_DOUBLE_TO_LONG       = 26;
    static int [] to_long_conversion = {
                        FNID_no_conversion,  // fake entry to switch from 0 based to 1 based
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_FROM_INT_TO_LONG,
                        FNID_identity,
                        FNID_FROM_BIGINTEGER_TO_LONG,
                        FNID_FROM_DECIMAL_TO_LONG,
                        FNID_FROM_DOUBLE_TO_LONG,
                        FNID_FROM_STRING_TO_LONG,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion
                    };


    //to_bigInteger_conversion;
    protected final static int FNID_FROM_INT_TO_BIGINTEGER          = 27;
    protected final static int FNID_FROM_LONG_TO_BIGINTEGER         = 28;
    protected final static int FNID_FROM_DECIMAL_TO_BIGINTEGER      = 29;
    protected final static int FNID_FROM_DOUBLE_TO_BIGINTEGER       = 30;
    static int [] to_bigInteger_conversion = {
                        FNID_no_conversion,  // fake entry to switch from 0 based to 1 based
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_FROM_INT_TO_BIGINTEGER,
                        FNID_FROM_LONG_TO_BIGINTEGER,
                        FNID_identity,
                        FNID_FROM_DECIMAL_TO_BIGINTEGER,
                        FNID_FROM_DOUBLE_TO_BIGINTEGER,
                        FNID_FROM_STRING_TO_BIGINTEGER,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion
                    };

    //to_decimal_conversion;
    protected final static int FNID_FROM_INT_TO_DECIMAL          = 31;
    protected final static int FNID_FROM_LONG_TO_DECIMAL         = 32;
    protected final static int FNID_FROM_BIGINTEGER_TO_DECIMAL   = 33;
    protected final static int FNID_FROM_DOUBLE_TO_DECIMAL       = 34;
    static int [] to_decimal_conversion = {
                        FNID_no_conversion,  // fake entry to switch from 0 based to 1 based
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_FROM_INT_TO_DECIMAL,
                        FNID_FROM_LONG_TO_DECIMAL,
                        FNID_FROM_BIGINTEGER_TO_DECIMAL,
                        FNID_identity,
                        FNID_FROM_DOUBLE_TO_DECIMAL,
                        FNID_FROM_STRING_TO_DECIMAL,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion
                    };

    //to_double_conversion;
    protected final static int FNID_FROM_INT_TO_DOUBLE          = 35;
    protected final static int FNID_FROM_LONG_TO_DOUBLE         = 36;
    protected final static int FNID_FROM_BIGINTEGER_TO_DOUBLE   = 37;
    protected final static int FNID_FROM_DECIMAL_TO_DOUBLE      = 38;
    static int [] to_double_conversion = {
                        FNID_no_conversion,  // fake entry to switch from 0 based to 1 based
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_FROM_INT_TO_DOUBLE,
                        FNID_FROM_LONG_TO_DOUBLE,
                        FNID_FROM_BIGINTEGER_TO_DOUBLE,
                        FNID_FROM_DECIMAL_TO_DOUBLE,
                        FNID_identity,
                        FNID_FROM_STRING_TO_DOUBLE,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion,
                        FNID_no_conversion
                    };
    protected final static int FNID_FROM_TIMESTAMP_TO_DATE      = 39;
    protected final static int FNID_FROM_DATE_TO_TIMESTAMP      = 40;

    public final static String getAllValueTypeNames(int value_type) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        int bit = 1;
        for (int ii=0; ii<Integer.SIZE; ii++) {
            if ((value_type & bit) != 0) {
                sb.append(getValueTypeName(bit));
                sb.append(' ');
            }
            bit <<= 1;
        }
        sb.append(')');
        return sb.toString();
    }

    /**
     * from a values authoritative type (the type of the original data
     * for the value) and the desired type this returns the conversion
     * functions id, or throws a CantConvertException in the event the
     * authoritative type cannot be cast to the desired type.
     * @param authoritative_type
     * @param new_type
     * @return id of the conversion function required
     */
    protected final static int getConversionFnid(int authoritative_type, int new_type) {
        if (new_type == authoritative_type) return 0;
        switch (new_type) {
            case AS_TYPE.null_value:
                assert( authoritative_type == AS_TYPE.string_value );
                return from_string_conversion[AS_TYPE.null_value];
            case AS_TYPE.boolean_value:
                assert( authoritative_type == AS_TYPE.string_value );
                return from_string_conversion[AS_TYPE.boolean_value];
            case AS_TYPE.int_value:
                return to_int_conversion[authoritative_type];
            case AS_TYPE.long_value:
                return to_long_conversion[authoritative_type];
            case AS_TYPE.bigInteger_value:
                return to_bigInteger_conversion[authoritative_type];
            case AS_TYPE.decimal_value:
                return to_decimal_conversion[authoritative_type];
            case AS_TYPE.double_value:
                return to_double_conversion[authoritative_type];
            case AS_TYPE.string_value:
                return to_string_conversions[authoritative_type];
            case AS_TYPE.date_value:
                assert( authoritative_type == AS_TYPE.timestamp_value );
                return FNID_FROM_TIMESTAMP_TO_DATE;
            case AS_TYPE.timestamp_value:
                assert( authoritative_type == AS_TYPE.date_value );
                return FNID_FROM_DATE_TO_TIMESTAMP;
        }
        String message = "can't convert from "
                       + getValueTypeName(authoritative_type)
                       + " to "
                       + getValueTypeName(new_type);
        throw new CantConvertException(message);
    }

    public static class ConversionException extends IonException {
        private static final long serialVersionUID = 1L;
        ConversionException(String msg) {
            super(msg);
        }
        ConversionException(Exception e) {
            super(e);
        }
        ConversionException(String msg, Exception e) {
            super(msg, e);
        }
    }
    static public class CantConvertException extends ConversionException {
        private static final long serialVersionUID = 1L;
        CantConvertException(String msg) {
            super(msg);
        }
        CantConvertException(Exception e) {
            super(e);
        }
        CantConvertException(String msg, Exception e) {
            super(msg, e);
        }
    }
    static public class ValueNotSetException extends ConversionException {
        private static final long serialVersionUID = 1L;
        ValueNotSetException(String msg) {
            super(msg);
        }
        ValueNotSetException(Exception e) {
            super(e);
        }
        ValueNotSetException(String msg, Exception e) {
            super(msg, e);
        }
    }

    public static final class ValueVariant
    {
        private static final BigInteger min_int_value = BigInteger.valueOf(Integer.MIN_VALUE);
        private static final BigInteger max_int_value = BigInteger.valueOf(Integer.MAX_VALUE);
        private static final BigInteger min_long_value = BigInteger.valueOf(Long.MIN_VALUE);
        private static final BigInteger max_long_value = BigInteger.valueOf(Long.MAX_VALUE);
        private static final BigDecimal min_int_decimal_value = BigDecimal.valueOf(Integer.MIN_VALUE);
        private static final BigDecimal max_int_decimal_value = BigDecimal.valueOf(Integer.MAX_VALUE);
        private static final BigDecimal min_long_decimal_value = BigDecimal.valueOf(Long.MIN_VALUE);
        private static final BigDecimal max_long_decimal_value = BigDecimal.valueOf(Long.MAX_VALUE);

        // member variables for the variant
        int         _authoritative_type_idx;             // the original type - this not the bit mask
        int         _types_set;                          // all the various type it's been cast to (includes the original)
        boolean     _is_null;
        IonType     _null_type;
        boolean     _boolean_value;
        int         _int_value;
        long        _long_value;
        double      _double_value;
        String      _string_value;
        BigInteger  _bigInteger_value;
        Decimal     _decimal_value;
        Date        _date_value;
        Timestamp   _timestamp_value;

        //
        // public accessors
        //
        public final boolean isEmpty() {
            return (_authoritative_type_idx == 0);
        }
        public final void clear() {
            _authoritative_type_idx = 0;
            _types_set = 0;
        }
        public final boolean hasValueOfType(int value_type) {
            return ((_types_set & AS_TYPE.idx_to_bit_mask(value_type)) != 0);
        }
        public final boolean hasNumericType() {
            return ((AS_TYPE.numeric_types & _types_set) != 0);
        }
        public static final boolean isNumericType(int type_idx) {
            int type_flag = AS_TYPE.idx_to_bit_mask(type_idx);
            return ((AS_TYPE.numeric_types & type_flag) != 0);
        }
        public final boolean hasDatetimeType() {
            return ((AS_TYPE.datetime_types & _types_set) != 0);
        }
        public final void setAuthoritativeType(int value_type) {
            if (_authoritative_type_idx == value_type) return;
            if (!hasValueOfType(value_type)) {
                String message = "you must set the "
                               + getValueTypeName(value_type)
                               + " value before you can set the authoritative type to "
                               + getValueTypeName(value_type);
                throw new IllegalStateException(message);
            }
            _authoritative_type_idx = value_type;
        }
        public final void setValueToNull(IonType t) {
            _is_null = true;
            _null_type = t;
            set_value_type(AS_TYPE.null_value);
        }
        public final void setValue(boolean value) {
            _boolean_value = value;
            set_value_type(AS_TYPE.boolean_value);
        }
        public final void setValue(int value) {
            _int_value = value;
            set_value_type(AS_TYPE.int_value);
        }
        public final void setValue(long value) {
            _long_value = value;
            set_value_type(AS_TYPE.long_value);
        }
        public final void setValue(double value) {
            _double_value = value;
            set_value_type(AS_TYPE.double_value);
        }
        public final void setValue(String value) {
            _string_value = value;
            set_value_type(AS_TYPE.string_value);
        }
        public final void setValue(BigInteger value) {
            _bigInteger_value = value;
            set_value_type(AS_TYPE.bigInteger_value);
        }
        public final void setValue(Decimal value) {
            _decimal_value = value;
            set_value_type(AS_TYPE.decimal_value);
        }
        public final void setValue(Date value) {
            _date_value = value;
            set_value_type(AS_TYPE.date_value);
        }
        public final void setValue(Timestamp value) {
            _timestamp_value = value;
            set_value_type(AS_TYPE.timestamp_value);
        }

        public final void addValueToNull(IonType t) {
            _is_null = true;
            _null_type = t;
            add_value_type(AS_TYPE.null_value);
        }
        public final void addValue(boolean value) {
            _boolean_value = value;
            add_value_type(AS_TYPE.boolean_value);
        }
        public final void addValue(int value) {
            _int_value = value;
            add_value_type(AS_TYPE.int_value);
        }
        public final void addValue(long value) {
            _long_value = value;
            add_value_type(AS_TYPE.long_value);
        }
        public final void addValue(double value) {
            _double_value = value;
            add_value_type(AS_TYPE.double_value);
        }
        public final void addValue(String value) {
            _string_value = value;
            add_value_type(AS_TYPE.string_value);
        }
        public final void addValue(BigInteger value) {
            _bigInteger_value = value;
            add_value_type(AS_TYPE.bigInteger_value);
        }
        public final void addValue(BigDecimal value) {
            _decimal_value = (Decimal)value;
            add_value_type(AS_TYPE.decimal_value);
        }
        public final void addValue(Decimal value) {
            _decimal_value = value;
            add_value_type(AS_TYPE.decimal_value);
        }
        public final void addValue(Date value) {
            _date_value = value;
            add_value_type(AS_TYPE.date_value);
        }
        public final void addValue(Timestamp value) {
            _timestamp_value = value;
            add_value_type(AS_TYPE.timestamp_value);
        }


        public final int getAuthoritativeType() {
            return _authoritative_type_idx;
        }
        public final boolean isNull() {
            return (hasValueOfType(AS_TYPE.null_value));
        }
        public final IonType getNullType() {
            if (!hasValueOfType(AS_TYPE.null_value)) throw new ValueNotSetException("null value not set");
            return _null_type;
        }
        public final boolean getBoolean() {
            if (!hasValueOfType(AS_TYPE.boolean_value)) throw new ValueNotSetException("boolean not set");
            return _boolean_value;
        }
        public final int getInt() {
            if (!hasValueOfType(AS_TYPE.int_value)) throw new ValueNotSetException("int value not set");
            return _int_value;
        }
        public final long getLong() {
            if (!hasValueOfType(AS_TYPE.long_value)) throw new ValueNotSetException("long value not set");
            return _long_value;
        }
        public final double getDouble() {
            if (!hasValueOfType(AS_TYPE.double_value)) throw new ValueNotSetException("double value not set");
            return _double_value;
        }
        public final String getString() {
            if (!hasValueOfType(AS_TYPE.string_value)) throw new ValueNotSetException("String value not set");
            return _string_value;
        }
        public final BigInteger getBigInteger() {
            if (!hasValueOfType(AS_TYPE.bigInteger_value)) throw new ValueNotSetException("BigInteger value not set");
            return _bigInteger_value;
        }
        public final BigDecimal getBigDecimal() {
            if (!hasValueOfType(AS_TYPE.decimal_value)) throw new ValueNotSetException("BigDecimal value not set");
            return Decimal.bigDecimalValue(_decimal_value);
        }
        public final Decimal getDecimal() {
            if (!hasValueOfType(AS_TYPE.decimal_value)) throw new ValueNotSetException("BigDecimal value not set");
            return _decimal_value;
        }
        public final Date getDate() {
            if (!hasValueOfType(AS_TYPE.date_value)) throw new ValueNotSetException("Date value not set");
            return _date_value;
        }
        public final Timestamp getTimestamp() {
            if (!hasValueOfType(AS_TYPE.timestamp_value)) throw new ValueNotSetException("timestamp value not set");
            return _timestamp_value;
        }

        public final boolean can_convert(int new_type) {
            boolean can = false;

            switch (new_type) {
                case AS_TYPE.null_value:
                case AS_TYPE.boolean_value:
                    can = (_authoritative_type_idx == AS_TYPE.string_value);
                    break;

                case AS_TYPE.int_value:
                case AS_TYPE.long_value:
                case AS_TYPE.bigInteger_value:
                case AS_TYPE.decimal_value:
                case AS_TYPE.double_value:
                    can = (is_numeric_type(_authoritative_type_idx) || (_authoritative_type_idx == AS_TYPE.string_value));
                    break;

                case AS_TYPE.string_value:
                    can = true;
                    break;

                case AS_TYPE.date_value:
                case AS_TYPE.timestamp_value:
                    can = (is_datetime_type(_authoritative_type_idx) || (_authoritative_type_idx == AS_TYPE.string_value));
                    break;

                default:
                    can = false;
                    break;
            }
            assert( can ? (get_conversion_fnid(new_type) != -1) : (get_conversion_fnid(new_type) == -1));
            return can;
        }

        public final int get_conversion_fnid(int new_type) {
            return PrivateScalarConversions.getConversionFnid(_authoritative_type_idx, new_type);
        }

        public final void cast(int castfnid) {
            switch(castfnid) {
            case FNID_FROM_STRING_TO_NULL:           fn_from_string_to_null();                break;
            case FNID_FROM_STRING_TO_BOOLEAN:        fn_from_string_to_boolean();             break;
            case FNID_FROM_STRING_TO_INT:            fn_from_string_to_int();                 break;
            case FNID_FROM_STRING_TO_LONG:           fn_from_string_to_long();                break;
            case FNID_FROM_STRING_TO_BIGINTEGER:     fn_from_string_to_biginteger();          break;
            case FNID_FROM_STRING_TO_DECIMAL:        fn_from_string_to_decimal();             break;
            case FNID_FROM_STRING_TO_DOUBLE:         fn_from_string_to_double();              break;
            case FNID_FROM_STRING_TO_DATE:           fn_from_string_to_date();                break;
            case FNID_FROM_STRING_TO_TIMESTAMP:      fn_from_string_to_timestamp();           break;
            case FNID_FROM_NULL_TO_STRING:           fn_from_null_to_string();                break;
            case FNID_FROM_BOOLEAN_TO_STRING:        fn_from_boolean_to_string();             break;
            case FNID_FROM_INT_TO_STRING:            fn_from_int_to_string();                 break;
            case FNID_FROM_LONG_TO_STRING:           fn_from_long_to_string();                break;
            case FNID_FROM_BIGINTEGER_TO_STRING:     fn_from_biginteger_to_string();          break;
            case FNID_FROM_DECIMAL_TO_STRING:        fn_from_decimal_to_string();             break;
            case FNID_FROM_DOUBLE_TO_STRING:         fn_from_double_to_string();              break;
            case FNID_FROM_DATE_TO_STRING:           fn_from_date_to_string();                break;
            case FNID_FROM_TIMESTAMP_TO_STRING:      fn_from_timestamp_to_string();           break;
            case FNID_FROM_LONG_TO_INT:              fn_from_long_to_int();                   break;
            case FNID_FROM_BIGINTEGER_TO_INT:        fn_from_biginteger_to_int();             break;
            case FNID_FROM_DECIMAL_TO_INT:           fn_from_decimal_to_int();                break;
            case FNID_FROM_DOUBLE_TO_INT:            fn_from_double_to_int();                 break;
            case FNID_FROM_INT_TO_LONG:              fn_from_int_to_long();                   break;
            case FNID_FROM_BIGINTEGER_TO_LONG:       fn_from_biginteger_to_long();            break;
            case FNID_FROM_DECIMAL_TO_LONG:          fn_from_decimal_to_long();               break;
            case FNID_FROM_DOUBLE_TO_LONG:           fn_from_double_to_long();                break;
            case FNID_FROM_INT_TO_BIGINTEGER:        fn_from_int_to_biginteger();             break;
            case FNID_FROM_LONG_TO_BIGINTEGER:       fn_from_long_to_biginteger();            break;
            case FNID_FROM_DECIMAL_TO_BIGINTEGER:    fn_from_decimal_to_biginteger();         break;
            case FNID_FROM_DOUBLE_TO_BIGINTEGER:     fn_from_double_to_biginteger();          break;
            case FNID_FROM_INT_TO_DECIMAL:           fn_from_int_to_decimal();                break;
            case FNID_FROM_LONG_TO_DECIMAL:          fn_from_long_to_decimal();               break;
            case FNID_FROM_BIGINTEGER_TO_DECIMAL:    fn_from_biginteger_to_decimal();         break;
            case FNID_FROM_DOUBLE_TO_DECIMAL:        fn_from_double_to_decimal();             break;
            case FNID_FROM_INT_TO_DOUBLE:            fn_from_int_to_double();                 break;
            case FNID_FROM_LONG_TO_DOUBLE:           fn_from_long_to_double();                break;
            case FNID_FROM_BIGINTEGER_TO_DOUBLE:     fn_from_biginteger_to_double();          break;
            case FNID_FROM_DECIMAL_TO_DOUBLE:        fn_from_decimal_to_double();             break;
            case FNID_FROM_TIMESTAMP_TO_DATE:        fn_from_timestamp_to_date();             break;
            case FNID_FROM_DATE_TO_TIMESTAMP:        fn_from_date_to_timestamp();             break;
            default: throw new ConversionException("unrecognized conversion fnid ["+castfnid+"]invoked");
            }
        }

        private final void set_value_type(int type_idx) {
            _types_set = AS_TYPE.idx_to_bit_mask(type_idx);
            _authoritative_type_idx = type_idx;
        }
        private final void add_value_type(int type_idx) {
            _types_set |= AS_TYPE.idx_to_bit_mask(type_idx);
        }
        private final boolean is_numeric_type(int type_idx) {
            return (AS_TYPE.numeric_types & AS_TYPE.idx_to_bit_mask(type_idx)) != 0;
        }
        private final boolean is_datetime_type(int type_idx) {
            return (AS_TYPE.datetime_types & AS_TYPE.idx_to_bit_mask(type_idx)) != 0;
        }

        private final void fn_from_string_to_null() {
            _null_type = IonTokenConstsX.getNullType(_string_value);
            _is_null = true;
            add_value_type(AS_TYPE.null_value);
        }
        private final void fn_from_string_to_boolean() {
            _boolean_value = Boolean.parseBoolean(_string_value);
            add_value_type(AS_TYPE.boolean_value);
        }
        private final void fn_from_string_to_int() {
            _int_value = Integer.parseInt(_string_value);
            add_value_type(AS_TYPE.int_value);
        }
        private final void fn_from_string_to_long() {
            _long_value = Long.parseLong(_string_value);
            add_value_type(AS_TYPE.long_value);
        }
        private final void fn_from_string_to_biginteger() {
            _bigInteger_value = new BigInteger(_string_value);
            add_value_type(AS_TYPE.bigInteger_value);
        }
        private final void fn_from_string_to_decimal() {
            _decimal_value = Decimal.valueOf(_string_value);
            add_value_type(AS_TYPE.decimal_value);
        }
        private final void fn_from_string_to_double() {
            _double_value = Double.parseDouble(_string_value);
            add_value_type(AS_TYPE.double_value);
        }
        private final void fn_from_string_to_date() {
            if (!hasValueOfType(AS_TYPE.timestamp_value)) {
                fn_from_string_to_timestamp();
            }
            _date_value = new Date(_timestamp_value.getMillis());
            add_value_type(AS_TYPE.date_value);
        }
        private final void fn_from_string_to_timestamp() {
            _timestamp_value = Timestamp.valueOf(_string_value);
            add_value_type(AS_TYPE.timestamp_value);
            add_value_type(AS_TYPE.timestamp_value);
        }
        private final void fn_from_null_to_string() {
            _string_value = IonTokenConstsX.getNullImage(_null_type);
            add_value_type(AS_TYPE.string_value);
        }
        private final void fn_from_boolean_to_string() {
            _string_value = _boolean_value ? "true" : "false";
            add_value_type(AS_TYPE.string_value);
        }
        private final void fn_from_int_to_string() {
            _string_value = Integer.toString(_int_value);
            add_value_type(AS_TYPE.string_value);
        }
        private final void fn_from_long_to_string() {
            _string_value = Long.toString(_long_value);
            add_value_type(AS_TYPE.string_value);
        }
        private final void fn_from_biginteger_to_string() {
            _string_value = _bigInteger_value.toString();
            add_value_type(AS_TYPE.string_value);
        }
        private final void fn_from_decimal_to_string() {
            _string_value = _decimal_value.toString();
            add_value_type(AS_TYPE.string_value);
        }
        private final void fn_from_double_to_string() {
            _string_value = Double.toString(_double_value);
            add_value_type(AS_TYPE.string_value);
        }
        private final void fn_from_date_to_string() {
            if (!hasValueOfType(AS_TYPE.timestamp_value)) {
                fn_from_date_to_timestamp();
            }
            fn_from_timestamp_to_string();
        }
        private final void fn_from_timestamp_to_string() {
            _string_value = _timestamp_value.toString();
            add_value_type(AS_TYPE.string_value);
        }
        private final void fn_from_long_to_int() {
            if (_long_value < Integer.MIN_VALUE || _long_value > Integer.MAX_VALUE) {
                throw new CantConvertException("long is too large to fit in an int");
            }
            _int_value = (int)_long_value;
            add_value_type(AS_TYPE.int_value);
        }
        private final void fn_from_biginteger_to_int() {
            if (min_int_value.compareTo(_bigInteger_value) > 0
             || max_int_value.compareTo(_bigInteger_value) < 0
            ) {
                throw new CantConvertException("bigInteger value is too large to fit in an int");
            }
            _int_value = _bigInteger_value.intValue();
            add_value_type(AS_TYPE.int_value);
        }
        private final void fn_from_decimal_to_int() {
            if (min_int_decimal_value.compareTo(_decimal_value) > 0
             || max_int_decimal_value.compareTo(_decimal_value) < 0
             ) {
                throw new CantConvertException("BigDecimal value is too large to fit in an int");
            }
            _int_value = _decimal_value.intValue();
            add_value_type(AS_TYPE.int_value);
        }
        private final void fn_from_double_to_int() {
            if (_double_value < Integer.MIN_VALUE
             || _double_value > Integer.MAX_VALUE
            ) {
                throw new CantConvertException("double is too large to fit in an int");
            }
            _int_value = (int)_double_value;
            add_value_type(AS_TYPE.int_value);
        }
        private final void fn_from_int_to_long() {
            _long_value = _int_value;
            add_value_type(AS_TYPE.long_value);
        }
        private final void fn_from_biginteger_to_long() {
            if (min_long_value.compareTo(_bigInteger_value) > 0
             || max_long_value.compareTo(_bigInteger_value) < 0
            ) {
                throw new CantConvertException("BigInteger is too large to fit in a long");
            }
            _long_value = _bigInteger_value.longValue();
            add_value_type(AS_TYPE.long_value);
        }
        private final void fn_from_decimal_to_long() {
            if (min_long_decimal_value.compareTo(_decimal_value) > 0
             || max_long_decimal_value.compareTo(_decimal_value) < 0
            ) {
                throw new CantConvertException("BigDecimal value is too large to fit in a long");
            }
            _long_value = _decimal_value.intValue();
            add_value_type(AS_TYPE.long_value);
        }
        private final void fn_from_double_to_long() {
            if (_double_value < Long.MIN_VALUE
             || _double_value > Long.MAX_VALUE
            ) {
                throw new CantConvertException("double is too large to fit in a long");
            }
            _long_value = (long)_double_value;
            add_value_type(AS_TYPE.long_value);
        }
        private final void fn_from_int_to_biginteger() {
            _bigInteger_value = BigInteger.valueOf(_int_value);
            add_value_type(AS_TYPE.bigInteger_value);
        }
        private final void fn_from_long_to_biginteger() {
            _bigInteger_value = BigInteger.valueOf(_long_value);
            add_value_type(AS_TYPE.bigInteger_value);
        }
        private final void fn_from_decimal_to_biginteger() {
            _bigInteger_value = _decimal_value.toBigInteger();
            add_value_type(AS_TYPE.bigInteger_value);
        }
        private final void fn_from_double_to_biginteger() {
            // To avoid decapitating values that are > Long.MAX_VALUE, we must
            // convert to BigDecimal first.
            _bigInteger_value =
                Decimal.valueOf(_double_value).toBigInteger();
            add_value_type(AS_TYPE.bigInteger_value);
        }
        private final void fn_from_int_to_decimal() {
            _decimal_value = Decimal.valueOf(_int_value);
            add_value_type(AS_TYPE.decimal_value);
        }
        private final void fn_from_long_to_decimal() {
            _decimal_value = Decimal.valueOf(_long_value);
            add_value_type(AS_TYPE.decimal_value);
        }
        private final void fn_from_biginteger_to_decimal() {
            _decimal_value = Decimal.valueOf(_bigInteger_value);
            add_value_type(AS_TYPE.decimal_value);
        }
        private final void fn_from_double_to_decimal() {
            _decimal_value = Decimal.valueOf(_double_value);
            add_value_type(AS_TYPE.decimal_value);
        }
        private final void fn_from_int_to_double() {
            _double_value = _int_value;
            add_value_type(AS_TYPE.double_value);
        }
        private final void fn_from_long_to_double() {
            _double_value = _long_value;
            add_value_type(AS_TYPE.double_value);
        }
        private final void fn_from_biginteger_to_double() {
            _double_value = _bigInteger_value.doubleValue();
            add_value_type(AS_TYPE.double_value);
        }
        private final void fn_from_decimal_to_double() {
            _double_value = _decimal_value.doubleValue();
            add_value_type(AS_TYPE.double_value);
        }
        private final void fn_from_timestamp_to_date() {
            _date_value = _timestamp_value.dateValue();
            add_value_type(AS_TYPE.date_value);
        }
        private final void fn_from_date_to_timestamp() {
            _timestamp_value = Timestamp.forMillis(_date_value.getTime(), null);
            add_value_type(AS_TYPE.timestamp_value);
        }
    }
}
