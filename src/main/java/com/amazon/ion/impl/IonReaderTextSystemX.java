// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.impl._Private_ScalarConversions.getValueTypeName;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
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
import com.amazon.ion.MacroAwareIonReader;
import com.amazon.ion.MacroAwareIonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.UnsupportedIonVersionException;
import com.amazon.ion.impl.IonReaderTextRawTokensX.IonReaderTextTokenException;
import com.amazon.ion.impl.IonTokenConstsX.CharacterSequence;
import com.amazon.ion.impl._Private_ScalarConversions.AS_TYPE;
import com.amazon.ion.impl._Private_ScalarConversions.CantConvertException;
import com.amazon.ion.impl.bin.PresenceBitmap;
import com.amazon.ion.impl.macro.EExpressionArgsReader;
import com.amazon.ion.impl.macro.EncodingContext;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroEvaluator;
import com.amazon.ion.impl.macro.MacroEvaluatorAsIonReader;
import com.amazon.ion.impl.macro.MacroRef;
import com.amazon.ion.impl.macro.MacroTable;
import com.amazon.ion.impl.macro.MutableMacroTable;
import com.amazon.ion.impl.macro.ReaderAdapter;
import com.amazon.ion.impl.macro.ReaderAdapterIonReader;
import com.amazon.ion.impl.macro.SystemMacro;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This reader calls the {@link IonReaderTextRawX} for low level events.
 * It surfaces the reader functions that construct instances
 * of various sorts (numbers, java strings, etc). It also
 * caches the fieldname and annotations of the current value.
 *
 *  It does not understand symbol tables nor care about them
 *  the IonTextUserReader is responsible for that.
 *
 */
class IonReaderTextSystemX
    extends IonReaderTextRawX
    implements _Private_ReaderWriter, MacroAwareIonReader
{
    private static int UNSIGNED_BYTE_MAX_VALUE = 255;

    SymbolTable _system_symtab;

    SymbolTable _symbols;

    // The core MacroEvaluator that this core reader delegates to when evaluating a macro invocation.
    private final MacroEvaluator macroEvaluator = new MacroEvaluator();

    // The IonReader-like MacroEvaluator that this core reader delegates to when evaluating a macro invocation.
    protected final MacroEvaluatorAsIonReader macroEvaluatorIonReader = new MacroEvaluatorAsIonReader(macroEvaluator);

    // The encoding context (macro table) that is currently active.
    private EncodingContext encodingContext = EncodingContext.getDefault();

    // Adapts this reader for use in code that supports multiple reader types.
    private final ReaderAdapter readerAdapter = new ReaderAdapterIonReader(this);

    // Reads encoding directives from the stream.
    private EncodingDirectiveReader encodingDirectiveReader = null;

    // Reads macro invocation arguments as expressions and feeds them to the MacroEvaluator.
    private final EExpressionArgsReader expressionArgsReader = new TextEExpressionArgsReader();

    // Indicates whether the reader is currently evaluating an e-expression.
    boolean isEvaluatingEExpression = false;

    // The writer that will perform a macro-aware transcode, if requested.
    private MacroAwareIonWriter macroAwareTranscoder = null;

    protected IonReaderTextSystemX(UnifiedInputStreamX iis)
    {
        _system_symtab = _Private_Utils.systemSymtab(1); // TODO check IVM to determine version: amazon-ion/ion-java/issues/19
        _symbols = _system_symtab;
        init_once();
        init(iis, IonType.DATAGRAM);
    }

    // TODO getIntegerType() is duplicated in IonReaderBinarySystemX. It could
    // be consolidated into a single location, but that would have to be part
    // of a larger refactor of common logic from both IonReader*SystemX classes
    // into a base class (the *Value() methods also share a lot of similarity).
    public IntegerSize getIntegerSize()
    {
        if (_value_type != IonType.INT || isNullValue())
        {
            return null;
        }
        load_once();
        return _Private_ScalarConversions.getIntegerSize(_v.getAuthoritativeType());
    }

    /**
     * Loads a scalar value (except lob values) from the macro evaluator.
     */
    private void loadScalarValueFromMacro() {
        switch (_value_type) {
            case NULL:
                _v.setValueToNull(_value_type);
                break;
            case BOOL:
                _v.setValue(macroEvaluatorIonReader.booleanValue());
                break;
            case INT:
                switch (macroEvaluatorIonReader.getIntegerSize()) {
                    case INT:
                        _v.setValue(macroEvaluatorIonReader.intValue());
                        break;
                    case LONG:
                        _v.setValue(macroEvaluatorIonReader.longValue());
                        break;
                    case BIG_INTEGER:
                        _v.setValue(macroEvaluatorIonReader.bigIntegerValue());
                        break;
                }
                break;
            case FLOAT:
                _v.setValue(macroEvaluatorIonReader.doubleValue());
                break;
            case DECIMAL:
                _v.setValue(macroEvaluatorIonReader.decimalValue());
                break;
            case TIMESTAMP:
                _v.setValue(macroEvaluatorIonReader.timestampValue());
                break;
            case SYMBOL:
                // TODO determine how to handle symbols with unknown text.
                _v.setValue(macroEvaluatorIonReader.stringValue());
                break;
            case STRING:
                _v.setValue(macroEvaluatorIonReader.stringValue());
                break;
            case CLOB: // see load_lob_contents
            case BLOB: // see load_lob_contents
            case LIST:
            case SEXP:
            case STRUCT:
            case DATAGRAM:
                throw new IllegalStateException(String.format("Type %s is not loaded by this method.", _value_type));
        }
    }

    private void load_once()
    {
        if (isEvaluatingEExpression) {
            loadScalarValueFromMacro();
            return;
        }
        if (_v.isEmpty()) {
            try {
                load_scalar_value();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
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
    private final void load_or_cast_cached_value(int value_type) {
        load_once();
        if (value_type != 0 && !_v.hasValueOfType(value_type)) {
            cast_cached_value(value_type);
        }
    }

    enum Radix
    {
        DECIMAL
        {

            @Override
            boolean isInt(String image, int len)
            {
                return valueWithinBounds(image, len, MIN_INT_IMAGE, MAX_INT_IMAGE);
            }

            @Override
            boolean isLong(String image, int len)
            {
                return valueWithinBounds(image, len, MIN_LONG_IMAGE, MAX_LONG_IMAGE);
            }

        },
        HEX
        {

            @Override
            boolean isInt(String image, int len)
            {
                return valueWithinBounds(image, len, MIN_HEX_INT_IMAGE, MAX_HEX_INT_IMAGE);
            }

            @Override
            boolean isLong(String image, int len)
            {
                return valueWithinBounds(image, len, MIN_HEX_LONG_IMAGE, MAX_HEX_LONG_IMAGE);
            }

        },
        BINARY
        {

            @Override
            boolean isInt(String image, int len)
            {
                return valueWithinBounds(image, len, MIN_BINARY_INT_IMAGE, MAX_BINARY_INT_IMAGE);
            }

            @Override
            boolean isLong(String image, int len)
            {
                return valueWithinBounds(image, len, MIN_BINARY_LONG_IMAGE, MAX_BINARY_LONG_IMAGE);
            }

        };

        private static final char[] MAX_INT_IMAGE = Integer.toString(Integer.MAX_VALUE).toCharArray();
        private static final char[] MIN_INT_IMAGE = Integer.toString(Integer.MIN_VALUE).toCharArray();
        private static final char[] MAX_LONG_IMAGE = Long.toString(Long.MAX_VALUE).toCharArray();
        private static final char[] MIN_LONG_IMAGE = Long.toString(Long.MIN_VALUE).toCharArray();
        private static final char[] MAX_BINARY_INT_IMAGE = Integer.toBinaryString(Integer.MAX_VALUE).toCharArray();
        private static final char[] MIN_BINARY_INT_IMAGE = ("-" + Integer.toBinaryString(Integer.MIN_VALUE)).toCharArray();
        private static final char[] MAX_BINARY_LONG_IMAGE = Long.toBinaryString(Long.MAX_VALUE).toCharArray();
        private static final char[] MIN_BINARY_LONG_IMAGE = ("-" + Long.toBinaryString(Long.MIN_VALUE)).toCharArray();
        private static final char[] MAX_HEX_INT_IMAGE = Integer.toHexString(Integer.MAX_VALUE).toCharArray();
        private static final char[] MIN_HEX_INT_IMAGE = ("-" + Integer.toHexString(Integer.MIN_VALUE)).toCharArray();
        private static final char[] MAX_HEX_LONG_IMAGE = Long.toHexString(Long.MAX_VALUE).toCharArray();
        private static final char[] MIN_HEX_LONG_IMAGE = ("-" + Long.toHexString(Long.MIN_VALUE)).toCharArray();

        abstract boolean isInt(String image, int len);
        abstract boolean isLong(String image, int len);

        private static boolean valueWithinBounds(String value, int len, char[] minImage, char[] maxImage)
        {
            boolean negative = value.charAt(0) == '-';
            char[] boundaryImage = negative ? minImage : maxImage;
            int maxImageLength = boundaryImage.length;
            return len < maxImageLength || (len == maxImageLength && magnitudeLessThanOrEqualTo(value, len, boundaryImage));
        }

        private static boolean magnitudeLessThanOrEqualTo(String lhs, int lhsLen, char[] rhs)
        {
            assert lhsLen == rhs.length;
            for (int i = lhsLen - 1; i >= 0; i--)
            {
                if (lhs.charAt(i) > rhs[i])
                {
                    return false;
                }
            }
            return true;
        }
    }

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
        } else if (token_type == IonTokenConstsX.TOKEN_HEX || token_type == IonTokenConstsX.TOKEN_BINARY) {
            boolean isNegative = (cs.charAt(0) == '-');
            // prefix = is_negative ? "-0x" : "0x";
            int pos = isNegative ? 1 : 0;
            char caseChar = token_type == IonTokenConstsX.TOKEN_HEX ? 'x' : 'b';
            if (cs.length() <= (isNegative ? 3 : 2) || Character.toLowerCase(cs.charAt(pos + 1)) != caseChar) {
                parse_error("Invalid " + (caseChar == 'x' ? "hexadecimal" : "binary") + " int value.");
            }
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
                if (Radix.DECIMAL.isInt(s, len)) {
                    _v.setValue(Integer.parseInt(s));
                }
                else if (Radix.DECIMAL.isLong(s, len)) {
                    _v.setValue(Long.parseLong(s));
                }
                else {
                    _v.setValue(new BigInteger(s));
                }
                break;
            case DECIMAL:
                // note that the string was modified above when it was a charsequence
                try {
                _v.setValue(Decimal.valueOf(s));
                }
                catch (NumberFormatException e) {
                    parse_error(e);
                }
                break;
            case FLOAT:
                try {
                    _v.setValue(Double.parseDouble(s));
                }
                catch (NumberFormatException e) {
                    parse_error(e);
                }
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
            if (Radix.DECIMAL.isInt(s, len)) {
                _v.setValue(Integer.parseInt(s));
            }
            else if (Radix.DECIMAL.isLong(s, len)) {
                _v.setValue(Long.parseLong(s));
            }
            else {
                _v.setValue(new BigInteger(s));
            }
            break;
        case IonTokenConstsX.TOKEN_BINARY:
            if (Radix.BINARY.isInt(s, len)) {
                _v.setValue(Integer.parseInt(s, 2));
            }
            else if (Radix.BINARY.isLong(s, len)) {
                _v.setValue(Long.parseLong(s, 2));
            }
            else {
                _v.setValue(new BigInteger(s, 2));
            }
            break;
        case IonTokenConstsX.TOKEN_HEX:
            if (Radix.HEX.isInt(s, len)) {
                int v_int = Integer.parseInt(s, 16);
                _v.setValue(v_int);
            }
            else if (Radix.HEX.isLong(s, len)) {
                long v_long = Long.parseLong(s, 16);
                _v.setValue(v_long);
            }
            else {
                BigInteger v_big_int = new BigInteger(s, 16);
                _v.setValue(v_big_int);
            }
            break;
        case IonTokenConstsX.TOKEN_DECIMAL:
            try {
            _v.setValue(Decimal.valueOf(s));
            }
            catch (NumberFormatException e) {
                parse_error(e);
            }
            break;
        case IonTokenConstsX.TOKEN_FLOAT:
            try {
                _v.setValue(Double.parseDouble(s));
            }
            catch (NumberFormatException e) {
                parse_error(e);
            }

            break;
        case IonTokenConstsX.TOKEN_TIMESTAMP:
            Timestamp t = null;
            try {
                t = Timestamp.valueOf(s);
            }
            catch (IllegalArgumentException e) {
                parse_error(e);
            }
            _v.setValue(t);
            break;
        case IonTokenConstsX.TOKEN_SYMBOL_IDENTIFIER:
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
                    // TODO this is catching SIDs too, using wrong text.
                    _v.setValue(s);
                    break;
                case FLOAT:
                    switch (_value_keyword) {
                    case IonTokenConstsX.KEYWORD_NAN:
                        _v.setValue(Double.NaN);
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
    private final void cast_cached_value(int new_type)
    {
        // this should only be called when it actually has to do some work
        assert !_v.hasValueOfType(new_type);

        if (_v.isNull()) {
            return;
        }

        if (IonType.SYMBOL.equals(_value_type)) {
            switch(new_type) {
                case AS_TYPE.string_value:
                    int sid = _v.getInt();
                    String sym = getSymbolTable().findKnownSymbol(sid);
                    _v.addValue(sym);
                    break;
                case AS_TYPE.int_value:
                    sym = _v.getString();
                    sid = getSymbolTable().findSymbol(sym);
                    _v.addValue(sid);
                    break;
                default:
                {   String message = "can't cast symbol from "
                        +getValueTypeName(_v.getAuthoritativeType())
                        +" to "
                        +getValueTypeName(new_type);
                    throw new CantConvertException(message);
                }
            }
        }
        else {
            if (!_v.can_convert(new_type)) {
                String message = "can't cast from "
                    +getValueTypeName(_v.getAuthoritativeType())
                    +" to "
                    +getValueTypeName(new_type);
                throw new CantConvertException(message);
            }
            int fnid = _v.get_conversion_fnid(new_type);
            _v.cast(fnid);
        }
    }

    /**
     * Loads annotations, either from the stream or from a macro.
     * @return the annotations.
     */
    private SymbolToken[] loadAnnotations() {
        SymbolToken[] annotations;
        if (isEvaluatingEExpression) {
            annotations = macroEvaluatorIonReader.getTypeAnnotationSymbols();
            _annotation_count = annotations == null ? 0 : annotations.length;
        } else {
            // The annotations are eagerly read from the stream into `_annotations`.
            annotations = _annotations;
        }
        return annotations;
    }

    //
    // public value routines
    //

    public SymbolToken[] getTypeAnnotationSymbols()
    {
        SymbolToken[] annotations = loadAnnotations();
        final int count = _annotation_count;
        if (count == 0) return SymbolToken.EMPTY_ARRAY;

        resolveAnnotationSymbols(annotations, count);

        SymbolToken[] result = new SymbolToken[count];
        System.arraycopy(annotations, 0, result, 0, count);

        return result;
    }

    public String[] getTypeAnnotations()
    {
        SymbolToken[] annotations = loadAnnotations();
        resolveAnnotationSymbols(annotations, _annotation_count);
        return _Private_Utils.toStrings(annotations, _annotation_count);
    }

    /**
     * Resolve annotations with the current symbol table.
     */
    private void resolveAnnotationSymbols(SymbolToken[] annotations, int count) {
        SymbolTable symbols = getSymbolTable();
        for (int i = 0; i < count; i++) {
            SymbolToken sym = annotations[i];
            SymbolToken updated = _Private_Utils.localize(symbols, sym);
            if (updated != sym) {
                annotations[i] = updated;
            }
        }
    }

    public boolean isNullValue()
    {
        return (isEvaluatingEExpression && macroEvaluatorIonReader.isNullValue()) || _v.isNull();
    }

    public boolean booleanValue()
    {
        load_or_cast_cached_value(AS_TYPE.boolean_value);
        return _v.getBoolean();
    }

    public double doubleValue()
    {
        load_or_cast_cached_value(AS_TYPE.double_value);
        return _v.getDouble();
    }

    private void checkIsIntApplicableType()
    {
        if (_value_type != IonType.INT &&
          _value_type != IonType.DECIMAL &&
          _value_type != IonType.FLOAT)
        {
          throw new IllegalStateException("Unexpected value type: " + _value_type);
        }
    }

    public int intValue()
    {
        checkIsIntApplicableType();

        load_or_cast_cached_value(AS_TYPE.int_value);
        return _v.getInt();
    }

    public long longValue()
    {
        checkIsIntApplicableType();

        load_or_cast_cached_value(AS_TYPE.long_value);
        return _v.getLong();
    }

    private void checkIsBigIntegerApplicableType()
    {
        if (_value_type != IonType.INT &&
                _value_type != IonType.FLOAT)
        {
            throw new IllegalStateException("Unexpected value type: " + _value_type);
        }
    }

    @Override
    public BigInteger bigIntegerValue()
    {
        checkIsBigIntegerApplicableType();

        load_or_cast_cached_value(AS_TYPE.bigInteger_value);
        if (_v.isNull()) return null;
        return _v.getBigInteger();
    }

    public BigDecimal bigDecimalValue()
    {
        load_or_cast_cached_value(AS_TYPE.decimal_value);
        if (_v.isNull()) return null;
        return _v.getBigDecimal();
    }

    public Decimal decimalValue()
    {
        load_or_cast_cached_value(AS_TYPE.decimal_value);
        if (_v.isNull()) return null;
        return _v.getDecimal();
    }

    public Date dateValue()
    {
        load_or_cast_cached_value(AS_TYPE.date_value);
        if (_v.isNull()) return null;
        return _v.getDate();
    }

    public Timestamp timestampValue()
    {
        load_or_cast_cached_value(AS_TYPE.timestamp_value);
        if (_v.isNull()) return null;
        return _v.getTimestamp();
    }

    public final String stringValue()
    {
        if (! IonType.isText(_value_type)) throw new IllegalStateException("Unexpected value type: " + _value_type);
        if (isNullValue()) return null;

        load_or_cast_cached_value(AS_TYPE.string_value);
        String text = _v.getString();
        if (text == null) {
            int sid = _v.getInt();
            throw new UnknownSymbolException(sid);
        }
        return text;
    }

    /**
     * Horrible temporary hack.
     *
     * @return not null.
     */
    @Override
    public SymbolTable getSymbolTable()
    {
        SymbolTable symtab = super.getSymbolTable();
        if (symtab == null)
        {
            symtab = _symbols;
        }
        return symtab;
    }


    @Override
    public final int getFieldId()
    {
        int id;
        if (isEvaluatingEExpression) {
            id = getFieldNameSymbol().getSid();
        } else {
            // Superclass handles hoisting logic
            id = super.getFieldId();
        }
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
    public final String getFieldName()
    {
        if (isEvaluatingEExpression) {
            _field_name = macroEvaluatorIonReader.getFieldName();
        }
        // Superclass handles hoisting logic
        String text = getRawFieldName();
        if (text == null)
        {
            int id = getFieldId();
            if (id != SymbolTable.UNKNOWN_SYMBOL_ID)
            {
                SymbolTable symbols = getSymbolTable();
                text = symbols.findKnownSymbol(id);
                if (text == null)
                {
                    throw new UnknownSymbolException(id);
                }
            }
        }
        return text;
    }

    @Override
    public SymbolToken getFieldNameSymbol()
    {
        SymbolToken sym;
        if (isEvaluatingEExpression) {
            sym = macroEvaluatorIonReader.getFieldNameSymbol();
        } else {
            sym = super.getFieldNameSymbol();
        }
        if (sym != null)
        {
            sym = _Private_Utils.localize(getSymbolTable(), sym);
        }
        return sym;
    }

    public SymbolToken symbolValue()
    {
        if (_value_type != IonType.SYMBOL) throw new IllegalStateException("Unexpected value type: " + _value_type);
        if (isNullValue()) return null;

        load_or_cast_cached_value(AS_TYPE.string_value);
        if (! _v.hasValueOfType(AS_TYPE.int_value))
        {
            cast_cached_value(AS_TYPE.int_value);
        }

        String text = _v.getString();
        int    sid  = _v.getInt();
        return new SymbolTokenImpl(text, sid);
    }

    //
    // blob and clob support routines
    //

    public int byteSize()
    {
        ensureLob("byteSize");

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
        if (_lob_loaded == LOB_STATE.EMPTY) {
            assert(!_current_value_save_point_loaded && _current_value_save_point.isClear());
            _scanner.save_point_start(_current_value_save_point);
            _scanner.skip_over_lob(_lob_token, _current_value_save_point);
            _current_value_save_point_loaded = true;
            tokenValueIsFinished();
            _lob_loaded = LOB_STATE.READ;
        }

        long size = _current_value_save_point.length();
        return size;
    }
    private int load_lob_contents() throws IOException
    {
        if (isEvaluatingEExpression) {
            // TODO performance: reduce allocation / copying. Can getBytes() be used?
            _lob_bytes = macroEvaluatorIonReader.newBytes();
            _lob_actual_len = _lob_bytes.length;
            _lob_loaded = LOB_STATE.FINISHED;
            return _lob_actual_len;
        }
        if (_lob_loaded == LOB_STATE.EMPTY) {
            load_lob_save_point();
        }
        if (_lob_loaded == LOB_STATE.READ) {
            long raw_size =  _current_value_save_point.length();
            if (raw_size < 0 || raw_size > _Private_IonConstants.ARRAY_MAXIMUM_SIZE) {
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

    private void ensureLob(String apiName)
    {
        switch (_value_type) {
            case CLOB:
            case BLOB:
                break;
            default:
            {
                String msg =
                    apiName +
                    " is only valid if the reader is on a lob value, not a " +
                    _value_type +
                    " value";
                throw new IllegalStateException(msg);
            }
        }
    }

    public byte[] newBytes()
    {
        ensureLob("newBytes");

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

    public int getBytes(byte[] buffer, int offset, int len)
    {
        ensureLob("getBytes");

        if (_lob_loaded == LOB_STATE.READ) {
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

        int len_read;
        if (_lob_loaded == LOB_STATE.FINISHED) {
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

    private int readBytes(byte[] buffer, int offset, int len)
        throws IOException
    {
        int starting_offset = offset;
        int c = -1;

        switch (_lob_token) {
        // BLOB
        case IonTokenConstsX.TOKEN_OPEN_DOUBLE_BRACE:
            while (len-- > 0) {
                c = _scanner.read_base64_byte();
                if (c < 0) break;
                buffer[offset++] = (byte)c;
            }
            break;
        // CLOB
        case IonTokenConstsX.TOKEN_STRING_DOUBLE_QUOTE:
            while (len-- > 0) {
                c = _scanner.read_double_quoted_char(true);
                if (c < 0) {
                    if (c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1
                     || c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2
                     || c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3
                    ) {
                        continue;
                    }
                    break;
                }
                assert(c <= UNSIGNED_BYTE_MAX_VALUE);
                buffer[offset++] = (byte)c;
            }
            break;
        // CLOB
        case IonTokenConstsX.TOKEN_STRING_TRIPLE_QUOTE:
            while (len-- > 0) {
                c = _scanner.read_triple_quoted_char(true);
                if (c < 0) {
                    if (c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_1
                     || c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_2
                     || c == CharacterSequence.CHAR_SEQ_ESCAPED_NEWLINE_SEQUENCE_3
                     || c == CharacterSequence.CHAR_SEQ_STRING_NON_TERMINATOR
                    ) {
                        continue;
                    }
                    if (c == CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_1
                     || c == CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_2
                     || c == CharacterSequence.CHAR_SEQ_NEWLINE_SEQUENCE_3
                    ) {
                        buffer[offset++] = (byte)'\n';
                        continue;
                    }
                    break;
                }
                assert(c >= 0 && c <= UNSIGNED_BYTE_MAX_VALUE);
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

    // system readers don't skip any symbol tables
    public SymbolTable pop_passed_symbol_table()
    {
        return null;
    }

    /**
     * Sets the active symbol table.
     * @param symbolTable the symbol table to make active.
     */
    protected void setSymbolTable(SymbolTable symbolTable) {
        _symbols = symbolTable;
    }

    /**
     * While reading an encoding directive, the reader allows itself to be controlled by the MacroCompiler during
     * compilation of a macro. While this is happening, the reader should never attempt to read another encoding
     * directive.
     * @return true if the reader is not in the process of compiling a macro; false if it is.
     */
    private boolean macroCompilationNotInProgress() {
        return encodingDirectiveReader == null || !encodingDirectiveReader.isMacroCompilationInProgress();
    }

    /**
     * @return true if current value has a sequence of annotations that begins with `$ion`; otherwise, false.
     */
    boolean startsWithIonAnnotation() {
        if (isEvaluatingEExpression) {
            return SystemSymbols_1_1.ION.getText().equals(macroEvaluatorIonReader.iterateTypeAnnotations().next());
        }
        // TODO also resolve symbol identifiers and compare against text that looks like $ion
        return SystemSymbols_1_1.ION.getText().equals(_annotations[0].getText());
    }

    /**
     * @return true if the current value has at least one annotation.
     */
    private boolean hasAnnotations() {
        return _annotation_count > 0 || (isEvaluatingEExpression && macroEvaluatorIonReader.hasAnnotations());
    }

    /**
     * @return true if the reader is positioned on an encoding directive; otherwise, false.
     */
    private boolean isPositionedOnEncodingDirective() {
        return hasAnnotations()
            && _value_type == IonType.SEXP
            && !isNullValue()
            && macroCompilationNotInProgress()
            && startsWithIonAnnotation();
    }

    /**
     * Reads an encoding directive and installs any symbols and/or macros found within. Upon calling this method,
     * the reader must be positioned on a top-level s-expression annotated with `$ion`.
     */
    private void readEncodingDirective() {
        if (encodingDirectiveReader == null) {
            encodingDirectiveReader = new EncodingDirectiveReader(this, readerAdapter);
        }
        encodingDirectiveReader.reset();
        encodingDirectiveReader.readEncodingDirective(encodingContext);
        List<String> newSymbols = encodingDirectiveReader.getNewSymbols();
        if (encodingDirectiveReader.isSymbolTableAppend()) {
            SymbolTable current = getSymbolTable();
            if (current.isSystemTable()) {
                // TODO determine the best way to handle the Ion 1.1 system symbols.
                List<String> withSystemSymbols = new ArrayList<>(SystemSymbols_1_1.allSymbolTexts());
                withSystemSymbols.addAll(newSymbols);
                setSymbolTable(new LocalSymbolTable(
                    LocalSymbolTableImports.EMPTY,
                    withSystemSymbols
                ));
            } else {
                LocalSymbolTable currentLocal = (LocalSymbolTable) current;
                for (String appendedSymbol : newSymbols) {
                    currentLocal.putSymbol(appendedSymbol);
                }
            }
        } else {
            setSymbolTable(new LocalSymbolTable(
                // TODO handle shared symbol table imports declared in the encoding directive
                LocalSymbolTableImports.EMPTY,
                newSymbols
            ));
        }
        installMacros();
    }

    // This is essentially copied from IonReaderContinuableCoreBinary.EncodingDirectiveReader.installMacros
    // See the comment for EncodingDirectiveReader.kt
    private void installMacros() {
        boolean isMacroTableAppend = encodingDirectiveReader.isMacroTableAppend();
        Map<MacroRef, Macro> newMacros = encodingDirectiveReader.getNewMacros();

        if (!isMacroTableAppend) {
            encodingContext = new EncodingContext(new MutableMacroTable(MacroTable.empty()), true);
        } else if (!encodingContext.isMutable() && !newMacros.isEmpty()){ // we need to append, but can't
            encodingContext = new EncodingContext(new MutableMacroTable(encodingContext.getMacroTable()), true);
        }
        if (!newMacros.isEmpty()) encodingContext.getMacroTable().putAll(newMacros);
    }


    /**
     * Reads macro invocation arguments as expressions and feeds them to the MacroEvaluator.
     */
    private class TextEExpressionArgsReader extends EExpressionArgsReader {

        TextEExpressionArgsReader() {
            super(readerAdapter);
        }

        @Override
        protected void readParameter(Macro.Parameter parameter, long parameterPresence, boolean isTrailing) {
            if (IonReaderTextSystemX.this.nextRaw() == null) {
                // Add an empty expression group if nothing present.
                int index = expressions.size() + 1;
                expressions.add(expressionPool.createExpressionGroup(index, index));
                return;
            }
            readValueAsExpression(isTrailing && parameter.getCardinality().canBeMulti);
        }

        @Override
        protected Macro loadMacro() {
            IonReaderTextSystemX.this.stepIn();
            if (IonReaderTextSystemX.this.nextRaw() == null) {
                throw new IonException("Macro invocation missing address.");
            }
            List<SymbolToken> annotations = getAnnotations();
            boolean isSystemMacro = !annotations.isEmpty() && SystemSymbols_1_1.ION.getText().equals(annotations.get(0).getText());
            MacroRef address;
            if (_value_type == IonType.SYMBOL) {
                String name = stringValue();
                if (name == null) {
                    throw new IonException("Macros invoked by name must have non-null name.");
                }
                address = MacroRef.byName(name);
            } else if (_value_type == IonType.INT) {
                long id = longValue();
                if (id > Integer.MAX_VALUE) {
                    throw new IonException("Macro addresses larger than 2147483647 are not supported by this implementation.");
                }
                address = MacroRef.byId((int) id);
            } else {
                throw new IonException("E-expressions must begin with an address.");
            }

            Macro macro = isSystemMacro ? SystemMacro.get(address) : encodingContext.getMacroTable().get(address);
            if (macro == null) {
                throw new IonException(String.format("Encountered an unknown macro address: %s.", address));
            }
            return macro;
        }

        @Override
        protected PresenceBitmap loadPresenceBitmapIfNecessary(List<Macro.Parameter> signature) {
            // Text Ion does not use a presence bitmap.
            return null;
        }

        @Override
        protected boolean isMacroInvocation() {
            return _container_is_e_expression;
        }

        @Override
        protected boolean isContainerAnExpressionGroup() {
            return _container_is_expression_group;
        }

        @Override
        protected List<SymbolToken> getAnnotations() {
            return _annotation_count == 0 ? Collections.emptyList() : Arrays.asList(getTypeAnnotationSymbols());
        }

        @Override
        protected boolean nextRaw() {
            return IonReaderTextSystemX.this.nextRaw() != null;
        }

        @Override
        protected void stepInRaw() {
            IonReaderTextSystemX.this.stepIn();
        }

        @Override
        protected void stepOutRaw() {
            IonReaderTextSystemX.this.stepOut();
        }

        @Override
        protected void stepIntoEExpression() {
            // Do nothing; the text reader must have already stepped into the e-expression in order to read its address.
        }

        @Override
        protected void stepOutOfEExpression() {
            // In text, e-expressions are traversed handled in the same way as s-expressions.
            IonReaderTextSystemX.this.stepOut();
        }
    }

    /**
     * Consumes the next value (if any) from the MacroEvaluator, setting `_value_type` based on the result.
     * @return true if this call causes the evaluator to reach the end of the current invocation; otherwise, false.
     */
    private boolean evaluateNext() {
        _value_type = macroEvaluatorIonReader.next();
        if (_value_type == null && macroEvaluatorIonReader.getDepth() == 0) {
            // Evaluation of this macro is complete. Resume reading from the stream.
            isEvaluatingEExpression = false;
            return true;
        }
        return false;
    }

    /**
     * Advances the reader, if necessary and possible, to the next value, reading any Ion 1.1+ encoding directives
     * found along the way.
     * @return true if the reader is positioned on a value; otherwise, false.
     */
    protected final boolean has_next_system_value() {
        while (!_has_next_called && !_eof) {
            if (isEvaluatingEExpression) {
                if (evaluateNext()) {
                    continue;
                }
                _has_next_called = true;
            } else {
                has_next_raw_value();
            }
            if (minorVersion > 0 && _value_type != null && IonType.DATAGRAM.equals(getContainerType()) && isPositionedOnEncodingDirective()) {
                readEncodingDirective();
                continue;
            }
            if (_container_is_e_expression) {
                expressionArgsReader.beginEvaluatingMacroInvocation(macroEvaluator);
                isEvaluatingEExpression = true;
                continue;
            }
            break;
        }
        return !_eof;
    }

    @Override
    public boolean hasNext()
    {
        return has_next_system_value();
    }

    @Override
    public void transcodeAllTo(MacroAwareIonWriter writer) throws IOException {
        prepareTranscodeTo(writer);
        while (transcodeNext());
    }

    @Override
    public void prepareTranscodeTo(@NotNull MacroAwareIonWriter writer) {
        macroAwareTranscoder = writer;
    }

    @Override
    public boolean transcodeNext() throws IOException {
        // TODO consider improving the readability of this method and its binary counterpart: https://github.com/amazon-ion/ion-java/issues/1004
        if (macroAwareTranscoder == null) {
            throw new IllegalArgumentException("prepareTranscodeTo must be called before transcodeNext.");
        }
        boolean isSystemValue = false;
        while (true) {
            if (isEvaluatingEExpression) {
                if (evaluateNext()) {
                    if (isSystemValue) {
                        continue;
                    }
                    return !_eof;
                }
            } else {
                nextRaw();
            }
            isSystemValue = false;
            if (_value_type != null && getDepth() == 0) {
                if (IonType.SYMBOL == getType() && handlePossibleIonVersionMarker()) {
                    // Which IVM to write is inherent to the writer implementation.
                    // We don't have a single implementation that writes both formats.
                    macroAwareTranscoder.startEncodingSegmentWithIonVersionMarker();
                    isSystemValue = true;
                    continue;
                }
                if (minorVersion > 0 && isPositionedOnEncodingDirective()) {
                    boolean isEncodingDirectiveFromEExpression = isEvaluatingEExpression;
                    readEncodingDirective();
                    macroAwareTranscoder.startEncodingSegmentWithEncodingDirective(
                        encodingDirectiveReader.getNewMacros(),
                        encodingDirectiveReader.isMacroTableAppend(),
                        encodingDirectiveReader.getNewSymbols(),
                        encodingDirectiveReader.isSymbolTableAppend(),
                        isEncodingDirectiveFromEExpression
                    );
                    isSystemValue = true;
                    continue;
                }
            }
            if (_container_is_e_expression) {
                expressionArgsReader.beginEvaluatingMacroInvocation(macroEvaluator);
                macroEvaluatorIonReader.transcodeArgumentsTo(macroAwareTranscoder);
                isEvaluatingEExpression = true;
                continue;
            } else if (isEvaluatingEExpression) {
                // This is an e-expression that yields user values. Its arguments have already been transcoded.
                continue;
            }
            if (_eof) {
                return false;
            }
            transcodeValueLiteral();
            return !_eof;
        }
    }

    /**
     * @return true if the reader is positioned on an Ion 1.0 symbol table; otherwise, false. Note: the caller must
     *  ensure this is called only at the top level.
     */
    boolean isPositionedOnSymbolTable() {
        return _annotation_count > 0 && (ION_SYMBOL_TABLE.equals(_annotations[0].getText()) || ION_SYMBOL_TABLE_SID == _annotations[0].getSid());
    }

    // Matches "$ion_x_y", where x and y are integers.
    private static final Pattern ION_VERSION_MARKER_REGEX = Pattern.compile("^\\$ion_[0-9]+_[0-9]+$");

    /**
     * @param text the text of a symbol value.
     * @return true if the text denotes an IVM; otherwise, false.
     */
    static boolean isIonVersionMarker(String text)
    {
        return text != null && ION_VERSION_MARKER_REGEX.matcher(text).matches();
    }

    /**
     * Resets the symbol table after an IVM is encountered. May be overridden if additional side effects are required.
     */
    void symbol_table_reset() {
        setSymbolTable(_system_symtab);
    }

    /**
     * Determines whether the top-level symbol value on which the reader is positioned is an Ion version marker.
     * If it is, sets the reader's Ion version accordingly and resets the symbol table.
     * @return true if the symbol represented an Ion version marker; otherwise, false. Note: the caller must
     *  ensure this is called only at the top level with the reader positioned on a symbol value.
     */
    boolean handlePossibleIonVersionMarker() {
        if (_annotation_count == 0)
        {
            // $ion_1_0 is read as an IVM only if it is not annotated
            String version = symbolValue().getText();
            if (isIonVersionMarker(version))
            {
                if (ION_1_0.equals(version) || "$ion_1_1".equals(version))
                {
                    setMinorVersion(version.charAt(version.length() - 1) - '0');
                    if (_value_keyword != IonTokenConstsX.KEYWORD_sid)
                    {
                        symbol_table_reset();
                    }
                    _has_next_called = false;
                }
                else
                {
                    throw new UnsupportedIonVersionException(version);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Transcodes a value literal to the macroAwareTranscoder. The caller must ensure that the reader is positioned
     * on a value literal (i.e. a scalar or container value not expanded from an e-expression) before calling this
     * method.
     * @throws IOException if thrown by the writer during transcoding.
     */
    private void transcodeValueLiteral() throws IOException {
        if (getDepth() == 0 && isPositionedOnSymbolTable()) {
            if (minorVersion > 0) {
                // TODO finalize handling of Ion 1.0-style symbol tables in Ion 1.1: https://github.com/amazon-ion/ion-java/issues/1002
                throw new IonException("Macro-aware transcoding of Ion 1.1 data containing Ion 1.0-style symbol tables not yet supported.");
            }
            // Ion 1.0 symbol tables are transcoded verbatim for now; this may change depending on the resolution to
            // https://github.com/amazon-ion/ion-java/issues/1002.
            macroAwareTranscoder.writeValue(this);
        } else if (IonType.isContainer(getType()) && !isNullValue()) {
            // Containers need to be transcoded recursively to avoid expanding macro invocations at any depth.
            if (isInStruct()) {
                macroAwareTranscoder.setFieldNameSymbol(getFieldNameSymbol());
            }
            macroAwareTranscoder.setTypeAnnotationSymbols(getTypeAnnotationSymbols());
            macroAwareTranscoder.stepIn(getType());
            super.stepIn();
            while (transcodeNext()); // TODO make this iterative.
            super.stepOut();
            macroAwareTranscoder.stepOut();
        } else {
            // The reader is now positioned on a scalar literal. Write the value.
            // Note: writeValue will include any field name and/or annotations on the scalar.
            macroAwareTranscoder.writeValue(this);
        }
    }

    @Override
    public void stepIn() {
        if (isEvaluatingEExpression) {
            macroEvaluatorIonReader.stepIn();
        }
        super.stepIn();
    }

    @Override
    public void stepOut() {
        if (isEvaluatingEExpression) {
            macroEvaluatorIonReader.stepOut();
            // The reader is already positioned after the container. Simply pop the information about this container
            // from the stack without seeking forward to find the delimiter.
            endContainerRaw();
            return;
        }
        super.stepOut();
    }

    /**
     * @return the {@link EncodingContext} currently active, or {@code null}.
     */
    EncodingContext getEncodingContext() {
        return encodingContext;
    }
}
