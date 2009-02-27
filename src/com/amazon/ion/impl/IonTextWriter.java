/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonConstants.tidList;
import static com.amazon.ion.impl.IonConstants.tidSexp;
import static com.amazon.ion.impl.IonConstants.tidStruct;

import com.amazon.ion.IonNumber;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.Base64Encoder.TextStream;
import com.amazon.ion.impl.IonBinary.BufferManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.util.Date;

/**
 * A concrete implementation of IonWriter which writes the
 * values out as text.  It writes the value out as UTF-8.  The
 * constructor offers the option of pretty printing the value
 * and also offers the option of generating pure ascii output.
 * In the event pure ascii is chosen (utf8asascii) non-ascii
 * characters are emitted as \\u or \\U escaped hex values. If
 * this is not chosen the output stream may contain unicode
 * character directly, which is often not a readable.  Either
 * form will be parsed, by an Ion parser, into the same Ion
 * values however the u encodings may be longer in the text
 * form and less readable in some environments.
 */
public final class IonTextWriter
    extends IonBaseWriter
{
    boolean      _pretty;
    boolean      _utf8_as_ascii;
    PrintStream  _output;

    BufferManager _manager;
    OutputStream  _user_out;

    boolean     _in_struct;
    boolean     _pending_separator;
    int         _separator_character;

    int         _top;
    int []      _stack_parent_type = new int[10];
    boolean[]   _stack_in_struct = new boolean[10];
    boolean[]   _stack_pending_comma = new boolean[10];


    public IonTextWriter() {
        this(false, true);
    }
    public IonTextWriter(OutputStream out) {
        this(out, false, true);
    }
    public IonTextWriter(boolean prettyPrint) {
        this(prettyPrint,true);
    }
    public IonTextWriter(OutputStream out, boolean prettyPrint) {
        this(out, prettyPrint,true);
    }
    public IonTextWriter(boolean prettyPrint, boolean utf8AsAscii) {
        _user_out = null;
        _manager = new BufferManager();
        _output = new PrintStream(_manager.openWriter());
        initFlags(prettyPrint, utf8AsAscii);
    }
    public IonTextWriter(OutputStream out, boolean prettyPrint, boolean utf8AsAscii) {
        _user_out = out;
        _manager = null;
        if (out instanceof PrintStream) {
            _output = (PrintStream)out;
        }
        else {
            _output = new PrintStream(out);
        }
        initFlags(prettyPrint, utf8AsAscii);
    }
    void initFlags(boolean prettyPrint, boolean utf8AsAscii) {
        _pretty = prettyPrint;
        _utf8_as_ascii = utf8AsAscii;
        _separator_character = ' ';
    }


    @Override
    protected void setSymbolTable(SymbolTable symbols)
        throws IOException
    {
        if (_top != 0)
        {
            throw new IllegalStateException("not at top level");
        }

        // This ensures that symbols is system or local
        super.setSymbolTable(symbols);

        startValue();
        _output.append(symbols.getIonVersionId());
        closeValue();

        if (symbols.isLocalTable())
        {
            symbols.writeTo(this);
        }
    }


    public boolean isInStruct() {
        return this._in_struct;
    }
    void push(int typeid)
    {
        if (_top >= _stack_in_struct.length) {
            growStack();
        }
        _stack_parent_type[_top] = typeid;
        _stack_in_struct[_top] = _in_struct;
        _stack_pending_comma[_top] = _pending_separator;
        switch (typeid) {
        case IonConstants.tidSexp:
            _separator_character = ' ';
            break;
        case IonConstants.tidList:
        case IonConstants.tidStruct:
            _separator_character = ',';
            break;
        default:
            _separator_character = _pretty ? '\n' : ' ';
        break;
        }
        _top++;
    }
    void growStack() {
        int newlen = _stack_in_struct.length * 2;
        int[] temp1 = new int[newlen];
        boolean[] temp2 = new boolean[newlen];
        boolean[] temp3 = new boolean[newlen];
        System.arraycopy(_stack_parent_type, 0, temp1, 0, _top - 1);
        _stack_parent_type = temp1;
        System.arraycopy(_stack_in_struct, 0, temp2, 0, _top - 1);
        _stack_in_struct = temp2;
        System.arraycopy(_stack_pending_comma, 0, temp3, 0, _top - 1);
        _stack_pending_comma = temp3;
    }
    int pop() {
        _top--;
        int typeid = _stack_parent_type[_top];  // popped parent

        int parentid = (_top > 0) ? _stack_parent_type[_top - 1] : -1;
        switch (parentid) {
        case -1:
        case IonConstants.tidSexp:
            _separator_character = ' ';
            break;
        case IonConstants.tidList:
        case IonConstants.tidStruct:
            _separator_character = ',';
            break;
        default:
            _separator_character = _pretty ? '\n' : ' ';
        break;
        }

        return typeid;
    }
    int topType() {
        return _stack_parent_type[_top - 1];
    }
    boolean topInStruct() {
        if (_top == 0) return false;
        return _stack_in_struct[_top - 1];
    }
    boolean topPendingComma() {
        if (_top == 0) return false;
        return _stack_pending_comma[_top - 1];
    }
    void printLeadingWhiteSpace() {
        for (int ii=0; ii<_top; ii++) {
            _output.append(' ');
            _output.append(' ');
        }
    }
    void closeCollection(char closeChar) {
       if (_pretty) {
           _output.println();
           printLeadingWhiteSpace();
       }
       _output.append(closeChar);
    }
    void startValue() throws IOException
    {
        if (_pretty) {
            if (_pending_separator && _separator_character != '\n') {
                _output.append((char)_separator_character);
            }
            _output.println();
            printLeadingWhiteSpace();
        }
        else if (_pending_separator) {
            _output.append((char)_separator_character);
            // _output.append(',');

        }

        // write field name
        if (_in_struct) {
            String name = super.get_field_name_as_string();
            if (name == null) {
                throw new IllegalArgumentException("structure members require a field name");
            }
            CharSequence escapedname = escapeSymbol(name);
            _output.append(escapedname);
            _output.append(':');
            super.clearFieldName();
        }

        // write annotations
        int annotation_count = super._annotation_count;
        if (annotation_count > 0) {
            String[] annotations = super.get_annotations_as_strings();
            for (int ii=0; ii<annotation_count; ii++) {
                String name = annotations[ii];
                CharSequence escapedname = escapeSymbol(name);
                _output.append(escapedname);
                _output.append(':');
                _output.append(':');
            }
            super.clearAnnotations();
        }
    }

    void closeValue() {
        _pending_separator = true;
    }



    public void stepIn(IonType containerType) throws IOException
    {
        startValue();

        int tid;
        char opener;
        switch (containerType)
        {
            case LIST:   tid = tidList;   _in_struct = false; opener = '['; break;
            case SEXP:   tid = tidSexp;   _in_struct = false; opener = '('; break;
            case STRUCT: tid = tidStruct; _in_struct = true;  opener = '{'; break;
            default:
                throw new IllegalArgumentException();
        }

        push(tid);
        _output.append(opener);
        _pending_separator = false;
    }


    public void stepOut() throws IOException
    {
        _pending_separator = topPendingComma();
        int tid = pop();

        char closer;
        switch (tid)
        {
            case tidList:   closer = ']'; break;
            case tidSexp:   closer = ')'; break;
            case tidStruct: closer = '}'; break;
            default:
                throw new IllegalStateException();
        }
        closeCollection(closer);
        closeValue();
        _in_struct = topInStruct();

    }



    public void writeNull()
        throws IOException
    {
        startValue();
        _output.append("null");
        closeValue();
    }
    public void writeNull(IonType type) throws IOException
    {
        startValue();

        String nullimage = null;

        switch (type) {
        case NULL:      nullimage = "null";           break;
        case BOOL:      nullimage = "null.bool";      break;
        case INT:       nullimage = "null.int";       break;
        case FLOAT:     nullimage = "null.float";     break;
        case DECIMAL:   nullimage = "null.decimal";   break;
        case TIMESTAMP: nullimage = "null.timestamp"; break;
        case SYMBOL:    nullimage = "null.symbol";    break;
        case STRING:    nullimage = "null.string";    break;
        case BLOB:      nullimage = "null.blob";      break;
        case CLOB:      nullimage = "null.clob";      break;
        case SEXP:      nullimage = "null.sexp";      break;
        case LIST:      nullimage = "null.list";      break;
        case STRUCT:    nullimage = "null.struct";    break;

        default: throw new IllegalStateException("unexpected type " + type);
        }

        _output.append(nullimage);
        closeValue();
    }
    public void writeBool(boolean value)
        throws IOException
    {
        startValue();
        _output.append(value ? "true" : "false");
        closeValue();
    }
    public void writeInt(byte value)
        throws IOException
    {
        startValue();
        _output.append(value+"");
        closeValue();
    }
    public void writeInt(short value)
        throws IOException
    {
        startValue();
        _output.append(value+"");
        closeValue();
    }
    public void writeInt(int value)
        throws IOException
    {
        startValue();
        _output.append(value+"");
        closeValue();
    }
    public void writeInt(long value)
        throws IOException
    {
        startValue();
        _output.append(value+"");
        closeValue();
    }
    public void writeFloat(float value)
        throws IOException
    {
        writeFloat((double)value);
    }

    public void writeFloat(double value)
        throws IOException
    {
        startValue();

        // shortcut zero cases
        if (value == 0.0) {
            // XXX use the raw bits to avoid boxing and distinguish +/-0e0
            long bits = Double.doubleToLongBits(value);
            if (bits == 0L) {
                // positive zero
                _output.append("0e0");
            }
            else {
                // negative zero
                _output.append("-0e0");
            }
        }
        else if (Double.isNaN(value)) {
            _output.append("nan");
        }
        else if (Double.isInfinite(value)) {
            if (value > 0) {
                _output.append("+inf");
            }
            else {
                _output.append("-inf");
            }
        }
        else {
            BigDecimal decimal = new BigDecimal(value);
            BigInteger unscaled = decimal.unscaledValue();

            _output.append(unscaled.toString());
            _output.append('e');
            _output.append(Integer.toString(-decimal.scale()));
        }

        closeValue();
    }

    public void writeDecimal(BigDecimal value, IonNumber.Classification classification)
        throws IOException
    {
    	boolean is_negative_zero = IonNumber.Classification.NEGATIVE_ZERO.equals(classification);

    	if (is_negative_zero) {
    		if (value == null || value.signum() != 0) throw new IllegalArgumentException("the value must be zero to write a negative zero");
    	}

        startValue();
        BigInteger unscaled = value.unscaledValue();
        
        if (is_negative_zero) {
        	assert value.signum() == 0;
        	_output.append('-');
        }

        _output.append(unscaled.toString());
        _output.append('d');
        _output.append(Integer.toString(-value.scale()));

        closeValue();
    }

// TODO - should this be removed? use writeTimestamp(long millis, ... ??
    public void writeTimestamp(Date value, Integer localOffset)
        throws IOException
    {
        Timestamp ts =
            (value == null ? null : new Timestamp(value.getTime(), localOffset));
        writeTimestamp(ts);
    }

    public void writeTimestampUTC(Date value)
        throws IOException
    {
        writeTimestamp(value, Timestamp.UTC_OFFSET);
    }

    public void writeTimestamp(Timestamp value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
        }
        else {
            startValue();
            value.print(_output);
            closeValue();
        }
    }

    public void writeString(String value)
        throws IOException
    {
        startValue();
        _output.append('"');
        CharSequence cs = escapeString(value);
        _output.append(cs);
        _output.append('"');
        closeValue();
    }
    CharSequence escapeString(String value) {
        for (int ii=0; ii<value.length(); ii++) {
            char c = value.charAt(ii);
            switch (c) {
            case '"': //   \"  double quote
            case '\\': //   \\  backslash
                return escapeStringHelper(value, ii);
            default:
                if (c < 32 || c > 127) {
                    return escapeStringHelper(value, ii);
                }
            }
        }
        return value;
    }
    CharSequence escapeStringHelper(String value, int firstNonAscii) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int jj=0; jj<firstNonAscii; jj++) {
            sb.append(value.charAt(jj));
        }
        for (int ii=firstNonAscii; ii<value.length(); ii++) {
            char c = value.charAt(ii);
            if (c < 32) {
                sb.append(lowEscapeSequence(c));
            }
            else if (c < 128) {
                switch (c) {
                case '"':   //   \"  double quote
                //case '\'':  //   \'  single quote
                case '\\':  //   \\  backslash
                    sb.append('\\');
                    break;
                default:
                    break;
                }
                sb.append(c);
            }
            else if (c == 128) {
                sb.append('\\'+"u0100");
            }
            else if (IonConstants.isHighSurrogate(c)) {
                ii++;
                char c2 = value.charAt(ii);
                if (ii >= value.length() || !IonConstants.isLowSurrogate(c2)) {
                    throw new IllegalArgumentException("string is not valid UTF-16");
                }
                int uc = IonConstants.makeUnicodeScalar(c, c2);
                appendUTF8Char(sb, uc);
            }
            else if (IonConstants.isLowSurrogate(c)) {
                throw new IllegalArgumentException("string is not valid UTF-16");
            }
            else {
                appendUTF8Char(sb, c);
            }
        }
        return sb;
    }
    // escape sequences for character below ascii 32 (space)
    static final String [] LOW_ESCAPE_SEQUENCES = {
          "0",   "x01", "x02", "x03",
          "x04", "x05", "x06", "a",
          "b",   "t",   "n",   "v",
          "f",   "r",   "x0e", "x0f",
          "x10", "x11", "x12", "x13",
          "x14", "x15", "x16", "x17",
          "x18", "x19", "x1a", "x1b",
          "x1c", "x1d", "x1e", "x1f",
    };
    String lowEscapeSequence(char c) {
        if (c == 13) {
            return '\\'+LOW_ESCAPE_SEQUENCES[c];
        }
        return '\\'+LOW_ESCAPE_SEQUENCES[c];
    }
    void appendUTF8Char(StringBuilder sb, int uc) {
        String image;
        if (this._utf8_as_ascii) {
            image = Integer.toHexString(uc);
            int len = image.length();
            if (len <= 4) {
                sb.append('\\'+"u0000", 0, 6 - len);
            }
            else {
                sb.append('\\'+"U00000000", 0, 10 - len);
            }
            sb.append(image);
        }
        else {
            appendUTF8Bytes(sb, uc);
        }
    }
    void appendUTF8Bytes(StringBuilder sb, int c) {
        if (c <= 128) {
            throw new IllegalArgumentException("only non-ascii code points (characters) are accepted here");
        }
        else if ((c & (~0x7ff)) == 0) {
            sb.append((char)( 0xff & (0xC0 | (c >> 6)) ));
            sb.append((char)( 0xff & (0x80 | (c & 0x3F)) ));
        }
        else if ((c & (~0xffff)) == 0) {
            sb.append((char)( 0xff & (0xE0 |  (c >> 12)) ));
            sb.append((char)( 0xff & (0x80 | ((c >> 6) & 0x3F)) ));
            sb.append((char)( 0xff & (0x80 |  (c & 0x3F)) ));
        }
        else if ((c & (~0x7ffff)) == 0) {
            sb.append((char)( 0xff & (0xF0 |  (c >> 18)) ));
            sb.append((char)( 0xff & (0x80 | ((c >> 12) & 0x3F)) ));
            sb.append((char)( 0xff & (0x80 | ((c >> 6) & 0x3F)) ));
            sb.append((char)( 0xff & (0x80 | (c & 0x3F)) ));
        }
        else {
            throw new IllegalArgumentException("invalid character for UTF-8 output");
        }
    }

    public void writeSymbol(int symbolId)
        throws IOException
    {
        if (_symbol_table == null) {
            throw new IllegalStateException("a symbol table is required if you use symbol ids");
        }
        writeSymbol(_symbol_table.findSymbol(symbolId));
    }

    public void writeSymbol(String value)
        throws IOException
    {
        startValue();
        CharSequence cs = escapeSymbol(value);
        _output.append(cs);
        closeValue();
    }
    static final boolean [] VALID_SYMBOL_CHARACTERS = makeValidSymbolCharacterHelperArray();
    static boolean [] makeValidSymbolCharacterHelperArray() {
        boolean [] is_valid = makeValidLeadingSymbolCharacterHelperArray();
        // basically we add the digits '0' through '9' to the list
        // of characters that are valid in an unquoted symbol to
        // the list of characters that are valid to start a symbol
        for (int c = '0'; c <= '9'; c++) is_valid[c] = true;
        return is_valid;
    }
    static final boolean [] VALID_LEADING_SYMBOL_CHARACTERS = makeValidLeadingSymbolCharacterHelperArray();
    static boolean [] makeValidLeadingSymbolCharacterHelperArray() {
        boolean [] is_valid = new boolean[128];
        for (int c = 'a'; c <= 'z'; c++) is_valid[c] = true;
        for (int c = 'A'; c <= 'Z'; c++) is_valid[c] = true;
        is_valid['_'] = true;
        is_valid['$'] = true;
        return is_valid;
    }
    CharSequence escapeSymbol(String value) {
        char c = value.charAt(0);
        if (c > 127 || !VALID_LEADING_SYMBOL_CHARACTERS[c]) {
            return escapeSymbolHelper(value, 0);
        }
        for (int ii=1; ii<value.length(); ii++) {
            c = value.charAt(ii);
            if (c > 127 || !VALID_SYMBOL_CHARACTERS[c]) {
                return escapeSymbolHelper(value, ii);
            }
        }
        return value;
    }
    CharSequence escapeSymbolHelper(String value, int firstNonAscii) {
        StringBuilder sb = new StringBuilder(value.length());
        sb.append('\'');
        for (int jj=0; jj<firstNonAscii; jj++) {
            sb.append(value.charAt(jj));
        }
        for (int ii=firstNonAscii; ii<value.length(); ii++) {
            char c = value.charAt(ii);
            if (c < 32) {
                sb.append(lowEscapeSequence(c));
            }
            else if (c < 128) {
                switch (c) {
                case '\'': //   \'  single quote
                case '\\': //   \\  backslash
                    sb.append('\\');
                    break;
                default:
                    break;
                }
                sb.append(c);
            }
            else if (c == 128) {
                sb.append('\\'+"u0100");
            }
            else if (IonConstants.isHighSurrogate(c)) {
                ii++;
                char c2 = value.charAt(ii);
                if (ii >= value.length() || !IonConstants.isLowSurrogate(c2)) {
                    throw new IllegalArgumentException("string is not valid UTF-16");
                }
                int uc = IonConstants.makeUnicodeScalar(c, c2);
                appendUTF8Char(sb, uc);
            }
            else if (IonConstants.isLowSurrogate(c)) {
                throw new IllegalArgumentException("string is not valid UTF-16");
            }
            else {
                appendUTF8Char(sb, c);
            }
        }
        sb.append('\'');
        return sb;
    }

    public void writeBlob(byte[] value)
        throws IOException
    {
        writeBlob(value, 0, value.length);
    }
    public void writeBlob(byte[] value, int start, int len)
        throws IOException
    {
        TextStream ts = new TextStream(new ByteArrayInputStream(value, start, len));

        startValue();
        _output.append("{{");
        // base64 encoding is 6 bits per char so
        // it evens out at 3 bytes in 4 characters
        char[] buf = new char[_pretty ? 80 : 400];
        CharBuffer cb = CharBuffer.wrap(buf);
        if (_pretty) _output.append(" ");
        for (;;) {
            int clen = ts.read(buf, 0, buf.length);
            if (clen < 1) break;
            _output.append(cb, 0, clen);
        }
        if (_pretty) _output.append(" ");
        _output.append("}}");
        closeValue();
    }
    public void writeClob(byte[] value)
        throws IOException
    {
        writeClob(value, 0, value.length);

    }
    public void writeClob(byte[] value, int start, int len)
        throws IOException
    {
        startValue();
        _output.append("{{");

        if (_pretty) _output.append(" ");
        _output.append('"');

        int end = start + len;
        for (int ii=start; ii<end; ii++) {
            char c = (char)(value[ii] & 0xff);
            if (c < 32 ) {
                _output.append(lowEscapeSequence(c));
            }
            else if (c == 128) {
                _output.append('\\'+"u0100");
            }
            else {
                switch (c) {
                case '"':  //   \"  double quote
                case '\\': //   \\  backslash
                    _output.append('\\');
                    break;
                default:
                    break;
                }
                _output.append(c);
            }
        }
        _output.append('"');
        if (_pretty) _output.append(" ");
        _output.append("}}");
        closeValue();
    }

    public byte[] getBytes()
        throws IOException
    {
        if (_manager == null) {
            throw new IllegalStateException("this writer was not created with buffer backing");
        }
        byte[] bytes = null;
        int len = _manager.buffer().size();
        bytes = new byte[len];
        IonBinary.Reader r = _manager.openReader();
        r.sync();
        r.setPosition(0); // just in case
        len = r.read(bytes);
        if (len != _manager.buffer().size()) {
            throw new IllegalStateException("inconsistant buffer sizes encountered");
        }
        return bytes;
    }
    public int getBytes(byte[] bytes, int offset, int maxlen)
        throws IOException
    {
        if (_manager == null) {
            throw new IllegalStateException("this writer was not created with buffer backing");
        }
        int buffer_length = _manager.buffer().size();
        if (buffer_length > maxlen) {
            throw new IllegalArgumentException();
        }
        IonBinary.Reader r = _manager.openReader();
        r.sync();
        r.setPosition(0); // just in case
        int len = r.read(bytes, offset, buffer_length);
        if (buffer_length != _manager.buffer().size()) {
            throw new IllegalStateException("inconsistant buffer sizes encountered");
        }
        return len;
    }
    public int writeBytes(SimpleByteBuffer.SimpleByteWriter out) // OutputStream out)
        throws IOException
    {
        if (_manager == null) {
            throw new IllegalStateException("this writer was not created with buffer backing");
        }
        int buffer_length = _manager.buffer().size();

        IonBinary.Reader r = _manager.openReader();
        r.sync();
        r.setPosition(0); // just in case

        int len = r.writeTo((ByteWriter)out, buffer_length);
        if (buffer_length != buffer_length) {
            throw new IllegalStateException("inconsistant buffer sizes encountered");
        }
        return len;
    }

}
