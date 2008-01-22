/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.IonType;
import com.amazon.ion.impl.IonBinary;
import com.amazon.ion.impl.IonConstants;
import com.amazon.ion.impl.Base64Encoder.TextStream;
import com.amazon.ion.impl.IonBinary.BufferManager;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.CharBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 */
public class IonTextWriter
    extends IonBaseWriter
{
    boolean      _pretty;
    boolean      _utf8_as_ascii;
    PrintStream  _output;

    BufferManager _manager; 
    OutputStream  _user_out;

    boolean     _in_struct;
    boolean     _pending_comma;
    
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
        _pretty = prettyPrint;
        _utf8_as_ascii = utf8AsAscii;
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
        _pretty = prettyPrint;
        _utf8_as_ascii = utf8AsAscii;
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
        _stack_pending_comma[_top] = _pending_comma;
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
        return _stack_parent_type[_top];
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
        if (_pending_comma) {
            _output.append(',');
            
        }
        if (_pretty) {
            _output.println();
            printLeadingWhiteSpace();
        }

        // write field name
        if (_in_struct) {
            String name = super.get_field_name_as_string();
            if (name == null) {
                throw new IllegalArgumentException("structure members require a field name");
            }
            _output.append(name);
            _output.append(':');
            super.clearFieldName();
        }

        // write annotations
        int annotation_count = super._annotation_count;
        if (annotation_count > 0) {
            String[] annotations = super.get_annotations_as_strings();
            for (int ii=0; ii<annotation_count; ii++) {
                String name = annotations[ii];
                _output.append(name);
                _output.append(':');
                _output.append(':');
            }
            super.clearAnnotations();
        }
    }
    
    void closeValue() {
        _pending_comma = true;
    }

    public void startList()
        throws IOException
    {
        startValue();
        _in_struct = false;
        push(IonConstants.tidList);        
        _output.append('[');
        _pending_comma = false;
      }
    public void startSexp()
        throws IOException
    {
        startValue();
        _in_struct = false;
        push(IonConstants.tidSexp);        
        _output.append('(');
        _pending_comma = false;
    }
    public void startStruct()
        throws IOException
    {
        startValue();
        _in_struct = true;
        push(IonConstants.tidStruct);        
        _output.append('{');
        _pending_comma = false;
    }
    public void closeList()
        throws IOException
    {
        _pending_comma = topPendingComma();
        if (pop() != IonConstants.tidList) {
            throw new IllegalStateException("mismatched close");
        }
        closeCollection(']');
        closeValue();
        _in_struct = topInStruct();
    }

    public void closeSexp()
        throws IOException
    {
        _pending_comma = topPendingComma();
        if (pop() != IonConstants.tidSexp) {
            throw new IllegalStateException("mismatched close");
        }
        closeCollection(')');
        closeValue();
        _in_struct = topInStruct();
    }

    public void closeStruct()
        throws IOException
    {
        _pending_comma = topPendingComma();
        if (pop() != IonConstants.tidStruct) {
            throw new IllegalStateException("mismatched close");
        }
        closeCollection('}');
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
        _output.append("null.");
        
        String typename = null;

        switch (type) {
        case NULL: typename = "null"; break;
        case BOOL: typename = "bool"; break;
        case INT: typename = "int"; break;
        case FLOAT: typename = "float"; break;
        case DECIMAL: typename = "decimal"; break;
        case TIMESTAMP: typename = "timestamp"; break;
        case SYMBOL: typename = "symbol"; break;
        case STRING: typename = "string"; break;
        case BLOB: typename = "blob"; break;
        case CLOB: typename = "clob"; break;
        case SEXP: typename = "sexp"; break;
        case LIST: typename = "list"; break;
        case STRUCT: typename = "struct"; break;
        }
        
        _output.append(typename);
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
        startValue();
        _output.append(Double.toString(value));
        closeValue();
    }

    public void writeFloat(double value)
        throws IOException
    {
        startValue();
        _output.append(Double.toString(value));
        closeValue();
    }

    public void writeDecimal(BigDecimal value)
        throws IOException
    {
        startValue();
        String image = value.toPlainString();
        image = image.replace('e', 'd');
        image = image.replace('E', 'd');
        _output.append(image);
        closeValue();
    }
    static final long SECONDS_IN_A_DAY = 24 * 60 * 60;
    public void writeTimestamp(Date value, int localOffset)
        throws IOException
    {
        //  "yyyy-MM-dd'T'HH:mm:ss.SSS"
        //  2001-07-04T12:08:56.235 
        startValue();
        long day = value.getTime();
        long temp1 = day / 1000;
        long temp2 = temp1 / SECONDS_IN_A_DAY;
        
        SimpleDateFormat sdf;
        
        if (temp1 * 1000 != day) {
            // there are fractional seconds, use everything
            // use fractional seconds (3 decimal place for java
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"); 
        }
        else if (temp2 * SECONDS_IN_A_DAY != day) {
            // we can round to seconds (there's no fration, but is some time)
            sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        }
        else  {
            // the time value is empty, just use the day
            sdf = new SimpleDateFormat("yyyy-MM-dd");
        }
        String offset;
        if (localOffset != 0) {
            offset = "Z";
        }
        else {
            offset = Integer.toString(localOffset);
        }
        String datetime = sdf.format(value);
        _output.append(datetime);
        _output.append(offset);
        closeValue();
    }
    public void writeTimestampUTC(Date value)
        throws IOException
    {
        writeTimestamp(value, 0);
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
                    case '"': //   \"  double quote
                        sb.append("\\\"");
                        break;
                    case '\'': //   \'  single quote 
                        sb.append("\\\'");
                        break;
                    case '\\': //   \\  backslash
                        sb.append("\\\\");
                        break;
                    default: 
                        sb.append(c);
                        break;
                }
            }
            if (c == 128) {
                sb.append("\\u0100");
            }
            else if (Character.isHighSurrogate(c)) {
                ii++;
                char c2 = value.charAt(ii);
                if (ii >= value.length() || !Character.isLowSurrogate(c2)) {
                    throw new IllegalArgumentException("string is not valid UTF-16");
                }
                int uc = Character.toCodePoint(c, c2);
                appendUTF8Char(sb, uc);
            }
        }
        return sb;
    }
    // escape sequences for character below ascii 32 (space)
    static final String [] LOW_ESCAPE_SEQUENCES = {
          "\\0",   "\\x01", "\\x02", "\\x03", 
          "\\x04", "\\x05", "\\x06", "\\a",
          "\\b",   "\\t",   "\\n",   "\\v",
          "\\f",   "\\r",   "\\x0e", "\\x0f",
          "\\x10", "\\x11", "\\x12", "\\x13",
          "\\x14", "\\x15", "\\x16", "\\x17",
          "\\x18", "\\x19", "\\x1a", "\\x1b",
          "\\x1c", "\\x1d", "\\x1e", "\\x1f",
    };
    String lowEscapeSequence(char c) {
        return LOW_ESCAPE_SEQUENCES[c];
    }
    void appendUTF8Char(StringBuilder sb, int uc) {
        String image;
        if (this._utf8_as_ascii) {
            image = Integer.toHexString(uc);
            int len = image.length();
            if (len <= 4) {
                sb.append("\\u0000", 0, 6 - 4);
            }
            else {
                sb.append("\\U00000000", 0, 10 - len);
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
        boolean [] is_valid = new boolean[128];
        for (int c = 'a'; c <= 'z'; c++) is_valid[c] = true;
        for (int c = 'A'; c <= 'Z'; c++) is_valid[c] = true;
        for (int c = '0'; c <= '9'; c++) is_valid[c] = true;
        is_valid['_'] = true;
        is_valid['$'] = true;
        return is_valid;
    }
    CharSequence escapeSymbol(String value) {
        for (int ii=0; ii<value.length(); ii++) {
            char c = value.charAt(ii);
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
                        sb.append("\\\'");
                        break;
                    case '\\': //   \\  backslash
                        sb.append("\\\\");
                        break;
                    default: 
                        sb.append(c);
                        break;
                }
            }
            if (c == 128) {
                sb.append("\\u0100");
            }
            else if (Character.isHighSurrogate(c)) {
                ii++;
                char c2 = value.charAt(ii);
                if (ii >= value.length() || !Character.isLowSurrogate(c2)) {
                    throw new IllegalArgumentException("string is not valid UTF-16");
                }
                int uc = Character.toCodePoint(c, c2);
                appendUTF8Char(sb, uc);
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
                _output.append("\\u0100");
            }
            else {
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
        byte[] bytes = new byte[_manager.buffer().size()];
        IonBinary.Reader r = _manager.openReader();
        r.setPosition(0); // just in case
        int len = r.read(bytes);
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
        r.setPosition(0); // just in case
        int len = r.read(bytes, offset, buffer_length);
        if (buffer_length != _manager.buffer().size()) {
            throw new IllegalStateException("inconsistant buffer sizes encountered");
        }
        return len;
    }
    public int writeBytes(OutputStream out)
        throws IOException
    {
        if (_manager == null) {
            throw new IllegalStateException("this writer was not created with buffer backing"); 
        }
        int buffer_length = _manager.buffer().size();
        
        IonBinary.Reader r = _manager.openReader();
        r.setPosition(0); // just in case
        
        int len = r.writeTo(out, buffer_length);
        if (buffer_length != buffer_length) {
            throw new IllegalStateException("inconsistant buffer sizes encountered");
        }
        return len;
    }

}
