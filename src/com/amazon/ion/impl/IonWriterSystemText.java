// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.SYMBOLS;
import static com.amazon.ion.impl.IonWriterUserText.builderFor;
import static com.amazon.ion.impl._Private_IonConstants.tidList;
import static com.amazon.ion.impl._Private_IonConstants.tidSexp;
import static com.amazon.ion.impl._Private_IonConstants.tidStruct;
import static com.amazon.ion.system.IonTextWriterBuilder.ASCII;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.Base64Encoder.TextStream;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonWriterUserText.TextOptions;
import com.amazon.ion.system.IonTextWriterBuilder.LstMinimizing;
import com.amazon.ion.util.IonTextUtils;
import com.amazon.ion.util.IonTextUtils.SymbolVariant;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;

/**
 *
 */
class IonWriterSystemText  // TODO ION-271 make final after IMS is migrated
    extends IonWriterSystem
{
    /** Not null. */
    private final Appendable _output;
    /** Not null. */
    private final _Private_IonTextWriterBuilder _options;
    /** At least one. */
    private final int _long_string_threshold;

    BufferManager _manager;

    /** Ensure we don't use a closed {@link #output} stream. */
    private boolean _closed;
    boolean     _in_struct;
    boolean     _pending_separator;

    /**
     * True when the last data written was a triple-quoted string, meaning we
     * cannot write another long string lest it be incorrectly concatenated.
     */
    private boolean _following_long_string;

    int         _separator_character;

    int         _top;
    int []      _stack_parent_type = new int[10];
    boolean[]   _stack_in_struct = new boolean[10];
    boolean[]   _stack_pending_comma = new boolean[10];


    /**
     * @throws NullPointerException if any parameter is null.
     */
    protected IonWriterSystemText(SymbolTable defaultSystemSymtab,
                                  _Private_IonTextWriterBuilder options,
                                  OutputStream out)
    {
        this(defaultSystemSymtab,
             options,
             (out instanceof Appendable
                 ? (Appendable) out
                 : new IonUTF8.CharToUTF8(out)));
    }

    /**
     * @throws NullPointerException if any parameter is null.
     */
    protected IonWriterSystemText(SymbolTable defaultSystemSymtab,
                                  _Private_IonTextWriterBuilder options,
                                  Appendable out)
    {
        super(defaultSystemSymtab,
              options.getInitialIvmHandling(),
              options.getIvmMinimizing());

        out.getClass(); // Efficient null check

        _output = out;
        _options = options;

        if (_options.isPrettyPrintOn()) {
            _separator_character = '\n';
        }
        else {
            _separator_character = ' ';
        }

        int threshold = _options.getLongStringThreshold();
        if (threshold < 1) threshold = Integer.MAX_VALUE;
        _long_string_threshold = threshold;
    }

    @Deprecated // TODO ION-271 remove after IMS is migrated
    IonWriterSystemText(IonSystem sys,
                        SymbolTable defaultSystemSymtab,
                        Appendable out,
                        IonWriterUserText.TextOptions options)
    {
        this(defaultSystemSymtab,
             builderFor(sys.getCatalog(), options),
             out);
    }

    /**
     * FIXME HACK for IMSv3 that WILL NOT BE MAINTAINED
     */
    @Deprecated
    Appendable getOutput()
    {
        return _output;
    }

    @Deprecated // TODO ION-271 remove after IMS is migrated
    TextOptions getOptions()
    {
        return new TextOptions(_options.isPrettyPrintOn(),
                               ASCII.equals(_options.getCharset()));
    }

    _Private_IonTextWriterBuilder getBuilder()
    {
        return _options;
    }

    @Override
    public int getDepth()
    {
        return _top;
    }
    public boolean isInStruct() {
        return this._in_struct;
    }
    protected IonType getContainer()
    {
        IonType container;

        if (_top < 1) {
            container = IonType.DATAGRAM;
        }
        else {
            switch(_stack_parent_type[_top-1]) {
            case _Private_IonConstants.tidDATAGRAM:
                container = IonType.DATAGRAM;
                break;
            case _Private_IonConstants.tidSexp:
                container = IonType.SEXP;
                break;
            case _Private_IonConstants.tidList:
                container = IonType.LIST;
                break;
            case _Private_IonConstants.tidStruct:
                container = IonType.STRUCT;
                break;
            default:
                throw new IonException("unexpected container in parent stack: "+_stack_parent_type[_top-1]);
            }
        }
        return container;
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
        case _Private_IonConstants.tidSexp:
            _separator_character = ' ';
            break;
        case _Private_IonConstants.tidList:
        case _Private_IonConstants.tidStruct:
            _separator_character = ',';
            break;
        default:
            _separator_character = _options.isPrettyPrintOn() ? '\n' : ' ';
        break;
        }
        _top++;
    }
    void growStack() {
        int oldlen = _stack_in_struct.length;
        int newlen = oldlen * 2;
        int[] temp1 = new int[newlen];
        boolean[] temp2 = new boolean[newlen];
        boolean[] temp3 = new boolean[newlen];

        System.arraycopy(_stack_parent_type, 0, temp1, 0, oldlen);
        System.arraycopy(_stack_in_struct, 0, temp2, 0, oldlen);
        System.arraycopy(_stack_pending_comma, 0, temp3, 0, oldlen);

        _stack_parent_type = temp1;
        _stack_in_struct = temp2;
        _stack_pending_comma = temp3;
    }
    int pop() {
        _top--;
        int typeid = _stack_parent_type[_top];  // popped parent

        int parentid = (_top > 0) ? _stack_parent_type[_top - 1] : -1;
        switch (parentid) {
        case -1:
        case _Private_IonConstants.tidSexp:
            _separator_character = ' ';
            break;
        case _Private_IonConstants.tidList:
        case _Private_IonConstants.tidStruct:
            _separator_character = ',';
            break;
        default:
            _separator_character = _options.isPrettyPrintOn() ? '\n' : ' ';
        break;
        }

        return typeid;
    }

    /**
     * @return a tid
     * @throws ArrayIndexOutOfBoundsException if _top < 1
     */
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

    private boolean containerIsSexp()
    {
        if (_top == 0) return false;
        int topType = topType();
        return (topType == tidSexp);
    }

    void printLeadingWhiteSpace() throws IOException {
        for (int ii=0; ii<_top; ii++) {
            _output.append(' ');
            _output.append(' ');
        }
    }
    void closeCollection(char closeChar) throws IOException {
       if (_options.isPrettyPrintOn()) {
           _output.append(_options.lineSeparator());
           printLeadingWhiteSpace();
       }
       _output.append(closeChar);
    }


    private void writeSidLiteral(int sid)
        throws IOException
    {
        assert sid > 0;

        // No extra handling needed for JSON strings, this is already legal.

        boolean asString = _options._symbol_as_string;
        if (asString) _output.append('"');

        _output.append('$');
        // TODO optimize to avoid intermediate string
        _output.append(Integer.toString(sid));

        if (asString) _output.append('"');
    }


    /**
     * @param value must not be null.
     */
    private void writeSymbolToken(String value) throws IOException
    {
        if (_options._symbol_as_string)
        {
            if (_options._string_as_json)
            {
                IonTextUtils.printJsonString(_output, value);
            }
            else
            {
                IonTextUtils.printString(_output, value);
            }
        }
        else
        {
            SymbolVariant variant = IonTextUtils.symbolVariant(value);
            switch (variant)
            {
                case IDENTIFIER:
                {
                    _output.append(value);
                    break;
                }
                case OPERATOR:
                {
                    if (containerIsSexp())
                    {
                        _output.append(value);
                        break;
                    }
                    // else fall through...
                }
                case QUOTED:
                {
                    IonTextUtils.printQuotedSymbol(_output, value);
                    break;
                }
            }
        }
    }


    @Override
    void startValue() throws IOException
    {
        super.startValue();

        boolean followingLongString = _following_long_string;

        if (_options.isPrettyPrintOn()) {
            if (_pending_separator && _separator_character > ' ') {
                // Only bother if the separator is non-whitespace.
                _output.append((char)_separator_character);
                followingLongString = false;
            }
            _output.append(_options.lineSeparator());
            printLeadingWhiteSpace();
        }
        else if (_pending_separator) {
            _output.append((char)_separator_character);
            if (_separator_character > ' ') followingLongString = false;
        }

        // write field name
        if (_in_struct) {
            SymbolToken sym = assumeFieldNameSymbol();
            String name = sym.getText();
            if (name == null) {
                int sid = sym.getSid();
                writeSidLiteral(sid);
            }
            else {
                writeSymbolToken(name);
            }
            _output.append(':');
            clearFieldName();
            followingLongString = false;
        }

        // write annotations
        if (hasAnnotations()) {
            if (! _options._skip_annotations) {
                SymbolToken[] annotations = getTypeAnnotationSymbols();
                for (SymbolToken ann : annotations) {
                    String name = ann.getText();
                    if (name == null) {
                        _output.append('$');
                        _output.append(Integer.toString(ann.getSid()));
                    }
                    else {
                        IonTextUtils.printSymbol(_output, name);
                    }
                    _output.append("::");
                }
                followingLongString = false;
            }
            clearAnnotations();
        }

        _following_long_string = followingLongString;
    }

    void closeValue() {
        super.endValue();
        _pending_separator = true;
        _following_long_string = false;  // Caller overwrites this as needed.
    }



    @Override
    void writeIonVersionMarkerAsIs(SymbolTable systemSymtab)
        throws IOException
    {
        writeSymbolAsIs(systemSymtab.getIonVersionId());
    }

    @Override
    void writeLocalSymtab(SymbolTable symtab)
        throws IOException
    {
        SymbolTable[] imports = symtab.getImportedTables();

        LstMinimizing min = _options.getLstMinimizing();
        if (min == null)
        {
            symtab.writeTo(this);
        }
        else if (min == LstMinimizing.LOCALS && imports.length > 0)
        {
            // Copy the symtab, but filter out local symbols.

            IonReader reader = new UnifiedSymbolTableReader(symtab);

            // move onto and write the struct header
            IonType t = reader.next();
            assert(IonType.STRUCT.equals(t));
            SymbolToken[] a = reader.getTypeAnnotationSymbols();
            // you (should) always have the $ion_symbol_table annotation
            assert(a != null && a.length >= 1);

            // now we'll start a local symbol table struct
            // in the underlying system writer
            setTypeAnnotationSymbols(a);
            stepIn(IonType.STRUCT);

            // step into the symbol table struct and
            // write the values - EXCEPT the symbols field
            reader.stepIn();
            for (;;) {
                t = reader.next();
                if (t == null) break;
                // get the field name and skip over 'symbols'
                String name = reader.getFieldName();
                if (SYMBOLS.equals(name)) {
                    continue;
                }
                writeValue(reader);
            }

            // we're done step out and move along
            stepOut();
        }
        else  // Collapse to IVM
        {
            SymbolTable systemSymtab = symtab.getSystemSymbolTable();
            writeIonVersionMarker(systemSymtab);
        }

        super.writeLocalSymtab(symtab);
    }


    public void stepIn(IonType containerType) throws IOException
    {
        startValue();

        int tid;
        char opener;
        switch (containerType)
        {
            case SEXP:
            {
                if (!_options._sexp_as_list)
                {
                    tid = tidSexp; _in_struct = false; opener = '('; break;
                }
                // else fall through and act just like list
            }
            case LIST:   tid = tidList;   _in_struct = false; opener = '['; break;
            case STRUCT: tid = tidStruct; _in_struct = true;  opener = '{'; break;
            default:
                throw new IllegalArgumentException();
        }

        push(tid);
        _output.append(opener);
        _pending_separator = false;
        _following_long_string = false;
    }

    public void stepOut() throws IOException
    {
        if (_top < 1) {
            throw new IllegalStateException(IonMessages.CANNOT_STEP_OUT);
        }
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


    //========================================================================


    @Override
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

        String nullimage;

        if (_options._untyped_nulls)
        {
            nullimage = "null";
        }
        else
        {
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
    public void writeInt(int value)
        throws IOException
    {
        startValue();
        _output.append(Integer.toString(value));
        closeValue();
    }

    public void writeInt(long value)
        throws IOException
    {
        startValue();
        _output.append(Long.toString(value));
        closeValue();
    }

    public void writeInt(BigInteger value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.INT);
            return;
        }

        startValue();
        _output.append(value.toString());
        closeValue();
    }

    public void writeFloat(double value)
        throws IOException
    {
        startValue();

        // shortcut zero cases
        if (value == 0.0) {
            if (Double.compare(value, 0d) == 0) {
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


    @Override
    public void writeDecimal(BigDecimal value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.DECIMAL);
            return;
        }

        startValue();
        BigDecimal decimal = value;
        BigInteger unscaled = decimal.unscaledValue();

        int signum = decimal.signum();
        if (signum < 0)
        {
            _output.append('-');
            unscaled = unscaled.negate();
        }
        else if (decimal instanceof Decimal
             && ((Decimal)decimal).isNegativeZero())
        {
            // for the various forms of negative zero we have to
            // write the sign ourselves, since neither BigInteger
            // nor BigDecimal recognize negative zero, but Ion does.
            _output.append('-');
        }

        final String unscaledText = unscaled.toString();
        final int significantDigits = unscaledText.length();

        final int scale = decimal.scale();
        final int exponent = -scale;

        if (_options._decimal_as_float)
        {
            _output.append(unscaledText);
            _output.append('e');
            _output.append(Integer.toString(exponent));
        }
        else if (exponent == 0)
        {
            _output.append(unscaledText);
            _output.append('.');
        }
        else if (0 < scale)
        {
            int wholeDigits;
            int remainingScale;
            if (significantDigits > scale)
            {
                wholeDigits = significantDigits - scale;
                remainingScale = 0;
            }
            else
            {
                wholeDigits = 1;
                remainingScale = scale - significantDigits + 1;
            }

            _output.append(unscaledText, 0, wholeDigits);
            if (wholeDigits < significantDigits)
            {
                _output.append('.');
                _output.append(unscaledText, wholeDigits,
                             significantDigits);
            }

            if (remainingScale != 0)
            {
                _output.append("d-");
                _output.append(Integer.toString(remainingScale));
            }
        }
        else // (exponent > 0)
        {
            // We cannot move the decimal point to the right, adding
            // rightmost zeros, because that would alter the precision.
            _output.append(unscaledText);
            _output.append('d');
            _output.append(Integer.toString(exponent));
        }
        closeValue();
    }

    public void writeTimestamp(Timestamp value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
            return;
        }

        startValue();

        if (_options._timestamp_as_millis)
        {
            long millis = value.getMillis();
            _output.append(Long.toString(millis));
        }
        else if (_options._timestamp_as_string)
        {
            // Timestamp is ASCII-safe so this is easy
            _output.append('"');
            value.print(_output);
            _output.append('"');
        }
        else
        {
            value.print(_output);
        }

        closeValue();
    }

    public void writeString(String value)
        throws IOException
    {
        startValue();
        if (value != null
            && ! _following_long_string
            && _long_string_threshold < value.length())
        {
            // TODO This can lead to mixed newlines in the output.
            // It assumes NL line separators, but _options could use CR+NL
            IonTextUtils.printLongString(_output, value);

            // This sets _following_long_string = false so we must overwrite
            closeValue();
            _following_long_string = true;
        }
        else
        {
            if (_options._string_as_json)
            {
                IonTextUtils.printJsonString(_output, value);
            }
            else
            {
                IonTextUtils.printString(_output, value);
            }
            closeValue();
        }
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

    @Override
    void writeSymbolAsIs(int symbolId)
        throws IOException
    {
        SymbolTable symtab = getSymbolTable();
        String text = symtab.findKnownSymbol(symbolId);
        if (text != null)
        {
            writeSymbolAsIs(text);
        }
        else
        {
            startValue();
            writeSidLiteral(symbolId);
            closeValue();
        }
    }

    @Override
    public void writeSymbolAsIs(String value)
        throws IOException
    {
        if (value == null)
        {
            writeNull(IonType.SYMBOL);
            return;
        }

        startValue();
        writeSymbolToken(value);
        closeValue();
    }

    public void writeBlob(byte[] value, int start, int len)
        throws IOException
    {
        if (value == null)
        {
            writeNull(IonType.BLOB);
            return;
        }

        TextStream ts = new TextStream(new ByteArrayInputStream(value, start, len));

        // base64 encoding is 6 bits per char so
        // it evens out at 3 bytes in 4 characters
        char[] buf = new char[_options.isPrettyPrintOn() ? 80 : 400];
        CharBuffer cb = CharBuffer.wrap(buf);

        startValue();

        if (_options._blob_as_string) {
            _output.append('"');
        }
        else {
            _output.append("{{");
            if (_options.isPrettyPrintOn()) {
                _output.append(' ');
            }
        }

        for (;;) {
            // TODO is it better to fill up the CharBuffer before outputting?
            int clen = ts.read(buf, 0, buf.length);
            if (clen < 1) break;
            _output.append(cb, 0, clen);
        }


        if (_options._blob_as_string) {
            _output.append('"');
        }
        else {
            if (_options.isPrettyPrintOn()) {
                _output.append(' ');
            }
            _output.append("}}");
        }
        closeValue();
    }

    public void writeClob(byte[] value, int start, int len)
        throws IOException
    {
        if (value == null)
        {
            writeNull(IonType.CLOB);
            return;
        }

        startValue();

        if (! _options._clob_as_string) {
            _output.append("{{");

            if (_options.isPrettyPrintOn()) {
                _output.append(" ");
            }
        }

        final boolean json =
            _options._clob_as_string && _options._string_as_json;

        boolean longString = (_long_string_threshold < value.length);

        if (longString) {
            _output.append("'''");
        } else {
            _output.append('"');
        }

        int end = start + len;
        for (int ii=start; ii<end; ii++) {
            char c = (char)(value[ii] & 0xff);
            if (c < 32 ) {
                if (json) {
                    IonTextUtils.printJsonCodePoint(_output, c);
                }
                else if (c == '\n' && longString) {
                    // TODO account for NL versus CR+NL streams
                    _output.append(c);
                } else {
                    _output.append(lowEscapeSequence(c));
                }
            }
            else if (c > 127) {
                if (json) {
                    // JSON uses u00HH
                    IonTextUtils.printJsonCodePoint(_output, c);
                }
                else
                {
                    // Ion uses \xHH
                    _output.append("\\x");
                    // this should always be 2 hex chars
                    assert (c > 0x7f && c <= 0xff);
                    _output.append(Integer.toHexString(c));
                }
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

        if (longString) {
            _output.append("'''");
        } else {
            _output.append('"');
        }

        if (! _options._clob_as_string) {
            if (_options.isPrettyPrintOn()) {
                _output.append(" ");
            }
            _output.append("}}");
        }

        closeValue();
    }


    /**
     * {@inheritDoc}
     * <p>
     * The {@link OutputStream} spec is mum regarding the behavior of flush on
     * a closed stream, so we shouldn't assume that our stream can handle that.
     */
    public void flush() throws IOException
    {
        if (! _closed) {
            if (_output instanceof Flushable) {
                ((Flushable)_output).flush();
            }
        }
    }

    public void close() throws IOException
    {
        if (! _closed) {
            try
            {
                if (getDepth() == 0) {
                    finish();
                }
            }
            finally
            {

                // Do this first so we are closed even if the call below throws.
                _closed = true;

                if (_output instanceof Closeable) {
                    ((Closeable)_output).close();
                }
            }
        }
    }
}

