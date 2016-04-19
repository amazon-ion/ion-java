/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static software.amazon.ion.SystemSymbols.SYMBOLS;
import static software.amazon.ion.impl.PrivateIonConstants.tidList;
import static software.amazon.ion.impl.PrivateIonConstants.tidSexp;
import static software.amazon.ion.impl.PrivateIonConstants.tidStruct;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;
import software.amazon.ion.system.IonTextWriterBuilder.LstMinimizing;
import software.amazon.ion.util.IonTextUtils;
import software.amazon.ion.util.PrivateFastAppendable;
import software.amazon.ion.util.IonTextUtils.SymbolVariant;

class IonWriterSystemText
    extends IonWriterSystem
{
    /** Not null. */
    private final PrivateIonTextWriterBuilder _options;
    /** At least one. */
    private final int _long_string_threshold;

    private final PrivateIonTextAppender _output;

    /** Ensure we don't use a closed {@link #output} stream. */
    private boolean _closed;

    /**
     * True when the current container is a struct, so we write field names.
     */
    boolean     _in_struct;
    boolean     _pending_separator;

    /**
     * Indicates whether we're currently writing an IVM.
     */
    private boolean _is_writing_ivm;

    /**
     * True when the last data written was a triple-quoted string, meaning we
     * cannot write another long string lest it be incorrectly concatenated.
     */
    private boolean _following_long_string;

    int         _separator_character;

    int         _top;
    int []      _stack_parent_type = new int[10];
    boolean[]   _stack_pending_comma = new boolean[10];


    /**
     * @throws NullPointerException if any parameter is null.
     */
    protected IonWriterSystemText(SymbolTable defaultSystemSymtab,
                                  PrivateIonTextWriterBuilder options,
                                  PrivateFastAppendable out)
    {
        super(defaultSystemSymtab,
              options.getInitialIvmHandling(),
              options.getIvmMinimizing());

        _output =
            PrivateIonTextAppender.forFastAppendable(out,
                                                       options.getCharset());
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


    PrivateIonTextWriterBuilder getBuilder()
    {
        return _options;
    }

    @Override
    public int getDepth()
    {
        return _top;
    }
    public boolean isInStruct() {
        return _in_struct;
    }
    protected IonType getContainer()
    {
        IonType container;

        if (_top < 1) {
            container = IonType.DATAGRAM;
        }
        else {
            switch(_stack_parent_type[_top-1]) {
            case PrivateIonConstants.tidDATAGRAM:
                container = IonType.DATAGRAM;
                break;
            case PrivateIonConstants.tidSexp:
                container = IonType.SEXP;
                break;
            case PrivateIonConstants.tidList:
                container = IonType.LIST;
                break;
            case PrivateIonConstants.tidStruct:
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
        if (_top + 1 == _stack_parent_type.length) {
            growStack();
        }
        _stack_parent_type[_top] = typeid;
        _stack_pending_comma[_top] = _pending_separator;
        switch (typeid) {
        case PrivateIonConstants.tidSexp:
            _separator_character = ' ';
            break;
        case PrivateIonConstants.tidList:
        case PrivateIonConstants.tidStruct:
            _separator_character = ',';
            break;
        default:
            _separator_character = _options.isPrettyPrintOn() ? '\n' : ' ';
        break;
        }
        _top++;
    }
    void growStack() {
        int oldlen = _stack_parent_type.length;
        int newlen = oldlen * 2;
        int[] temp1 = new int[newlen];
        boolean[] temp3 = new boolean[newlen];

        System.arraycopy(_stack_parent_type, 0, temp1, 0, oldlen);
        System.arraycopy(_stack_pending_comma, 0, temp3, 0, oldlen);

        _stack_parent_type = temp1;
        _stack_pending_comma = temp3;
    }
    int pop() {
        _top--;
        int typeid = _stack_parent_type[_top];  // popped parent

        int parentid = (_top > 0) ? _stack_parent_type[_top - 1] : -1;
        switch (parentid) {
        case -1:
        case PrivateIonConstants.tidSexp:
            _in_struct = false;
            _separator_character = ' ';
            break;
        case PrivateIonConstants.tidList:
            _in_struct = false;
            _separator_character = ',';
            break;
        case PrivateIonConstants.tidStruct:
            _in_struct = true;
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
            _output.appendAscii(' ');
            _output.appendAscii(' ');
        }
    }
    void closeCollection(char closeChar) throws IOException {
       if (_options.isPrettyPrintOn()) {
           _output.appendAscii(_options.lineSeparator());
           printLeadingWhiteSpace();
       }
       _output.appendAscii(closeChar);
    }


    private void writeSidLiteral(int sid)
        throws IOException
    {
        assert sid > 0;

        // No extra handling needed for JSON strings, this is already legal.

        boolean asString = _options._symbol_as_string;
        if (asString) _output.appendAscii('"');

        _output.appendAscii('$');
        _output.printInt(sid);

        if (asString) _output.appendAscii('"');
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
                _output.printJsonString(value);
            }
            else
            {
                _output.printString(value);
            }
        }
        else
        {
            SymbolVariant variant = IonTextUtils.symbolVariant(value);
            switch (variant)
            {
                case IDENTIFIER:
                {
                    _output.appendAscii(value);
                    break;
                }
                case OPERATOR:
                {
                    if (containerIsSexp())
                    {
                        _output.appendAscii(value);
                        break;
                    }
                    // else fall through...
                }
                case QUOTED:
                {
                    _output.printQuotedSymbol(value);
                    break;
                }
            }
        }
    }

    void writeFieldNameToken(SymbolToken sym)
        throws IOException
    {
        String name = sym.getText();
        if (name == null) {
            int sid = sym.getSid();
            writeSidLiteral(sid);
        }
        else {
            writeSymbolToken(name);
        }
    }

    void writeAnnotations(SymbolToken[] annotations)
        throws IOException
    {
        for (SymbolToken ann : annotations) {
            writeAnnotationToken(ann);
            _output.appendAscii("::");
        }
    }

    void writeAnnotationToken(SymbolToken ann)
        throws IOException
    {
        String name = ann.getText();
        if (name == null) {
            _output.appendAscii('$');
            _output.appendAscii(Integer.toString(ann.getSid()));
        }
        else {
            _output.printSymbol(name);
        }
    }

    boolean writeSeparator(boolean followingLongString)
        throws IOException
    {
        if (_options.isPrettyPrintOn()) {
            if (_pending_separator && _separator_character > ' ') {
                // Only bother if the separator is non-whitespace.
                _output.appendAscii((char)_separator_character);
                followingLongString = false;
            }
            _output.appendAscii(_options.lineSeparator());
            printLeadingWhiteSpace();
        }
        else if (_pending_separator) {
            _output.appendAscii((char)_separator_character);
            if (_separator_character > ' ') followingLongString = false;
        }
        return followingLongString;
    }

    @Override
    void startValue() throws IOException
    {
        super.startValue();

        boolean followingLongString = _following_long_string;

        followingLongString = writeSeparator(followingLongString);

        // write field name
        if (_in_struct) {
            SymbolToken sym = assumeFieldNameSymbol();
            writeFieldNameToken(sym);
            _output.appendAscii(':');
            clearFieldName();
            followingLongString = false;
        }

        // write annotations only if they exist and we're not currently
        // writing an IVM
        if (hasAnnotations() && !_is_writing_ivm) {
            if (! _options._skip_annotations) {
                SymbolToken[] annotations = getTypeAnnotationSymbols();
                writeAnnotations(annotations);
                followingLongString = false;
            }
            clearAnnotations();
        }

        _following_long_string = followingLongString;
    }

    void closeValue()
        throws IOException
    {
        super.endValue();
        _pending_separator = true;
        _following_long_string = false;  // Caller overwrites this as needed.

        // Flush if a top-level-value was written
        if (getDepth() == 0)
        {
            try
            {
                flush();
            }
            catch (IOException e)
            {
                throw new IonException(e);
            }
        }
    }

    @Override
    void writeIonVersionMarkerAsIs(SymbolTable systemSymtab)
        throws IOException
    {
        _is_writing_ivm = true;
        writeSymbolAsIs(systemSymtab.getIonVersionId());
        _is_writing_ivm = false;
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

            IonReader reader = new SymbolTableReader(symtab);

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
                if (!_options._sexp_as_list) {
                    tid = tidSexp;
                    _in_struct = false;
                    opener = '('; break;
                }
                // else fall through and act just like list
            case LIST:
                tid = tidList;
                _in_struct = false;
                opener = '[';
                break;
            case STRUCT:
                tid = tidStruct;
                _in_struct = true;
                opener = '{';
                break;
            default:
                throw new IllegalArgumentException();
        }

        push(tid);
        _output.appendAscii(opener);
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
    }


    //========================================================================


    @Override
    public void writeNull()
        throws IOException
    {
        startValue();
        _output.appendAscii("null");
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

        _output.appendAscii(nullimage);
        closeValue();
    }

    public void writeBool(boolean value)
        throws IOException
    {
        startValue();
        _output.appendAscii(value ? "true" : "false");
        closeValue();
    }


    public void writeInt(long value)
        throws IOException
    {
        startValue();
        _output.printInt(value);
        closeValue();
    }

    public void writeInt(BigInteger value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.INT);
            return;
        }

        startValue();
        _output.printInt(value);
        closeValue();
    }

    public void writeFloat(double value)
        throws IOException
    {
        startValue();
        _output.printFloat(value);
        closeValue();
    }


    @Override
    public void writeDecimal(BigDecimal value)
        throws IOException
    {
        if (value == null) {
            writeNull(IonType.DECIMAL);
            return;
        }

        startValue();
        _output.printDecimal(_options, value);
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
            _output.appendAscii(Long.toString(millis));
        }
        else if (_options._timestamp_as_string)
        {
            // Timestamp is ASCII-safe so this is easy
            _output.appendAscii('"');
            _output.appendAscii(value.toString());
            _output.appendAscii('"');
        }
        else
        {
            _output.appendAscii(value.toString());
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
            // TODO amznlabs/ion-java#57 This can lead to mixed newlines in the output.
            // It assumes NL line separators, but _options could use CR+NL
            _output.printLongString(value);

            // This sets _following_long_string = false so we must overwrite
            closeValue();
            _following_long_string = true;
        }
        else
        {
            if (_options._string_as_json)
            {
                _output.printJsonString(value);
            }
            else
            {
                _output.printString(value);
            }
            closeValue();
        }
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

        startValue();
        _output.printBlob(_options, value, start, len);
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
        _output.printClob(_options, value, start, len);
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
            _output.flush();
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

                _output.close();
            }
        }
    }
}

