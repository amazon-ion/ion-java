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

import java.io.IOException;
import java.math.BigDecimal;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnknownSymbolException;

/**
 * Base type for Ion writers.  This handles the writeIonEvents and provides default
 * handlers for the list forms of write.  This also resolves symbols if a symbol
 * table is available (which it will not be if the underlying writer is a system
 * writer).
 *
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public abstract class PrivateIonWriterBase
    implements IonWriter, PrivateReaderWriter
{
    protected static final String ERROR_MISSING_FIELD_NAME =
        "IonWriter.setFieldName() must be called before writing a value into a struct.";

    static final String ERROR_FINISH_NOT_AT_TOP_LEVEL =
        "IonWriter.finish() can only be called at top-level.";

    private static final boolean _debug_on = false;



    /**
     * Returns the current depth of containers the writer is at.  This is
     * 0 if the writer is at top-level.
     * @return int depth of container nesting
     */
    protected abstract int getDepth();


    //========================================================================
    // Context management


    /**
     * Write an Ion version marker symbol to the output.  This
     * is the $ion_1_0 value currently (in later versions the
     * number may change).  In text output this appears as the
     * text symbol.  In binary this will be the symbol id if
     * the writer is in a list, sexp or struct.  If the writer
     * is currently at the top level this will write the
     * "magic cookie" value.
     *
     *  Writing a version marker will reset the symbol table
     *  to be the system symbol table.
     */
    abstract void writeIonVersionMarker() throws IOException;

    /**
     * Sets the symbol table to use for encoding to be the passed
     * in symbol table.  The can only be done between top-level values.
     * As symbols are written
     * this symbol table is used to resolve them.  If the symbols
     * are undefined this symbol table is updated to include them
     * as local symbols.  The updated symbol table will be
     * written before any of the local values are emitted.
     * <p>
     * If the symbol table is the system symbol table an Ion
     * version marker will be written to the output.  If symbols
     * not in the system symbol table are written a local
     * symbol table will be created and written before the
     * current top level value.
     *
     * @param symbols base symbol table for encoding. Must not be null.
     * @throws IllegalArgumentException if symbols is null or a shared symbol
     * table, or if this writer isn't at top level.
     */
    public abstract void setSymbolTable(SymbolTable symbols)
        throws IOException;


    /**
     * Translates a symbol using the current symtab.
     *
     * @return not null.
     *
     * @throws UnknownSymbolException if the text is unknown.
     *
     * @see SymbolTable#findKnownSymbol(int)
     */
    abstract String assumeKnownSymbol(int sid);


    //========================================================================
    // Field names


    /**
     * Returns true if the field name has been set either through setFieldName.
     * This is generally more efficient than calling getFieldName and
     * checking the return type as it does not need to resolve the
     * name through a symbol table.  This returns false if the field name has not
     * been set.
     * @return true if a field name has been set false otherwise
     */
    public abstract boolean isFieldNameSet();


    //========================================================================
    // Annotations


    abstract boolean has_annotation(String name, int id);


    /**
     * Gets the current list of pending annotations.
     * This is the contents of the current {@code annotations} array
     * of this writer.
     * <p>
     * If the annotations were set as IDs they
     * will be converted if a symbol table is available.  In the event
     * a symbol table is not available a null array will be returned.
     * If no annotations are set a 0 length array will be returned.
     * <p>
     * @return pending type annotations as strings, null if the
     * annotations cannot be expressed as strings.
     */
    abstract String[] getTypeAnnotations();


    /**
     * Gets the current list of pending annotations.
     * This is the contents of the current {@code annotations} array
     * of this writer.
     * <p>
     * If the annotations were set as string they
     * will be converted to symbol IDs (ints) if a symbol table is
     * available.  In the event a symbol table is not available a
     * null array will be returned.
     * If no annotations are set a 0 length array will be returned.
     * <p>
     * @return pending type annotations as symbol ID ints, null if the
     * annotations cannot be expressed as IDs.
     */
    abstract int[] getTypeAnnotationIds();

    /**
     * Write symbolId out as an IonSymbol value.  The value does not
     * have to be valid in the symbol table, unless the output is
     * text, in which case it does.
     *
     * @param symbolId symbol table id to write
     */
    abstract void writeSymbol(int symbolId) throws IOException;


    //========================================================================


    //
    // default overload implementations.  These generally
    // convert the users value to a value of the intrinsic
    // underlying type and then write that type using
    // the concrete writers method.
    //
    public void writeBlob(byte[] value) throws IOException
    {
        if (value == null) {
            this.writeNull(IonType.BLOB);
        }
        else {
            this.writeBlob(value, 0, value.length);
        }
        return;
    }
    public void writeClob(byte[] value) throws IOException
    {
        if (value == null) {
            this.writeNull(IonType.CLOB);
        }
        else {
            this.writeClob(value, 0, value.length);
        }
        return;
    }

    abstract public void writeDecimal(BigDecimal value) throws IOException;


    public void writeFloat(float value) throws IOException
    {
        writeFloat((double)value);
    }

    public void writeNull() throws IOException
    {
        writeNull(IonType.NULL);
    }


    public final void writeSymbolToken(SymbolToken tok)
        throws IOException
    {
        if (tok == null) {
            writeNull(IonType.SYMBOL);
            return;
        }

        String text = tok.getText();
        if (text != null)
        {
            writeSymbol(text);
        }
        else
        {
            int sid = tok.getSid();
            writeSymbol(sid);
        }
    }


    //
    //  default value and reader implementations.
    //  note that these could be optimized, especially
    //  the reader versions, when the reader is of the
    //  same format as the writer.
    //
    public void writeValues(IonReader reader) throws IOException
    {
        if (reader.getDepth() == 0) {
            clear_system_value_stack();
        }

        if (reader.getType() == null) reader.next();

        if (getDepth() == 0 && reader instanceof PrivateReaderWriter) {
            // Optimize symbol table copying
            PrivateReaderWriter private_reader =
                (PrivateReaderWriter)reader;
            while (reader.getType() != null) {
                transfer_symbol_tables(private_reader);
                writeValue(reader);
                reader.next();
            }
        }
        else {
            while (reader.getType() != null) {
                writeValue(reader);
                reader.next();
            }
        }
    }

    private final void transfer_symbol_tables(PrivateReaderWriter reader)
        throws IOException
    {
        SymbolTable reader_symbols = reader.pop_passed_symbol_table();
        if (reader_symbols != null) {
            clear_system_value_stack();
            setSymbolTable(reader_symbols);
            while (reader_symbols != null) {
                // TODO these symtabs are never popped!
                // Why bother pushing them?
                push_symbol_table(reader_symbols);
                reader_symbols = reader.pop_passed_symbol_table();
            }
        }
    }

    /**
     * @throws UnknownSymbolException if the text of the field name is
     *  unknown.
     */
    private final void write_value_field_name_helper(IonReader reader)
    {
        if (this.isInStruct() && !isFieldNameSet())
        {
            SymbolToken tok = reader.getFieldNameSymbol();
            if (tok == null)
            {
                throw new IllegalStateException("Field name not set");
            }

            setFieldNameSymbol(tok);
            if (_debug_on) System.out.print(":");
        }
    }

    private final void write_value_annotations_helper(IonReader reader)
    {
        SymbolToken[] a = reader.getTypeAnnotationSymbols();
        // At present, we must always call this, even when the list is empty,
        // because local symtab diversion leaves the $ion_symbol_table
        // dangling on the system writer!  TODO fix that, it's broken.
        setTypeAnnotationSymbols(a);
        if (_debug_on) System.out.print(";");
    }


    public boolean isStreamCopyOptimized()
    {
        return false;
    }

    /**
     * Overrides can optimize special cases.
     */
    public void writeValue(IonReader reader) throws IOException
    {
        // TODO this should do symtab optimization as per writeValues()
        IonType type = reader.getType();
        writeValueRecursively(type, reader);
    }

    /**
     * Unoptimized copy. This must not recurse back to the public
     * {@link #writeValue(IonReader)} method since that will cause the
     * optimization test to happen repeatedly.
     */
    final void writeValueRecursively(IonType type, IonReader reader)
        throws IOException
    {
        write_value_field_name_helper(reader);
        write_value_annotations_helper(reader);

        if (reader.isNullValue()) {
            this.writeNull(type);
        }
        else {
            switch (type) {
            case NULL:
                writeNull();
                if (_debug_on) System.out.print("-");
                break;
            case BOOL:
                writeBool(reader.booleanValue());
                if (_debug_on) System.out.print("b");
                break;
            case INT:
                writeInt(reader.bigIntegerValue());
                if (_debug_on) System.out.print("i");
                break;
            case FLOAT:
                writeFloat(reader.doubleValue());
                if (_debug_on) System.out.print("f");
                break;
            case DECIMAL:
                writeDecimal(reader.decimalValue());
                if (_debug_on) System.out.print("d");
                break;
            case TIMESTAMP:
                writeTimestamp(reader.timestampValue());
                if (_debug_on) System.out.print("t");
                break;
            case STRING:
                writeString(reader.stringValue());
                if (_debug_on) System.out.print("$");
                break;
            case SYMBOL:
                writeSymbolToken(reader.symbolValue());
                if (_debug_on) System.out.print("y");
                break;
            case BLOB:
                writeBlob(reader.newBytes());
                if (_debug_on) System.out.print("B");
                break;
            case CLOB:
                writeClob(reader.newBytes());
                if (_debug_on) System.out.print("L");
                break;
            case STRUCT:
                if (_debug_on) System.out.print("{");
                writeContainerRecursively(IonType.STRUCT, reader);
                if (_debug_on) System.out.print("}");
                break;
            case LIST:
                if (_debug_on) System.out.print("[");
                writeContainerRecursively(IonType.LIST, reader);
                if (_debug_on) System.out.print("]");
                break;
            case SEXP:
                if (_debug_on) System.out.print("(");
                writeContainerRecursively(IonType.SEXP, reader);
                if (_debug_on) System.out.print(")");
                break;
            default:
                throw new IllegalStateException();
            }
        }
    }

    private void writeContainerRecursively(IonType type, IonReader reader)
        throws IOException
    {
        stepIn(type);
        reader.stepIn();
        while ((type = reader.next()) != null)
        {
            writeValueRecursively(type, reader);
        }
        reader.stepOut();
        stepOut();
    }


    //
    //  This code handles the skipped symbol table
    //  support - it is cloned in IonReaderTextUserX,
    //  IonReaderBinaryUserX and _Private_IonWriterBase
    //
    //  SO ANY FIXES HERE WILL BE NEEDED IN THOSE
    //  THREE LOCATIONS AS WELL.
    //
    private int _symbol_table_top = 0;
    private SymbolTable[] _symbol_table_stack = new SymbolTable[3]; // 3 is rare, IVM followed by a local sym tab with open content
    private void clear_system_value_stack()
    {
        while (_symbol_table_top > 0) {
            _symbol_table_top--;
            _symbol_table_stack[_symbol_table_top] = null;
        }
    }
    private void push_symbol_table(SymbolTable symbols)
    {
        assert(symbols != null);
        if (_symbol_table_top >= _symbol_table_stack.length) {
            int new_len = _symbol_table_stack.length * 2;
            SymbolTable[] temp = new SymbolTable[new_len];
            System.arraycopy(_symbol_table_stack, 0, temp, 0, _symbol_table_stack.length);
            _symbol_table_stack = temp;
        }
        _symbol_table_stack[_symbol_table_top++] = symbols;
    }
    public final SymbolTable pop_passed_symbol_table()
    {
        if (_symbol_table_top <= 0) {
            return null;
        }
        _symbol_table_top--;
        SymbolTable symbols = _symbol_table_stack[_symbol_table_top];
        _symbol_table_stack[_symbol_table_top] = null;
        return symbols;
    }
}
