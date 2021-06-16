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

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnknownSymbolException;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * Base type for Ion writers.  This handles the writeIonEvents and provides default
 * handlers for the list forms of write.  This also resolves symbols if a symbol
 * table is available (which it will not be if the underlying writer is a system
 * writer).
 */
public abstract class _Private_IonWriterBase
    implements IonWriter, _Private_ReaderWriter
{
    protected static final String ERROR_MISSING_FIELD_NAME =
        "IonWriter.setFieldName() must be called before writing a value into a struct.";

    static final String ERROR_FINISH_NOT_AT_TOP_LEVEL =
        "IonWriter.finish() can only be called at top-level.";

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
     * Returns true if the field name has been set either through setFieldName or
     * setFieldId.  This is generally more efficient than calling getFieldName or
     * getFieldId and checking the return type as it does not need to resolve the
     * name through a symbol table.  This returns false if the field name has not
     * been set.
     * @return true if a field name has been set false otherwise
     */
    public abstract boolean isFieldNameSet();


    //========================================================================
    // Annotations

    /**
     * Returns the given annotation's index in the value's annotations list, or -1 if not present.
     * @param name the annotation to find.
     * @return the index or -1.
     */
    abstract int findAnnotation(String name);


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
     * Write symbolId out as an IonSymbol value.  The value must
     * be valid in the symbol table.
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

    final void validateSymbolId(int sid) {
        if (sid > getSymbolTable().getMaxId()) {
            // There is no slot for this symbol ID in the symbol table,
            // so an error would be raised on read. Fail early on write.
            throw new UnknownSymbolException(sid);
        }
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
            validateSymbolId(sid);
            writeSymbol(sid);
        }
    }


    public void writeTimestampUTC(Date value) throws IOException
    {
        Timestamp time = Timestamp.forDateZ(value);
        writeTimestamp(time);
    }



    //
    //  default value and reader implementations.
    //  note that these could be optimized, especially
    //  the reader versions, when the reader is of the
    //  same format as the writer.
    //
    @Deprecated
    public void writeValue(IonValue value) throws IOException
    {
        if (value != null)
        {
            value.writeTo(this);
        }
    }

    public void writeValues(IonReader reader) throws IOException
    {
        if (reader.getDepth() == 0) {
            clear_system_value_stack();
        }

        if (reader.getType() == null) reader.next();

        if (getDepth() == 0 && reader instanceof _Private_ReaderWriter) {
            // Optimize symbol table copying
            _Private_ReaderWriter private_reader =
                (_Private_ReaderWriter)reader;
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

    private final void transfer_symbol_tables(_Private_ReaderWriter reader)
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
        }
    }

    private final void write_value_annotations_helper(IonReader reader)
    {
        SymbolToken[] a = reader.getTypeAnnotationSymbols();
        // At present, we must always call this, even when the list is empty,
        // because local symtab diversion leaves the $ion_symbol_table
        // dangling on the system writer!  TODO fix that, it's broken.
        setTypeAnnotationSymbols(a);
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
        writeValueRecursively(reader);
    }

    /**
     * Writes the provided IonReader's current value including any annotations. This function will not advance the
     * IonReader beyond the end of the current value; users wishing to continue using the IonReader at the current
     * depth will need to call {@link IonReader#next()} again.
     *
     * - If the IonReader is not positioned over a value (for example: because it is at the beginning or end of a
     *   stream), then this function does nothing.
     * - If the current value is a container, this function will visit all of its child values and write those too,
     *   advancing the IonReader to the end of the container in the process.
     * - If both this writer and the IonReader are in a struct, the writer will write the current value's field name.
     * - If the writer is not in a struct but the reader is, the writer will ignore the current value's field name.
     * - If the writer is in a struct but the IonReader is not, this function throws an IllegalStateException.
     *
     * @param reader       The IonReader that will provide a value to write.
     * @throws IOException if either the provided IonReader or this writer's underlying OutputStream throw an
     *                     IOException.
     * @throws IllegalStateException if this writer is inside a struct but the IonReader is not.
     */
    final void writeValueRecursively(IonReader reader) throws IOException
    {
        // The IonReader does not need to be at the top level (getDepth()==0) when the function is called.
        // We take note of its initial depth so we can avoid advancing the IonReader beyond the starting value.
        int startingDepth = getDepth();

        // The IonReader will be at `startingDepth` when the function is first called and then again when we
        // have finished traversing all of its children. This boolean tracks which of those two states we are
        // in when `getDepth() == startingDepth`.
        boolean alreadyProcessedTheStartingValue = false;

        // The IonType of the IonReader's current value.
        IonType type;

        while (true) {
            // Each time we reach the top of the loop we are in one of three states:
            // 1. We have not yet begun processing the starting value.
            // 2. We are currently traversing the starting value's children.
            // 3. We have finished processing the starting value.
            if (getDepth() == startingDepth) {
                // The IonReader is at the starting depth. We're either beginning our traversal or finishing it.
                if (alreadyProcessedTheStartingValue) {
                    // We're finishing our traversal.
                    break;
                }
                // We're beginning our traversal. Don't advance the cursor; instead, use the current
                // value's IonType.
                type = reader.getType();
                // We've begun processing the starting value.
                alreadyProcessedTheStartingValue = true;
            } else {
                // We're traversing the starting value's children (that is: values at greater depths). We need to
                // advance the cursor by calling next().
                type = reader.next();
            }

            if (type == null) {
                // There are no more values at this level. If we're at the starting level, we're done.
                if (getDepth() == startingDepth) {
                    break;
                }
                // Otherwise, step out once and then try to move forward again.
                reader.stepOut();
                stepOut();
                continue;
            }

            // We found a value. Write out its field name and annotations, if any.
            write_value_field_name_helper(reader);
            write_value_annotations_helper(reader);

            if (reader.isNullValue()) {
                this.writeNull(type);
                continue;
            }

            switch (type) {
                case NULL:
                    // The isNullValue() check above will handle this.
                    throw new IllegalStateException("isNullValue() was false but IonType was NULL.");
                case BOOL:
                    writeBool(reader.booleanValue());
                    break;
                case INT:
                    writeInt(reader.bigIntegerValue());
                    break;
                case FLOAT:
                    writeFloat(reader.doubleValue());
                    break;
                case DECIMAL:
                    writeDecimal(reader.decimalValue());
                    break;
                case TIMESTAMP:
                    writeTimestamp(reader.timestampValue());
                    break;
                case STRING:
                    writeString(reader.stringValue());
                    break;
                case SYMBOL:
                    writeSymbolToken(reader.symbolValue());
                    break;
                case BLOB:
                    writeBlob(reader.newBytes());
                    break;
                case CLOB:
                    writeClob(reader.newBytes());
                    break;
                case STRUCT: // Intentional fallthrough
                case LIST:   // Intentional fallthrough
                case SEXP:
                    reader.stepIn();
                    stepIn(type);
                    break;
                default:
                    throw new IllegalStateException("Unknown value type: " + type);
            }
        }
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

    public <T> T asFacet(Class<T> facetType)
    {
        // This implementation has no facets.
        return null;
    }
}
