// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
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

    private final boolean requireSymbolValidation;

    /**
     * @param requireSymbolValidation true if SID validation should be performed; otherwise, false.
     *                                See {@link _Private_IonTextWriterBuilder#withInvalidSidsAllowed(boolean)}
     */
    public _Private_IonWriterBase(boolean requireSymbolValidation) {
        this.requireSymbolValidation = requireSymbolValidation;
    }

    /**
     * Returns the current depth of containers the writer is at.  This is
     * 0 if the writer is at top-level.
     * @return int depth of container nesting
     */
    public abstract int getDepth();


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
        if (requireSymbolValidation && sid > getSymbolTable().getMaxId()) {
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
        DefaultReaderToWriterTransfer.writeValue(reader, this);
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
