// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonImplUtils.EMPTY_STRING_ARRAY;
import static com.amazon.ion.impl.UnifiedSymbolTable.isNonSystemSharedTable;
import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

/**
 *  Base type for Ion writers.  This handles the writeIonEvents and provides default
 *  handlers for the list forms of write.  This also resolves symbols if a symbol
 *  table is available (which it will not be if the underlying writer is a system
 *  writer).
 */
public abstract class IonWriterBaseImpl
    implements IonWriter, IonReaderWriterPrivate

{
    protected static final String ERROR_MISSING_FIELD_NAME =
        "IonWriter.setFieldName() must be called before writing a value into a struct.";

    private static final boolean _debug_on = false;

    /**
     * The system symtab used when resetting the stream.
     * Must not be null.
     */
    protected final SymbolTable _default_system_symbol_table;

    /**
     * Must be either local or system table.  It can be null
     * if the concrete writer is a system writer.  This
     * value may only be changed when the writer is at the top
     * (i.e. the datagram level).
     */
    protected SymbolTable _symbol_table;



    /**
     *
     * @param defaultSystemSymbolTable must not be null.
     *
     * @throws NullPointerException if the parameter is null.
     */
    protected IonWriterBaseImpl(SymbolTable defaultSystemSymbolTable)
    {
        defaultSystemSymbolTable.getClass(); // Efficient null check
        _default_system_symbol_table = defaultSystemSymbolTable;
    }

    /**
     * Returns the current depth of containers the writer is at.  This is
     * 0 if the writer is at the datagram level.
     * @return int depth of container nesting
     */
    protected abstract int getDepth();

    /**
     * Must only be called at top-level!
     */
    protected void writeAllBufferedData() throws IOException
    {
    }

    /**
     * Called by finish after flushing all the known data, to prepare for the
     * case where the user sends some more.
     * <p>
     * This implementation just sets the {@link #_symbol_table} to the
     * {@link #_default_system_symbol_table}.
     */
    void finishSystemContext()
    {
        _symbol_table = _default_system_symbol_table;
    }

    public final void finish() throws IOException
    {
        if (getDepth() != 0) {
            String message =
                "IonWriter.finish() can only be called at top-level.";
            throw new IllegalStateException(message);
        }

        writeAllBufferedData();
        flush();
        finishSystemContext();
    }


    //
    // symbol table support methods.  These handle the
    // symbol table state and are not generally overridden
    // except to return an UnsupportedOperationException
    // when they are not supported by a system writer.
    //

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
     * <p>
     * This implmentation simply validates that the argument is not a
     * shared symbol table, and assigns it to {@link #_symbol_table}.
     *
     * @param symbols base symbol table for encoding. Must not be null.
     * @throws IllegalArgumentException if symbols is null or a shared symbol table
     */
    public void setSymbolTable(SymbolTable symbols)
        throws IOException
    {
        if (symbols == null || isNonSystemSharedTable(symbols)) {
            throw new IllegalArgumentException("symbol table must be local or system to be set, or reset");
        }
        if (getDepth() > 0) {
            throw new IllegalStateException("the symbol table cannot be set, or reset, while a container is open");
        }
        _symbol_table = symbols;
    }

    public SymbolTable getSymbolTable()
    {
        return _symbol_table;
    }

    /**
     * Builds a new local symbol table from the current contextual symtab
     * (a system symtab).
     * @return not null.
     */
    UnifiedSymbolTable inject_local_symbol_table() throws IOException
    {
        assert _symbol_table.isSystemTable();
        // no catalog since it doesn't matter as this is a
        // pure local table, with no imports
        return makeNewLocalSymbolTable(null /*system*/, _symbol_table);
    }

    protected int add_symbol(String name) throws IOException
    {
        if (_symbol_table == null) {
            _symbol_table = _default_system_symbol_table;
        }

        int sid = _symbol_table.findSymbol(name);
        if (sid > 0) return sid;

        if (_symbol_table.isSystemTable()) {
            _symbol_table = inject_local_symbol_table();
        }
        assert _symbol_table.isLocalTable();

        sid = _symbol_table.addSymbol(name);
        return sid;
    }

    String find_symbol(int sid)
    {
        SymbolTable symbol_table = _symbol_table;
        if (symbol_table == null) {
            symbol_table = _default_system_symbol_table;
        }
        String name = symbol_table.findSymbol(sid);
        assert(name != null && name.length() > 0);
        return name;
    }


    //========================================================================
    // Writing field names

    //
    // field name support.  This handles converting
    // string to int (or the reverse) using the current
    // symbol table, if that is needed.  These routines
    // are not generally overridden except to return
    // an UnsupportedOperationException when they are
    // not supported by a system writer.
    //


    /**
     * Returns true if the field name has been set either through setFieldName or
     * setFieldId.  This is generally more efficient than calling getFieldName or
     * getFieldId and checking the return type as it does not need to resolve the
     * name through a symbol table.  This returns false if the field name has not
     * been set.
     * @return true if a field name has been set false otherwise
     */
    abstract boolean isFieldNameSet();


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

    public void writeTimestamp(Date value, Integer localOffset)
        throws IOException
    {
        Timestamp time;
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
        }
        else {
            time = new Timestamp(value.getTime(), localOffset);
            writeTimestamp(time);
        }
    }

    public void writeTimestampUTC(Date value) throws IOException
    {
        Timestamp time = Timestamp.forDateZ(value);
        writeTimestamp(time);
    }

    //
    //  default list implementations which just call the
    //  underlying (normal) write operations
    //
    public void writeBoolList(boolean[] values)throws IOException
    {
        stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writeBool(values[ii]);
        }
        stepOut();
    }
    public void writeFloatList(float[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writeFloat(values[ii]);
        }
        stepOut();
    }
    public void writeFloatList(double[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writeFloat(values[ii]);
        }
        stepOut();
    }
    public void writeIntList(byte[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        stepOut();
    }
    public void writeIntList(short[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        stepOut();
    }
    public void writeIntList(int[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        stepOut();
    }
    public void writeIntList(long[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        stepOut();
    }
    public void writeStringList(String[] values) throws IOException
    {
        stepIn(IonType.LIST);
        for (int ii=0; ii<values.length; ii++) {
            writeString(values[ii]);
        }
        stepOut();
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

        if (reader instanceof IonReaderWriterPrivate) {
            IonReaderWriterPrivate private_reader = (IonReaderWriterPrivate)reader;
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

    private final void transfer_symbol_tables(IonReaderWriterPrivate private_reader) throws IOException
    {
        SymbolTable reader_symbols = null;
        reader_symbols = private_reader.pop_passed_symbol_table();
        if (reader_symbols != null) {
            clear_system_value_stack();
            setSymbolTable(reader_symbols);
            while (reader_symbols != null) {
                push_symbol_table(reader_symbols);
                reader_symbols = private_reader.pop_passed_symbol_table();
            }
        }
    }

    private final void write_value_field_name_helper(IonReader reader)
    {
        if (this.isInStruct() && !isFieldNameSet())
        {
            String field_name = reader.getFieldName();
            if (field_name == null) {
                int field_sid = reader.getFieldId();
                if (field_sid > 0) {
                    field_name = find_symbol(field_sid);
                }
            }
            if (field_name == null) {
                throw new IllegalStateException("a field name is required for members of a struct");
            }
            setFieldName(field_name);
            if (_debug_on) System.out.print(":");
        }
    }

    private final void write_value_annotations_helper(IonReader reader)
    {
        String [] a = reader.getTypeAnnotations();
        // FIXME ION-172 this is broken
        // First, array `a` will never be null per spec of getTypeAnnotations.
        // Second, we need to check each individual annotation, not the list
        // as a whole.
        if (a == null) {
            int[] a_sids = reader.getTypeAnnotationIds();
            if (a_sids.length > 1) {
                a = new String[a_sids.length];
                for (int ii=0; ii<a.length; ii++) {
                    a[ii] = find_symbol(a_sids[ii]);
                }
            }
            else {
                a = EMPTY_STRING_ARRAY;
            }
        }
        setTypeAnnotations(a);
        if (_debug_on) System.out.print(";");
    }

    /**
     * Overrides can optimize special cases.
     */
    public void writeValue(IonReader reader) throws IOException
    {
        writeValueSlowly(reader.getType(), reader);
    }

    /**
     * Unoptimized copy. This must not recurse back to the public
     * {@link #writeValue(IonReader)} method since that will cause the
     * optimization test to happen repeatedly.
     */
    protected final void writeValueSlowly(IonType type, IonReader reader)
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
                String name = reader.stringValue();
                if (name == null) {
                    int sid = reader.getSymbolId();
                    name = find_symbol(sid);
                }
                writeSymbol(name);
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
                writeContainerSlowly(IonType.STRUCT, reader);
                if (_debug_on) System.out.print("}");
                break;
            case LIST:
                if (_debug_on) System.out.print("[");
                writeContainerSlowly(IonType.LIST, reader);
                if (_debug_on) System.out.print("]");
                break;
            case SEXP:
                if (_debug_on) System.out.print("(");
                writeContainerSlowly(IonType.SEXP, reader);
                if (_debug_on) System.out.print(")");
                break;
            default:
                throw new IllegalStateException();
            }
        }
    }

    protected final void writeContainerSlowly(IonType type, IonReader reader)
        throws IOException
    {
        stepIn(type);
        reader.stepIn();
        while ((type = reader.next()) != null)
        {
            writeValueSlowly(type, reader);
        }
        reader.stepOut();
        stepOut();
    }


    //
    //  This code handles the skipped symbol table
    //  support - it is cloned in IonReaderTextUserX,
    //  IonReaderBinaryUserX and IonWriterBaseImpl
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
    public SymbolTable pop_passed_symbol_table()
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
