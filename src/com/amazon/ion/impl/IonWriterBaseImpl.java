// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonImplUtils.EMPTY_INT_ARRAY;
import static com.amazon.ion.impl.IonImplUtils.EMPTY_STRING_ARRAY;

import com.amazon.ion.Decimal;
import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonNumber;
import com.amazon.ion.IonNumber.Classification;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
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

    /** Really only needed to create local symtabs. Not null. */
    protected final IonSystem _system;

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

    protected IonType     _field_name_type;     // really ion type is only used for int, string or null (unknown)
    protected String      _field_name;
    protected int         _field_name_sid;

    private static final int DEFAULT_ANNOTATION_COUNT = 4;
    protected IonType     _annotations_type;     // really ion type is only used for int, string or null (unknown)
    protected int         _annotation_count;
    protected String[]    _annotations = new String[DEFAULT_ANNOTATION_COUNT];
    protected int[]       _annotation_sids = new int[DEFAULT_ANNOTATION_COUNT];


    /**
     *
     * @param defaultSystemSymbolTable must not be null.
     *
     * @throws NullPointerException if the parameter is null.
     */
    protected IonWriterBaseImpl(IonSystem system,
                                SymbolTable defaultSystemSymbolTable)
    {
        system.getClass();
        defaultSystemSymbolTable.getClass(); // Efficient null check
        _system = system;
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
     * FIXME this should never write anything and should never throw!
     */
    protected abstract void resetSystemContext() throws IOException;

    public final void finish() throws IOException
    {
        if (getDepth() != 0) {
            String message =
                "IonWriter.finish() can only be called at top-level.";
            throw new IllegalStateException(message);
        }

        writeAllBufferedData();
        flush();
        resetSystemContext();
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
     * in symbol table.  The can only be done outside an Ion value,
     * that is at the datagram level.  As symbols are written
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
     * @param symbols base symbol table for encoding
     * @throws IllegalArgumentException if symbols is null or a shared symbol table
     */
    public void setSymbolTable(SymbolTable symbols)
        throws IOException
    {
        if (UnifiedSymbolTable.isAssignableTable(symbols) == false) {
            throw new IllegalArgumentException("symbol table must be local or system to be set, or reset");
        }
        if (getDepth() > 0) {
            throw new IllegalStateException("the symbol table cannot be set, or reset, while a container is open");
        }
        _symbol_table = symbols;
    }

    // note that system writer will overload this and return a null.
    public SymbolTable getSymbolTable()
    {
        return _symbol_table;
    }

    abstract UnifiedSymbolTable inject_local_symbol_table() throws IOException;
/*    {
        // no catalog since it doesn't matter as this is a
        // pure local table, with no imports
        UnifiedSymbolTable symbols
            = UnifiedSymbolTable.makeNewLocalSymbolTable(_symbol_table);
        setSymbolTable(symbols);
        return symbols;
    }
*/
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

    private String find_symbol(int sid)
    {
        SymbolTable symbol_table = _symbol_table;
        if (symbol_table == null) {
            symbol_table = _default_system_symbol_table;
        }
        String name = symbol_table.findSymbol(sid);
        assert(name != null && name.length() > 0);
        return name;
    }

    //
    // field name support.  This handles converting
    // string to int (or the reverse) using the current
    // symbol table, if that is needed.  These routines
    // are not generally overridden except to return
    // an UnsupportedOperationException when they are
    // not supported by a system writer.
    //
    public void setFieldName(String name)
    {
        if (!this.isInStruct()) {
            throw new IllegalStateException();
        }
        if (name.length() == 0) {
            throw new EmptySymbolException();
        }
        _field_name_type = IonType.STRING;
        _field_name = name;
    }
    public void setFieldId(int id)
    {
        if (!this.isInStruct()) {
            throw new IllegalStateException();
        }
        _field_name_type = IonType.INT;
        _field_name_sid = id;
    }

    /**
     * Returns the symbol id of the current field name, if the field name
     * has been set.  If the name has not been set, either as either a String
     * or a symbol id value, this returns -1 (undefined symbol).
     * @return symbol id of the name of the field about to be written or -1 if
     * it is not set
     */
    public int getFieldId()
    {
        int id;

        if (_field_name_type == null) {
            throw new IllegalStateException("the field has not be set");
        }
        switch (_field_name_type) {
        case STRING:
                try {
                    id = add_symbol(_field_name);
                }
                catch (IOException e) {
                    throw new IonException(e);
                }
            break;
        case INT:
            id = _field_name_sid;
            break;
        default:
            throw new IllegalStateException("the field has not be set");
        }

        return id;
    }

    /**
     * This returns the field name of the value about to be written
     * if the field name has been set.  If the field name has not been
     * defined this will return null.
     *
     * @return String name of the field about to be written or null if it is
     * not yet set.
     */
    public String getFieldName()
    {
        String name;

        if (_field_name_type == null) {
            throw new IllegalStateException("the field has not be set");
        }
        switch (_field_name_type) {
        case STRING:
            name = _field_name;
            break;
        case INT:
            name = this.find_symbol(_field_name_sid);
            break;
        default:
            throw new IllegalStateException("the field has not be set");
        }

        return name;
    }

    /**
     * Returns true if the field name has been set either through setFieldName or
     * setFieldId.  This is generally more efficient than calling getFieldName or
     * getFieldId and checking the return type as it does not need to resolve the
     * name through a symbol table.  This returns false if the field name has not
     * been set.
     * @return true if a field name has been set false otherwise
     */
    public boolean isFieldNameSet()
    {
        if (_field_name_type != null) {
            switch (_field_name_type) {
            case STRING:
                return _field_name != null && _field_name.length() > 0;
            case INT:
                return _field_name_sid > 0;
            default:
                break;
            }
        }
        return false;
    }

    protected void clearFieldName()
    {
        _field_name_type = null;
        _field_name = null;
        _field_name_sid = UnifiedSymbolTable.UNKNOWN_SYMBOL_ID;
    }


    //
    // user type annotation support.  This impl is generally used
    // and handles symbol processing and array managment. Note that
    // the underlying add_symbol and find_symbol will throw if
    // there is not symbol table.
    //
    public void setTypeAnnotations(String[] annotations)
    {
        _annotations_type = IonType.STRING;
        if (annotations == null) {
            annotations = IonImplUtils.EMPTY_STRING_ARRAY;
        }
        else if (annotations.length > _annotation_count) {
            ensureAnnotationCapacity(annotations.length);
        }
        System.arraycopy(annotations, 0, _annotations, 0, annotations.length);
        _annotation_count = annotations.length;
        assert(no_illegal_annotations() == true);
    }
    private final boolean no_illegal_annotations()
    {
        for (int ii=0; ii<_annotation_count; ii++) {
            String a = _annotations[ii];
            if (a == null || a.length() < 1) {
                return false;
            }
        }
        return true;
    }
    public void setTypeAnnotationIds(int[] annotationIds)
    {
        _annotations_type = IonType.INT;
        if (annotationIds == null) {
            annotationIds = IonImplUtils.EMPTY_INT_ARRAY;
        }
        else if (annotationIds.length > _annotation_count) {
            ensureAnnotationCapacity(annotationIds.length);
        }
        System.arraycopy(annotationIds, 0, _annotation_sids, 0, annotationIds.length);
        _annotation_count = annotationIds.length;
    }
    public void addTypeAnnotation(String annotation)
    {
        ensureAnnotationCapacity(_annotation_count + 1);
        if (_annotations_type == IonType.INT) {
            int sid;
            try {
                sid = add_symbol(annotation);
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            addTypeAnnotationId(sid);
        }
        else {
            _annotations_type = IonType.STRING;
            // FIXME: annotations need to be "washed" through a symbol
            //        table to address issues like $0234 -> $234 or 'xyzzx'
            _annotations[_annotation_count++] = annotation;
        }
    }
    public void addTypeAnnotationId(int annotationId)
    {
        ensureAnnotationCapacity(_annotation_count + 1);
        if (_annotations_type == IonType.STRING) {
            SymbolTable symtab = getSymbolTable();
            String annotation = symtab.findSymbol(annotationId);
            addTypeAnnotation(annotation);
        }
        else {
            _annotations_type = IonType.INT;
            _annotation_sids[_annotation_count++] = annotationId;
        }
    }

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
    public String[] getTypeAnnotations()
    {
        if (_annotation_count < 1) {
            // no annotations, just give them the empty array
            return EMPTY_STRING_ARRAY;
        }
        if (IonType.INT.equals(_annotations_type) && getSymbolTable() == null) {
            // the native form of the annotations are ints
            // but there's no symbol table to convert them
            // we're done - no data for the caller
            return null;
        }

        // go get the string (original or converted from ints)
        String[] user_copy = new String[_annotation_count];
        String[] annotations = get_type_annotations_as_strings();
        System.arraycopy(annotations, 0, user_copy, 0, _annotation_count);

        return user_copy;
    }

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
    public int[] getTypeAnnotationIds()
    {
        if (_annotation_count < 1) {
            // no annotations, just give them the empty array
            return EMPTY_INT_ARRAY;
        }
        if (IonType.STRING.equals(_annotations_type) && getSymbolTable() == null) {
            // the native form of the annotations are strings
            // but there's no symbol table to convert them
            // we're done - no data for the caller
            return null;
        }

        // get the user the ids, either native or converted
        // throught the current symbol table
        int[] user_copy = new int[_annotation_count];
        int[] annotations = get_type_annotations_as_ints();
        System.arraycopy(annotations, 0, user_copy, 0, _annotation_count);

        return user_copy;
    }


    /**
     * Ensures that our {@link #_annotations} and {@link #_annotation_sids}
     * arrays have enough capacity to hold the given number of annotations.
     * Does not increase {@link #_annotation_count}.
     */
    private void ensureAnnotationCapacity(int length) {
        int oldlen = (_annotations == null) ? 0 : _annotations.length;
        if (length < oldlen) return;

        int newlen = (_annotations == null) ? 10 : (_annotations.length * 2);
        if (length > newlen) {
            newlen = length;
        }

        String[] temp1 = new String[newlen];
        int[]    temp2 = new int[newlen];

        if (oldlen > 0) {
            if (_annotations_type == IonType.STRING) {
                System.arraycopy(_annotations, 0, temp1, 0, oldlen);
            }
            if (_annotations_type == IonType.INT) {
                System.arraycopy(_annotation_sids, 0, temp2, 0, oldlen);
            }
        }
        _annotations = temp1;
        _annotation_sids = temp2;
    }
    public void clearAnnotations()
    {
        _annotation_count = 0;
        _annotations_type = IonType.NULL;
    }
    protected boolean has_annotation(String name, int id)
    {
        if (this._symbol_table != null) {
            assert(this._symbol_table.findKnownSymbol(id).equals(name));
        }
        if (_annotation_count < 1) {
            return false;
        }
        else if (_annotations_type == IonType.INT) {
            for (int ii=0; ii<_annotation_count; ii++) {
                if (_annotation_sids[ii] == id) {
                    return true;
                }
            }
        }
        else if (_annotations_type == IonType.STRING) {
            for (int ii=0; ii<_annotation_count; ii++) {
                // TODO: currently this method is only called internally for
                //       system symbols.  If this is to be expanded to user
                //       symbols (or our system symbols get more complex)
                //       these names will have to be "washed" to handle
                //       escape characters and $15 style names
                if (name.equals(_annotations[ii])) {
                    return true;
                }
            }
        }
        else {
            assert("if there are annotation they have to be either string or int".length() < 0);
        }
        return false;
    }

    private String[] get_type_annotations_as_strings()
    {
        if (_annotation_count < 1) {
            return EMPTY_STRING_ARRAY;
        }
        else if (_annotations_type == IonType.INT) {
            for (int ii=0; ii<_annotation_count; ii++) {
                String name;
                int id = _annotation_sids[ii];
                name = this.find_symbol(id);
                _annotations[ii] = name;
            }
        }
        return _annotations;
    }

    protected int[] get_type_annotations_as_ints()
    {
        if (_annotation_count < 1) {
            return EMPTY_INT_ARRAY;
        }
        else if (_annotations_type == IonType.STRING) {
            for (int ii=0; ii<_annotation_count; ii++) {
                String name = _annotations[ii];
                try {
                    _annotation_sids[ii] = add_symbol(name);
                }
                catch (IOException e) {
                    throw new IonException(e);
                }
            }
        }
        return _annotation_sids;
    }


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

    public void writeDecimal(BigDecimal value, IonNumber.Classification classification)
        throws IOException
    {
        if (value == null)
        {
            writeNull(IonType.DECIMAL);
            return;
        }
        if (Classification.NEGATIVE_ZERO.equals(classification)) {
            if (!BigDecimal.ZERO.equals(value)) {
                throw new IllegalArgumentException("the value must be zero if the classification is negative zero");
            }
            if (Decimal.isNegativeZero(value)) {
                value = Decimal.negativeZero(value.scale());
            }
        }
        writeDecimal(value);
    }

    public void writeDecimal(IonNumber.Classification classification)
        throws IOException
    {
        switch(classification) {
        case NEGATIVE_ZERO:
            writeDecimal(Decimal.NEGATIVE_ZERO, classification);
            break;
        default:
            throw new IllegalArgumentException("classification for IonDecimal special values may only be NEGATIVE_ZERO");
        }
    }
    public void writeFloat(float value) throws IOException
    {
        writeFloat((double)value);
    }
    public void writeInt(byte value) throws IOException
    {
        writeInt((int)value);
    }
    public void writeInt(short value) throws IOException
    {
        writeInt((int)value);
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
    public void writeValue(IonValue value) throws IOException
    {
        IonReader valueReader = new IonReaderTreeSystem(value);
        writeValues(valueReader);
    }

    public void writeValues(IonReader reader) throws IOException
    {
        if (reader.getDepth() == 0) {
            clear_system_value_stack();
        }

        if (reader instanceof IonReaderWriterPrivate) {
            IonReaderWriterPrivate private_reader = (IonReaderWriterPrivate)reader;
            for (;;) {
                IonType type = reader.next();
                if (type == null) {
                    break;
                }
                transfer_symbol_tables(private_reader);
                writeValue(reader);
            }
        }
        else {
            write_values_helper(reader);
        }
    }

    public void write_values_helper(IonReader reader) throws IOException
    {
        IonType t;
        for (;;) {
            t = reader.next();
            if (t == null) break;
            writeValue(reader);
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
