// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonNumber;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import com.amazon.ion.IonNumber.Classification;
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
    implements IonWriter
{
    protected static final String ERROR_MISSING_FIELD_NAME =
        "IonWriter.setFieldName() must be called before writing a value into a struct.";

    private static final boolean _debug_on = false;


    /**
     * this should be set by the concrete
     * writers constructor.
     */
    protected final IonSystem _system;


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

    protected IonWriterBaseImpl(IonSystem system)
    {
        _system = system;
    }

    protected void reset() throws IOException
    {
        if (getDepth() != 0) {
            throw new IllegalStateException("you can't reset a writer that is in the middle of writing a value");
        }
        setSymbolTable(_system.getSystemSymbolTable());
    }

    public final IonSystem getSystem() {
        return _system;
    }

    //
    // symbol table support methods.  These handle the
    // symbol table state and are not generally overridden
    // except to return an UnsupportedOperationException
    // when they are not supported by a system writer.
    //
    public void setSymbolTable(SymbolTable symbols) throws IOException
    {
        if (symbols != null
            && ! symbols.isLocalTable()
            && ! symbols.isSystemTable())
        {
            throw new IllegalArgumentException("table must be local or system");
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
    protected int add_symbol(String name) throws IOException
    {
        if (_symbol_table == null) {
            _symbol_table = _system.getSystemSymbolTable();
        }

        int sid = _symbol_table.findSymbol(name);
        if (sid > 0) return sid;

        if (_symbol_table.isSystemTable()) {
            UnifiedSymbolTable symbols = UnifiedSymbolTable.makeNewLocalSymbolTable(_symbol_table);
            setSymbolTable(symbols);
        }
        assert _symbol_table.isLocalTable();

        sid = _symbol_table.addSymbol(name);
        return sid;
    }
    private String find_symbol(int sid)
    {
        if (_symbol_table == null) {
            throw new UnsupportedOperationException("a symbol table is required for MIXED id and string use");
        }
        String name = _symbol_table.findSymbol(sid);
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
            growAnnotations(annotations.length);
        }
        System.arraycopy(annotations, 0, _annotations, 0, annotations.length);
        _annotation_count = annotations.length;
    }
    public void setTypeAnnotationIds(int[] annotationIds)
    {
        _annotations_type = IonType.INT;
        if (annotationIds == null) {
            annotationIds = IonImplUtils.EMPTY_INT_ARRAY;
        }
        else if (annotationIds.length > _annotation_count) {
            growAnnotations(annotationIds.length);
        }
        System.arraycopy(annotationIds, 0, _annotation_sids, 0, annotationIds.length);
        _annotation_count = annotationIds.length;
    }
    public void addTypeAnnotation(String annotation)
    {
        growAnnotations(_annotation_count + 1);
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
        growAnnotations(_annotation_count + 1);
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
     * Grow does not increase _annotation_count.
     * And it keeps both the int and string arrays
     * allocated and sized together.
     */
    private void growAnnotations(int length) {
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

    private static final String[] EMPTY_STRING_ARRAY = new String[0];
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
    private static final int[] EMPTY_INT_ARRAY = new int[0];
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
        return;
    }
    public void writeTimestampUTC(Date value) throws IOException
    {
        Timestamp time;
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
        }
        else {
            time = new Timestamp(value.getTime(), Timestamp.UTC_OFFSET);
            writeTimestamp(time);
        }
        return;
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
        while (reader.next() != null) {
            writeValue(reader);
        }
        return;
    }
    private final void write_value_field_name_helper(IonReader reader)
    {
        String fieldname;

        if (this.isInStruct() && !isFieldNameSet()) {
            fieldname = reader.getFieldName();
            if (fieldname == null) {
                throw new IllegalStateException(ERROR_MISSING_FIELD_NAME);
            }
            setFieldName(fieldname);
            if (_debug_on) System.out.print(":");
        }

        return;
    }

    private final void write_value_annotations_helper(IonReader reader)
    {
        String [] a = reader.getTypeAnnotations();
        if (a != null && a.length > 0) {
            setTypeAnnotations(a);
            if (_debug_on) System.out.print(";");
        }
        return;
    }
    public void writeValue(IonReader reader) throws IOException
    {
        write_value_field_name_helper(reader);
        write_value_annotations_helper(reader);

        if (reader.isNullValue()) {
            this.writeNull(reader.getType());  // TODO hoist getType
        }
        else {
            switch (reader.getType()) {
            case NULL:
                writeNull();
                if (_debug_on) System.out.print("-");
                break;
            case BOOL:
                writeBool(reader.booleanValue());
                if (_debug_on) System.out.print("b");
                break;
            case INT:
                writeInt(reader.longValue());  // FIXME should use bigInteger
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
//                if (reader.getIterationType().isBinary() && this.getIterationType().isBinary()) {
//                    int sid = reader.getSymbolId();
//                    writeSymbol(sid);
//                }
//                else {
                    String name = reader.stringValue();
                    writeSymbol(name);
//                }
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
                stepIn(IonType.STRUCT);
                reader.stepIn();
                writeValues(reader);
                reader.stepOut();
                stepOut();
                if (_debug_on) System.out.print("}");
                break;
            case LIST:
                if (_debug_on) System.out.print("[");
                stepIn(IonType.LIST);
                reader.stepIn();
                writeValues(reader);
                reader.stepOut();
                stepOut();
                if (_debug_on) System.out.print("]");
                break;
            case SEXP:
                if (_debug_on) System.out.print("(");
                stepIn(IonType.SEXP);
                reader.stepIn();
                writeValues(reader);
                reader.stepOut();
                stepOut();
                if (_debug_on) System.out.print(")");
                break;
            default:
                throw new IllegalStateException();
            }
        }
    }
}
