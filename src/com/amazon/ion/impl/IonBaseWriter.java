/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import java.io.IOException;

/**
 *  Base type for Ion writers.  This handles the writeIonEvents and default
 *  handlers for array writers, annotations, the symbol table, and the field name.
 */
public abstract class IonBaseWriter
    implements IonWriter
{
    private static final boolean _debug_on = false;

    /**
     * FIXME when can this be null?  When can it be changed?
     */
    protected SymbolTable _symbol_table;
    protected boolean     _no_local_symbols = true;

    protected IonType     _field_name_type;     // really ion type is only used for int, string or null (unknown)
    protected String      _field_name;
    protected int         _field_name_sid;

    protected IonType     _annotations_type;     // really ion type is only used for int, string or null (unknown)
    protected int         _annotation_count;
    protected String[]    _annotations;
    protected int[]       _annotation_sids = new int[10];


    public SymbolTable getSymbolTable()
    {
        return _symbol_table;
    }

    public void setSymbolTable(SymbolTable symbols)
    {
        assert symbols.isLocalTable();
        _symbol_table = symbols;
    }

    public void importSharedSymbolTable(UnifiedSymbolTable sharedSymbolTable) {
        if (_symbol_table == null) {
            UnifiedSymbolTable symbol_table =
                new UnifiedSymbolTable(sharedSymbolTable.getSystemSymbolTable());
            _symbol_table = symbol_table;
        }
        ((UnifiedSymbolTable)_symbol_table).addImportedTable(sharedSymbolTable, 0);
    }

    protected String getSymbolTableName() {
        if (_symbol_table != null) {
            return _symbol_table.getName();
        }
        return null;
    }
    protected int getSymbolTableVersion() {
        if (_symbol_table != null) {
            return _symbol_table.getVersion();
        }
        return 0;
    }
    protected UnifiedSymbolTable[] getSymbolTableImportedTables() {
        if (_symbol_table instanceof UnifiedSymbolTable) {
            return ((UnifiedSymbolTable)_symbol_table).getImportedTables();
        }
        return null;
    }
    protected UnifiedSymbolTable.Symbol[] getSymbolArray() {
        if (_symbol_table instanceof UnifiedSymbolTable) {
            return ((UnifiedSymbolTable)_symbol_table)._symbols;
        }
        else if (_symbol_table != null) {
            int count = _symbol_table.getMaxId();
            UnifiedSymbolTable.Symbol[] symbols = new UnifiedSymbolTable.Symbol[count];
            SymbolTable system = _symbol_table.getSystemSymbolTable();
            int systemidmax = system == null ? 0 : system.getMaxId();
            for (int ii=systemidmax; ii<count; ii++) {
                String name = _symbol_table.findKnownSymbol(ii);
                if (name != null) {
                    UnifiedSymbolTable.Symbol sym = new UnifiedSymbolTable.Symbol();
                    sym.name = name;
                    sym.sid = ii;
                    symbols[ii] = sym;
                }
            }
            return symbols;
        }
        return null;
    }
    /* TODO delete this if the routine above proves more useful
    Iterator<UnifiedSymbolTable.Symbol> xxgetSymbolTableSymbols() {
        if (_symbol_table instanceof UnifiedSymbolTable) {
            return ((UnifiedSymbolTable)_symbol_table).getLocalSymbols();
        }
        else if (_symbol_table != null) {
            int count = _symbol_table.getMaxId();
            Stack<UnifiedSymbolTable.Symbol> symbols = new Stack<UnifiedSymbolTable.Symbol>();
            SymbolTable system = _symbol_table.getSystemSymbolTable();
            int systemidmax = system == null ? 0 : system.getMaxId();
            for (int ii=systemidmax; ii<count; ii++) {
                String name = _symbol_table.findKnownSymbol(ii);
                if (name != null) {
                    UnifiedSymbolTable.Symbol sym = new UnifiedSymbolTable.Symbol();
                    sym.name = name;
                    sym.sid = ii;
                    symbols.push(sym);
                }
            }
            return symbols.iterator();
        }
        return null;
    }
    */
    protected int getSymbolTableMaxId() {
        if (_symbol_table != null) {
            return _symbol_table.getMaxId();
        }
        return 0;
    }

    public void clearAnnotations()
    {
        _annotation_count = 0;
        _annotations_type = IonType.NULL;
    }

    protected String[] get_annotations_as_strings() {
        if (_annotations_type == IonType.INT) {
            SymbolTable symtab = getSymbolTable();
            if (symtab == null) {
                throw new IllegalStateException("a symbol table is required for MIXED id and string use");
            }
            for (int ii=0; ii<_annotation_count; ii++) {
                String name;
                int id = _annotation_sids[ii];
                name = symtab.findKnownSymbol(id);
                _annotations[ii] = name;
            }
        }
        return _annotations;
    }

    protected int[] get_annotations_as_ints() {
        if (_annotations_type == IonType.STRING) {
            for (int ii=0; ii<_annotation_count; ii++) {
                String name = _annotations[ii];
                _annotation_sids[ii] = add_local_symbol(name);
            }
        }
        return _annotation_sids;

    }

    /**
     * Does not increase _annotation_count.
     */
    protected void growAnnotations(int length) {
        int oldlen = (_annotations == null) ? 0 : _annotations.length;
        if (length < oldlen) return;
        int newlen = (_annotations == null) ? 10 : (_annotations.length * 2);
        if (length > newlen) {
            newlen = length;
        }

        String[] temp1 = new String[newlen];
        int[]    temp2 = new int[newlen];

        if (_annotation_count > 0) {
            if (_annotations_type == IonType.STRING) {
                System.arraycopy(_annotations, 0, temp1, 0, _annotation_count);
            }
            if (_annotations_type == IonType.INT) {
                System.arraycopy(_annotation_sids, 0, temp2, 0, _annotation_count);
            }
        }
        _annotations = temp1;
        _annotation_sids = temp2;
    }

    public void setTypeAnnotations(String[] annotations)
    {
        _annotations_type = IonType.STRING;
        if (annotations.length > _annotation_count) {
            growAnnotations(annotations.length);
        }
        System.arraycopy(annotations, 0, _annotations, 0, annotations.length);
        _annotation_count = annotations.length;
    }

    public void setTypeAnnotationIds(int[] annotationIds)
    {
        _annotations_type = IonType.INT;
        if (annotationIds.length > _annotation_count) {
            growAnnotations(annotationIds.length);
        }
        System.arraycopy(annotationIds, 0, _annotation_sids, 0, annotationIds.length);
        _annotation_count = annotationIds.length;
    }

    public void addTypeAnnotation(String annotation)
    {
        growAnnotations(_annotation_count + 1);
        if (_annotations_type == IonType.INT) {
            int sid = this.add_local_symbol(annotation);
            this.addTypeAnnotationId(sid);
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

    protected void clearFieldName() {
        _field_name_type = IonType.NULL;
        _field_name = null;
        _field_name_sid = 0;
    }
    protected String get_field_name_as_string() {
        if (_field_name_type == IonType.INT) {
            SymbolTable symtab = getSymbolTable();
            if (symtab == null) {
                throw new IllegalStateException("a symbol table is required for mixed id and string use");
            }
            String name;
            int id = _field_name_sid;
            name = symtab.findKnownSymbol(id);
            _field_name = name;
        }
        return _field_name;
    }

    protected int get_field_name_as_int() {
        if (_field_name_type == IonType.STRING) {
            _field_name_sid = add_local_symbol(_field_name);
        }
        return _field_name_sid;
    }
    protected int add_local_symbol(String name)
    {
        SymbolTable symtab = getSymbolTable();
        if (symtab == null) {
            UnifiedSymbolTable temp = new UnifiedSymbolTable(UnifiedSymbolTable.getSystemSymbolTableInstance());
            setSymbolTable(temp);  // TODO why is this get-then-set needed?
            symtab = getSymbolTable();
        }

        int sid = symtab.findSymbol(name);
        if (sid < 1) {
            UnifiedSymbolTable utab = null;
            if (symtab instanceof UnifiedSymbolTable) {
                utab = ((UnifiedSymbolTable)symtab);
                if (utab.isSystemTable()) {
                    UnifiedSymbolTable localtable = new UnifiedSymbolTable(utab);
                    this.setSymbolTable(localtable);
                    utab = localtable;
                }
            }
            else {
                UnifiedSymbolTable localtable = UnifiedSymbolTable.copyFrom(symtab);
                this.setSymbolTable(localtable);
                utab = localtable;
            }
            sid = utab.addSymbol(name);
            _no_local_symbols = false;
        }
        return sid;
    }
    public void setFieldName(String name)
    {
        if (!this.isInStruct()) {
            throw new IllegalStateException();
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

    public void writeStringList(String[] values)
        throws IOException
    {
        openList();
        for (int ii=0; ii<values.length; ii++) {
            writeString(values[ii]);
        }
        closeList();
    }

    public void writeBoolList(boolean[] values)
        throws IOException
    {
        openList();
        for (int ii=0; ii<values.length; ii++) {
            writeBool(values[ii]);
        }
        closeList();
    }

    public void writeFloatList(float[] values)
        throws IOException
    {
        openList();
        for (int ii=0; ii<values.length; ii++) {
            writeFloat(values[ii]);
        }
        closeList();
    }

    public void writeFloatList(double[] values)
        throws IOException
    {
        openList();
        for (int ii=0; ii<values.length; ii++) {
            writeFloat(values[ii]);
        }
        closeList();
    }

    public void writeIntList(byte[] values)
        throws IOException
    {
        openList();
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        closeList();
    }

    public void writeIntList(short[] values)
        throws IOException
    {
        openList();
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        closeList();
    }

    public void writeIntList(int[] values)
        throws IOException
    {
        openList();
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        closeList();
    }

    public void writeIntList(long[] values)
        throws IOException
    {
        openList();
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        closeList();
    }

    public void writeIonValue(IonValue value) throws IOException
    {
        IonReader value_iterator = new IonTreeReader(value);
        writeIonEvents(value_iterator);
    }

    public void writeIonEvents(IonReader iterator) throws IOException
    {
        while (iterator.hasNext()) {
            IonType t = iterator.next();
            writeIonValue(t, iterator);
        }
        return;
    }

    public void writeIonValue(IonType t, IonReader iterator) throws IOException
    {
        if (/* iterator.isInStruct() && */ this.isInStruct()) {
            String fieldname = iterator.getFieldName();
            setFieldName(fieldname);
            if (_debug_on) System.out.print(":");
        }
        String [] a = iterator.getTypeAnnotations();
        if (a != null) {
            setTypeAnnotations(a);
            if (_debug_on) System.out.print(";");
        }

        if (iterator.isNullValue()) {
            this.writeNull(iterator.getType());
        }
        else {
            switch (t) {
            case NULL:
                writeNull();
                if (_debug_on) System.out.print("-");
                break;
            case BOOL:
                writeBool(iterator.booleanValue());
                if (_debug_on) System.out.print("b");
                break;
            case INT:
                writeInt(iterator.longValue());  // FIXME should use bigInteger
                if (_debug_on) System.out.print("i");
                break;
            case FLOAT:
                writeFloat(iterator.doubleValue());
                if (_debug_on) System.out.print("f");
                break;
            case DECIMAL:
                writeDecimal(iterator.bigDecimalValue());
                if (_debug_on) System.out.print("d");
                break;
            case TIMESTAMP:
                writeTimestamp(iterator.timestampValue());
                if (_debug_on) System.out.print("t");
                break;
            case STRING:
                writeString(iterator.stringValue());
                if (_debug_on) System.out.print("$");
                break;
            case SYMBOL:
                writeSymbol(iterator.stringValue());
                if (_debug_on) System.out.print("y");
                break;
            case BLOB:
                writeBlob(iterator.newBytes());
                if (_debug_on) System.out.print("B");
                break;
            case CLOB:
                writeClob(iterator.newBytes());
                if (_debug_on) System.out.print("L");
                break;
            case STRUCT:
                if (_debug_on) System.out.print("{");
                openStruct();
                iterator.stepIn();
                writeIonEvents(iterator);
                iterator.stepOut();
                closeStruct();
                if (_debug_on) System.out.print("}");
                break;
            case LIST:
                if (_debug_on) System.out.print("[");
                openList();
                iterator.stepIn();
                writeIonEvents(iterator);
                iterator.stepOut();
                closeList();
                if (_debug_on) System.out.print("]");
                break;
            case SEXP:
                if (_debug_on) System.out.print("(");
                openSexp();
                iterator.stepIn();
                writeIonEvents(iterator);
                iterator.stepOut();
                closeSexp();
                if (_debug_on) System.out.print(")");
                break;
            default:
                throw new IllegalStateException();
            }
        }
    }
}
