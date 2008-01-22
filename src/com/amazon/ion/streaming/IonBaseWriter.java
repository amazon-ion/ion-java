/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonTokenReader.Type.timeinfo;
import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;

/**
 *  Base type for Ion writers.  This handles the writeIonEvents and default
 *  handlers for array writers, annotations, the symbol table, and the field name.
 */
public abstract class IonBaseWriter
    implements IonWriter
{
static final boolean _debug_on = false;

    UnifiedSymbolTable  _external_symbol_table;
    LocalSymbolTable    _symbol_table;
    boolean             _no_local_symbols = true;
    
    IonType     _field_name_type;     // really ion type is only used for int, string or null (unknown)
    String      _field_name;
    int         _field_name_sid;
    
    IonType     _annotations_type;     // really ion type is only used for int, string or null (unknown)
    int         _annotation_count;
    String[]    _annotations;
    int[]       _annotation_sids = new int[10];


    public SymbolTable getSymbolTable()
    {
        return _symbol_table;
    }

    public void setSymbolTable(UnifiedSymbolTable symbols)
    {
        _symbol_table = symbols;
    }

    public void setSymbolTable(LocalSymbolTable symbols)
    {
        _symbol_table = symbols;
    }
    
    public void setExternalSymbolTable(UnifiedSymbolTable externalSymbolTable) {
        if (_external_symbol_table != null) {
            throw new IllegalStateException("only 1 external symbol table is valid");
        }
        _external_symbol_table = externalSymbolTable;
        if (_symbol_table == null) {
            UnifiedSymbolTable symbol_table = new UnifiedSymbolTable(_external_symbol_table);
            symbol_table.addImportedTable(_external_symbol_table);
            _symbol_table = symbol_table;
        }
    }
    
    String getSymbolTableName() {
        if (_symbol_table instanceof UnifiedSymbolTable) {
            return ((UnifiedSymbolTable)_symbol_table).getName();
        }
        return null;
    }
    int getSymbolTableVersion() {
        if (_symbol_table instanceof UnifiedSymbolTable) {
            return ((UnifiedSymbolTable)_symbol_table).getVersion();
        }
        return 0;
    }
    UnifiedSymbolTable[] getSymbolTableImportedTables() {
        if (_symbol_table instanceof UnifiedSymbolTable) {
            return ((UnifiedSymbolTable)_symbol_table).getImportedTables();
        }
        return null;
    }
    
    Iterator<UnifiedSymbolTable.Symbol> getSymbolTableSymbols() {
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
    int getSymbolTableMaxId() {
        if (_symbol_table instanceof UnifiedSymbolTable) {
            return ((UnifiedSymbolTable)_symbol_table).getMaxId();
        }
        return 0;
    }
    
    public void clearAnnotations()
    {
        _annotation_count = 0;
        _annotations_type = IonType.NULL;
    }
    
    String[] get_annotations_as_strings() {
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
    
    int[] get_annotations_as_ints() {
        if (_annotations_type == IonType.STRING) {
            for (int ii=0; ii<_annotation_count; ii++) {
                String name = _annotations[ii];
                _annotation_sids[ii] = add_local_symbol(name);
            }
        }
        return _annotation_sids;

    }

    void growAnnotations(int length) {
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
    
    public void writeAnnotations(String[] annotations)
    {
        _annotations_type = IonType.STRING;
        if (annotations.length > _annotation_count) {
            growAnnotations(annotations.length);
        }
        System.arraycopy(annotations, 0, _annotations, 0, annotations.length);
        _annotation_count = annotations.length;
    }

    public void writeAnnotationIds(int[] annotationIds)
    {
        _annotations_type = IonType.INT;
        if (annotationIds.length > _annotation_count) {
            growAnnotations(annotationIds.length);
        }
        System.arraycopy(annotationIds, 0, _annotation_sids, 0, annotationIds.length);
        _annotation_count = annotationIds.length;
    }
    
    public void addAnnotation(String annotation)
    {
        growAnnotations(_annotation_count + 1);
        if (_annotations_type == IonType.INT) {
            int sid = this.add_local_symbol(annotation);
            this.addAnnotationId(sid);
        }
        else {
            _annotations_type = IonType.STRING;
            _annotations[_annotation_count++] = annotation;
        }
        return;
    }

    public void addAnnotationId(int annotationId)
    {
        growAnnotations(_annotation_count + 1);
        if (_annotations_type == IonType.STRING) {
            SymbolTable symtab = getSymbolTable();
            String annotation = symtab.findSymbol(annotationId);
            addAnnotation(annotation);
        }
        else {
            _annotations_type = IonType.INT;
            _annotation_sids[_annotation_count++] = annotationId;
        }
        return;
    }

    void clearFieldName() {
        _field_name_type = IonType.NULL;
        _field_name = null;
        _field_name_sid = 0;
    }
    String get_field_name_as_string() {
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
    
    int get_field_name_as_int() {
        if (_field_name_type == IonType.STRING) {
            _field_name_sid = add_local_symbol(_field_name);
        }
        return _field_name_sid;
    }
    int add_local_symbol(String name) 
    {
        SymbolTable symtab = getSymbolTable();
        if (symtab == null) {
            UnifiedSymbolTable temp = new UnifiedSymbolTable(UnifiedSymbolTable.getSystemSymbolTableInstance());
            setSymbolTable(temp);
            symtab = getSymbolTable();
        }
        
        int sid = symtab.findSymbol(_field_name); 
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
                UnifiedSymbolTable localtable = new UnifiedSymbolTable(symtab);
                this.setSymbolTable(localtable);
                utab = localtable;
            }
            sid = utab.addSymbol(_field_name);
            _no_local_symbols = false;
        }
        return sid;
    }
    public void writeFieldname(String name)
    {
        if (!this.isInStruct()) {
            throw new IllegalStateException();
        }
        _field_name_type = IonType.STRING;
        _field_name = name;
    }

    public void writeFieldnameId(int id)
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
        startList();
        for (int ii=0; ii<values.length; ii++) {
            writeString(values[ii]);
        }
        closeList();
    }
 
    public void writeBoolList(boolean[] values)
        throws IOException
    {
        startList();
        for (int ii=0; ii<values.length; ii++) {
            writeBool(values[ii]);
        }
        closeList();
    }

    public void writeFloatList(float[] values)
        throws IOException
    {
        startList();
        for (int ii=0; ii<values.length; ii++) {
            writeFloat(values[ii]);
        }
        closeList();
    }

    public void writeFloatList(double[] values)
        throws IOException
    {
        startList();
        for (int ii=0; ii<values.length; ii++) {
            writeFloat(values[ii]);
        }
        closeList();
    }

    public void writeIntList(byte[] values)
        throws IOException
    {
        startList();
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        closeList();
    }

    public void writeIntList(short[] values)
        throws IOException
    {
        startList();
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        closeList();
    }

    public void writeIntList(int[] values)
        throws IOException
    {
        startList();
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        closeList();
    }

    public void writeIntList(long[] values)
        throws IOException
    {
        startList();
        for (int ii=0; ii<values.length; ii++) {
            writeInt(values[ii]);
        }
        closeList();
    }

    public void writeIonValue(IonValue value) throws IOException
    {
        IonIterator value_iterator = new IonTreeIterator(value);
        writeIonEvents(value_iterator);
    }
    
    public void writeIonEvents(IonIterator iterator) throws IOException
    {
        while (iterator.hasNext()) {
            IonType t = iterator.next();
            writeIonValue(t, iterator);
        }
        return;
    }
    
    public void writeIonValue(IonType t, IonIterator iterator) throws IOException
    {
        if (iterator.isInStruct()) {
            String fieldname = iterator.getFieldName();
            writeFieldname(fieldname);
            if (_debug_on) System.out.print(":");
        }
        String [] a = iterator.getAnnotations(); 
        if (a != null) {
            writeAnnotations(a);
            if (_debug_on) System.out.print(";");
        }
        
        switch (t) {
            case NULL:
                writeNull();
                if (_debug_on) System.out.print("-");
                break;
            case BOOL:
                writeBool(iterator.getBool());
                if (_debug_on) System.out.print("b");
                break;
            case INT:
                writeInt(iterator.getLong());
                if (_debug_on) System.out.print("i");
                break;
            case FLOAT:
                writeFloat(iterator.getDouble());
                if (_debug_on) System.out.print("f");
                break;
            case DECIMAL:
                writeDecimal(iterator.getBigDecimal());
                if (_debug_on) System.out.print("d");
                break;
            case TIMESTAMP:
                timeinfo ti = iterator.getTimestamp();
                writeTimestamp(ti.d, ti.localOffset);
                if (_debug_on) System.out.print("t");
                break;
            case STRING:
                writeString(iterator.getString());
                if (_debug_on) System.out.print("$");
                break;
            case SYMBOL:
                writeSymbol(iterator.getString());
                if (_debug_on) System.out.print("y");
                break;
            case BLOB:
                writeBlob(iterator.getBytes());
                if (_debug_on) System.out.print("B");
                break;
            case CLOB:
                writeClob(iterator.getBytes());
                if (_debug_on) System.out.print("L");
                break;
            case STRUCT:
                if (_debug_on) System.out.print("{");
                startStruct();
                iterator.stepInto();
                writeIonEvents(iterator);
                iterator.stepOut();
                closeStruct();
                if (_debug_on) System.out.print("}");
                break;
            case LIST:
                if (_debug_on) System.out.print("[");
                startList();
                iterator.stepInto();
                writeIonEvents(iterator);
                iterator.stepOut();
                closeList();
                if (_debug_on) System.out.print("]");
                break;
            case SEXP:
                if (_debug_on) System.out.print("(");
                startSexp();
                iterator.stepInto();
                writeIonEvents(iterator);
                iterator.stepOut();
                closeSexp();
                if (_debug_on) System.out.print(")");
                break;
        }
    }

}
