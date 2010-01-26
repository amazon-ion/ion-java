// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonIterationType;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.Timestamp;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * This writer handles the symbol table processing and
 * provides default implementations for the list forms
 * of the write methods as often the list form is not
 * susceptible to optimization.
 *
 * This also detects system values as they are written
 * at the datagram level and injects
 *
 *
 */
public abstract class IonWriterUser
    extends IonWriterBaseImpl  // should be IonWriterSystem ?
{
    // these track the state needed to put version markers
    // in place as needed
    boolean   _any_values_written; // we always write the version marker, but we also skip the users IVM it they write it as a symbol as the first value
    boolean   _after_ion_version_marker;
    boolean   _is_tree_writer;
    boolean   _root_is_datagram;

    // this is the underlying system writer that writes to the
    // raw format (text, binary, or ion values)
    IonWriter         _system_writer;
    IonWriterBaseImpl _system_writer_as_base;

    // values to manage the diversion of the users input
    // to a local symbol table
    boolean           _symbol_table_being_copied;
    IonStruct         _symbol_table_value;
    IonWriterBaseImpl _symbol_table_writer;

    // this will be either the system writer or the symbol table writer
    // depending on whether we're diverting the user values to a
    // local symbol table ... or not.
    IonWriter         _current_writer;
    IonWriterBaseImpl _current_writer_as_base;


    protected IonWriterUser(IonWriter systemWriter, IonValue container)
    {
        super(systemWriter.getSystem());
        _system_writer = systemWriter;
        _current_writer = _system_writer;
        if (_system_writer instanceof IonWriterBaseImpl) {
            _current_writer_as_base = (IonWriterBaseImpl)_system_writer;
            _system_writer_as_base = (IonWriterBaseImpl)_system_writer;
        }

        IonIterationType ot = systemWriter.getIterationType();
        _is_tree_writer = ot.isIonValue();
        if (_is_tree_writer) {
            if (container == null) {
                throw new IllegalArgumentException("container is required for IonValue writers");
            }
            // Datagrams have an implicit initial IVM
            _after_ion_version_marker = true;
            if (IonType.DATAGRAM.equals(container.getType())) {
                _root_is_datagram = true;
            }
        }
        else {
            if (container != null) {
                throw new IllegalArgumentException("container is invalid except on IonValue writers");
            }
            _root_is_datagram = true;
            if (_system_writer.getSymbolTable() == null) {
                try {
                    writeIonVersionMarker();
                    _any_values_written = false; // we don't count this one, so we have to clean up after writeIVM set this to true
                }
                catch (IOException e) {
                    throw new IonException(e);
                }
            }
        }
    }

    @Override
    protected boolean has_annotation(String name, int id)
    {
        // mostly our current writer is one we can peek into
        if (_current_writer_as_base != null) {
            return _current_writer_as_base.has_annotation(name, id);
        }

        // otherwise we have to do this the hard way
        int[] ids = null;
        String[] names = null;
        if (_current_writer.getIterationType().isBinary()
         || _current_writer.getIterationType().isUser()
        ) {
            ids = _current_writer.getTypeAnnotationIds();
        }
        if (ids == null) {
            names = _current_writer.getTypeAnnotations();
        }
        if (names == null) {
            ids = _current_writer.getTypeAnnotationIds();
        }

        if (ids != null) {
            for (int ii=0; ii<ids.length; ii++) {
                if (ids[ii] == id) return true;
            }
            return false;
        }
        else if (names != null) {
            for (int ii=0; ii<names.length; ii++) {
                if (name.equals(names[ii])) return true;
            }
            return false;
        }

        return false;
    }

    public int getDepth()
    {
        return _current_writer.getDepth();
    }
    public boolean isInStruct()
    {
        return _current_writer.isInStruct();
    }

    public IonIterationType getIterationType()
    {
        assert(_system_writer != null);

        IonIterationType systype = _system_writer.getIterationType();
        IonIterationType usertype = null;
        switch (systype) {
        case SYSTEM_TEXT:
            usertype = IonIterationType.USER_TEXT;
            break;
        case SYSTEM_BINARY:
            usertype = IonIterationType.USER_BINARY;
            break;
        case SYSTEM_ION_VALUE:
            usertype = IonIterationType.USER_ION_VALUE;
            break;
        case USER_TEXT:
        case USER_BINARY:
        case USER_ION_VALUE:
            // do we want to assert or throw here with the
            // expectation (demand) that the underlying
            // system writer be, in fact, a system writer?
            usertype = systype;
            break;
        }
        return usertype;
    }

    public void flush() throws IOException
    {
        if (getDepth() != 0) {
            throw new IllegalStateException("you can't reset a writer that is in the middle of writing a value");
        }
        if (_symbol_table_being_copied) {
            throw new IllegalStateException("you can't reset a user writer while a local symbol table value is being written");
        }
        assert(_current_writer == _system_writer);

        _current_writer.flush();

        reset();
    }

    @Override
    protected void reset() throws IOException
    {
        if (_symbol_table_being_copied) {
            throw new IllegalStateException("you can't reset a user writer while a local symbol table value is being written");
        }

        super.reset();
        _any_values_written       = false;
        _after_ion_version_marker = false;
        _current_writer           = _system_writer;
        _current_writer_as_base   = _system_writer_as_base;


        // Note:
        //
        // You don't have to reset the system writer
        // since this is called from flush and you
        // just need to flush the system write - it
        // will call reset if it needs to.
        //
        // this avoids having to add reset to the
        // public interface or cast the writers
        // to an internal interface.
    }


    private void open_local_symbol_table_copy()
    {
        assert(!_symbol_table_being_copied);
        assert(_system != null && _system instanceof IonSystemImpl);

        _symbol_table_value = _system.newEmptyStruct();
        _symbol_table_value.addTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE);

        _symbol_table_writer       = new IonWriterSystemTree(_system, _symbol_table_value);

        _symbol_table_being_copied = true;
        _current_writer            = _symbol_table_writer;
        _current_writer_as_base    = _symbol_table_writer;
    }
    private void close_local_symbol_table_copy() throws IOException
    {
        assert(_symbol_table_being_copied);
        assert(_system != null && _system instanceof IonSystemImpl);

        // convert the struct we just wrote with the TreeWriter to a
        // local symbol table
        UnifiedSymbolTable symtab = UnifiedSymbolTable.makeNewLocalSymbolTable(_symbol_table_value);

        // and we have to write the symbol table out to the
        // system writer, since we diverted the events up
        // to this point
        symtab.writeTo(_system_writer);

        // now make this symbol table the current symbol table
        this.setSymbolTable(symtab);

        _symbol_table_being_copied = false;
        _symbol_table_value        = null;
        _symbol_table_writer       = null;

        _current_writer            = _system_writer;
        _current_writer_as_base    = _system_writer_as_base;



        return;
    }
    private final void start_user_value() throws IOException
    {
        _any_values_written = true;
    }

    @Override
    public void setSymbolTable(SymbolTable symbols) throws IOException
    {
        super.setSymbolTable(symbols);
        _system_writer.setSymbolTable(symbols);
    }
    @Override
    public SymbolTable getSymbolTable()
    {
        SymbolTable symbols = _system_writer.getSymbolTable();
        return symbols;
    }

    @Override
    public void setFieldName(String name)
    {
        _current_writer.setFieldName(name);
    }
    @Override
    public void setFieldId(int id)
    {
        _current_writer.setFieldId(id);
    }
    @Override
    public String getFieldName()
    {
        String fieldname = _current_writer.getFieldName();
        return fieldname;
    }
    @Override
    public int getFieldId()
    {
        int sid = _current_writer.getFieldId();
        return sid;
    }
    @Override
    public boolean isFieldNameSet() {
        boolean is_set = _current_writer.isFieldNameSet();
        return is_set;
    }
    @Override
    protected void clearFieldName() {
        if (_current_writer_as_base != null) {
            _current_writer_as_base.clearFieldName();
        }
    }


    @Override
    public void addTypeAnnotation(String annotation)
    {
        _current_writer.addTypeAnnotation(annotation);
    }
    @Override
    public void addTypeAnnotationId(int annotationId)
    {
        _current_writer.addTypeAnnotationId(annotationId);
    }
    @Override
    public void setTypeAnnotationIds(int[] annotationIds)
    {
        _current_writer.setTypeAnnotationIds(annotationIds);
    }
    @Override
    public void setTypeAnnotations(String[] annotations)
    {
        _current_writer.setTypeAnnotations(annotations);
    }
    @Override
    public String[] getTypeAnnotations()
    {
        return _current_writer.getTypeAnnotations();
    }
    @Override
    public int[] getTypeAnnotationIds()
    {
        return _current_writer.getTypeAnnotationIds();
    }

    public void writeIonVersionMarker() throws IOException
    {
        _system_writer.writeIonVersionMarker();
        _after_ion_version_marker = true;
        _any_values_written = true;
        setSymbolTable(_system.getSystemSymbolTable());
    }

    public void stepIn(IonType containerType) throws IOException
    {
        start_user_value();

        // see if it looks like we're starting a local symbol table
        if (IonType.STRUCT.equals(containerType)
         && getDepth() == 0
         && has_annotation(UnifiedSymbolTable.ION_SYMBOL_TABLE, UnifiedSymbolTable.ION_SYMBOL_TABLE_SID)
         ) {
            // if so we'll divert all the data until it's finished
            open_local_symbol_table_copy();
        }
        else {
            // if not we'll just pass the work on to whatever
            // writer is currently in scope
            _current_writer.stepIn(containerType);
        }
    }
    public void stepOut() throws IOException
    {
        if (_symbol_table_being_copied && _current_writer.getDepth() == 0) {
            close_local_symbol_table_copy();
        }
        else {
            _current_writer.stepOut();
        }
    }

    public void writeBlob(byte[] value, int start, int len) throws IOException
    {
        if (value == null || start < 0 || len < 0 || start+len > value.length) {
            throw new IllegalArgumentException("the start and len must be contained in the byte array");
        }
        start_user_value();
        _current_writer.writeBlob(value, start, len);
    }
    public void writeBool(boolean value) throws IOException
    {
        start_user_value();
        _current_writer.writeBool(value);
    }
    public void writeClob(byte[] value, int start, int len) throws IOException
    {
        if (value == null || start < 0 || len < 0 || start+len > value.length) {
            throw new IllegalArgumentException("the start and len must be contained in the byte array");
        }
        start_user_value();
        _current_writer.writeClob(value, start, len);
    }
    @Override
    public void writeDecimal(BigDecimal value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.DECIMAL);
        }
        else {
            start_user_value();
            _current_writer.writeDecimal(value);
        }
    }
    public void writeFloat(double value) throws IOException
    {
        start_user_value();
        _current_writer.writeFloat(value);
    }
    public void writeInt(int value) throws IOException
    {
        start_user_value();
        _current_writer.writeInt(value);
    }
    public void writeInt(long value) throws IOException
    {
        start_user_value();
        _current_writer.writeInt(value);
    }
    public void writeInt(BigInteger value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.INT);
        }
        else {
            start_user_value();
            _current_writer.writeInt(value);
        }
    }
    public void writeNull(IonType type) throws IOException
    {
        if (type == null) {
            writeNull(IonType.NULL);
        }
        else {
            start_user_value();
            _current_writer.writeNull(type);
        }
    }
    public void writeString(String value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.STRING);
        }
        else {
            start_user_value();
            _current_writer.writeString(value);
        }
    }
    public void writeSymbol(int symbolId) throws IOException
    {
        if (symbolId == UnifiedSymbolTable.ION_1_0_SID && !_any_values_written) {
            // we already did this writeIonVersionMarker();
            assert(_after_ion_version_marker);
            return;
        }
        start_user_value();
        _current_writer.writeSymbol(symbolId);
        if (_root_is_datagram && symbolId == UnifiedSymbolTable.ION_1_0_SID && _current_writer.getDepth() == 0) {
            _current_writer.setSymbolTable(getSystem().getSystemSymbolTable());
        }
    }
    public void writeSymbol(String value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.SYMBOL);
        }
        else if (!_any_values_written && UnifiedSymbolTable.ION_1_0.equals(value)) {
            // we already did this writeIonVersionMarker();
            assert(_after_ion_version_marker);
        }
        else {
            start_user_value();
            _current_writer.writeSymbol(value);
            if (_root_is_datagram && _current_writer.getDepth() == 0 && UnifiedSymbolTable.ION_1_0.equals(value)) {
                _current_writer.setSymbolTable(getSystem().getSystemSymbolTable());
            }
        }
    }
    public void writeTimestamp(Timestamp value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
        }
        else {
            start_user_value();
            _current_writer.writeTimestamp(value);

        }
    }

    //
    // list version of the write methods these have to be intercepted
    // since someone might write a list of strings into a local symbol
    // table and we need to divert the values.
    //
    // and the underlying system writer might be a binary writer
    // in which case we want to invoke the optimized versions of these
    // methods.
    //
    // in all likelihood this is not a worthwhile optimization <sigh>
    //
    @Override
    public void writeBoolList(boolean[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            start_user_value();
            _current_writer.writeBoolList(values);
        }
    }
    @Override
    public void writeIntList(byte[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            start_user_value();
            _current_writer.writeIntList(values);
        }
    }
    @Override
    public void writeIntList(short[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            start_user_value();
            _current_writer.writeIntList(values);
        }
    }
    @Override
    public void writeIntList(int[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            start_user_value();
            _current_writer.writeIntList(values);
        }
    }
    @Override
    public void writeIntList(long[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            start_user_value();
            _current_writer.writeIntList(values);
        }
    }
    @Override
    public void writeFloatList(float[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            start_user_value();
            _current_writer.writeFloatList(values);
        }
    }
    @Override
    public void writeFloatList(double[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            start_user_value();
            _current_writer.writeFloatList(values);
        }
    }
    @Override
    public void writeStringList(String[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            start_user_value();
            _current_writer.writeStringList(values);
        }
    }
}
