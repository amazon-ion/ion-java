// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonIterationType;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
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
abstract class IonWriterUser
    extends IonWriterBaseImpl  // should be IonWriterSystem ?
{
    /*package*/ final IonSystem _system;
    // needed to make correct local symbol tables
    IonCatalog _catalog;

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


    protected IonWriterUser(IonSystem system, IonWriterBaseImpl systemWriter, IonCatalog catalog, IonValue container, boolean suppressIVM)
    {
        super(system.getSystemSymbolTable());
        _system = system;
        _catalog = catalog;
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
            if (suppressIVM == false) {
                if (_current_writer.getSymbolTable() == null) {
                    try {
                        setSymbolTable(_system.getSystemSymbolTable());
                    }
                    catch (IOException e) {
                        throw new IonException(e);
                    }
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
        _current_writer.flush();

        if (getDepth() == 0) {
            assert(_current_writer == _system_writer);
            reset();
        }
    }

    public void close() throws IOException
    {
        _current_writer.close();
        _system_writer.close();
    }

    @Override
    protected void reset() throws IOException
    {
        if (_symbol_table_being_copied) {
            throw new IllegalStateException("you can't reset a user writer while a local symbol table value is being written");
        }

        _system_writer.flush();

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


    // TODO this method needs documentation, I have no idea what's going on.
    private void open_local_symbol_table_copy()
    {
        assert(!_symbol_table_being_copied);
        assert(_system != null);

        _symbol_table_value = _system.newEmptyStruct();
        _symbol_table_value.addTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE);

        _symbol_table_writer       = new IonWriterSystemTree(_system, _catalog, _symbol_table_value);

        _symbol_table_being_copied = true;
        _current_writer            = _symbol_table_writer;
        _current_writer_as_base    = _symbol_table_writer;
    }

    private void close_local_symbol_table_copy() throws IOException
    {
        assert(_symbol_table_being_copied);
        assert(_system != null && _system instanceof IonSystemPrivate);

        // convert the struct we just wrote with the TreeWriter to a
        // local symbol table
        UnifiedSymbolTable symtab = UnifiedSymbolTable.makeNewLocalSymbolTable(_system, _catalog, _symbol_table_value);

        _symbol_table_being_copied = false;
        _symbol_table_value        = null;
        _symbol_table_writer       = null;

        _current_writer            = _system_writer;
        _current_writer_as_base    = _system_writer_as_base;

        // now make this symbol table the current symbol table
        this.setSymbolTable(symtab);

        return;
    }

    abstract void set_symbol_table_helper(SymbolTable prev_symbols, SymbolTable new_symbols) throws IOException;

    @Override
    public final void setSymbolTable(SymbolTable symbols) throws IOException
    {
        SymbolTable prev = _symbol_table;

        // checks validity and set the member variable
        super.setSymbolTable(symbols);

        // the subclass should do what they want to on
        // this transition often nothing, sometimes we
        // write the symbol table to the system writer
        // comparing prev and new prevents recursing
        // on writeIVM calls
        set_symbol_table_helper(prev, symbols);

        _system_writer.setSymbolTable(symbols);
    }

    /**
     * to do when they are called on to now actually write the
     * symbol table.  The symbol table has already been "captured"
     * and constructed and is will be set as the current symbol
     * table upon return.
     *
     * @param symbols
     */
    protected final void xxx_writeUserSymbolTable(SymbolTable symbols) throws IOException
    {
        // if our symbol table changed we need to write it out
        // to the system writer ... IF, and only if, this is binary writer
        if (symbols.isSystemTable()) {
            if (!_after_ion_version_marker) {
                // writing to the system writer keeps us from
                // recursing on the writeIonVersionMarker call
                _system_writer.writeIonVersionMarker();
            }
        }
        else if (symbols.isLocalTable()) {
            //really we have to patch in symbol tables, not write them
            //since they will may get updated as the value is
            //built up
            symbols.writeTo(_system_writer);
        }
        else {
            assert("symbol table must be a system or a local table".length() < 0);
        }
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

    private final void finish_value()
    {
        // note that as we finish most values we aren't after
        // an IVM - except when it's an IVM.  In that case the
        // caller (writeIVM) sets this back to true.
        _after_ion_version_marker = false;  // this will gets set back to true when we really write an IVM
    }
    public void stepIn(IonType containerType) throws IOException
    {
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
        finish_value();
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
        _current_writer.writeBlob(value, start, len);
        finish_value();
    }
    public void writeBool(boolean value) throws IOException
    {
        _current_writer.writeBool(value);
        finish_value();
    }
    public void writeClob(byte[] value, int start, int len) throws IOException
    {
        if (value == null || start < 0 || len < 0 || start+len > value.length) {
            throw new IllegalArgumentException("the start and len must be contained in the byte array");
        }
        _current_writer.writeClob(value, start, len);
        finish_value();
    }
    @Override
    public void writeDecimal(BigDecimal value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.DECIMAL);
        }
        else {
            _current_writer.writeDecimal(value);
        }
        finish_value();
    }
    public void writeFloat(double value) throws IOException
    {
        _current_writer.writeFloat(value);
        finish_value();
    }
    public void writeInt(int value) throws IOException
    {
        _current_writer.writeInt((long)value);
        finish_value();
    }
    public void writeInt(long value) throws IOException
    {
        _current_writer.writeInt(value);
        finish_value();
    }
    public void writeInt(BigInteger value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.INT);
        }
        else {
            _current_writer.writeInt(value);
        }
        finish_value();
    }
    public void writeNull(IonType type) throws IOException
    {
        if (type == null) {
            writeNull(IonType.NULL);
        }
        else {
            _current_writer.writeNull(type);
        }
        finish_value();
    }
    public void writeString(String value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.STRING);
        }
        else {
            _current_writer.writeString(value);
        }
        finish_value();
    }
    public void writeSymbol(int symbolId) throws IOException
    {
        if (write_as_ivm(symbolId)) {
            if (need_to_write_ivm()) {
                writeIonVersionMarker();
            }
            // since writeIVM calls finish and when
            // we don't write the IVM we don't want
            // to call finish, we're done here.
            return;
        }
        _current_writer.writeSymbol(symbolId);
        finish_value();
    }
    public void writeSymbol(String value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.SYMBOL);
        }
        else if (value.equals(UnifiedSymbolTable.ION_1_0)
              && write_as_ivm(UnifiedSymbolTable.ION_1_0_SID)
        ) {
            if (need_to_write_ivm()) {
                writeIonVersionMarker();
            }
            // since writeIVM calls finish and when
            // we don't write the IVM we don't want
            // to call finish, we're done here.
            return;
        }
        else {
            _current_writer.writeSymbol(value);
        }
        finish_value();
    }
    private final boolean write_as_ivm(int sid)
    {
        // we only treat the $ion_1_0 symbol as an IVM
        // if we're at the top level in a datagram
        boolean treat_as_ivm = false;

        if (sid == UnifiedSymbolTable.ION_1_0_SID
         && _root_is_datagram
         && _current_writer.getDepth() == 0
         ) {
            treat_as_ivm = true;
        }
        return treat_as_ivm;
    }
    private final boolean need_to_write_ivm()
    {
        // we skip this IVM symbol if it is a redundant IVM
        // either at the beginning (which is common if this
        // is a binary writer) or at any other time.
        boolean write_it;
        write_it = (_after_ion_version_marker == false);
        return write_it;
    }
    public void writeIonVersionMarker() throws IOException
    {
        if (getDepth() != 0 || _root_is_datagram == false) {
            throw new IllegalStateException("IonVersionMarkers are only value at the top level of a datagram");
        }
        assert(_current_writer == _system_writer);

        _current_writer.writeIonVersionMarker();
        _after_ion_version_marker = true;

        setSymbolTable(_system.getSystemSymbolTable());

        finish_value();
        // we reset this after our call to finish since
        // finish sets this to false (which is the correct
        // behavior except here)  We could add a flag to
        // finish to tell it whether we were finishing a
        // IVM or not, but since this is the only time
        // it's the case it's easier to just patch it here
        _after_ion_version_marker = true;
    }
    public void writeTimestamp(Timestamp value) throws IOException
    {
        if (value == null) {
            writeNull(IonType.TIMESTAMP);
        }
        else {
            _current_writer.writeTimestamp(value);

        }
        finish_value();
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
            _current_writer.writeBoolList(values);
        }
        finish_value();
    }
    @Override
    public void writeIntList(byte[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            _current_writer.writeIntList(values);
        }
        finish_value();
    }
    @Override
    public void writeIntList(short[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            _current_writer.writeIntList(values);
        }
        finish_value();
    }
    @Override
    public void writeIntList(int[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            _current_writer.writeIntList(values);
        }
        finish_value();
    }
    @Override
    public void writeIntList(long[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            _current_writer.writeIntList(values);
        }
        finish_value();
    }
    @Override
    public void writeFloatList(float[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            _current_writer.writeFloatList(values);
        }
        finish_value();
    }
    @Override
    public void writeFloatList(double[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            _current_writer.writeFloatList(values);
        }
        finish_value();
    }
    @Override
    public void writeStringList(String[] values) throws IOException
    {
        if (values == null) {
            writeNull(IonType.LIST);
        }
        else {
            _current_writer.writeStringList(values);
        }
        finish_value();
    }
}
