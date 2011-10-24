// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.IonType.DATAGRAM;
import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
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
 * <p>
 * This writer has a ({@link #_system_writer}) to which the actual data is
 * written, but data flows through the {@link #_current_writer} most of the
 * time.
 * <p>
 * The critical responsibility here is the recognition of IVMs and local symbol
 * tables. When the user starts writing a local symtab, the stream is diverted
 * away from the {@link #_system_writer} into a temporary tree writer that
 * collects the symtab data into an {@link IonStruct} instance.  When that
 * struct is stepped-out, the diversion is stopped and the new
 * {@link SymbolTable} is installed.
 */
abstract class IonWriterUser
    extends IonWriterBaseImpl  // should be IonWriterSystem ?
{
    /** Not null. */
    protected final IonSystem _system;

    /** Used to make correct local symbol tables. May be null. */
    private   final IonCatalog _catalog;

    /**
     * Indicates whether the (immediately) previous value was an IVM.
     * This is cleared by {@link #finish_value()}.
     */
    protected       boolean   _previous_value_was_ivm;
    protected final boolean   _root_is_datagram;

    /**
     * The underlying system writer that writing the raw format (text, binary,
     * or ion values).  Not null.
     */
    protected final IonWriterBaseImpl _system_writer;

    /**
     * This will be either our {@link #_system_writer} or a symbol table writer
     * depending on whether we're diverting the user values to a
     * local symbol table ... or not.
     * Not null.
     */
    protected IonWriterBaseImpl _current_writer;

    /**
     * While the stream is diverted to collect local symtab data, it is
     * being written to this instance.
     * This is null IFF {@link #_current_writer} == {@link #_system_writer}.
     */
    private IonStruct _symbol_table_value;



    /**
     * Constructor for text and binary writers.
     *
     * @param system must not be null.
     * @param catalog may be null.
     * @param systemWriter must not be null.
     */
    private IonWriterUser(IonSystem system,
                          IonCatalog catalog,
                          IonWriterBaseImpl systemWriter,
                          boolean rootIsDatagram)
    {
        super(system.getSystemSymbolTable());
        _system = system;
        _catalog = catalog;

        assert systemWriter != null;
        _system_writer = systemWriter;
        _current_writer = systemWriter;
        _root_is_datagram = rootIsDatagram;
    }

    /**
     * Constructor for text and binary writers.
     *
     * @param system must not be null.
     * @param systemWriter must not be null.
     * @param catalog may be null.
     */
    protected IonWriterUser(IonSystem system, IonWriterBaseImpl systemWriter,
                            IonCatalog catalog,
                            boolean suppressIVM)
    {
        this(system, catalog, systemWriter, /* rootIsDatagram */ true);

        // TODO Why is root_is_datagram always true?  Can we not construct a
        // text writer that injects into an existing data stream?

        assert ! (systemWriter instanceof IonWriterUserTree);

        if (suppressIVM == false) {
            // TODO When do/don't we have a symtab?
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

    /**
     * Constructor for Tree writer.
     *
     * @param system must not be null.
     * @param catalog may be null.
     * @param systemWriter must not be null.
     * @param container must not be null.
     */
    protected IonWriterUser(IonSystem system,
                            IonCatalog catalog,
                            IonWriterSystemTree systemWriter,
                            IonValue container)
    {
        this(system, catalog, systemWriter,
             /* rootIsDatagram */ container.getType() == DATAGRAM);

        // Datagrams have an implicit initial IVM
        _previous_value_was_ivm = true;
    }

    //========================================================================

    @Override
    protected boolean has_annotation(String name, int id)
    {
        return _current_writer.has_annotation(name, id);
    }

    @Override
    public int getDepth()
    {
        // TODO this is wrong when the stream is diverted.
        return _current_writer.getDepth();
    }

    public boolean isInStruct()
    {
        return _current_writer.isInStruct();
    }


    public void flush() throws IOException
    {
        // TODO this doesn't behave correctly when the stream is diverted.
        _current_writer.flush();
    }

    public void close() throws IOException
    {
        try
        {
            try
            {
                if (getDepth() == 0) {
                    assert(_current_writer == _system_writer);
                    finish();
                }
            }
            finally
            {
                _current_writer.close();
            }
        }
        finally
        {
            _system_writer.close();
        }
    }

    @Override
    protected void writeAllBufferedData() throws IOException
    {
        if (symbol_table_being_collected()) {
            throw new IllegalStateException("you can't finish a user writer while a local symbol table value is being written");
        }
        assert(_current_writer == _system_writer);

        _system_writer.writeAllBufferedData();
    }

    @Override
    final void finishSystemContext()
    {
        _previous_value_was_ivm = false;
        _system_writer.finishSystemContext();
        super.finishSystemContext();
    }


    //========================================================================


    @Override
    final UnifiedSymbolTable inject_local_symbol_table() throws IOException
    {
        // no catalog since it doesn't matter as this is a
        // pure local table, with no imports
        // we let the system writer handle this work
        UnifiedSymbolTable symbols = _system_writer.inject_local_symbol_table();
        return symbols;
    }

    private boolean symbol_table_being_collected()
    {
//        assert _symbol_table_being_copied == (_current_writer != _system_writer);
        return (_current_writer != _system_writer);
    }

    /**
     * Diverts the data stream to a temporary tree writer which collects
     * local symtab data into an IonStruct from which we'll later construct a
     * {@link SymbolTable} instance.
     * <p>
     * Once the value image of the symbol table is complete (which
     * happens when the caller steps out of the containing struct)
     * the diverted stream is abandonded and the symbol table gets constructed.
     * <p>
     * If there was a makeSymbolTable(Reader) this copy might be,
     * at least partially, avoided.
     *
     * CSuver@
     */
    private void open_local_symbol_table_copy()
    {
        assert(! symbol_table_being_collected());

        _symbol_table_value = _system.newEmptyStruct();

        // WAS: _symbol_table_value.addTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE);
        // while the previous version did create a valid symbol table, it dropped
        // any extra annotations.  The local symbol table annotation will exist in the
        // annotation list, since it's presence is what got us here.
        assert(_current_writer.has_annotation(ION_SYMBOL_TABLE, ION_SYMBOL_TABLE_SID)
        );
        _symbol_table_value.setTypeAnnotations(getTypeAnnotations());

        _current_writer = new IonWriterSystemTree(_system, _catalog,
                                                  _symbol_table_value);
    }

    /**
     * Closes the diverted writer since the local symbol table
     * is complete (i.e. the struct is closed, on {@link #stepOut()}).
     */
    private void close_local_symbol_table_copy() throws IOException
    {
        assert(symbol_table_being_collected());

        // convert the struct we just wrote with the TreeWriter to a
        // local symbol table
        UnifiedSymbolTable symtab =
            makeNewLocalSymbolTable(_catalog, _symbol_table_value);

        _symbol_table_value = null;
        _current_writer     = _system_writer;

        // now make this symbol table the current symbol table
        this.setSymbolTable(symtab);
    }

    /**
     * @param new_symbols Must not be null.
     */
    abstract void set_symbol_table_helper(SymbolTable prev_symbols,
                                          SymbolTable new_symbols)
        throws IOException;

    @Override
    public final void setSymbolTable(SymbolTable symbols)
        throws IOException
    {
        SymbolTable prev = _symbol_table; // TODO this isn't always up-to-date!

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
            if (!_previous_value_was_ivm) {
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

    //========================================================================

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
        _current_writer.clearFieldName();
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
    public void setTypeAnnotationIds(int... annotationIds)
    {
        _current_writer.setTypeAnnotationIds(annotationIds);
    }

    @Override
    public void setTypeAnnotations(String... annotations)
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

    /**
     * To be called at the end of every value.
     * Sets {@link #_previous_value_was_ivm} to false, althought that may
     * be overwritten by the caller.
     */
    private final void finish_value()
    {
        _previous_value_was_ivm = false;
    }

    public void stepIn(IonType containerType) throws IOException
    {
        // see if it looks like we're starting a local symbol table
        if (IonType.STRUCT.equals(containerType)
         && getDepth() == 0
         && has_annotation(ION_SYMBOL_TABLE, ION_SYMBOL_TABLE_SID)
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
        if (symbol_table_being_collected() && _current_writer.getDepth() == 0)
        {
            close_local_symbol_table_copy();
        }
        else {
            _current_writer.stepOut();
        }
    }

    public void writeBlob(byte[] value, int start, int len) throws IOException
    {
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
        _current_writer.writeClob(value, start, len);
        finish_value();
    }

    @Override
    public void writeDecimal(BigDecimal value) throws IOException
    {
        _current_writer.writeDecimal(value);
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
        _current_writer.writeInt(value);
        finish_value();
    }

    public void writeNull(IonType type) throws IOException
    {
        _current_writer.writeNull(type);
        finish_value();
    }

    public void writeString(String value) throws IOException
    {
        _current_writer.writeString(value);
        finish_value();
    }

    public void writeSymbol(int symbolId) throws IOException
    {
        if (write_as_ivm(symbolId)) {
            if (! previousValueWasIvm()) {
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
        if (ION_1_0.equals(value) && write_as_ivm(ION_1_0_SID))
        {
            // TODO this swallows duplicate IVMs.  May not always be desired.
            if (! previousValueWasIvm()) {
                writeIonVersionMarker();
                // calls finish_value() for us
            }
        }
        else {
            _current_writer.writeSymbol(value);
            finish_value();
        }
    }

    private final boolean write_as_ivm(int sid)
    {
        // we only treat the $ion_1_0 symbol as an IVM
        // if we're at the top level in a datagram

        // TODO do we need to check whether _current_writer == _system_writer?
        // That's done in writeIonVersionMarker()

        boolean treat_as_ivm =
            (sid == ION_1_0_SID
             && _root_is_datagram
             && _current_writer.getDepth() == 0);
        return treat_as_ivm;
    }

    private final boolean previousValueWasIvm()
    {
        // we skip this IVM symbol if it is a redundant IVM
        // either at the beginning (which is common if this
        // is a binary writer) or at any other time.
        return _previous_value_was_ivm;
    }

    @Override
    public void writeIonVersionMarker() throws IOException
    {
        if (getDepth() != 0 || _root_is_datagram == false) {
            String message =
                "Ion Version Markers are only valid at the top level of a " +
                "data stream";
            throw new IllegalStateException(message);
        }
        assert(_current_writer == _system_writer);

        _current_writer.writeIonVersionMarker();
        _previous_value_was_ivm = true;

        setSymbolTable(_system.getSystemSymbolTable());

        finish_value();
        // we reset this after our call to finish since
        // finish sets this to false (which is the correct
        // behavior except here)  We could add a flag to
        // finish to tell it whether we were finishing a
        // IVM or not, but since this is the only time
        // it's the case it's easier to just patch it here
        _previous_value_was_ivm = true;
    }


    public void writeTimestamp(Timestamp value) throws IOException
    {
        _current_writer.writeTimestamp(value);
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
        _current_writer.writeBoolList(values);
        finish_value();
    }

    @Override
    public void writeIntList(byte[] values) throws IOException
    {
        _current_writer.writeIntList(values);
        finish_value();
    }

    @Override
    public void writeIntList(short[] values) throws IOException
    {
        _current_writer.writeIntList(values);
        finish_value();
    }

    @Override
    public void writeIntList(int[] values) throws IOException
    {
        _current_writer.writeIntList(values);
        finish_value();
    }

    @Override
    public void writeIntList(long[] values) throws IOException
    {
        _current_writer.writeIntList(values);
        finish_value();
    }

    @Override
    public void writeFloatList(float[] values) throws IOException
    {
        _current_writer.writeFloatList(values);
        finish_value();
    }

    @Override
    public void writeFloatList(double[] values) throws IOException
    {
        _current_writer.writeFloatList(values);
        finish_value();
    }

    @Override
    public void writeStringList(String[] values) throws IOException
    {
        _current_writer.writeStringList(values);
        finish_value();
    }
}
