// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.impl._Private_Utils.initialSymtab;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.ValueFactory;
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
    extends IonWriterBaseImpl
    implements _Private_IonWriter
{
    /** Factory for constructing the DOM of local symtabs. Not null. */
    private final ValueFactory _symtab_value_factory;

    /** Used to make correct local symbol tables. May be null. */
    private final IonCatalog _catalog;

    /**
     * Indicates whether the (immediately) previous value was an IVM.
     * This is cleared by {@link #finish_value()}.
     */
          boolean _previous_value_was_ivm; // TODO push into system level
    final boolean _root_is_datagram;

    /**
     * The underlying system writer that writing the raw format (text, binary,
     * or ion values).  Not null.
     */
    final IonWriterSystem _system_writer;

    /**
     * This will be either our {@link #_system_writer} or a symbol table writer
     * depending on whether we're diverting the user values to a
     * local symbol table ... or not.
     * Not null.
     */
    IonWriterSystem _current_writer;

    /**
     * While the stream is diverted to collect local symtab data, it is
     * being written to this instance.
     * This is null IFF {@link #_current_writer} == {@link #_system_writer}.
     */
    private IonStruct _symbol_table_value;



    /**
     * Base constructor.
     * <p>
     * POSTCONDITION: {@link IonWriterUser#_system_writer} ==
     * {@link #_current_writer} == systemWriter
     *
     * @param catalog may be null.
     * @param symtabValueFactory must not be null.
     * @param systemWriter must not be null.
     */
    IonWriterUser(IonCatalog catalog,
                  ValueFactory symtabValueFactory,
                  IonWriterSystem systemWriter)
    {
        _symtab_value_factory = symtabValueFactory;
        _catalog = catalog;

        assert systemWriter != null;
        _system_writer = systemWriter;
        _current_writer = systemWriter;
        _root_is_datagram = systemWriter.getDepth() == 0;
    }


    /**
     * Constructor for text and binary writers.
     * <p>
     * POSTCONDITION: {@link IonWriterUser#_system_writer} ==
     * {@link #_current_writer} == systemWriter
     *
     * @param catalog may be null.
     * @param symtabValueFactory must not be null.
     * @param systemWriter must not be null.
     */
    IonWriterUser(IonCatalog catalog,
                  ValueFactory symtabValueFactory,
                  IonWriterSystem systemWriter,
                  SymbolTable... imports)
    {
        this(catalog, symtabValueFactory, systemWriter);

        SymbolTable defaultSystemSymtab =
            systemWriter.getDefaultSystemSymtab();

        SymbolTable initialSymtab =
            initialSymtab(symtabValueFactory, defaultSystemSymtab, imports);
        if (initialSymtab.isLocalTable() || initialSymtab != defaultSystemSymtab)
        {
            try {
                setSymbolTable(initialSymtab);
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }
    }

    //========================================================================

    public IonCatalog getCatalog()
    {
        return _catalog;
    }


    @Override
    boolean has_annotation(String name, int id)
    {
        return _current_writer.has_annotation(name, id);
    }

    @Override
    public int getDepth()
    {
        return _current_writer.getDepth();
    }

    public boolean isInStruct()
    {
        return _current_writer.isInStruct();
    }


    public void flush() throws IOException
    {
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


    public final void finish() throws IOException
    {
        if (symbol_table_being_collected()) {
            throw new IllegalStateException(ERROR_FINISH_NOT_AT_TOP_LEVEL);
        }

        _system_writer.finish();

        _previous_value_was_ivm = false;
    }

    //========================================================================

    SymbolTable activeSystemSymbolTable()
    {
        return getSymbolTable().getSystemSymbolTable();
    }


    private boolean symbol_table_being_collected()
    {
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

        _symbol_table_value = _symtab_value_factory.newEmptyStruct();

        SymbolToken[] anns = _system_writer.getTypeAnnotationSymbols();
        _system_writer.clearAnnotations();

        _symbol_table_value.setTypeAnnotationSymbols(anns);

        _current_writer = new IonWriterSystemTree(activeSystemSymbolTable(),
                                                  _catalog,
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
        SymbolTable symtab =
            _Private_Utils.newLocalSymtab(activeSystemSymbolTable(),
                                          _catalog,
                                          _symbol_table_value);

        _symbol_table_value = null;
        _current_writer     = _system_writer;

        // now make this symbol table the current symbol table
        this.setSymbolTable(symtab);
    }


    @Override
    public final void setSymbolTable(SymbolTable symbols)
        throws IOException
    {
        if (symbols == null || _Private_Utils.symtabIsSharedNotSystem(symbols)) {
            throw new IllegalArgumentException("symbol table must be local or system to be set, or reset");
        }

        if (symbols.isSystemTable())
        {
            writeIonVersionMarker(symbols);
            return;
        }

        if (getDepth() > 0) {
            throw new IllegalStateException("the symbol table cannot be set, or reset, while a container is open");
        }

        _system_writer.writeLocalSymtab(symbols);
    }


    public final SymbolTable getSymbolTable()
    {
        SymbolTable symbols = _system_writer.getSymbolTable();
        return symbols;
    }


    @Override
    final String assumeKnownSymbol(int sid)
    {
        return _system_writer.assumeKnownSymbol(sid);
    }

    //========================================================================
    // Field names


    public final void setFieldName(String name)
    {
        _current_writer.setFieldName(name);
    }

    @Deprecated
    public final void setFieldId(int id)
    {
        _current_writer.setFieldId(id);
    }

    public final void setFieldNameSymbol(SymbolToken name)
    {
        _current_writer.setFieldNameSymbol(name);
    }

    @Override
    final boolean isFieldNameSet()
    {
        return _current_writer.isFieldNameSet();
    }


    //========================================================================
    // Annotations


    public void addTypeAnnotation(String annotation)
    {
        _current_writer.addTypeAnnotation(annotation);
    }

    public void addTypeAnnotationId(int annotationId)
    {
        _current_writer.addTypeAnnotationId(annotationId);
    }

    public void setTypeAnnotationIds(int... annotationIds)
    {
        _current_writer.setTypeAnnotationIds(annotationIds);
    }

    public void setTypeAnnotations(String... annotations)
    {
        _current_writer.setTypeAnnotations(annotations);
    }

    public void setTypeAnnotationSymbols(SymbolToken... annotations)
    {
        _current_writer.setTypeAnnotationSymbols(annotations);
    }

    @Override
    String[] getTypeAnnotations()
    {
        return _current_writer.getTypeAnnotations();
    }

    @Override
    int[] getTypeAnnotationIds()
    {
        return _current_writer.getTypeAnnotationIds();
    }

    final SymbolToken[] getTypeAnnotationSymbols()
    {
        return _current_writer.getTypeAnnotationSymbols();
    }

    /**
     * To be called at the end of every value.
     * Sets {@link #_previous_value_was_ivm} to false,
     * althought they may be overwritten by the caller.
     */
    private final void finish_value()
    {
        _previous_value_was_ivm = false;
    }

    public void stepIn(IonType containerType) throws IOException
    {
        // see if it looks like we're starting a local symbol table
        if (containerType == IonType.STRUCT
            && _root_is_datagram
            && _current_writer.getDepth() == 0
            && has_annotation(ION_SYMBOL_TABLE, ION_SYMBOL_TABLE_SID))
        {
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
        if (symbol_table_being_collected() && _current_writer.getDepth() == 1)
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

    @SuppressWarnings("cast")
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

    @Deprecated
    public final void writeSymbol(int symbolId) throws IOException
    {
        if (write_as_ivm(symbolId)) {
            if (! _previous_value_was_ivm) {
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

    public final void writeSymbol(String value) throws IOException
    {
        if (ION_1_0.equals(value) && write_as_ivm(ION_1_0_SID))
        {
            // TODO make sure to get the right symtab, default may differ.
            writeIonVersionMarker();
            // calls finish_value() for us
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
        boolean treat_as_ivm =
            (sid == ION_1_0_SID
             && _root_is_datagram
             && _current_writer.getDepth() == 0);
        return treat_as_ivm;
    }


    final void writeIonVersionMarker(SymbolTable systemSymtab)
        throws IOException
    {
        if (getDepth() != 0 || _root_is_datagram == false) {
            String message =
                "Ion Version Markers are only valid at the top level of a " +
                "data stream";
            throw new IllegalStateException(message);
        }
        assert(_current_writer == _system_writer);

        if (_previous_value_was_ivm)
        {
            // TODO This always minimizes adjacent IVMs; should be optional.
            // TODO What if previous IVM is different from given system?
            assert _system_writer.getSymbolTable() == _system_writer._default_system_symbol_table;
        }
        else
        {
            _system_writer.writeIonVersionMarker(systemSymtab);
            _previous_value_was_ivm = true;
        }

        finish_value();
        // We reset these since finish sets them to false (which is the correct
        // behavior except here).  We could add a flag to finish to tell it
        // whether we were finishing a IVM or not, but since this is the only
        // time it's the case it's easier to just patch it here.
        _previous_value_was_ivm = true;
    }

    @Override
    public final void writeIonVersionMarker()
        throws IOException
    {
        writeIonVersionMarker(_system_writer._default_system_symbol_table);
    }


    public void writeTimestamp(Timestamp value) throws IOException
    {
        _current_writer.writeTimestamp(value);
        finish_value();
    }
}
