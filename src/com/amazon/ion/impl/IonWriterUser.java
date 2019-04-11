/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;

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
 * <p>
 * This writer handles the symbol table processing and
 * provides default implementations for the list forms
 * of the write methods as often the list form is not
 * susceptible to optimization.
 * </p>
 *
 * <p>
 * This writer has a ({@link #_system_writer}) to which the actual data is
 * written, but data flows through the {@link #_current_writer} most of the
 * time.
 * </p>
 *
 * <p>
 * The critical responsibility here is the recognition of IVMs and local symbol
 * tables. When the user starts writing a local symtab, the stream is diverted
 * away from the {@link #_system_writer} into a temporary tree writer that
 * collects the symtab data into an {@link IonStruct} instance.  When that
 * struct is stepped-out, the diversion is stopped and the new
 * {@link SymbolTable} is installed.
 * </p>
 */
class IonWriterUser
    extends _Private_IonWriterBase
    implements _Private_IonWriter
{
    /** Factory for constructing the DOM of local symtabs. Not null. */
    private final ValueFactory _symtab_value_factory;

    /** Used to make correct local symbol tables. May be null. */
    private final IonCatalog _catalog;

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
    }


    /**
     * Constructor for text and binary writers.
     * <p>
     * POSTCONDITION: {@link IonWriterUser#_system_writer} ==
     * {@link #_current_writer} == systemWriter
     *
     * @param catalog
     *          may be null
     * @param symtabValueFactory
     *          must not be null
     * @param systemWriter
     *          must not be null
     * @param symtab
     *          must not be null
     */
    IonWriterUser(IonCatalog catalog,
                  ValueFactory symtabValueFactory,
                  IonWriterSystem systemWriter,
                  SymbolTable symtab)
    {
        this(catalog, symtabValueFactory, systemWriter);

        SymbolTable defaultSystemSymtab =
            systemWriter.getDefaultSystemSymtab();

        if (symtab.isLocalTable() || symtab != defaultSystemSymtab)
        {
            try {
                setSymbolTable(symtab);
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }

        assert _system_writer == _current_writer &&
               _system_writer == systemWriter;
    }

    //========================================================================

    public IonCatalog getCatalog()
    {
        return _catalog;
    }

    @Override
    int findAnnotation(String name) {
        return _current_writer.findAnnotation(name);
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
     * <p>
     * Diverts the data stream to a temporary tree writer which collects
     * local symtab data into an IonStruct from which we'll later construct a
     * {@link SymbolTable} instance.
     * </p>
     * <p>
     * Once the value image of the symbol table is complete (which
     * happens when the caller steps out of the containing struct)
     * the diverted stream is abandonded and the symbol table gets constructed.
     * </p>
     * <p>
     * If there was a makeSymbolTable(Reader) this copy might be,
     * at least partially, avoided.
     * </p>
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
                                                  _symbol_table_value,
                                                  null /* initialIvmHandling */);
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
        LocalSymbolTableAsStruct.Factory lstFactory =
            (LocalSymbolTableAsStruct.Factory)((_Private_ValueFactory)_symtab_value_factory).getLstFactory();
        SymbolTable symtab = lstFactory.newLocalSymtab(_catalog, _symbol_table_value);

        _symbol_table_value = null;
        _current_writer     = _system_writer;

        // now make this symbol table the current symbol table
        this.setSymbolTable(symtab);
    }


    @Override
    public final void setSymbolTable(SymbolTable symbols)
        throws IOException
    {
        if (symbols == null ||
            _Private_Utils.symtabIsSharedNotSystem(symbols))
        {
            String message =
                "symbol table must be local or system to be set, or reset";
            throw new IllegalArgumentException(message);
        }

        if (getDepth() > 0)
        {
            String message =
                "the symbol table cannot be set, or reset, while a container " +
                "is open";
            throw new IllegalStateException(message);
        }

        if (symbols.isSystemTable())
        {
            writeIonVersionMarker(symbols);
        }
        else
        {
            _system_writer.writeLocalSymtab(symbols);
        }
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

    public final void setFieldNameSymbol(SymbolToken name)
    {
        _current_writer.setFieldNameSymbol(name);
    }

    @Override
    public final boolean isFieldNameSet()
    {
        return _current_writer.isFieldNameSet();
    }


    //========================================================================
    // Annotations


    public void addTypeAnnotation(String annotation)
    {
        _current_writer.addTypeAnnotation(annotation);
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

    public void stepIn(IonType containerType) throws IOException
    {
        // see if it looks like we're starting a local symbol table
        if (containerType == IonType.STRUCT
            && _current_writer.getDepth() == 0
            && findAnnotation(ION_SYMBOL_TABLE) == 0)
        {
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
    }

    public void writeBool(boolean value) throws IOException
    {
        _current_writer.writeBool(value);
    }

    public void writeClob(byte[] value, int start, int len) throws IOException
    {
        _current_writer.writeClob(value, start, len);
    }

    @Override
    public void writeDecimal(BigDecimal value) throws IOException
    {
        _current_writer.writeDecimal(value);
    }

    public void writeFloat(double value) throws IOException
    {
        _current_writer.writeFloat(value);
    }

    @SuppressWarnings("cast")
    public void writeInt(int value) throws IOException
    {
        _current_writer.writeInt((long)value);
    }

    public void writeInt(long value) throws IOException
    {
        _current_writer.writeInt(value);
    }

    public void writeInt(BigInteger value) throws IOException
    {
        _current_writer.writeInt(value);
    }

    public void writeNull(IonType type) throws IOException
    {
        _current_writer.writeNull(type);
    }

    public void writeString(String value) throws IOException
    {
        _current_writer.writeString(value);
    }

    @Override
    final void writeSymbol(int symbolId) throws IOException
    {
        _current_writer.writeSymbol(symbolId);
    }

    public final void writeSymbol(String value) throws IOException
    {
        _current_writer.writeSymbol(value);
    }


    final void writeIonVersionMarker(SymbolTable systemSymtab)
        throws IOException
    {
        _current_writer.writeIonVersionMarker(systemSymtab);
    }

    @Override
    public final void writeIonVersionMarker()
        throws IOException
    {
        _current_writer.writeIonVersionMarker();
    }


    public void writeTimestamp(Timestamp value) throws IOException
    {
        _current_writer.writeTimestamp(value);
    }
}
