// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.impl.IonImplUtils.addAllNonNull;
import static com.amazon.ion.impl.IonWriterFactory.makeWriter;
import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;
import static com.amazon.ion.util.IonTextUtils.printString;

import com.amazon.ion.IonBlob;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.UnsupportedIonVersionException;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonReaderFactoryX;
import com.amazon.ion.impl.IonReaderWriterPrivate;
import com.amazon.ion.impl.IonScalarConversionsX.CantConvertException;
import com.amazon.ion.impl.IonSystemPrivate;
import com.amazon.ion.impl.IonWriterBaseImpl;
import com.amazon.ion.impl.IonWriterBinaryCompatibility;
import com.amazon.ion.impl.IonWriterFactory;
import com.amazon.ion.impl.IonWriterUserBinary;
import com.amazon.ion.impl.IonWriterUserText.TextOptions;
import com.amazon.ion.impl.SystemValueIterator;
import com.amazon.ion.impl.UnifiedSymbolTable;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
public final class IonSystemLite
    extends ValueFactoryLite
    implements IonSystemPrivate, IonContext
{
    private static int DEFAULT_CONTEXT_FREE_LIST_SIZE = 1000;

    private final UnifiedSymbolTable _system_symbol_table = UnifiedSymbolTable.makeSystemSymbolTable(this, 1);

    /** Not null. */
    private       IonCatalog         _catalog;
    private       ValueFactoryLite   _value_factory;
    private final IonLoader          _loader;


    /**
     * @param catalog must not be null.
     */
    public IonSystemLite(IonCatalog catalog)
    {
        this(catalog, DEFAULT_CONTEXT_FREE_LIST_SIZE);
    }

    /**
     * @param catalog must not be null.
     * @param context_free_list_size
     */
    private IonSystemLite(IonCatalog catalog, int context_free_list_size)
    {
        assert catalog != null;

        set_context_free_list_max(context_free_list_size);

        _catalog = catalog;
        _loader = new IonLoaderLite(this, catalog);

        // whacked but I'm not going to figure this out right now
        _value_factory = this;
        _value_factory.set_system(this);
    }

    /**
     * IonSystem Methods
     */

    @SuppressWarnings("unchecked")
    public <T extends IonValue> T clone(T value) throws IonException
    {
        // Use "fast clone" when the system is the same.
        if (value.getSystem() == this)
        {
            return (T) value.clone();
        }

        IonDatagram datagram = newDatagram();
        IonWriter writer = IonWriterFactory.makeWriter(datagram);
        IonReader reader = IonReaderFactoryX.makeSystemReader(value);

        try {
            writer.writeValues(reader);
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        if (value instanceof IonDatagram)
        {
            return (T) datagram;
        }

        IonValue copy = datagram.get(0);
        copy.removeFromContainer();
        return (T) copy;
    }

    public IonCatalog getCatalog()
    {
        return _catalog;
    }

    public synchronized IonLoader getLoader()
    {
        return _loader;
    }

    public IonLoader newLoader()
    {
        return new IonLoaderLite(this, _catalog);
    }

    public IonLoader newLoader(IonCatalog catalog)
    {
        if (catalog == null) catalog = getCatalog();
        return new IonLoaderLite(this, catalog);
    }

    public final SymbolTable getSystemSymbolTable()
    {
        return _system_symbol_table;
    }

    public SymbolTable getSystemSymbolTable(String ionVersionId)
        throws UnsupportedIonVersionException
    {
        if (!UnifiedSymbolTable.ION_1_0.equals(ionVersionId)) {
            throw new UnsupportedIonVersionException(ionVersionId);
        }
        return getSystemSymbolTable();
    }

    public Iterator<IonValue> iterate(Reader ionText)
    {
        IonReader reader = IonReaderFactoryX.makeReader(this, ionText);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(InputStream ionData)
    {
        IonReader reader = IonReaderFactoryX.makeReader(this, ionData);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(String ionText)
    {
        IonReader reader = IonReaderFactoryX.makeReader(this, ionText);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(byte[] ionData)
    {
        IonReader reader = IonReaderFactoryX.makeReader(this, ionData);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    @Deprecated
    public com.amazon.ion.IonBinaryWriter newBinaryWriter()
    {
        IonWriterBinaryCompatibility.User writer =
            new IonWriterBinaryCompatibility.User(this, _catalog);
        return writer;
    }

    @Deprecated
    public com.amazon.ion.IonBinaryWriter newBinaryWriter(SymbolTable... imports)
    {
        UnifiedSymbolTable symbols =
            makeNewLocalSymbolTable(this, this.getSystemSymbolTable(), imports);
        IonWriterBinaryCompatibility.User writer =
            new IonWriterBinaryCompatibility.User(this, _catalog);
        try {
            writer.setSymbolTable(symbols);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return writer;
    }


    public IonWriter newBinaryWriter(OutputStream out, SymbolTable... imports)
    {
        IonWriterUserBinary writer =
            IonWriterFactory.makeWriter(this, getCatalog(), out, imports);
        return writer;
    }

    public IonWriter newTextWriter(Appendable out)
    {
        TextOptions options = new TextOptions(false /* prettyPrint */, true /* printAscii */, true /* filterOutSymbolTables */);
        IonWriter userWriter = newTextWriter(out, options);
        return userWriter;
    }

    public IonWriter newTextWriter(Appendable out, boolean pretty)
    {
        TextOptions options = new TextOptions(pretty /* prettyPrint */, true /* printAscii */, true /* filterOutSymbolTables */);
        IonWriter userWriter = newTextWriter(out, options);
        return userWriter;
    }

    public IonWriterBaseImpl newTextWriter(Appendable out, TextOptions options)
    {
        IonWriterBaseImpl userWriter = makeWriter(this, out, options);
        return userWriter;
    }

    public IonWriter newTextWriter(Appendable out, SymbolTable... imports)
        throws IOException
    {
        TextOptions options = new TextOptions(false /* prettyPrint */, true /* printAscii */, true /* filterOutSymbolTables */);
        IonWriter writer = newTextWriter(out, options, imports);
        return writer;
    }

    public IonWriter newTextWriter(Appendable out, TextOptions options, SymbolTable... imports)
        throws IOException
    {
        UnifiedSymbolTable lst = newLocalSymbolTable(imports);
        IonWriterBaseImpl writer = newTextWriter(out, options);
        writer.setSymbolTable(lst);
        return writer;
    }

    public IonWriter newTextWriter(OutputStream out)
    {
        TextOptions options = new TextOptions(false /* prettyPrint */, true /* printAscii */, true /* filterOutSymbolTables */);
        IonWriter userWriter = newTextWriter(out, options);
        return userWriter;
    }

    public IonWriterBaseImpl newTextWriter(OutputStream out, TextOptions options)
    {
        IonWriterBaseImpl userWriter = makeWriter(this, out, options);
        return userWriter;
    }

    public IonWriter newTextWriter(OutputStream out, SymbolTable... imports)
        throws IOException
    {
        TextOptions options = new TextOptions(false /* prettyPrint */, true /* printAscii */, true /* filterOutSymbolTables */);
        IonWriter writer = newTextWriter(out, options, imports);
        return writer;
    }

    public IonWriter newTextWriter(OutputStream out, TextOptions options, SymbolTable... imports)
        throws IOException
    {
        UnifiedSymbolTable lst = newLocalSymbolTable(imports);
        IonWriterBaseImpl writer = newTextWriter(out, options);
        writer.setSymbolTable(lst);
        return writer;
    }

    public UnifiedSymbolTable newLocalSymbolTable(SymbolTable... imports)
    {
        UnifiedSymbolTable st =
            makeNewLocalSymbolTable(this, getSystemSymbolTable(), imports);
        return st;
    }

    public UnifiedSymbolTable newSharedSymbolTable(IonStruct ionRep)
    {
        UnifiedSymbolTable st = UnifiedSymbolTable.makeNewSharedSymbolTable(ionRep);
        return st;
    }

    public UnifiedSymbolTable newSharedSymbolTable(IonReader reader)
    {
        UnifiedSymbolTable st = UnifiedSymbolTable.makeNewSharedSymbolTable(this, reader, false);
        return st;
    }

    public UnifiedSymbolTable newSharedSymbolTable(IonReader reader, boolean isOnStruct)
    {
        UnifiedSymbolTable st = UnifiedSymbolTable.makeNewSharedSymbolTable(this, reader, isOnStruct);
        return st;
    }

    public UnifiedSymbolTable newSharedSymbolTable(String name,
                                                   int version,
                                                   Iterator<String> newSymbols,
                                                   SymbolTable... imports)
    {
        // TODO streamline to avoid making this collection
        ArrayList<String> syms = new ArrayList<String>();

        if (version > 1)
        {
            int priorVersion = version - 1;
            SymbolTable prior = _catalog.getTable(name, priorVersion);
            if (prior == null || prior.getVersion() != priorVersion)
            {
                String message =
                    "Catalog does not contain symbol table " +
                    printString(name) + " version " + priorVersion +
                    " required to create version " + version;
                throw new IonException(message);
            }

            // FIXME ION-188 This is wrong, we need to retain the exact
            // symbols from the prior version.
            addAllNonNull(syms, prior.iterateDeclaredSymbolNames());
        }

        for (SymbolTable imported : imports)
        {
            addAllNonNull(syms, imported.iterateDeclaredSymbolNames());
        }

        addAllNonNull(syms, newSymbols);

        UnifiedSymbolTable st = UnifiedSymbolTable.makeNewSharedSymbolTable(this, name, version, syms.iterator());

        return st;
    }

    public IonValue newValue(IonReader reader)
    {
        IonValue value = load_value(reader);
        return value;
    }

    private IonValueLite load_value(IonReader reader)
    {
        IonValueLite value = load_value_helper(reader);
        if (value == null) {
            return null;
        }
        if (value._isSymbolPresent()) {
            value.populateSymbolValues(null);
        }
        return value;
    }

    private IonValueLite load_value_helper(IonReader reader)
    {
        boolean symbol_encountered = false;

        IonType t = reader.getType();
        if (t == null) {
            return null;
        }
        IonValueLite v;
        if (reader.isNullValue()) {
            v = newNull(t);
        }
        else {
            switch (t) {
            case BOOL:
                v = newBool(reader.booleanValue());
                break;
            case INT:
                try {
                    v = newInt(reader.longValue());
                } catch (CantConvertException e) {
                    v = newInt(reader.bigIntegerValue());
                }
                break;
            case FLOAT:
                v = newFloat(reader.doubleValue());
                break;
            case DECIMAL:
                v = newDecimal(reader.decimalValue());
                break;
            case TIMESTAMP:
                v = newTimestamp(reader.timestampValue());
                break;
            case SYMBOL:
                v = newSymbol(reader.stringValue());
                symbol_encountered = true;
                break;
            case STRING:
                v = newString(reader.stringValue());
                break;
            case CLOB:
                v = newClob(reader.newBytes());
                break;
            case BLOB:
                v = newBlob(reader.newBytes());
                break;
            case LIST:
                v = newEmptyList();
                break;
            case SEXP:
                v = newEmptySexp();
                break;
            case STRUCT:
                v = newEmptyStruct();
                break;
            default: throw new IonException("unexpected type encountered reading value: "+t.toString());
            }
        }
        if (reader.isInStruct()) {
            String fieldName = reader.getFieldName();
            v.setFieldName(fieldName);
            symbol_encountered = true;
        }
        String[] uta = reader.getTypeAnnotations();
        if (uta != null && uta.length > 0) {
            for (int ii=0; ii<uta.length; ii++) {
                v.addTypeAnnotation(uta[ii]);
            }
            symbol_encountered = true;
        }
        if (!reader.isNullValue()) {
            switch (t) {
            case BOOL:
            case INT:
            case FLOAT:
            case DECIMAL:
            case TIMESTAMP:
            case SYMBOL:
            case STRING:
            case CLOB:
            case BLOB:
                break;
            case LIST:
            case SEXP:
            case STRUCT:
                // we have to load the children after we grabbed the
                // fieldname and annotations off of the parent container
                if (load_children((IonContainerLite)v, reader)) {
                    symbol_encountered = true;
                }
                break;
            default:
                throw new IonException("unexpected type encountered reading value: "+t.toString());
            }
        }
        if (symbol_encountered) {
            v._isSymbolPresent(true);
        }
        return v;
    }

    private boolean load_children(IonContainerLite container, IonReader reader)
    {
        boolean contains_symbol = false;
        //final boolean in_struct = (container instanceof IonStruct);
        //IonStruct struct_container = null;
        //if (in_struct) {
        //    struct_container = (IonStruct)container;
        //}
        reader.stepIn();
        for (;;) {
            IonType t = reader.next();
            if (t == null) {
                break;
            }
            IonValueLite child = load_value_helper(reader);
            //if (in_struct) {
            //    struct_container.add(child.getFieldName(), child);
            //}
            //else {
                container.add(child);
            //}
            if (child._isSymbolPresent()) {
                contains_symbol = true;
            }
        }
        reader.stepOut();
        return contains_symbol;
    }

    IonValueLite newValue(IonType valueType)
    {
        IonValueLite v;

        if (valueType == null) {
            throw new IllegalArgumentException("the value type must be specified");
        }
        switch (valueType) {
        case NULL:          v = newNull();          break;
        case BOOL:          v = newNullBool();      break;
        case INT:           v = newNullInt();       break;
        case FLOAT:         v = newNullFloat();     break;
        case DECIMAL:       v = newNullDecimal();   break;
        case TIMESTAMP:     v = newNullTimestamp(); break;
        case SYMBOL:        v = newNullSymbol();    break;
        case STRING:        v = newNullString();    break;
        case CLOB:          v = newNullClob();      break;
        case BLOB:          v = newNullBlob();      break;
        case LIST:          v = newEmptyList();     break;
        case SEXP:          v = newEmptySexp();     break;
        case STRUCT:        v = newEmptyStruct();   break;
        default: throw new IonException("unexpected type encountered reading value: "+valueType);
        }

        return v;
    }


    public IonWriter newWriter(IonContainer container)
    {
        IonWriter writer = IonWriterFactory.makeWriter(container);
        return writer;
    }

    public void setCatalog(IonCatalog catalog)
    {
        if (catalog == null) throw new NullPointerException();
        this._catalog = catalog;
    }

    public IonValue singleValue(String ionText)
    {
        Iterator<IonValue> it = this.iterate(ionText);
        IonValue value = null;
        try {
            value = it.next();
        }
        catch (NoSuchElementException e) {
            // value is already null, just
            // fall through where we'll throw
            // an IonException
        }
        if (value == null || it.hasNext()) {
            throw new IonException("not a single value");
        }
        return value;
    }

    public IonValue singleValue(byte[] ionData)
    {
        Iterator<IonValue> it = this.iterate(ionData);
        IonValue value = null;
        try {
            value = it.next();
        }
        catch (NoSuchElementException e) {
            // value is already null, just
            // fall through where we'll throw
            // an IonException
        }
        if (value == null || it.hasNext()) {
            throw new IonException("not a single value");
        }
        return value;
    }

    /**
     * IonContext methods
     */
    public SymbolTable getLocalSymbolTable(IonValueLite child)
    {
        // if this request makes it up to the system
        // it means there was no local symbol table
        // defined on the top level of the value.
        // So we'll make one there.
        SymbolTable local = this.newLocalSymbolTable();
        IonContext context = child.getContext();
        if (context == this) {
            context = new IonConcreteContext(this);
            context.setParentThroughContext(child, this);
            context.setSymbolTableOfChild(local, child);
        }
        child.setContext(context);
        return local;
    }

    public IonContainerLite getParentThroughContext()
    {
        return null;
    }

    public SymbolTable getSymbolTable()
    {
        return getSystemSymbolTable();
    }

    public SymbolTable getAssignedSymbolTable()
    {
        return null;
    }

    public SymbolTable getContextSymbolTable()
    {
        return null;
    }

    public IonSystem getSystem()
    {
        return this;
    }
    public IonSystemLite getSystemLite()
    {
        return this;
    }

    public void setParentThroughContext(IonValueLite child, IonContext context)
    {
        assert(child != null);
        child.setContext(context);
    }

    public void setSymbolTableOfChild(SymbolTable symbols, IonValueLite child)
    {
        assert(child != null);
        if (UnifiedSymbolTable.isAssignableTable(symbols) == false) {
            throw new IllegalArgumentException("shared symbol tables cannot be set as a current symbol table");
        }
        if (child.getAssignedSymbolTable() == symbols) {
            return;
        }
        IonContext context = child.getContext();
        if (context == this) {
            context = allocateConcreteContext();
            context.setParentThroughContext(child, this);
        }
        else {
            assert(context instanceof IonConcreteContext);
        }
        context.setSymbolTableOfChild(symbols, child);
    }

    private static final IonConcreteContext[] EMPTY_CONTEXT_ARRAY = new IonConcreteContext[0];
    private int                  _free_count;
    private IonConcreteContext[] _free_contexts;
    protected final void set_context_free_list_max(int size) {
        if (size < 1) {
            _free_contexts = EMPTY_CONTEXT_ARRAY;
        }
        else if (_free_contexts == null || size != _free_contexts.length) {
            IonConcreteContext[] temp = new IonConcreteContext[size];
            if (_free_count > 0) {
                if (_free_count > size) {
                    _free_count = size;
                }
                System.arraycopy(_free_contexts, 0, temp, 0, _free_count);
            }
            _free_contexts = temp;
        }
    }
    protected final IonConcreteContext allocateConcreteContext()
    {
        IonConcreteContext context = null;
        if (_free_count > 0) {
            synchronized (this._free_contexts) {
                if (_free_count > 0) {
                    _free_count--;
                    context = _free_contexts[_free_count];
                    _free_contexts[_free_count] = null;
                }
            }
        }
        if (context == null) {
            context = new IonConcreteContext(this);
        }
        return context;
    }
    protected final void releaseConcreteContext(IonConcreteContext context)
    {
        if (_free_contexts.length > 0) {
            synchronized (this._free_contexts) {
                if (_free_count < _free_contexts.length) {
                    _free_contexts[_free_count] = context;
                    _free_count++;
                }
            }
        }
        context.clear();
    }

    protected IonSymbolLite newSystemIdSymbol(String ionVersionMarker)
    {
        if (!UnifiedSymbolTable.ION_1_0.equals(ionVersionMarker)) {
            throw new IllegalArgumentException("name isn't an ion version marker");
        }
        IonSymbolLite ivm = new IonSymbolLite(this, false);
        ivm.setValue(ionVersionMarker);
        ivm.setIsIonVersionMarker(true);

        return ivm;
    }

    static class ReaderIterator
        implements SystemValueIterator, Iterator<IonValue>, Closeable
    {
        private final IonReader        _reader;
        private final IonSystemLite    _system;
        private       IonType          _next;
        private       SymbolTable      _previous_symbols; // we're faking the datagram behavior with this


        // TODO: do we need catalog, import support for this?
        //       we are creating ion values which might want
        //       a local symbol table in some cases.
        protected ReaderIterator(IonSystemLite system, IonReader reader)
        {
            _reader = reader;
            _system = system;
            _previous_symbols = _system.getSystemSymbolTable();
        }

        public SymbolTable getSymbolTable() {
            return _reader.getSymbolTable();
        }

        public boolean hasNext()
        {
            if (_next == null) {
                _next = _reader.next();
            }
            return (_next != null);
        }

        public IonValue next()
        {
            if (!hasNext()) {
                // IterationTest.testSimpleIteration() wants this
                throw new NoSuchElementException();
                // LoaderTest.testSingleValue is expecting null so
                // IonSystemLite.singleValue can throw an IonException - or
                // should we change testSingleValue ??
                // return null;
            }

            // make an ion value from our reader
            IonValueLite value = _system.load_value(_reader);

            // we've used up the value now, force a _reader._next() the next time through
            _next = null;

            // now set the values symbol table if necessary
            // this is the symbol table that was passed up
            // by the reader getting to this value
            SymbolTable symbols = get_values_local_symbol_table();

            // we still have to create a local symbol table if
            // we hit something with symbols and the current
            // symbol table is the system symbol table
            if (value._isSymbolPresent()) {
                if (symbols == null || !symbols.isLocalTable()) {
                    symbols = value.getContext().getLocalSymbolTable(value);
                }
            }

            // if we have a symbol table for the value, apply it
            if (symbols != null) {
                value.setSymbolTable(symbols);
                _previous_symbols = symbols;
            }

            return value;
        }

        private SymbolTable get_values_local_symbol_table()
        {
            SymbolTable symbols;

            if (_reader instanceof IonReaderWriterPrivate) {
                symbols = ((IonReaderWriterPrivate)_reader).pop_passed_symbol_table();
                if (symbols != null) {
                    _previous_symbols = symbols;
                }
            }

            return _previous_symbols;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public void close() throws IOException
        {
            // TODO _reader.close();
        }

        //
        //  Bonus methods to support old SystemReader iteration interface
        //
        public boolean canSetLocalSymbolTable()
        {
            return false;
        }

        public boolean currentIsHidden()
        {
            IonType t = _reader.getType();
            if (t == null) {
                return false;
            }
            switch(t) {
            case SYMBOL:
                String symbol = _reader.stringValue();
                if (UnifiedSymbolTable.ION_1_0.equals(symbol)) {
                    return true;
                }
                break;
            case STRUCT:
                String [] annotations = _reader.getTypeAnnotations();
                for (int ii=0; ii<annotations.length; ii++) {
                    if (UnifiedSymbolTable.ION_SYMBOL_TABLE.equals(annotations[ii])) {
                        return true;
                    }
                }
                break;
            default:
                break;
            }
            return false;
        }

        public BufferManager getBuffer()
        {
            return null;
        }

        public IonCatalog getCatalog()
        {
            // TODO: get catalog from reader
            return _system.getCatalog();
        }

        public SymbolTable getLocalSymbolTable()
        {
            return _reader.getSymbolTable();
        }

        public IonSystem getSystem()
        {
            return _system;
        }

        public void resetBuffer()
        {
            return;
        }

        public void setLocalSymbolTable(SymbolTable symbolTable)
        {
            throw new UnsupportedOperationException();
        }
    }

    public IonBlob newBlob()
    {
        return newNullBlob();
    }

    public IonBool newBool()
    {
        return newNullBool();
    }

    public IonClob newClob()
    {
        return newNullClob();
    }

    public IonTimestamp newCurrentUtcTimestamp()
    {
        IonTimestampLite result = super.newNullTimestamp();
        result.setCurrentTimeUtc();
        return result;
    }

    public IonDatagram newDatagram()
    {
        IonCatalog catalog = this.getCatalog();
        IonDatagram dg = newDatagram(catalog);
        return dg;
    }

    public IonDatagramLite newDatagram(IonCatalog catalog)
    {
        if (catalog == null) catalog = getCatalog();
        IonDatagramLite dg = new IonDatagramLite(this, catalog);
        return dg;
    }

    public IonDatagram newDatagram(IonValue initialChild)
    {
        IonDatagram dg = newDatagram(null, initialChild);
        return dg;
    }

    public IonDatagram newDatagram(IonCatalog catalog, IonValue initialChild)
    {
        IonDatagram dg = newDatagram(catalog);

        if (initialChild != null) {
            if (initialChild.getSystem() != this) {
                throw new IonException("this Ion system can't mix with instances from other system impl's");
            }

            // This is an API anomaly but it's documented so here we go.
            if (initialChild.getContainer() != null) {
                initialChild = clone(initialChild);
            }

            // This will fail if initialChild instanceof IonDatagram:
            dg.add(initialChild);
        }

        assert dg.getSystem() == this;
        return dg;
    }

    public IonDatagram newDatagram(SymbolTable... imports)
    {
        IonDatagram dg = newDatagram(null, imports);
        return dg;
    }

    public IonDatagram newDatagram(IonCatalog catalog, SymbolTable... imports)
    {
        UnifiedSymbolTable symbols =
            makeNewLocalSymbolTable(this, this.getSystemSymbolTable(), imports);
        IonDatagramLite dg = newDatagram(catalog);
        dg.setSymbolTable(symbols);
        return dg;
    }

    public IonDecimal newDecimal()
    {
        return newNullDecimal();
    }

    public IonFloat newFloat()
    {
        return newNullFloat();
    }

    public IonInt newInt()
    {
        return newNullInt();
    }

    public IonList newList()
    {
        return newNullList();
    }

    public IonTextReader newReader(String ionText)
    {
        IonTextReader reader = IonReaderFactoryX.makeReader(this, ionText);
        return reader;
    }

    public IonReader newReader(byte[] ionData)
    {
        IonReader reader = IonReaderFactoryX.makeReader(this, getCatalog(), ionData, 0, ionData.length);
        return reader;
    }

    public IonReader newReader(IonCatalog catalog, byte[] ionData)
    {
        IonReader reader = newReader(catalog, ionData, 0, ionData.length);
        return reader;
    }

    public IonReader newReader(byte[] ionData, int offset, int len)
    {
        IonReader reader = newReader(getCatalog(), ionData, 0, ionData.length);
        return reader;
    }

    public IonReader newReader(IonCatalog catalog, byte[] ionData, int offset, int len)
    {
        if (catalog == null) catalog = getCatalog();
        IonReader reader = IonReaderFactoryX.makeReader(this, catalog, ionData, offset, len);
        return reader;
    }

    public IonReader newReader(InputStream ionData)
    {
        IonReader reader = newReader(this.getCatalog(), ionData);
        return reader;
    }

    public IonReader newReader(IonCatalog catalog, InputStream ionData)
    {
        if (catalog == null) catalog = getCatalog();
        IonReader reader = IonReaderFactoryX.makeReader(this, catalog, ionData);
        return reader;
    }

    public IonReader newReader(IonValue value)
    {
        IonReader reader = newReader(this.getCatalog(), value);
        return reader;
    }

    public IonReader newReader(IonCatalog catalog, IonValue value)
    {
        if (catalog == null) catalog = getCatalog();
        IonReader reader = IonReaderFactoryX.makeReader(this, catalog, value);
        return reader;
    }

    public IonSexp newSexp()
    {
        return newNullSexp();
    }

    public IonString newString()
    {
        return newNullString();
    }

    public IonStruct newStruct()
    {
        return newNullStruct();
    }

    public IonSymbol newSymbol()
    {
        return newNullSymbol();
    }

    public IonTimestamp newTimestamp()
    {
        return newNullTimestamp();
    }

    public IonTimestamp newUtcTimestamp(Date utcDate)
    {
        IonTimestamp result = newTimestamp();
        if (utcDate != null)
        {
            result.setMillisUtc(utcDate.getTime());
        }
        return result;
    }

    public IonTimestamp newUtcTimestampFromMillis(long millis)
    {
        IonTimestamp result = newTimestamp();
        result.setMillisUtc(millis);
        return result;
    }


    /*************************************************************
     *
     * methods in IonSystemImpl (now declared in IonSystemPrivate)
     *
     */

    public SystemValueIterator newBinarySystemReader(IonCatalog catalog,
                                                     InputStream ionBinary)
        throws IOException
    {
        if (catalog == null) catalog = getCatalog();
        // TODO: do something with the catalog - update readers
        IonReader reader = IonReaderFactoryX.makeReader(ionBinary);
        SystemValueIterator sysreader = new ReaderIterator(this, reader);
        return sysreader;
    }

    public SystemValueIterator newLegacySystemReader(IonCatalog catalog,
                                                     byte[] ionData)
    {
        if (catalog == null) catalog = getCatalog();
        // TODO: do something with the catalog - update readers
        IonReader reader = IonReaderFactoryX.makeReader(ionData);
        SystemValueIterator sysreader = new ReaderIterator(this, reader);
        return sysreader;
    }

    public SystemValueIterator newPagedBinarySystemReader(IonCatalog catalog,
                                                          InputStream ionBinary)
        throws IOException
    {
        if (catalog == null) catalog = getCatalog();
        IonReader reader = IonReaderFactoryX.makeReader(ionBinary);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public IonTextReader newSystemReader(String ionText)
    {
        IonTextReader reader = IonReaderFactoryX.makeSystemReader(ionText);
        return reader;
    }

    public IonTextReader newSystemReader(Reader ionText)
    {
        IonTextReader reader = IonReaderFactoryX.makeSystemReader(ionText);
        return reader;
    }

    public IonReader newSystemReader(byte[] ionData)
    {
        IonReader reader = IonReaderFactoryX.makeReader(ionData);
        return reader;
    }

    public IonReader newSystemReader(byte[] ionData, int offset, int len)
    {
        IonReader reader = IonReaderFactoryX.makeReader(ionData, offset, len);
        return reader;
    }

    public IonReader newSystemReader(InputStream ionData)
    {
        IonReader reader = IonReaderFactoryX.makeReader(ionData);
        return reader;
    }

    public IonReader newSystemReader(IonValue value)
    {
        IonReader reader = IonReaderFactoryX.makeSystemReader(value);
        return reader;
    }

    public IonWriter newTextWriter(OutputStream out, boolean pretty)
    {
        // prettyPrint, boolean printAscii, boolean filterOutSymbolTables
        TextOptions options = new TextOptions(pretty, true, true);
        IonWriter writer = IonWriterFactory.makeWriter(this, out, options);
        return writer;
    }

    public IonWriter newTreeSystemWriter(IonContainer container)
    {
        IonWriter writer = IonWriterFactory.makeSystemWriter(container);
        return writer;
    }

    public IonWriter newTreeWriter(IonContainer container)
    {
        IonWriter writer = IonWriterFactory.makeWriter(container);
        return writer;
    }

    public Iterator<IonValue> systemIterate(String ionText)
    {
        IonReader reader = IonReaderFactoryX.makeSystemReader(this, ionText);
        Iterator<IonValue> iterator = make_system_iterator(reader);
        return iterator;
    }

    /**
     * FIXME ION-160 This method consumes the entire stream!
     */
    public Iterator<IonValue> systemIterate(InputStream ionData)
    {
        IonReader reader = IonReaderFactoryX.makeReader(this, ionData);
        Iterator<IonValue> iterator = make_system_iterator(reader);
        return iterator;
    }

    /**
     * FIXME This method consumes the entire stream!
     */
    private Iterator<IonValue> make_system_iterator(IonReader reader)
    {
        IonDatagram datagram = newDatagram();
        IonWriter writer = IonWriterFactory.makeWriter(datagram);
        try {
            writer.writeValues(reader);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        Iterator<IonValue> iterator = datagram.systemIterator();
        return iterator;
    }

    public boolean valueIsSharedSymbolTable(IonValue value)
    {
        if (value instanceof IonStruct) {
            if (value.hasTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE)) {
                return true;
            }
        }
        return false;
    }
}
