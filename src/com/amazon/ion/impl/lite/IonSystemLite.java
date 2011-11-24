// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.impl.IonImplUtils.addAllNonNull;
import static com.amazon.ion.impl.IonReaderFactoryX.makeReader;
import static com.amazon.ion.impl.IonReaderFactoryX.makeSystemReader;
import static com.amazon.ion.impl.IonWriterFactory.DEFAULT_OPTIONS;
import static com.amazon.ion.impl.IonWriterFactory.makeWriter;
import static com.amazon.ion.impl.UnifiedSymbolTable.initialSymbolTable;
import static com.amazon.ion.impl.UnifiedSymbolTable.isNonSystemSharedTable;
import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;
import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewSharedSymbolTable;
import static com.amazon.ion.impl.UnifiedSymbolTable.newSystemSymbolTable;
import static com.amazon.ion.util.IonTextUtils.printString;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.UnexpectedEofException;
import com.amazon.ion.UnsupportedIonVersionException;
import com.amazon.ion.impl.$PrivateTextOptions;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonReaderFactoryX;
import com.amazon.ion.impl.IonReaderWriterPrivate;
import com.amazon.ion.impl.IonScalarConversionsX.CantConvertException;
import com.amazon.ion.impl.IonSystemPrivate;
import com.amazon.ion.impl.IonWriterBaseImpl;
import com.amazon.ion.impl.IonWriterBinaryCompatibility;
import com.amazon.ion.impl.IonWriterFactory;
import com.amazon.ion.impl.IonWriterUserBinary;
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
    private static int DEFAULT_CONTEXT_FREE_LIST_SIZE = 120;

    private final UnifiedSymbolTable _system_symbol_table =
        newSystemSymbolTable(1);

    /** Not null. */
    private final IonCatalog         _catalog;
    private       ValueFactoryLite   _value_factory;
    private final IonLoader          _loader;
    private final boolean myStreamCopyOptimized;


    /**
     * @param catalog must not be null.
     */
    public IonSystemLite(IonCatalog catalog, boolean streamCopyOptimized)
    {
        this(catalog, streamCopyOptimized, DEFAULT_CONTEXT_FREE_LIST_SIZE);
    }

    /**
     * @param catalog must not be null.
     * @param context_free_list_size
     */
    private IonSystemLite(IonCatalog catalog, boolean streamCopyOptimized,
                          int context_free_list_size)
    {
        assert catalog != null;

        set_context_free_list_max(context_free_list_size);

        _catalog = catalog;
        _loader = new IonLoaderLite(this, catalog);
        myStreamCopyOptimized = streamCopyOptimized;

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
        IonReader reader = makeSystemReader(value.getSystem(), value);

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
        if (!ION_1_0.equals(ionVersionId)) {
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
            new IonWriterBinaryCompatibility.User(this, _catalog,
                                                  myStreamCopyOptimized);
        return writer;
    }

    @Deprecated
    public com.amazon.ion.IonBinaryWriter newBinaryWriter(SymbolTable... imports)
    {
        SymbolTable symbols = initialSymbolTable(this, imports);
        IonWriterBinaryCompatibility.User writer =
            new IonWriterBinaryCompatibility.User(this, _catalog,
                                                  myStreamCopyOptimized);
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
            IonWriterFactory.newBinaryWriter(this, getCatalog(),
                                             myStreamCopyOptimized, out, imports);
        return writer;
    }

    public IonWriter newTextWriter(Appendable out)
    {
        return makeWriter(this, _catalog, out, DEFAULT_OPTIONS);
    }

    public IonWriter newTextWriter(Appendable out, boolean pretty)
    {
        $PrivateTextOptions options =
            new $PrivateTextOptions(pretty /* prettyPrint */,
                                    true /* printAscii */,
                                    true /* filterOutSymbolTables */);
        IonWriter userWriter = newTextWriter(out, options);
        return userWriter;
    }

    public IonWriterBaseImpl newTextWriter(Appendable out,
                                           $PrivateTextOptions options)
    {
        IonWriterBaseImpl userWriter = makeWriter(this, out, options);
        return userWriter;
    }

    public IonWriter newTextWriter(Appendable out, SymbolTable... imports)
        throws IOException
    {
        return makeWriter(this, _catalog, out, DEFAULT_OPTIONS, imports);
    }

    public IonWriter newTextWriter(Appendable out,
                                   $PrivateTextOptions options,
                                   SymbolTable... imports)
        throws IOException
    {
        return makeWriter(this, _catalog, out, options, imports);
    }

    public IonWriter newTextWriter(OutputStream out)
    {
        return makeWriter(this, _catalog, out, DEFAULT_OPTIONS);
    }

    public IonWriterBaseImpl newTextWriter(OutputStream out,
                                           $PrivateTextOptions options)
    {
        return makeWriter(this, out, options);
    }

    public IonWriter newTextWriter(OutputStream out, SymbolTable... imports)
        throws IOException
    {
        return makeWriter(this, _catalog, out, DEFAULT_OPTIONS, imports);
    }

    public IonWriter newTextWriter(OutputStream out,
                                   $PrivateTextOptions options,
                                   SymbolTable... imports)
        throws IOException
    {
        return makeWriter(this, _catalog, out, options, imports);
    }

    public UnifiedSymbolTable newLocalSymbolTable(SymbolTable... imports)
    {
        UnifiedSymbolTable st =
            makeNewLocalSymbolTable(this, getSystemSymbolTable(), imports);
        return st;
    }

    public UnifiedSymbolTable newSharedSymbolTable(IonStruct ionRep)
    {
        UnifiedSymbolTable st = makeNewSharedSymbolTable(ionRep);
        return st;
    }

    public UnifiedSymbolTable newSharedSymbolTable(IonReader reader)
    {
        UnifiedSymbolTable st = makeNewSharedSymbolTable(reader, false);
        return st;
    }

    public UnifiedSymbolTable newSharedSymbolTable(IonReader reader,
                                                   boolean isOnStruct)
    {
        UnifiedSymbolTable st = makeNewSharedSymbolTable(reader, isOnStruct);
        return st;
    }

    public UnifiedSymbolTable newSharedSymbolTable(String name,
                                                   int version,
                                                   Iterator<String> newSymbols,
                                                   SymbolTable... imports)
    {
        // TODO streamline to avoid making this collection
        ArrayList<String> syms = new ArrayList<String>();

        SymbolTable prior = null;
        if (version > 1)
        {
            int priorVersion = version - 1;
            prior = _catalog.getTable(name, priorVersion);
            if (prior == null || prior.getVersion() != priorVersion)
            {
                String message =
                    "Catalog does not contain symbol table " +
                    printString(name) + " version " + priorVersion +
                    " required to create version " + version;
                throw new IonException(message);
            }
        }

        for (SymbolTable imported : imports)
        {
            addAllNonNull(syms, imported.iterateDeclaredSymbolNames());
        }

        addAllNonNull(syms, newSymbols);

        UnifiedSymbolTable st =
            makeNewSharedSymbolTable(name, version, prior, syms.iterator());

        return st;
    }

    public IonValueLite newValue(IonReader reader)
    {
        IonValueLite value = load_value_helper(reader);
        if (value == null) {
            throw new IonException("No value available");
        }
        if (value._isSymbolPresent()) {
            value.populateSymbolValues(null);
        }
        return value;
    }

    private IonValueLite load_value_helper(IonReader reader)
    {
        boolean symbol_is_present = false;

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
                v = newSymbol(reader.symbolValue());
                symbol_is_present = true;
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
            symbol_is_present = true;
        }
        String[] uta = reader.getTypeAnnotations();
        if (uta.length > 0) {
            v.setTypeAnnotations(uta);
            symbol_is_present = true;
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
                    symbol_is_present = true;
                }
                break;
            default:
                throw new IonException("unexpected type encountered reading value: "+t.toString());
            }
        }
        if (symbol_is_present) {
            v._isSymbolPresent(true);
        }
        return v;
    }

    /**
     * @return true iff any child contains a symbol
     * (including field names and annotations)
     */
    private boolean load_children(IonContainerLite container, IonReader reader)
    {
        boolean symbol_is_present = false;

        reader.stepIn();
        for (;;) {
            IonType t = reader.next();
            if (t == null) {
                break;
            }
            IonValueLite child = load_value_helper(reader);

            container.add(child);

            if (child._isSymbolPresent()) {
                symbol_is_present = true;
            }
        }
        reader.stepOut();

        return symbol_is_present;
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

    private IonValue singleValue(Iterator<IonValue> it)
    {
        IonValue value;
        try {
            value = it.next();
        }
        catch (NoSuchElementException e) {
            throw new UnexpectedEofException("no value found on input stream");
        }
        if (it.hasNext()) {
            throw new IonException("not a single value");
        }
        return value;
    }

    public IonValue singleValue(String ionText)
    {
        Iterator<IonValue> it = iterate(ionText);
        return singleValue(it);
    }

    public IonValue singleValue(byte[] ionData)
    {
        Iterator<IonValue> it = iterate(ionData);
        return singleValue(it);
    }

    /*
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
            context = TopLevelContext.wrap(this, local, child);
        }
        child.setContext(context);
        return local;
    }

    public void clearLocalSymbolTable()
    {
        // the only symbol table system actually owns is
        // a system symbol table.  Local symbol tables are
        // all owned by the children of system. (and often
        // shared with following siblings)
        return;
    }


    /**
     * Always returns null, since values in this context are top-level and
     * stand-alone.
     */
    public IonContainerLite getContextContainer()
    {
        return null;
    }

    public void setContextContainer(IonContainerLite container,
                                    IonValueLite child)
    {
        assert child._context == this;
        assert container.getSystem() == this : "system mismatch";

        // The new container becomes the context, we replace ourself.
        child.setContext(container);
    }


    public SymbolTable getAssignedSymbolTable()
    {
        return null;
    }

    public SymbolTable getContextSymbolTable()
    {
        return null;
    }

    public IonSystemLite getSystem()
    {
        return this;
    }

    public void setSymbolTableOfChild(SymbolTable symbols, IonValueLite child)
    {
        assert child._context == this;
        assert ! (child instanceof IonDatagram);

        if (isNonSystemSharedTable(symbols)) {
            throw new IllegalArgumentException("shared symbol tables cannot be set as a current symbol table");
        }

        // Need a TLC to hold the symtab for the child.
        TopLevelContext context = allocateConcreteContext(null, child);
        context.setSymbolTableOfChild(symbols, child);
    }

    private static final TopLevelContext[] EMPTY_CONTEXT_ARRAY = new TopLevelContext[0];
    private int               _free_count;
    private TopLevelContext[] _free_contexts;

    protected final void set_context_free_list_max(int size) {
        if (size < 1) {
            _free_contexts = EMPTY_CONTEXT_ARRAY;
        }
        else if (_free_contexts == null || size != _free_contexts.length) {
            TopLevelContext[] temp = new TopLevelContext[size];
            if (_free_count > 0) {
                if (_free_count > size) {
                    _free_count = size;
                }
                System.arraycopy(_free_contexts, 0, temp, 0, _free_count);
            }
            _free_contexts = temp;
        }
    }

    protected final TopLevelContext
    allocateConcreteContext(IonDatagramLite datagram, IonValueLite child)
    {
        TopLevelContext context = null;
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
            context = TopLevelContext.wrap(this, datagram, child);
        }
        else {
            context.rewrap(datagram, child);
        }
        return context;
    }

    protected final void releaseConcreteContext(TopLevelContext context)
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
        if (!ION_1_0.equals(ionVersionMarker)) {
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
            // We called _reader.next() inside hasNext() above
            IonValueLite value = _system.newValue(_reader);

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
                if (ION_1_0.equals(symbol)) {
                    return true;
                }
                break;
            case STRUCT:
                String [] annotations = _reader.getTypeAnnotations();
                for (int ii=0; ii<annotations.length; ii++) {
                    if (ION_SYMBOL_TABLE.equals(annotations[ii])) {
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
        SymbolTable symbols = initialSymbolTable(this, imports);
        IonDatagramLite dg = newDatagram(catalog);
        dg.appendTrailingSymbolTable(symbols);
        return dg;
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
        IonReader reader = makeReader(this, _catalog, ionData, offset, len);
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
        return makeReader(this, _catalog, ionData);
    }

    public IonReader newReader(IonCatalog catalog, InputStream ionData)
    {
        if (catalog == null) catalog = getCatalog();
        IonReader reader = makeReader(this, catalog, ionData);
        return reader;
    }

    public IonReader newReader(IonValue value)
    {
        IonReader reader = makeReader(this, _catalog, value);
        return reader;
    }

    public IonReader newReader(IonCatalog catalog, IonValue value)
    {
        if (catalog == null) catalog = getCatalog();
        IonReader reader = IonReaderFactoryX.makeReader(this, catalog, value);
        return reader;
    }


    public IonTimestamp newUtcTimestamp(Date utcDate)
    {
        IonTimestamp result = newNullTimestamp();
        if (utcDate != null)
        {
            result.setMillisUtc(utcDate.getTime());
        }
        return result;
    }

    public IonTimestamp newUtcTimestampFromMillis(long millis)
    {
        IonTimestamp result = newNullTimestamp();
        result.setMillisUtc(millis);
        return result;
    }


    /*************************************************************
     *
     * methods in IonSystemImpl (now declared in IonSystemPrivate)
     *
     */

    public IonTextReader newSystemReader(String ionText)
    {
        return makeSystemReader(this, ionText);
    }

    public IonTextReader newSystemReader(Reader ionText)
    {
        return makeSystemReader(this, ionText);
    }

    public IonReader newSystemReader(byte[] ionData)
    {
        return makeSystemReader(this, ionData);
    }

    public IonReader newSystemReader(byte[] ionData, int offset, int len)
    {
        return makeSystemReader(this, ionData, offset, len);
    }

    public IonReader newSystemReader(InputStream ionData)
    {
        return makeSystemReader(this, ionData);
    }

    public IonReader newSystemReader(IonValue value)
    {
        return makeSystemReader(this, value);
    }


    public IonWriter newTextWriter(OutputStream out, boolean pretty)
    {
        // prettyPrint, boolean printAscii, boolean filterOutSymbolTables
        $PrivateTextOptions options = new $PrivateTextOptions(pretty, true, true);
        IonWriter writer = IonWriterFactory.makeWriter(this, out, options);
        return writer;
    }

    /**
     * @param container must not be null.
     */
    public IonWriter newTreeSystemWriter(IonContainer container)
    {
        IonWriter writer = IonWriterFactory.makeSystemWriter(container);
        return writer;
    }

    /**
     * @param container must not be null.
     */
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
            if (value.hasTypeAnnotation(ION_SYMBOL_TABLE)) {
                return true;
            }
        }
        return false;
    }
}
