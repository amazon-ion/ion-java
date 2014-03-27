// Copyright (c) 2010-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.impl._Private_IonReaderFactory.makeReader;
import static com.amazon.ion.impl._Private_IonReaderFactory.makeSystemReader;
import static com.amazon.ion.impl._Private_Utils.addAllNonNull;
import static com.amazon.ion.impl._Private_Utils.initialSymtab;
import static com.amazon.ion.impl._Private_Utils.newSymbolToken;
import static com.amazon.ion.util.IonTextUtils.printString;

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnexpectedEofException;
import com.amazon.ion.UnsupportedIonVersionException;
import com.amazon.ion.impl._Private_IonBinaryWriterBuilder;
import com.amazon.ion.impl._Private_IonSystem;
import com.amazon.ion.impl._Private_IonWriterFactory;
import com.amazon.ion.impl._Private_ScalarConversions.CantConvertException;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.system.IonTextWriterBuilder;
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
@SuppressWarnings("deprecation")
final class IonSystemLite
    extends ValueFactoryLite
    implements _Private_IonSystem, IonContext
{
    private static int DEFAULT_CONTEXT_FREE_LIST_SIZE = 120;

    private final SymbolTable _system_symbol_table;

    /** Not null. */
    private final IonCatalog         _catalog;
    private       ValueFactoryLite   _value_factory;
    private final IonLoader          _loader;
    /** Immutable. */
    private final IonTextWriterBuilder myTextWriterBuilder;
    /** Immutable. */
    private final _Private_IonBinaryWriterBuilder myBinaryWriterBuilder;


    public IonSystemLite(IonTextWriterBuilder twb,
                         _Private_IonBinaryWriterBuilder bwb)
    {
        this(twb, bwb, DEFAULT_CONTEXT_FREE_LIST_SIZE);
    }

    private IonSystemLite(IonTextWriterBuilder twb,
                          _Private_IonBinaryWriterBuilder bwb,
                          int context_free_list_size)
    {
        IonCatalog catalog = twb.getCatalog();
        assert catalog != null;
        assert catalog == bwb.getCatalog();

        set_context_free_list_max(context_free_list_size);

        _catalog = catalog;
        _loader = new IonLoaderLite(this, catalog);
        _system_symbol_table = bwb.getInitialSymtab();
        assert _system_symbol_table.isSystemTable();

        myTextWriterBuilder = twb.immutable();

        // whacked but I'm not going to figure this out right now
        _value_factory = this;
        _value_factory.set_system(this);

        bwb.setSymtabValueFactory(_value_factory);
        myBinaryWriterBuilder = bwb.immutable();
    }

    //==========================================================================
    // IonSystem Methods
    //==========================================================================

    public boolean isStreamCopyOptimized()
    {
        return myBinaryWriterBuilder.isStreamCopyOptimized();
    }

    @SuppressWarnings("unchecked")
    public <T extends IonValue> T clone(T value) throws IonException
    {
        // Use "fast clone" when the system is the same.
        if (value.getSystem() == this)
        {
            return (T) value.clone();
        }

        // TODO ION-338 Materializing IonDatagram is an unnecessary overhead
        IonDatagram datagram = newDatagram();
        IonWriter writer = _Private_IonWriterFactory.makeWriter(datagram);
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
        IonReader reader = makeReader(this, _catalog, ionText);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(InputStream ionData)
    {
        IonReader reader = newReader(ionData);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(String ionText)
    {
        IonReader reader = makeReader(this, _catalog, ionText);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(byte[] ionData)
    {
        IonReader reader = makeReader(this, _catalog, ionData);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    @Deprecated
    public IonBinaryWriter newBinaryWriter()
    {
        _Private_IonBinaryWriterBuilder b = myBinaryWriterBuilder;
        return b.buildLegacy();
    }

    @Deprecated
    public IonBinaryWriter newBinaryWriter(SymbolTable... imports)
    {
        _Private_IonBinaryWriterBuilder b = (_Private_IonBinaryWriterBuilder)
            myBinaryWriterBuilder.withImports(imports);
        return b.buildLegacy();
    }


    public IonWriter newBinaryWriter(OutputStream out, SymbolTable... imports)
    {
        return myBinaryWriterBuilder.withImports(imports).build(out);
    }

    public IonWriter newTextWriter(Appendable out)
    {
        return myTextWriterBuilder.build(out);
    }

    public IonWriter newTextWriter(Appendable out, SymbolTable... imports)
        throws IOException
    {
        return myTextWriterBuilder.withImports(imports).build(out);
    }

    public IonWriter newTextWriter(OutputStream out)
    {
        return myTextWriterBuilder.build(out);
    }

    public IonWriter newTextWriter(OutputStream out, SymbolTable... imports)
        throws IOException
    {
        return myTextWriterBuilder.withImports(imports).build(out);
    }


    public SymbolTable newLocalSymbolTable(SymbolTable... imports)
    {
        return _Private_Utils.newLocalSymtab(this,
                                             getSystemSymbolTable(),
                                             null /* localSymbols */,
                                             imports);
    }

    public SymbolTable newSharedSymbolTable(IonStruct ionRep)
    {
        return _Private_Utils.newSharedSymtab(ionRep);
    }

    public SymbolTable newSharedSymbolTable(IonReader reader)
    {
        return _Private_Utils.newSharedSymtab(reader, false);
    }

    public SymbolTable newSharedSymbolTable(IonReader reader,
                                            boolean isOnStruct)
    {
        return _Private_Utils.newSharedSymtab(reader, isOnStruct);
    }

    public SymbolTable newSharedSymbolTable(String name,
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

        SymbolTable st =
            _Private_Utils.newSharedSymtab(name, version, prior,
                                           syms.iterator());
        return st;
    }

    public IonValueLite newValue(IonReader reader)
    {
        IonValueLite value = load_value_helper(reader);
        if (value == null) {
            throw new IonException("No value available");
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
                // TODO ION-176  Inefficient since we can't determine the size
                // of the integer in order to avoid making BigIntegers.
                if (true) {
                    v = newInt(reader.bigIntegerValue());
                    break;
                }
                try {
                    v = newInt(reader.longValue());
                    // FIXME ION-297 this is wrong!
                    // This exception is not specified or tested and is not
                    // thrown by all reader implementations.  ION-176
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

        // Forget any incoming SIDs on field names.
        if (reader.isInStruct()) {
            SymbolToken token = reader.getFieldNameSymbol();
            String text = token.getText();
            if (text != null && token.getSid() != UNKNOWN_SYMBOL_ID)
            {
                token = newSymbolToken(text, UNKNOWN_SYMBOL_ID);
            }
            v.setFieldNameSymbol(token);
            symbol_is_present = true;
        }

        // Forget any incoming SIDs on annotations.
        // This is a fresh array so we can modify it:
        SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
        if (annotations.length != 0)
        {
            for (int i = 0; i < annotations.length; i++)
            {
                SymbolToken token = annotations[i];
                String text = token.getText();
                if (text != null && token.getSid() != UNKNOWN_SYMBOL_ID )
                {
                    annotations[i] = newSymbolToken(text, UNKNOWN_SYMBOL_ID);
                }
            }
            v.setTypeAnnotationSymbols(annotations);
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
        IonWriter writer = _Private_IonWriterFactory.makeWriter(container);
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

    public SymbolTable ensureLocalSymbolTable(IonValueLite child)
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
    }


    /**
     * Always returns {@code null}, since all values in this context are
     * top-level and stand-alone.
     */
    public IonContainerLite getContextContainer()
    {
        return null;
    }

    public void setContextContainer(IonContainerLite container,
                                    IonValueLite child)
    {
        assert child._context == this;
//        assert container.getSystem() == this : "system mismatch";

        // We must unset the sids within this child as they may not be
        // correct in the context of the container!
        child.clearSymbolIDValues();

        // The new container becomes the context, we replace ourself.
        child.setContext(container);
    }

    /**
     * Always return {@code null}, since IonSystems do not own a symbol table
     * directly (they create them through factories).
     */
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

        if (_Private_Utils.symtabIsSharedNotSystem(symbols)) {
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
        implements Iterator<IonValue>, Closeable
    {
        private final IonReader        _reader;
        private final IonSystemLite    _system;
        private       IonType          _next;


        // TODO: do we need catalog, import support for this?
        //       we are creating ion values which might want
        //       a local symbol table in some cases.
        protected ReaderIterator(IonSystemLite system, IonReader reader)
        {
            _reader = reader;
            _system = system;
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

            SymbolTable symtab = _reader.getSymbolTable();

            // make an ion value from our reader
            // We called _reader.next() inside hasNext() above
            IonValueLite value = _system.newValue(_reader);

            // we've used up the value now, force a _reader._next() the next time through
            _next = null;

            value.setSymbolTable(symtab);

            return value;
        }


        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public void close() throws IOException
        {
            // TODO _reader.close();
        }
    }


    public IonTimestamp newUtcTimestampFromMillis(long millis)
    {
        IonTimestamp result = newNullTimestamp();
        result.setMillisUtc(millis);
        return result;
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
        SymbolTable defaultSystemSymtab = getSystemSymbolTable();
        SymbolTable symbols =
            initialSymtab(this, defaultSystemSymtab, imports);
        IonDatagramLite dg = newDatagram(catalog);
        dg.appendTrailingSymbolTable(symbols);
        return dg;
    }

    public IonReader newReader(byte[] ionData)
    {
        return makeReader(this, _catalog, ionData);
    }

    public IonReader newSystemReader(byte[] ionData)
    {
        return makeSystemReader(this, ionData);
    }


    public IonReader newReader(byte[] ionData, int offset, int len)
    {
        return makeReader(this, _catalog, ionData, offset, len);
    }

    public IonReader newSystemReader(byte[] ionData, int offset, int len)
    {
        return makeSystemReader(this, ionData, offset, len);
    }


    public IonTextReader newReader(String ionText)
    {
        return makeReader(this, _catalog, ionText);
    }

    public IonReader newSystemReader(String ionText)
    {
        return makeSystemReader(this, ionText);
    }


    public IonReader newReader(InputStream ionData)
    {
        return makeReader(this, _catalog, ionData);
    }

    public IonReader newSystemReader(InputStream ionData)
    {
        return makeSystemReader(this, ionData);
    }


    //==========================================================================
    // methods in IonSystemImpl (now declared in IonSystemPrivate)
    //==========================================================================

    public IonReader newReader(Reader ionText)
    {
        return makeReader(this, _catalog, ionText);
    }

    public IonReader newSystemReader(Reader ionText)
    {
        return makeSystemReader(this, ionText);
    }


    public IonReader newReader(IonValue value)
    {
        return makeReader(this, _catalog, value);
    }

    public IonReader newSystemReader(IonValue value)
    {
        return makeSystemReader(this, value);
    }


    //==========================================================================
    // IonWriter creation
    //==========================================================================

    /**
     * @param container must not be null.
     */
    public IonWriter newTreeSystemWriter(IonContainer container)
    {
        IonWriter writer = _Private_IonWriterFactory.makeSystemWriter(container);
        return writer;
    }

    /**
     * @param container must not be null.
     */
    public IonWriter newTreeWriter(IonContainer container)
    {
        IonWriter writer = _Private_IonWriterFactory.makeWriter(container);
        return writer;
    }


    public Iterator<IonValue> systemIterate(Reader ionText)
    {
        IonReader ir = newSystemReader(ionText);
        return _Private_Utils.iterate(this, ir);
    }

    public Iterator<IonValue> systemIterate(String ionText)
    {
        IonReader ir = newSystemReader(ionText);
        return _Private_Utils.iterate(this, ir);
    }

    public Iterator<IonValue> systemIterate(InputStream ionData)
    {
        IonReader ir = newSystemReader(ionData);
        return _Private_Utils.iterate(this, ir);
    }

    public Iterator<IonValue> systemIterate(byte[] ionData)
    {
        IonReader ir = newSystemReader(ionData);
        return _Private_Utils.iterate(this, ir);
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
