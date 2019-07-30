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
import com.amazon.ion.system.IonReaderBuilder;
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


@SuppressWarnings("deprecation")
final class IonSystemLite
    extends ValueFactoryLite
    implements _Private_IonSystem
{

    private final SymbolTable _system_symbol_table;

    /** Not null. */
    private final IonCatalog         _catalog;
    private final IonLoader          _loader;
    /** Immutable. */
    private final IonTextWriterBuilder myTextWriterBuilder;
    /** Immutable. */

    private final _Private_IonBinaryWriterBuilder myBinaryWriterBuilder;
    /** Immutable. **/
    private final IonReaderBuilder myReaderBuilder;

    public IonSystemLite(IonTextWriterBuilder twb,
                          _Private_IonBinaryWriterBuilder bwb,
                          IonReaderBuilder rb)
    {
        IonCatalog catalog = twb.getCatalog();
        assert catalog != null;
        assert catalog == bwb.getCatalog();
        assert catalog == rb.getCatalog();

        _catalog = catalog;
        _loader = new IonLoaderLite(this, catalog);
        _system_symbol_table = bwb.getInitialSymbolTable();
        assert _system_symbol_table.isSystemTable();

        myTextWriterBuilder = twb.immutable();

        set_system(this);

        bwb.setSymtabValueFactory(this);
        myBinaryWriterBuilder = bwb.immutable();

        myReaderBuilder = rb.immutable();
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

        if (value instanceof IonDatagram)
        {
            IonDatagram datagram = newDatagram();
            IonWriter writer = _Private_IonWriterFactory.makeWriter(datagram);
            IonReader reader = makeSystemReader(value.getSystem(), value);

            try {
                writer.writeValues(reader);
            }
            catch (IOException e) {
                throw new IonException(e);
            }

            return (T) datagram;
        }

        IonReader reader = newReader(value);
        reader.next();
        return (T) newValue(reader);
    }

    public IonCatalog getCatalog()
    {
        return _catalog;
    }

    public IonLoader getLoader()
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
        IonReader reader = makeReader(_catalog, ionText, _lstFactory);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(InputStream ionData)
    {
        // This method causes a memory leak when reading a gzipped stream, see deprecation notice.
        IonReader reader = makeReader(_catalog, ionData, _lstFactory);
        return iterate(reader);
    }

    public Iterator<IonValue> iterate(String ionText)
    {
        IonReader reader = makeReader(_catalog, ionText, _lstFactory);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(byte[] ionData)
    {
        // This method causes a memory leak when reading a gzipped stream, see deprecation notice.
        IonReader reader = makeReader(_catalog, ionData, _lstFactory);
        return iterate(reader);
    }

    public Iterator<IonValue> iterate(IonReader reader)
    {
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
        return _lstFactory.newLocalSymtab(getSystemSymbolTable(), imports);
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
        IonValueLite value = load_value_helper(reader, /*isTopLevel*/ true);
        if (value == null) {
            throw new IonException("No value available");
        }
        return value;
    }

    private IonValueLite load_value_helper(IonReader reader, boolean isTopLevel)
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
                // TODO amzn/ion-java/issues/9  Inefficient since we can't determine the size
                // of the integer in order to avoid making BigIntegers.
                v = newInt(reader.bigIntegerValue());
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
        if (!isTopLevel && reader.isInStruct()) {
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
            IonValueLite child = load_value_helper(reader, /*isTopLevel*/ false);

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
        return singleValue(ionData, 0, ionData.length);
    }

    @Override
    public IonValue singleValue(byte[] ionData, int offset, int len) {
        IonReader reader = newReader(ionData, offset, len);
        try {
            Iterator<IonValue> it = iterate(reader);
            return singleValue(it);
        }
        finally {
            try {
                reader.close();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }
    }

    protected IonSymbolLite newSystemIdSymbol(String ionVersionMarker)
    {
        if (!ION_1_0.equals(ionVersionMarker)) {
            throw new IllegalArgumentException("name isn't an ion version marker");
        }
        IonSymbolLite ivm = newSymbol(ionVersionMarker);
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
        SymbolTable symbols = initialSymtab(_lstFactory, defaultSystemSymtab, imports);
        IonDatagramLite dg = newDatagram(catalog);
        dg.appendTrailingSymbolTable(symbols);
        return dg;
    }

    public IonReader newReader(byte[] ionData)
    {
        return myReaderBuilder.build(ionData);
    }

    public IonReader newSystemReader(byte[] ionData)
    {
        return makeSystemReader(ionData);
    }


    public IonReader newReader(byte[] ionData, int offset, int len)
    {
        return myReaderBuilder.build(ionData, offset, len);
    }

    public IonReader newSystemReader(byte[] ionData, int offset, int len)
    {
        return makeSystemReader(ionData, offset, len);
    }


    public IonTextReader newReader(String ionText)
    {
        return myReaderBuilder.build(ionText);
    }

    public IonReader newSystemReader(String ionText)
    {
        return makeSystemReader(ionText);
    }


    public IonReader newReader(InputStream ionData)
    {
        return myReaderBuilder.build(ionData);
    }

    public IonReader newSystemReader(InputStream ionData)
    {
        return makeSystemReader(ionData);
    }


    //==========================================================================
    // methods in IonSystemImpl (now declared in IonSystemPrivate)
    //==========================================================================

    public IonReader newReader(Reader ionText)
    {
        return myReaderBuilder.build(ionText);
    }

    public IonReader newSystemReader(Reader ionText)
    {
        return makeSystemReader(ionText);
    }


    public IonReader newReader(IonValue value)
    {
        return myReaderBuilder.build(value);
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

    public Iterator<IonValue> systemIterate(IonReader reader)
    {
        return _Private_Utils.iterate(this, reader);
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
