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
import static com.amazon.ion.impl._Private_IonReaderFactory.makeSystemReader;
import static com.amazon.ion.impl._Private_Utils.addAllNonNull;
import static com.amazon.ion.impl._Private_Utils.initialSymtab;
import static com.amazon.ion.impl._Private_Utils.newSymbolToken;
import static com.amazon.ion.util.IonTextUtils.printString;

import com.amazon.ion.IntegerSize;
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
import com.amazon.ion.impl._Private_IonReaderBuilder;
import com.amazon.ion.impl._Private_IonSystem;
import com.amazon.ion.impl._Private_IonWriterFactory;
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
        myReaderBuilder = ((_Private_IonReaderBuilder) rb).withLstFactory(_lstFactory).immutable();
        _loader = new IonLoaderLite(this, catalog);
        _system_symbol_table = bwb.getInitialSymbolTable();
        assert _system_symbol_table.isSystemTable();

        myTextWriterBuilder = twb.immutable();

        set_system(this);

        bwb.setSymtabValueFactory(this);
        myBinaryWriterBuilder = bwb.immutable();
    }

    IonReaderBuilder getReaderBuilder() {
        return myReaderBuilder;
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
        IonReader reader = myReaderBuilder.build(ionText);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(InputStream ionData)
    {
        // This method causes a memory leak when reading a gzipped stream, see deprecation notice.
        IonReader reader = myReaderBuilder.build(ionData);
        return iterate(reader);
    }

    public Iterator<IonValue> iterate(String ionText)
    {
        IonReader reader = myReaderBuilder.build(ionText);
        ReaderIterator iterator = new ReaderIterator(this, reader);
        return iterator;
    }

    public Iterator<IonValue> iterate(byte[] ionData)
    {
        // This method causes a memory leak when reading a gzipped stream, see deprecation notice.
        IonReader reader = myReaderBuilder.build(ionData);
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
        IonValueLite value = load_value_helper(reader);
        if (value == null) {
            throw new IonException("No value available");
        }
        return value;
    }

    private IonValueLite load_value_helper(IonReader reader)
    {
        // Note: this method constructs a new `ValueLoader` on each call to preserve thread safety.
        // If this causes excessive GC pressure, we should consider making a thread-local ValueLoader member field
        // on the IonSystemLite class.
        return new ValueLoader().load(reader);
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

    private class ValueLoader {
        // This value was chosen somewhat arbitrarily; it can/should be changed if it is found to be insufficient.
        private static final int CONTAINER_STACK_INITIAL_CAPACITY = 16;
        private final ArrayList<IonContainerLite> containerStack;

        private IonReader reader;

        public ValueLoader() {
            this.containerStack = new ArrayList<>(CONTAINER_STACK_INITIAL_CAPACITY);
            // The reader is specified in each call to `load(IonReader)`.
            this.reader = null;
        }

        // Does a shallow materialization of the value over which the reader is currently positioned.
        // If the reader is positioned over a non-null container, the returned value will be an empty version
        // of that container. Subsequent processing is required to populate it.
        private IonValueLite shallowLoadCurrentValue() {
            IonType ionType = reader.getType();
            if (reader.isNullValue()) {
                return newNull(ionType);
            }

            switch (ionType) {
                case BOOL:
                    return newBool(reader.booleanValue());
                case INT:
                    // Only construct a BigInteger if it's necessary.
                    if (reader.getIntegerSize().equals(IntegerSize.BIG_INTEGER)) {
                        return newInt(reader.bigIntegerValue());
                    }
                    return newInt(reader.longValue());
                case FLOAT:
                    return newFloat(reader.doubleValue());
                case DECIMAL:
                    return newDecimal(reader.decimalValue());
                case TIMESTAMP:
                    return newTimestamp(reader.timestampValue());
                case SYMBOL:
                    return newSymbol(reader.symbolValue());
                case STRING:
                    return newString(reader.stringValue());
                case CLOB:
                    return newClob(reader.newBytes());
                case BLOB:
                    return newBlob(reader.newBytes());
                case LIST:
                    return newEmptyList();
                case SEXP:
                    return newEmptySexp();
                case STRUCT:
                    return newEmptyStruct();
                default:
                    // Includes the variants `NULL` (handled prior to the switch) and `DATAGRAM`.
                    throw new IonException("unexpected type encountered reading value: " + ionType);
            }
        }

        // If the reader is positioned inside a struct, copies the field name to `value`.
        // Note that this will NOT copy the field name over if the reader was inside a struct when `load(reader)` was
        // called. For example, if a reader is consuming the following data:
        //
        //   {
        //     foo: [1, 2, 3]
        //   }
        //
        // And the reader is positioned on the field value `[1, 2, 3]` when the `load(reader)` was called, this
        // method will NOT copy the field name `foo` over. ValueLoader treats the reader's initial state as the
        // "top level" for the purposes of materializing the current value.
        // For a test that enforces this behavior, see: IonReaderToIonValueTest.
        // Returns `true` if the reader's current value has a field name; otherwise, returns false.
        private boolean cloneFieldNameIfAny(IonValueLite value) {
            if (containerStack.isEmpty() || !reader.isInStruct()) {
                // This value is in a context that doesn't have a field name.
                return false;
            }
            SymbolToken token = reader.getFieldNameSymbol();
            String text = token.getText();
            if (text != null && token.getSid() != UNKNOWN_SYMBOL_ID)
            {
                token = newSymbolToken(text, UNKNOWN_SYMBOL_ID);
            }
            value.setFieldNameSymbol(token);
            return true;
        }

        // Copies any annotations found on the reader's current position over to the provided `value`.
        // Returns `true` if more than any annotations were found/copied. If no annotations were found/copied,
        // returns `false`.
        private boolean cloneAnnotationsIfAny(IonValueLite value) {
            // `getTypeAnnotationSymbols` returns a freshly allocated array, so we can safely modify it.
            SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
            if (annotations.length == 0) {
                return false;
            }

            for (int i = 0; i < annotations.length; i++)
            {
                SymbolToken token = annotations[i];
                String text = token.getText();
                if (text != null && token.getSid() != UNKNOWN_SYMBOL_ID )
                {
                    annotations[i] = newSymbolToken(text, UNKNOWN_SYMBOL_ID);
                }
            }
            value.setTypeAnnotationSymbols(annotations);
            return true;
        }

        // Appends the provided value to the container at the top of the container stack.
        // Callers must guarantee that the container stack is not empty before invoking this.
        private void attachToParent(IonValueLite value) {
            // Get a reference to the container at the top of the container stack.
            IonContainerLite parent = this.containerStack.get(this.containerStack.size() - 1);
            // If this is the first child value with its symbol-is-present flag set to `true`,
            // then we also need to set the parent's symbol-is-present flag to true as well.
            boolean childSymbolIsPresent = value._isSymbolPresent();
            boolean parentSymbolIsPresent = parent._isSymbolPresent();
            parent._isSymbolPresent(parentSymbolIsPresent | childSymbolIsPresent);
            // Append the child value to the end of the container.
            parent.add(value);
        }

        // Materializes the Ion value over which the provided `reader` is currently positioned.
        // If the reader is not positioned over a value, returns `null`.
        public IonValueLite load(IonReader reader) {
            // Set `this.reader` for the duration of the load() process.
            this.reader = reader;

            // If a previous attempt to read Ion data failed (because of invalid syntax, for example), the ValueLoader's
            // `containerStack` member field can be left with residual data. Clearing it at the outset of this method
            // call allows the ValueLoader to be reused after such failures.
            containerStack.clear();

            // This method does not advance the reader to the next value at the current level.
            // If the reader is not already positioned on a value, there is nothing to do.
            if (null == reader.getType()) {
                return null;
            }

            // This logic is done iteratively rather than recursively to avoid exhausting the stack when processing
            // deeply nested Ion data. Unfortunately, this does make it somewhat tougher for readers to digest.
            while(true) {
                // Create an IonValueLite from the reader's current value. If it's a container, it will not be populated yet.
                IonValueLite value = shallowLoadCurrentValue();
                // Copy any over any metadata from the reader, keeping track of whether this value or its metadata contain
                // a symbol.
                boolean isSymbolPresent = value.getType().equals(IonType.SYMBOL);
                isSymbolPresent |= cloneFieldNameIfAny(value);
                isSymbolPresent |= cloneAnnotationsIfAny(value);
                value._isSymbolPresent(isSymbolPresent);

                // If this value is a non-null container, add it to our container stack.
                if (!reader.isNullValue() && IonType.isContainer(reader.getType())) {
                    this.containerStack.add((IonContainerLite) value);
                    reader.stepIn();
                } else {
                    // If it was a scalar (including null containers)...
                    if (this.containerStack.isEmpty()) {
                        // ...and we're at the top level, we're done. Return it.
                        return value;
                    } else {
                        // ...and we're nested inside another container, attach it to the parent.
                        attachToParent(value);
                    }
                }

                // If we're inside a container and there are no more values, that container is now complete. We need
                // to finalize it. That completed container may itself have been the last value in its parent, so
                // we perform this container completion logic in a loop until we've either found another value at the
                // current level or the container stack is empty (i.e. all containers are complete).
                while (!containerStack.isEmpty() && null == reader.next()) {
                    // Pop the now-complete container value off of the stack.
                    IonContainerLite completedContainer = containerStack.remove(containerStack.size() - 1);
                    reader.stepOut();
                    // If stepping out put us back at the top level, we're done. Return the container we just popped.
                    if (this.containerStack.isEmpty()) {
                        return completedContainer;
                    } else {
                        // Otherwise, we just finished populating a container, but we're not at the top level yet. We need
                        // to append our newly populated container to the end of its parent.
                        attachToParent(completedContainer);
                    }
                }
            }
        }
    }
}
