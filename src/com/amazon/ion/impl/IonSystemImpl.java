// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbolTable.ION_SHARED_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbolTable.ION_SYMBOL_TABLE;
import static com.amazon.ion.impl.IonImplUtils.addAllNonNull;
import static com.amazon.ion.util.IonTextUtils.printString;

import com.amazon.ion.ContainedValueException;
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
import com.amazon.ion.IonNull;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnsupportedIonVersionException;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.system.SimpleCatalog;
import com.amazon.ion.util.Printer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * The standard implementation of Ion.
 */
public class IonSystemImpl
    implements IonSystem
{
    private final UnifiedSymbolTable mySystemSymbols =
        UnifiedSymbolTable.getSystemSymbolTableInstance();

    private IonCatalog myCatalog;
    private IonLoader  myLoader = new LoaderImpl(this);


    public IonSystemImpl()
    {
        myCatalog = new SimpleCatalog();
    }

    public IonSystemImpl(IonCatalog catalog)
    {
        myCatalog = catalog;
    }


    public UnifiedSymbolTable getSystemSymbolTable()
    {
        return mySystemSymbols;
    }


    public UnifiedSymbolTable getSystemSymbolTable(String systemId)
        throws UnsupportedIonVersionException
    {
        if (systemId.equals(SystemSymbolTable.ION_1_0))
        {
            return mySystemSymbols;
        }

        throw new UnsupportedIonVersionException(systemId);
    }


    public synchronized IonCatalog getCatalog()
    {
        return myCatalog;
    }


    public synchronized void setCatalog(IonCatalog catalog)
    {
        if (catalog == null) throw new NullPointerException();
        myCatalog = catalog;
    }


    public UnifiedSymbolTable newLocalSymbolTable(SymbolTable... imports)
    {
        if (imports == null || imports.length == 0)
        {
            UnifiedSymbolTable st = new UnifiedSymbolTable(mySystemSymbols);
            st.setSystem(this);
            return st;
        }

        SymbolTable systemTable;
        if (imports[0].isSystemTable())
        {
            systemTable = imports[0];

            if (imports.length != 0)
            {
                SymbolTable[] others = new SymbolTable[imports.length - 1];
                System.arraycopy(imports, 1, others, 0, imports.length - 1);
                imports = others;
            }
            else
            {
                imports = null;
            }
        }
        else
        {
            systemTable = mySystemSymbols;
        }

        UnifiedSymbolTable st = new UnifiedSymbolTable(systemTable, imports);
        st.setSystem(this); // XXX
        return st;
    }


    public UnifiedSymbolTable newSharedSymbolTable(IonStruct serialized)
    {
        UnifiedSymbolTable st = new UnifiedSymbolTable(serialized);
        return st;
    }

    public UnifiedSymbolTable newSharedSymbolTable(IonReader reader)
    {
        UnifiedSymbolTable st = new UnifiedSymbolTable(reader);
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
            SymbolTable prior = myCatalog.getTable(name, priorVersion);
            if (prior == null || prior.getVersion() != priorVersion)
            {
                String message =
                    "Catalog does not contain symbol table " +
                    printString(name) + " version " + priorVersion +
                    " required to create version " + version;
                throw new IonException(message);
            }

            addAllNonNull(syms, prior.iterateDeclaredSymbolNames());
        }

        for (SymbolTable imported : imports)
    {
            addAllNonNull(syms, imported.iterateDeclaredSymbolNames());
        }

        addAllNonNull(syms, newSymbols);

        return new UnifiedSymbolTable(name, version, syms.iterator());
    }


    public IonDatagramImpl newDatagram()
    {
        if (LoaderImpl.USE_NEW_READERS)
        {
            try
            {
                IonDatagramImpl dg =
                    new IonDatagramImpl(this, (IonReader) null);
                // Force symtab preparation  FIXME should not be necessary
                dg.byteSize();
                return dg;
            }
            catch (IOException e)
            {
                // Shouldn't happen actually
                throw new IonException(e);
            }
        }

        return new IonDatagramImpl(this);
    }

    public IonDatagram newDatagram(IonValue initialChild)
    {
        if (initialChild != null) {
            if (initialChild.getSystem() != this) {
                throw new IonException("this Ion system can't mix with instances from other system impl's");
            }
            // FIXME we shouldn't do this, it violates expectations
            if (initialChild.getContainer() != null) {
                initialChild = clone(initialChild);
            }
        }

        IonDatagramImpl datagram = newDatagram();

        if (initialChild != null) {
            //LocalSymbolTable symtab = initialChild.getSymbolTable();
            //if (symtab == null) {
            //    symtab = this.newLocalSymbolTable();
            //    IonValue ionRep = symtab.getIonRepresentation();
            //    datagram.add(ionRep, true);
            //    ((IonValueImpl)initialChild).setSymbolTable(symtab);
            //}

            // This will fail if initialChild instanceof IonDatagram:
            datagram.add(initialChild);
        }

        assert datagram._system == this;
        return datagram;
    }

    public IonDatagram newDatagram(SymbolTable... imports)
    {
        // TODO this implementation is awkward.
        UnifiedSymbolTable lst = newLocalSymbolTable(imports);

        IonDatagramImpl datagram = newDatagram();
        IonWriter newWriter = newWriter(datagram);
        try
        {
            lst.writeTo(newWriter);
        }
        catch (IOException e)
        {
            // Shouldn't happen
            throw new Error(e);
        }
        return datagram;
    }


    public IonLoader newLoader()
    {
        return new LoaderImpl(this);
    }

    public synchronized IonLoader getLoader()
    {
        return myLoader;
    }

    public synchronized void setLoader(IonLoader loader)
    {
        if (loader == null) throw new NullPointerException();
        myLoader = loader;
    }



    //=========================================================================
    // Iterator creation


    public Iterator<IonValue> iterate(Reader reader)
    {
        // TODO optimize to use IonTextReader, but first that must truly stream
        // instead of requiring a full-stream buffer.
        // See https://issue-tracking.amazon.com/browse/ION-31
        UserReader userReader =
            new UserReader(this, this.newLocalSymbolTable(), reader);
        userReader.setBufferToRecycle();
        return userReader;
    }


    public Iterator<IonValue> iterate(String ionText)
    {
        if (LoaderImpl.USE_NEW_READERS)
        {
            IonReader reader = newReader(ionText);
            return new IonIteratorImpl(this, reader);
        }

        UserReader userReader = new UserReader(this,
                                               this.newLocalSymbolTable(),
                                               new StringReader(ionText));
        userReader.setBufferToRecycle();
        return userReader;
    }

    public Iterator<IonValue> systemIterate(String ionText)
    {
        if (LoaderImpl.USE_NEW_READERS)
        {
            // TODO use IonIteratorImpl
        }

        return new SystemReader(this, ionText);
    }


    public Iterator<IonValue> iterate(byte[] ionData)
    {
        SystemReader systemReader = newLegacySystemReader(ionData);
        UserReader userReader = new UserReader(systemReader);
        // Don't use buffer-clearing!
        return userReader;
    }


    //=========================================================================
    // IonReader creation


    public IonReader newReader(String ionText)
    {
        return new IonTextReader(ionText, getCatalog());
    }

    public IonTextReader newSystemReader(String ionText)
    {
        return new IonTextReader(ionText, getCatalog(), true);
    }


    public IonReader newSystemReader(Reader ionText)
    {
        try
        {
            // FIXME we shouldn't have to load the whole stream into a String.
            String str = IonImplUtils.loadReader(ionText);
            return new IonTextReader(str, getCatalog(), true);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }


    public IonReader newReader(byte[] ionData)
    {
        return newReader(ionData, 0, ionData.length);
    }

    public IonReader newSystemReader(byte[] ionData)
    {
        return newSystemReader(ionData, 0, ionData.length);
    }


    public IonReader newReader(byte[] ionData, int offset, int len)
    {
        boolean isBinary = IonBinary.matchBinaryVersionMarker(ionData);

        IonReader reader;
        if (isBinary) {
            reader = new IonBinaryReader(ionData, offset, len, getCatalog());
        }
        else {
            reader =
                new IonTextReader(ionData, offset, len, getCatalog(), false);
        }
        return reader;
    }

    public IonReader newSystemReader(byte[] ionData, int offset, int len)
    {
        boolean isBinary = IonBinary.matchBinaryVersionMarker(ionData);

        IonReader reader;
        if (isBinary) {
            reader =
                new IonBinaryReader(ionData, offset, len, getCatalog(), true);
        }
        else {
            reader =
                new IonTextReader(ionData, offset, len, getCatalog(), true);
        }
        return reader;
    }


    public IonReader newReader(InputStream ionData)
    {
        // TODO optimize if stream is text!
        byte[] bytes;
        try
        {
            bytes = IonImplUtils.loadStreamBytes(ionData);
        }
        catch (IOException e)
        {
            throw new IonException("Error reading from stream", e);
        }

        return newReader(bytes, 0, bytes.length);
    }

    public IonReader newSystemReader(InputStream ionData)
    {
        byte[] bytes;
        try
        {
            bytes = IonImplUtils.loadStreamBytes(ionData);
        }
        catch (IOException e)
        {
            throw new IonException("Error reading from stream", e);
        }

        return newSystemReader(bytes, 0, bytes.length);
    }


    public IonReader newReader(IonValue value)
    {
        return new IonTreeReader(value);
    }

    public IonReader newSystemReader(IonDatagram dg)
    {
        return new IonTreeReader(dg, true);
    }


    //=========================================================================
    // IonWriter creation


    public IonWriter newWriter(IonContainer container)
    {
        return new IonTreeWriter(this, container);
    }

    public IonWriter newTextWriter(OutputStream out)
    {
        return new IonTextWriter(out);
    }

    public IonWriter newTextWriter(Appendable out)
    {
        return new IonTextWriter(out, false, true);
    }

    public IonWriter newTextWriter(OutputStream out, SymbolTable... imports)
        throws IOException
    {
        UnifiedSymbolTable lst = newLocalSymbolTable(imports);
        IonTextWriter writer = new IonTextWriter(out);
        writer.setSymbolTable(lst);
        return writer;
    }

    public IonWriter newTextWriter(Appendable out, SymbolTable... imports)
        throws IOException
    {
        UnifiedSymbolTable lst = newLocalSymbolTable(imports);
        IonTextWriter writer = new IonTextWriter(out);
        writer.setSymbolTable(lst);
        return writer;
    }

    // TODO also Utf8AsAscii flag
    public IonWriter newTextWriter(OutputStream out, boolean pretty)
    {
        return new IonTextWriter(out, pretty);
    }

    public IonBinaryWriterImpl newBinaryWriter()
    {
        SymbolTable systemSymbolTable = getSystemSymbolTable();
        return new IonBinaryWriterImpl(systemSymbolTable);
    }

    public IonBinaryWriterImpl newBinaryWriter(SymbolTable... imports)
    {
        UnifiedSymbolTable lst = newLocalSymbolTable(imports);
        return new IonBinaryWriterImpl(lst);
    }


    //=========================================================================
    // Internal SystemReader creation


    /**
     * Creates a new reader, wrapping an array of text or binary data.
     *
     * @param ionData may be (UTF-8) text or binary.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @throws NullPointerException if <code>ionData</code> is null.
     */
    public SystemReader newLegacySystemReader(byte[] ionData)
    {
        boolean isBinary =
            IonBinary.matchBinaryVersionMarker(ionData);

        SystemReader sysReader;
        if (isBinary) {
            sysReader = newBinarySystemReader(ionData);
        }
        else {
            sysReader = newTextSystemReader(ionData);
        }

        return sysReader;
    }


    /**
     * Creates a new reader, wrapping an array of binary data.
     *
     * @param ionBinary must be Ion binary data, not text.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @throws NullPointerException if <code>ionBinary</code> is null.
     */
    private SystemReader newBinarySystemReader(byte[] ionBinary)
    {
        BlockedBuffer bb = new BlockedBuffer(ionBinary);
        BufferManager buffer = new BufferManager(bb);
        return new SystemReader(this, buffer);
    }


    /**
     * Creates a new reader, wrapping bytes holding UTF-8 text.
     *
     * @param ionText must be UTF-8 encoded Ion text data, not binary.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @throws NullPointerException if <code>ionText</code> is null.
     */
    private SystemReader newTextSystemReader(byte[] ionText)
    {
        ByteArrayInputStream stream = new ByteArrayInputStream(ionText);
        Reader reader;
        try {
            reader = new InputStreamReader(stream, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new IonException(e);
        }

        return new SystemReader(this, getCatalog(), reader);
    }


    public SystemReader newBinarySystemReader(InputStream ionBinary)
        throws IOException
    {
        BufferManager buffer = new BufferManager(ionBinary);
        return new SystemReader(this, buffer);
    }

    //-------------------------------------------------------------------------
    // System Elements

    IonSymbolImpl newSystemIdSymbol(String systemId)
    {
        assert textIsSystemId(systemId);

        IonSymbolImpl ivm = (IonSymbolImpl) newSymbol(systemId);
        blessSystemIdSymbol(ivm);
        return ivm;
    }

    final void blessSystemIdSymbol(IonSymbolImpl systemId)
    {
        // TODO what if the symbol already has a symtab?
        // TODO what if the symbol already has a different system?

        SymbolTable symtab = getSystemSymbolTable(systemId.stringValue());

        systemId.setSymbolTable(symtab);  // This clears the sid
        systemId.setIsIonVersionMarker(true);
        assert systemId.getSymbolId() == 2;
    }

    /**
     * checks the value to see if it is a symbol and has the
     * form $ION_ddd_ddd.  Where ddd is 1 or more decimal
     * digits.  This includes the current value of $ION_1_0
     * which is really the only valid system id today, but
     * there may be more later.
     */
    final boolean valueIsSystemId(IonValue value)
    {
        if (value instanceof IonSymbol && ! value.isNullValue())
        {
            IonSymbol symbol = (IonSymbol) value;
            int sid = symbol.getSymbolId();
            if (sid == SystemSymbolTable.ION_1_0_SID)
            {
                return true;
            }
            // TODO quickly skip other sids
            String image = symbol.stringValue();
            return textIsSystemId(image);
        }
        return false;
    }

    private final boolean textIsSystemId(String image)
    {
        if (SystemSymbolTable.ION_1_0.equals(image))
        {
            return true;
        }
        if (!image.startsWith(SystemSymbolTable.ION)) {
            return false;
        }
        // now we see if the rest of the symbol is _DDD_DDD
        int underscore1 = SystemSymbolTable.ION.length();
        int underscore2 = image.indexOf('_', underscore1 + 1);
        if (underscore2 < 0)
        {
            return false;
        }
        if (!isUnderscoreAndDigits(image, underscore1, underscore2))
        {
            return false;
        }
        if (!isUnderscoreAndDigits(image, underscore2, image.length()))
        {
            return false;
        }
        return true;
    }


    private boolean isUnderscoreAndDigits(String image, int firstChar, int lastChar)
    {
        // you have to have enought characters for the underscore and
        // at least 1 digit
        if (lastChar - firstChar < 2) return false;

        // make sure the first character is the underscore
        if (image.charAt(firstChar) != '_') return false;

        // make sure all the remaining characters are digits
        for (int ii = firstChar + 1; ii < lastChar; ii++) {
            if (!Character.isDigit(image.charAt(ii))) return false;
        }

        // it must be "_ddd" then
        return true;
    }

    public final boolean valueIsLocalSymbolTable(IonValue value)
    {
        return (value instanceof IonStruct
                && value.hasTypeAnnotation(ION_SYMBOL_TABLE));
    }

    public final boolean valueIsSharedSymbolTable(IonValue value)
    {
        return (value instanceof IonStruct
                && value.hasTypeAnnotation(ION_SHARED_SYMBOL_TABLE));
    }


    //-------------------------------------------------------------------------
    // DOM creation

    private IonValue singleValue(Iterator<IonValue> iterator)
    {
        if (iterator.hasNext())
        {
            IonValue value = iterator.next();
            if (! iterator.hasNext())
            {
                return value;
            }
        }

        throw new IonException("not a single value");
    }

    public IonValue singleValue(String ionText)
    {
        Iterator<IonValue> iterator = iterate(ionText);
        return singleValue(iterator);
    }

    public IonValue singleValue(byte[] ionData)
    {
        Iterator<IonValue> iterator = iterate(ionData);
        return singleValue(iterator);
    }

    /**
     * @deprecated Use {@link #newNullBlob()} instead
     */
    @Deprecated
    public IonBlob newBlob()
    {
        return newNullBlob();
    }


    public IonBlob newNullBlob()
    {
        return new IonBlobImpl(this);
    }

    public IonBlob newBlob(byte[] value)
    {
        IonBlob result = new IonBlobImpl(this);
        result.setBytes(value);
        return result;
    }

    public IonBlob newBlob(byte[] value, int offset, int length)
    {
        IonBlob result = new IonBlobImpl(this);
        result.setBytes(value, offset, length);
        return result;
    }


    /**
     * @deprecated Use {@link #newNullBool()} instead
     */
    @Deprecated
    public IonBool newBool()
    {
        return newNullBool();
    }


    public IonBool newNullBool()
    {
        return new IonBoolImpl(this);
    }

    public IonBool newBool(boolean value)
    {
        IonBoolImpl result = new IonBoolImpl(this);
        result.setValue(value);
        return result;
    }

    public IonBool newBool(Boolean value)
    {
        IonBoolImpl result = new IonBoolImpl(this);
        result.setValue(value);
        return result;
    }


    /**
     * @deprecated Use {@link #newNullClob()} instead
     */
    @Deprecated
    public IonClob newClob()
    {
        return newNullClob();
    }


    public IonClob newNullClob()
    {
        return new IonClobImpl(this);
    }

    public IonClob newClob(byte[] value)
    {
        IonClob result = new IonClobImpl(this);
        result.setBytes(value);
        return result;
    }

    public IonClob newClob(byte[] value, int offset, int length)
    {
        IonClob result = new IonClobImpl(this);
        result.setBytes(value, offset, length);
        return result;
    }


    /**
     * @deprecated Use {@link #newNullDecimal()} instead
     */
    @Deprecated
    public IonDecimal newDecimal()
    {
        return newNullDecimal();
    }


    public IonDecimal newNullDecimal()
    {
        return new IonDecimalImpl(this);
    }

    public IonDecimal newDecimal(long value)
    {
        return new IonDecimalImpl(this, new BigDecimal(value));
    }

    public IonDecimal newDecimal(double value)
    {
        return new IonDecimalImpl(this, new BigDecimal(value));
    }

    public IonDecimal newDecimal(BigInteger value)
    {
        return new IonDecimalImpl(this, new BigDecimal(value));
    }

    public IonDecimal newDecimal(BigDecimal value)
    {
        return new IonDecimalImpl(this, value);
    }


    /**
     * @deprecated Use {@link #newNullFloat()} instead
     */
    @Deprecated
    public IonFloat newFloat()
    {
        return newNullFloat();
    }


    public IonFloat newNullFloat()
    {
        return new IonFloatImpl(this);
    }

    public IonFloat newFloat(long value)
    {
        return new IonFloatImpl(this, new Double(value));
    }

    public IonFloat newFloat(double value)
    {
        return new IonFloatImpl(this, new Double(value));
    }


    /**
     * @deprecated Use {@link #newNullInt()} instead
     */
    @Deprecated
    public IonInt newInt()
    {
        return newNullInt();
    }


    public IonInt newNullInt()
    {
        return new IonIntImpl(this);
    }

    public IonInt newInt(int content)
    {
        IonIntImpl result = new IonIntImpl(this);
        result.setValue(content);
        return result;
    }

    public IonInt newInt(long content)
    {
        IonIntImpl result = new IonIntImpl(this);
        result.setValue(content);
        return result;
    }

    public IonInt newInt(Number content)
    {
        IonIntImpl result = new IonIntImpl(this);
        result.setValue(content);
        return result;
    }


    /**
     * @deprecated Use {@link #newNullList()} instead
     */
    @Deprecated
    public IonList newList()
    {
        return newNullList();
    }


    public IonList newNullList()
    {
        return new IonListImpl(this);
    }

    public IonList newEmptyList()
    {
        return new IonListImpl(this, false);
    }

    public IonList newList(Collection<? extends IonValue> elements)
        throws ContainedValueException, NullPointerException
    {
        return new IonListImpl(this, elements);
    }

    public <T extends IonValue> IonList newList(T... elements)
        throws ContainedValueException, NullPointerException
    {
        List<T> e = (elements == null ? null : Arrays.asList(elements));
        return new IonListImpl(this, e);
    }

    public IonList newList(int[] elements)
    {
        ArrayList<IonInt> e = newInts(elements);
        return newList(e);
    }

    public IonList newList(long[] elements)
    {
        ArrayList<IonInt> e = newInts(elements);
        return newList(e);
    }


    public IonNull newNull()
    {
        return new IonNullImpl(this);
    }


    public IonValue newNull(IonType type)
    {
        switch (type)
        {
            case NULL:          return newNull();
            case BOOL:          return newNullBool();
            case INT:           return newNullInt();
            case FLOAT:         return newNullFloat();
            case DECIMAL:       return newNullDecimal();
            case TIMESTAMP:     return newNullTimestamp();
            case SYMBOL:        return newNullSymbol();
            case STRING:        return newNullString();
            case CLOB:          return newNullClob();
            case BLOB:          return newNullBlob();
            case LIST:          return newNullList();
            case SEXP:          return newNullSexp();
            case STRUCT:        return newNullStruct();
            default:
                throw new IllegalArgumentException();
        }
    }


    /**
     * @deprecated Use {@link #newNullSexp()} instead
     */
    @Deprecated
    public IonSexp newSexp()
    {
        return newNullSexp();
    }


    public IonSexp newNullSexp()
    {
        return new IonSexpImpl(this);
    }

    public IonSexp newEmptySexp()
    {
        return new IonSexpImpl(this, false);
    }

    public IonSexp newSexp(Collection<? extends IonValue> elements)
        throws ContainedValueException, NullPointerException
    {
        return new IonSexpImpl(this, elements);
    }

    public <T extends IonValue> IonSexp newSexp(T... elements)
        throws ContainedValueException, NullPointerException
    {
        List<T> e = (elements == null ? null : Arrays.asList(elements));
        return new IonSexpImpl(this, e);
    }

    public IonSexp newSexp(int[] elements)
    {
        ArrayList<IonInt> e = newInts(elements);
        return newSexp(e);
    }

    public IonSexp newSexp(long[] elements)
    {
        ArrayList<IonInt> e = newInts(elements);
        return newSexp(e);
    }


    /**
     * @deprecated Use {@link #newNullString()} instead
     */
    @Deprecated
    public IonString newString()
    {
        return newNullString();
    }


    public IonString newNullString()
    {
        return new IonStringImpl(this);
    }

    public IonString newString(String content)
    {
        IonStringImpl result = new IonStringImpl(this);
        result.setValue(content);
        return result;
    }


    /**
     * @deprecated Use {@link #newNullStruct()} instead
     */
    @Deprecated
    public IonStruct newStruct()
    {
        return newNullStruct();
    }


    public IonStruct newNullStruct()
    {
        return new IonStructImpl(this);
    }

    public IonStruct newEmptyStruct()
    {
        IonStructImpl result = new IonStructImpl(this);
        result.clear();
        return result;
    }


    /**
     * @deprecated Use {@link #newNullSymbol()} instead
     */
    @Deprecated
    public IonSymbol newSymbol()
    {
        return newNullSymbol();
    }


    public IonSymbol newNullSymbol()
    {
        return new IonSymbolImpl(this);
    }

    public IonSymbol newSymbol(String name)
    {
        return new IonSymbolImpl(this, name);
    }

    /**
     * @deprecated Use {@link #newNullTimestamp()} instead
     */
    @Deprecated
    public IonTimestamp newTimestamp()
    {
        return newNullTimestamp();
    }


    public IonTimestamp newNullTimestamp()
    {
        return new IonTimestampImpl(this);
    }

    public IonTimestamp newTimestamp(Timestamp timestamp)
    {
        IonTimestamp result = newNullTimestamp();
        result.setValue(timestamp);
        return result;
    }

    public IonTimestamp newUtcTimestampFromMillis(long millis)
    {
        IonTimestampImpl result = new IonTimestampImpl(this);
        result.setMillisUtc(millis);
        return result;
    }

    public IonTimestamp newUtcTimestamp(Date value)
    {
        IonTimestampImpl result = new IonTimestampImpl(this);
        if (value != null)
        {
            result.setMillisUtc(value.getTime());
        }
        return result;
    }

    public IonTimestamp newCurrentUtcTimestamp()
    {
        IonTimestampImpl result = new IonTimestampImpl(this);
        result.setCurrentTimeUtc();
        return result;
    }


    @SuppressWarnings("unchecked")
    public <T extends IonValue> T clone(T value)
    {
        // Use "fast clone" when the system is the same.
        if (value.getSystem() == this)
        {
            return (T) value.clone();
        }

        if (value instanceof IonDatagram)
        {
            byte[] data = ((IonDatagram)value).getBytes();

            // TODO This can probably be optimized further.
            return (T) new IonDatagramImpl(this, data);
        }

        StringBuilder buffer = new StringBuilder();
        Printer printer = new Printer();
        try
        {
            printer.print(value, buffer);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }

        String text = buffer.toString();
        return (T) singleValue(text);
    }


    //=========================================================================
    // Helpers

    private ArrayList<IonInt> newInts(int[] elements)
    {
        ArrayList<IonInt> e = null;

        if (elements != null)
        {
            e = new ArrayList<IonInt>(elements.length);
            for (int i = 0; i < elements.length; i++)
            {
                int value = elements[i];
                e.add(newInt(value));
            }
        }

        return e;
    }

    private ArrayList<IonInt> newInts(long[] elements)
    {
        ArrayList<IonInt> e = null;

        if (elements != null)
        {
            e = new ArrayList<IonInt>(elements.length);
            for (int i = 0; i < elements.length; i++)
            {
                long value = elements[i];
                e.add(newInt(value));
            }
        }

        return e;
    }
}
