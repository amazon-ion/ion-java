/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

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
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.UnsupportedSystemVersionException;
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
        throws UnsupportedSystemVersionException
    {
        if (systemId.equals(SystemSymbolTable.ION_1_0))
        {
            return mySystemSymbols;
        }

        throw new UnsupportedSystemVersionException(systemId);
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


    public UnifiedSymbolTable newLocalSymbolTable()
    {
        UnifiedSymbolTable st = new UnifiedSymbolTable(mySystemSymbols);
        st.setSystem(this);
        return st;
    }


    public UnifiedSymbolTable newLocalSymbolTable(SymbolTable systemSymbols)
    {
        if (! systemSymbols.isSystemTable())
        {
            String message = "systemSymbols is not a system symbol table.";
            throw new IllegalArgumentException(message);
        }

        UnifiedSymbolTable st = new UnifiedSymbolTable(systemSymbols);
        st.setSystem(this);
        return st;
    }


    public IonDatagram newDatagram(IonValue initialChild)
        throws ContainedValueException
    {
        if (initialChild != null) {
            if (!(initialChild instanceof IonValueImpl)) {
                throw new IonException("this Ion system can't mix with instances from other system impl's");
            }
            if (initialChild.getContainer() != null) {
                initialChild = clone(initialChild);
            }
        }

        IonDatagramImpl datagram = new IonDatagramImpl(this);

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
        return new UserReader(this, this.newLocalSymbolTable(), reader);
    }


    public Iterator<IonValue> iterate(String ionText)
    {
        return new UserReader(this,
                              this.newLocalSymbolTable(),
                              new StringReader(ionText));
    }


    public Iterator<IonValue> iterate(byte[] ionData)
    {
        SystemReader systemReader = newSystemReader(ionData);
        return new UserReader(systemReader);
    }


    //=========================================================================
    // IonReader creation


    public IonReader newReader(String ionText)
    {
        return new IonTextReader(ionText);
    }


    public IonReader newReader(byte[] ionData)
    {
        return newReader(ionData, 0, ionData.length);
    }

    public IonReader newReader(byte[] ionData, int offset, int len)
    {
        boolean isBinary = IonBinary.matchBinaryVersionMarker(ionData);

        IonReader reader;
        if (isBinary) {
            reader = new IonBinaryReader(ionData, offset, len);
        }
        else {
            reader = new IonTextReader(ionData, offset, len);
        }
        return reader;
    }


    public IonReader newReader(InputStream ionData)
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

        return newReader(bytes, 0, bytes.length);
    }


    public IonReader newReader(IonValue value)
    {
        return new IonTreeReader(value);
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

    // TODO also Utf8AsAscii flag
    public IonWriter newTextWriter(OutputStream out, boolean pretty)
    {
        return new IonTextWriter(out, pretty);
    }

    public IonBinaryWriterImpl newBinaryWriter()
    {
        return new com.amazon.ion.impl.IonBinaryWriterImpl();
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
    public SystemReader newSystemReader(byte[] ionData)
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

    /**
     * checks the value to see if it is a symbol and has the
     * form $ION_ddd_ddd.  Where ddd is 1 or more decimal
     * digits.  This includes the current value of $ION_1_0
     * which is really the only valid system id today, but
     * there may be more later.
     */
    private final boolean valueIsSystemId(IonValue value)
    {
        if (value instanceof IonSymbol && ! value.isNullValue())
        {
            IonSymbol symbol = (IonSymbol) value;
            int sid = symbol.getSymbolId();
            if (sid == SystemSymbolTableImpl.ION_1_0_SID)
            {
                return true;
            }
            String image = symbol.stringValue();
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
        return false;
    }

    boolean isUnderscoreAndDigits(String image, int firstChar, int lastChar)
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

    public final boolean valueIsStaticSymbolTable(IonValue value)
    {
        if (value instanceof IonStruct
            && value.hasTypeAnnotation(SystemSymbolTable.ION_SYMBOL_TABLE))
        {
            IonStruct struct = (IonStruct) value;
            IonValue nameField = struct.get(SystemSymbolTable.NAME);
            return (nameField != null && !nameField.isNullValue());
        }
        return false;
    }

    public final SymbolTable handleLocalSymbolTable(IonCatalog catalog,
                                                    IonValue value)
    {
        // This assumes that we only are handling 1_0

        SymbolTable symtab = null;

        if (value instanceof IonStruct)
        {
            if (AbstractSymbolTable.getSymbolTableType(value).equals(SymbolTableType.LOCAL))
            //if (value.hasTypeAnnotation(SystemSymbolTable.ION_SYMBOL_TABLE)) // cas 25 apr 2008 was: ION_1_0
            {
                symtab = new UnifiedSymbolTable(mySystemSymbols, // FIXME wrong symtab
                                                (IonStruct) value,
                                                catalog);
            }
        }
        else if (valueIsSystemId(value))
        {
            symtab = value.getSymbolTable();
            if (symtab == null
            || symtab.getMaxId() != mySystemSymbols.getMaxId()
            ) {
                // TODO Should $ion_1_0 really have a local symtab?
                String systemId = ((IonSymbol)value).stringValue();
                symtab = newLocalSymbolTable(getSystemSymbolTable(systemId));
                ((IonValueImpl)value).setSymbolTable(symtab);
            }
        }

        return symtab;

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
        if (((IonValueImpl)value)._system == this)
        {
            return (T) value.clone();
        }

        if (value instanceof IonDatagram)
        {
            byte[] data = ((IonDatagram)value).toBytes();

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
