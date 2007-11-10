/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.system;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonBlob;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonClob;
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
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.StaticSymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.UnsupportedSystemVersionException;
import com.amazon.ion.impl.BlockedBuffer;
import com.amazon.ion.impl.IonBinary;
import com.amazon.ion.impl.IonBlobImpl;
import com.amazon.ion.impl.IonBoolImpl;
import com.amazon.ion.impl.IonClobImpl;
import com.amazon.ion.impl.IonDatagramImpl;
import com.amazon.ion.impl.IonDecimalImpl;
import com.amazon.ion.impl.IonFloatImpl;
import com.amazon.ion.impl.IonIntImpl;
import com.amazon.ion.impl.IonListImpl;
import com.amazon.ion.impl.IonNullImpl;
import com.amazon.ion.impl.IonSexpImpl;
import com.amazon.ion.impl.IonStringImpl;
import com.amazon.ion.impl.IonStructImpl;
import com.amazon.ion.impl.IonSymbolImpl;
import com.amazon.ion.impl.IonTimestampImpl;
import com.amazon.ion.impl.LoaderImpl;
import com.amazon.ion.impl.LocalSymbolTableImpl;
import com.amazon.ion.impl.StaticSymbolTableImpl;
import com.amazon.ion.impl.SystemReader;
import com.amazon.ion.impl.SystemSymbolTableImpl;
import com.amazon.ion.impl.UserReader;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.util.Printer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * The standard, public implementation of Ion.
 */
public class StandardIonSystem
    implements IonSystem
{
    private SystemSymbolTableImpl mySystemSymbols = new SystemSymbolTableImpl();
    private IonCatalog myCatalog = new SimpleCatalog();
    private IonLoader  myLoader = new LoaderImpl(this);


    public StandardIonSystem()
    {

    }


    public SystemSymbolTableImpl getSystemSymbolTable()
    {
        return mySystemSymbols;
    }

    public SystemSymbolTable getSystemSymbolTable(String systemId)
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


    public LocalSymbolTable newLocalSymbolTable()
    {
        return new LocalSymbolTableImpl(mySystemSymbols);
    }


    public LocalSymbolTable newLocalSymbolTable(SystemSymbolTable systemSymbols)
    {
        return new LocalSymbolTableImpl(systemSymbols);
    }


    public StaticSymbolTable newStaticSymbolTable(IonStruct symbolTable)
    {
        return new StaticSymbolTableImpl(this, symbolTable);
    }


    public IonDatagram newDatagram(IonValue initialElement)
        throws ContainedValueException
    {
        // TODO what if value is IonDatagram?
        IonDatagramImpl datagram = new IonDatagramImpl(this);

        if (initialElement.getContainer() != null
         || initialElement instanceof IonDatagram
        ) {
            initialElement = clone(initialElement);
        }
        datagram.add(initialElement);

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
    // IonReader creation

    /**
     *  @deprecated Renamed to {@link #newTextReader(Reader)}.
     */
    public IonReader newReader(Reader reader)
    {
        return newTextReader(reader);
    }

    public IonReader newTextReader(Reader reader)
    {
        return new UserReader(this, this.newLocalSymbolTable(), reader);
    }

    /**
     *  @deprecated Renamed to {@link #newTextReader(String)}.
     */
    public IonReader newReader(String ionText)
    {
        return newTextReader(ionText);
    }

    public IonReader newTextReader(String ionText)
    {
        return new UserReader(this,
                              this.newLocalSymbolTable(),
                              new StringReader(ionText));
    }


    public IonReader newReader(byte[] ionData)
    {
        SystemReader systemReader = newSystemReader(ionData);
        return new UserReader(systemReader);
    }


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
        boolean isbinary =
            IonBinary.isMagicCookie(ionData, 0, ionData.length);

        SystemReader sysReader;
        if (isbinary) {
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
        BlockedBuffer bb = new BlockedBuffer();
        BufferManager buffer = new BufferManager(bb);
        IonBinary.Writer writer = buffer.writer();
        try {
            writer.write(ionBinary);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        finally {
            ionBinary.close();
        }

        return new SystemReader(this, buffer);
    }

    //-------------------------------------------------------------------------
    // System Elements

    /**
     * TODO this doesn't recognize general $ion_X_Y (and it should).
     */
    private final boolean valueIsSystemId(IonValue value)
    {
        if (value instanceof IonSymbol && ! value.isNullValue())
        {
            IonSymbol symbol = (IonSymbol) value;
            int sid = symbol.intValue();
            if (sid > 0)
            {
                return sid == SystemSymbolTableImpl.ION_1_0_SID;
            }

            return SystemSymbolTable.ION_1_0.equals(symbol.stringValue());
        }
        return false;
    }

    public final boolean valueIsStaticSymbolTable(IonValue value)
    {
        return (value instanceof IonStruct
                && value.hasTypeAnnotation(SystemSymbolTable.ION_SYMBOL_TABLE));
    }

    public final LocalSymbolTable handleLocalSymbolTable(IonCatalog catalog,
                                                         IonValue value)
    {
        // This assumes that we only are handling 1_0

        LocalSymbolTable symtab = null;

        if (value instanceof IonStruct)
        {
            if (value.hasTypeAnnotation(SystemSymbolTable.ION_1_0))
            {
                symtab = new LocalSymbolTableImpl(this, catalog,
                                                  (IonStruct) value,
                                                  mySystemSymbols);
            }
        }
        else if (valueIsSystemId(value))
        {
            symtab = new LocalSymbolTableImpl(mySystemSymbols);
        }

        return symtab;

    }


    //-------------------------------------------------------------------------
    // DOM creation

    private IonValue singleValue(IonReader reader)
    {
        try
        {
            if (reader.hasNext())
            {
                IonValue value = reader.next();
                if (! reader.hasNext())
                {
                    return value;
                }
            }
        }
        finally
        {
            reader.close();
        }

        throw new IonException("not a single value");
    }

    public IonValue singleValue(String ionText)
    {
        IonReader reader = newTextReader(ionText);
        return singleValue(reader);
    }

    public IonValue singleValue(byte[] ionData)
    {
        IonReader reader = newReader(ionData);
        return singleValue(reader);
    }

    public IonBlob newBlob()
    {
        return new IonBlobImpl();
    }


    public IonBool newBool()
    {
        return new IonBoolImpl();
    }

    public IonBool newBool(boolean value)
    {
        IonBool result = new IonBoolImpl();
        result.setValue(value);
        return result;
    }


    public IonClob newClob()
    {
        return new IonClobImpl();
    }


    public IonDecimal newDecimal()
    {
        return new IonDecimalImpl();
    }


    public IonFloat newFloat()
    {
        return new IonFloatImpl();
    }


    public IonInt newInt()
    {
        return new IonIntImpl();
    }

    public IonInt newInt(int content)
    {
        IonInt result = new IonIntImpl();
        result.setValue(content);
        return result;
    }

    public IonInt newInt(long content)
    {
        IonInt result = new IonIntImpl();
        result.setValue(content);
        return result;
    }

    public IonInt newInt(Number content)
    {
        IonInt result = new IonIntImpl();
        result.setValue(content);
        return result;
    }


    public IonList newList()
    {
        return new IonListImpl();
    }

    public IonList newEmptyList()
    {
        return new IonListImpl(false);
    }

    public IonList newList(Collection<? extends IonValue> elements)
        throws ContainedValueException, NullPointerException
    {
        return new IonListImpl(elements);
    }

    public <T extends IonValue> IonList newList(T... elements)
        throws ContainedValueException, NullPointerException
    {
        List<T> e = (elements == null ? null : Arrays.asList(elements));
        return new IonListImpl(e);
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
        return new IonNullImpl();
    }


    public IonSexp newSexp()
    {
        return new IonSexpImpl();
    }

    public IonSexp newEmptySexp()
    {
        return new IonSexpImpl(false);
    }

    public IonSexp newSexp(Collection<? extends IonValue> elements)
        throws ContainedValueException, NullPointerException
    {
        return new IonSexpImpl(elements);
    }

    public <T extends IonValue> IonSexp newSexp(T... elements)
        throws ContainedValueException, NullPointerException
    {
        List<T> e = (elements == null ? null : Arrays.asList(elements));
        return new IonSexpImpl(e);
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


    public IonString newString()
    {
        return new IonStringImpl();
    }

    public IonString newString(String content)
    {
        IonString result = new IonStringImpl();
        result.setValue(content);
        return result;
    }


    public IonStruct newStruct()
    {
        return new IonStructImpl();
    }

    public IonStruct newEmptyStruct()
    {
        IonStruct result = new IonStructImpl();
        result.clear();
        return result;
    }


    public IonSymbol newSymbol()
    {
        return new IonSymbolImpl();
    }

    public IonSymbol newSymbol(String name)
    {
        return new IonSymbolImpl(name);
    }

    public IonTimestamp newTimestamp()
    {
        return new IonTimestampImpl();
    }

    public IonTimestamp newUtcTimestampFromMillis(long millis)
    {
        IonTimestamp result = new IonTimestampImpl();
        result.setMillisUtc(millis);
        return result;
    }

    public IonTimestamp newUtcTimestamp(Date value)
    {
        IonTimestamp result = new IonTimestampImpl();
        if (value != null)
        {
            result.setMillisUtc(value.getTime());
        }
        return result;
    }

    public IonTimestamp newCurrentUtcTimestamp()
    {
        IonTimestamp result = new IonTimestampImpl();
        result.setCurrentTimeUtc();
        return result;
    }


    @SuppressWarnings("unchecked")
    public <T extends IonValue> T clone(T value)
    {
        // TODO make clone(IonDatagram) work!
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
