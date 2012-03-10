// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Iterator;

/**
 *
 */
public interface _Private_IonSystem
    extends IonSystem
{
    /* constructor */
    // public IonSystemImpl(IonCatalog catalog);

    public SymbolTable newSharedSymbolTable(IonStruct ionRep);

    /**
     * TODO Must correct ION-160 before exposing this or using from public API.
     * TODO Must correct ION-262 before exposing this or using from public API.
     */
    public Iterator<IonValue> systemIterate(String ionText);

    /**
     * TODO Must correct ION-160 before exposing this or using from public API.
     * TODO Must correct ION-262 before exposing this or using from public API.
     */
    public Iterator<IonValue> systemIterate(Reader ionText);

    public Iterator<IonValue> systemIterate(byte[] ionData);

    /**
     * TODO Must correct ION-160 before exposing this or using from public API.
     * TODO Must correct ION-262 before exposing this or using from public API.
     */
    public Iterator<IonValue> systemIterate(InputStream ionData);

    public IonTextReader newSystemReader(String ionText);

    public IonTextReader newSystemReader(Reader ionText);

    public IonReader newSystemReader(byte[] ionData);

    public IonReader newSystemReader(byte[] ionData, int offset, int len);

    public IonReader newSystemReader(InputStream ionData);

    public IonReader newSystemReader(IonValue value);

    @Deprecated // TODO ION-271 remove after IMS is migrated
    public IonWriter newTextWriter(OutputStream out, boolean pretty);

    public IonWriter newTreeWriter(IonContainer container);

    public IonWriter newTreeSystemWriter(IonContainer container);


    public boolean valueIsSharedSymbolTable(IonValue value);
}
