// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;

/**
 * NOT FOR APPLICATION USE!
 */
public interface _Private_IonSystem
    extends IonSystem
{
    public SymbolTable newSharedSymbolTable(IonStruct ionRep);

    /**
     * TODO Must correct ION-233 before exposing this or using from public API.
     */
    public Iterator<IonValue> systemIterate(String ionText);

    /**
     * TODO Must correct ION-233 before exposing this or using from public API.
     */
    public Iterator<IonValue> systemIterate(Reader ionText);

    public Iterator<IonValue> systemIterate(byte[] ionData);

    /**
     * TODO Must correct ION-233 before exposing this or using from public API.
     */
    public Iterator<IonValue> systemIterate(InputStream ionData);

    public IonReader newSystemReader(Reader ionText);

    public IonReader newSystemReader(byte[] ionData);

    public IonReader newSystemReader(byte[] ionData, int offset, int len);

    public IonReader newSystemReader(String ionText);

    public IonReader newSystemReader(InputStream ionData);

    public IonReader newSystemReader(IonValue value);


    public IonWriter newTreeWriter(IonContainer container);

    public IonWriter newTreeSystemWriter(IonContainer container);


    public boolean valueIsSharedSymbolTable(IonValue value);

    /**
     * Indicates whether writers built by this system may attempt to optimize
     * {@link IonWriter#writeValue(IonReader)} by copying raw source data.
     *
     * @see IonSystemBuilder#isStreamCopyOptimized()
     */
    public boolean isStreamCopyOptimized();
}
