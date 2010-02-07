// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonBinary.BufferManager;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;


public interface SystemReader
    extends Iterator<IonValue>, Closeable
{
    /********************************************************************
     *
     *                  Iterator<IonValue>
     *
     */
    public boolean hasNext();
    public IonValue next();
    public void remove();

    /********************************************************************
     *
     *                  SystemReader
     *
     */

    // constructors in original SystemReader:
    /*  make these static
    public SystemReader makeSystemReader(IonSystemImpl system, String s);
    public SystemReader makeSystemReader(IonSystemImpl system,
                        IonCatalog catalog,
                        Reader input);
    public SystemReader makeSystemReader(IonSystemImpl system,
                        IonCatalog catalog,
                        SymbolTable initialSymboltable,
                        Reader input);
    @Deprecated
    public SystemReader makeSystemReader(IonSystemImpl system,
                        IonCatalog catalog,
                        BufferManager buffer);

    public SystemReader makeSystemReader(IonSystemImpl system,
                        IonCatalog catalog,
                        InputStream stream);
    */

    public IonSystem getSystem();
    public IonCatalog getCatalog();
    public SymbolTable getLocalSymbolTable();
    public boolean currentIsHidden();
    public boolean canSetLocalSymbolTable();
    public void setLocalSymbolTable(SymbolTable symbolTable);
    public BufferManager getBuffer();
    public void resetBuffer();
    public void close() throws IOException;
}
