// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.SymbolTable;

/**
 * Context for IonValues that are not contained in any Container or Datagram
 */
public class StubContext
    implements IonContext
{
    private final IonSystemLite _system;
    private final SymbolTable _symbols;

    public static IonContext wrap(IonSystemLite system){
        return new StubContext(system, null);
    }

    public static IonContext wrap(IonSystemLite system, SymbolTable symbols){
        return new StubContext(system, symbols);
    }

    private StubContext(IonSystemLite system, SymbolTable symbols){
        _system = system;
        _symbols = symbols;
    }

    public IonContainerLite getContextContainer()
    {
        return null;
    }

    public IonSystemLite getSystem()
    {
        return _system;
    }

    public SymbolTable getContextSymbolTable()
    {
        return _symbols;
    }

}
