// Copyright (c) 2015-2016 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.SymbolTable;

/**
 * Context for IonValues that are not contained in any Container or Datagram
 */
/*package*/ class ContainerlessContext
    implements IonContext
{
    private final IonSystemLite _system;
    private final SymbolTable _symbols;

    public static ContainerlessContext wrap(IonSystemLite system){
        return new ContainerlessContext(system, null);
    }

    public static ContainerlessContext wrap(IonSystemLite system, SymbolTable symbols){
        return new ContainerlessContext(system, symbols);
    }

    private ContainerlessContext(IonSystemLite system, SymbolTable symbols){
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
