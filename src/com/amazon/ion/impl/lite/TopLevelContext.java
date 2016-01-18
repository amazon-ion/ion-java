// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.SymbolTable;


/**
 * Context for child values of an IonDatagramLite. The
 * datagram's child values that share the same local symbol table
 * will share the same TopLevelContext.
 */
final class TopLevelContext
    implements IonContext
{
    /**
     * References the containing datagram
     */
    private final IonDatagramLite _datagram;

    /**
     * This will be a local symbol table, or null.  It is not valid
     * for this to be a shared symbol table since shared
     * symbol tables are only shared.  It will not be a
     * system symbol table as the system object will be
     * able to resolve its symbol table to the system
     * symbol table and following the parent/owning_context
     * chain will lead to a system object.
     * <p>
     * TODO ION-258 we cannot assume that the IonSystem knows the proper IVM
     * in this context
     */
    private final SymbolTable _symbols;

    private TopLevelContext(SymbolTable symbols, IonDatagramLite datagram)
    {
        assert datagram != null;
        _symbols = symbols;
        _datagram = datagram;
    }

    static TopLevelContext wrap(SymbolTable symbols,
                                IonDatagramLite datagram)
    {
        TopLevelContext context = new TopLevelContext(symbols, datagram);
        return context;
    }

    public IonDatagramLite getContextContainer()
    {
        return _datagram;
    }

    public SymbolTable getContextSymbolTable()
    {
        return _symbols;
    }

    public IonSystemLite getSystem()
    {
        return _datagram.getSystem();
    }

}
