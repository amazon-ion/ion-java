// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl._Private_Utils;


/**
 *
 */
final class TopLevelContext
    implements IonContext
{
    /**
     * References the system used to construct values in this context.
     * Not null.
     */
    private final IonSystemLite _system;

    /**
     * References the containing datagram, if it exists.
     */
    private IonDatagramLite _datagram;

    /**
     * This will be a local symbol table.  It is not valid
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
    private SymbolTable _symbols;

    private TopLevelContext(IonSystemLite system, IonDatagramLite datagram)
    {
        assert system != null;
        _system = system;
        _datagram = datagram;
    }

    static TopLevelContext wrap(IonSystemLite system,
                                IonDatagramLite datagram,
                                IonValueLite child)
    {
        TopLevelContext context = new TopLevelContext(system, datagram);
        child._context = context;
        return context;
    }

    static TopLevelContext wrap(IonSystemLite system,
                                SymbolTable symbols,
                                IonValueLite child)
    {
        if (_Private_Utils.symtabIsSharedNotSystem(symbols)) {
            throw new IllegalArgumentException("you can only set a symbol table to a system or local table");
        }

        TopLevelContext context = new TopLevelContext(system, null);
        context._symbols = symbols;

        child._context = context;

        return context;
    }


    void rewrap(IonDatagramLite datagram, IonValueLite child)
    {
        _datagram = datagram;
        child._context = this;
    }


    private static boolean test_symbol_table_compatibility(IonContainerLite parent,
                                                           IonValueLite child)
    {
        SymbolTable parent_symbols = parent.getSymbolTable();
        SymbolTable child_symbols = child.getAssignedSymbolTable();

        if (_Private_Utils.symtabIsLocalAndNonTrivial(child_symbols)) {
            // we may have a problem here ...
            if (child_symbols != parent_symbols) {
                // perhaps we should throw
                // but for now we're just ignoring this since
                // in a valueLite all symbols have string values
                // we could throw or return false
            }
        }
        return true;
    }

    protected void clear()
    {
        _datagram = null;
        _symbols = null;
    }

    public void clearLocalSymbolTable()
    {
        _symbols = null;
    }

    public SymbolTable ensureLocalSymbolTable(IonValueLite child)
    {
        SymbolTable local;

        if (_symbols != null && _symbols.isLocalTable()) {
            local = _symbols;
        }
        else {
            IonSystem system = getSystem();
            local = system.newLocalSymbolTable();
            _symbols = local;
        }
        assert(local != null);

        return local;
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
        return _system;
    }

    /**
     * @param container must not be null
     */
    public void setContextContainer(IonContainerLite container,
                                    IonValueLite child)
    {
        assert child._context == this;
        assert _datagram == null;

        child.clearSymbolIDValues();

        // HACK: we need to refactor this to make it simpler and take
        //       away the need to check the parent type

        // but for now ...
        if (container instanceof IonDatagramLite)
        {
            // Leave this context between the TLV and the datagram, using the
            // same symbol table we already have.

            _datagram = (IonDatagramLite) container;
        }
        else {
            // Some other container (struct, list, sexp, templist) is taking
            // over, this context is no longer needed.

            assert(test_symbol_table_compatibility(container, child));

            // FIXME this should be recycling this context
            // TODO this assumes there's never >1 value with the same context
            clear();

            child.setContext(container);
        }
    }

    public void setSymbolTableOfChild(SymbolTable symbols, IonValueLite child)
    {
        assert child._context == this;

        // the only valid cases where you can set a concrete
        // contexts symbol table is when this is a top level
        // value.  That is the owning context is null, a datagram
        // of a system intance.

        if (_Private_Utils.symtabIsSharedNotSystem(symbols)) {
            throw new IllegalArgumentException("you can only set a symbol table to a system or local table");
        }
        _symbols = symbols;
    }
}
