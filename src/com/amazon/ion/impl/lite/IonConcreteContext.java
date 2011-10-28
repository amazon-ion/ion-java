// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.impl.UnifiedSymbolTable.isNonSystemSharedTable;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.UnifiedSymbolTable;


/**
 *
 */
class IonConcreteContext
    implements IonContext
{
    /**
     * Do we need this?  We should be able to follow
     * the _owning_context up until we get to a system.
     * And getSystem should not be called very often.
     */
    private final IonSystemLite _system;

    /**
     * Currently the _owning_context will be the system for a
     * loose value or a datagram when the top level value has
     * a local symbol table.  This could change is other
     * state starts being tracked in the context such as the
     * location in the binary buffer.
     * <p>
     * TODO is this ever null?
     * <p>
     * TODO what if this is an interior context?
     * Does the point to the IonContainer?
     */
    private IonContext _owning_context;

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

    IonConcreteContext(IonSystemLite system) {
        _system = system;
    }

    static void attachWithConcreteContext(IonContext parent,
                                          IonValueLite child,
                                          SymbolTable symbolTable)
    {
        IonConcreteContext concrete_context;

        if (child._context  instanceof IonConcreteContext) {
            concrete_context = (IonConcreteContext)child._context;
            // TODO Why not change the child's context to be the parent?
        }
        else {
            concrete_context = new IonConcreteContext(parent.getSystem());
        }

        concrete_context._owning_context = parent;
        child._context = concrete_context;
        concrete_context._symbols = symbolTable;
    }

    /**
     * @param parent must not be null.
     * @param child must not be null.
     */
    static void attachWithoutConcreteContext(IonContext parent,
                                             IonValueLite child)
    {
        assert(test_symbol_table_compatibility(parent, child));
        if (child._context instanceof IonConcreteContext) {
            ((IonConcreteContext)child._context).clear();
        }
        child._context = parent;
    }

    private static boolean test_symbol_table_compatibility(IonContext parent,
                                                           IonValueLite child)
    {
        SymbolTable parent_symbols = parent.getSymbolTable();
        SymbolTable child_symbols = child.getAssignedSymbolTable();

        if (UnifiedSymbolTable.isLocalAndNonTrivial(child_symbols)) {
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
        _owning_context = null;
        _symbols = null;
    }

    public void clearLocalSymbolTable()
    {
        _symbols = null;
    }

    public SymbolTable getLocalSymbolTable(IonValueLite child)
    {
        SymbolTable local;

        assert _owning_context != null;

        if (_symbols != null && _symbols.isLocalTable()) {
            local = _symbols;
        }
        //else if (_owning_context != null) {
        //    local = _owning_context.getLocalSymbolTable(child);
        //}
        else {
            IonSystem system = getSystem();
            local = system.newLocalSymbolTable();
            _symbols = local;
        }
        assert(local != null);

        return local;
    }

    public IonContainerLite getParentThroughContext()
    {
        if (_owning_context instanceof IonDatagramLite) {
            return (IonDatagramLite)_owning_context;
        }
        // a concrete context only exists on a top level
        // value to its parent should be system, datagram
        // or null
        assert(_owning_context == null || _owning_context instanceof IonSystem);

        return null;
    }

    public SymbolTable getSymbolTable()
    {
        if (_symbols != null) {
            return _symbols;
        }
        if (_owning_context != null) {
            return _owning_context.getSymbolTable();
        }
        return _system.getSystemSymbolTable();
    }

    public SymbolTable getContextSymbolTable()
    {
        return _symbols;
    }

    public IonSystemLite getSystem()
    {
        assert(_system != null);
        return _system;
    }

    /**
     * @param newParent must not be null
     */
    public void setParentThroughContext(IonValueLite child, IonContext newParent)
    {
        // HACK: we need to refactor this to make it simpler and take
        //       away the need to check the parent type

        // but for now ...
        if (newParent instanceof IonDatagram
         || newParent instanceof IonSystem
         || newParent instanceof IonConcreteContext
        ) {
            _owning_context = newParent;
            child._context = this;
        }
        else {
            // struct, list, sexp, templist
            attachWithoutConcreteContext(newParent, child);
        }
    }

    public void setSymbolTableOfChild(SymbolTable symbols, IonValueLite child)
    {
        assert (_owning_context == null
             || _owning_context instanceof IonSystem
             || _owning_context instanceof IonDatagram
        );

        // the only valid cases where you can set a concrete
        // contexts symbol table is when this is a top level
        // value.  That is the owning context is null, a datagram
        // of a system intance.

        if (isNonSystemSharedTable(symbols)) {
            throw new IllegalArgumentException("you can only set a symbol table to a system or local table");
        }
        _symbols = symbols;
        if (child._context != this) {
            assert(child._context != null && child._context == this._owning_context);
            child._context = this;
        }
    }
}
