// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.UnifiedSymbolTable;

/**
 *
 */
public class IonConcreteContext
    implements IonContext
{
    // Do we need this?  We should be able to follow
    // the _owning_context up until we get to a system.
    // And getSystem should not be called very often.
    private final IonSystemLite _system;

    // currently the _owning_context will be the system for a
    // loose value or a datagram when the top level value has
    // a local symbol table.  This could change is other
    // state starts being tracked in the context such as the
    // location in the binary buffer.
    private       IonContext    _owning_context;

    // This will be a local symbol table.  It is not valid
    // for this to be a shared symbol table since shared
    // symbol tables are only shared.  It will not be a
    // system symbol table as the system object will be
    // able to resolve its symbol table to the system
    // symbol table and following the parent/owning_context
    // chain will lead to a system object.
    private       SymbolTable   _symbols;

    protected IonConcreteContext(IonSystemLite system) {
        _system = system;
    }

    protected void clear()
    {
        _owning_context = null;
        _symbols = null;
    }

    public SymbolTable getLocalSymbolTable(IonValueLite child)
    {
        SymbolTable local;

        if (_symbols != null && _symbols.isLocalTable()) {
            local = _symbols;
        }
        //else if (_owning_context != null) {
        //    local = _owning_context.getLocalSymbolTable(child);
        //}
        else {
            IonSystem system = getSystemLite();
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

    public IonSystemLite getSystemLite()
    {
        assert(_system != null);
        return _system;
    }

    public void setParentThroughContext(IonValueLite child, IonContext newParent)
    {
        _owning_context = newParent;
        child._context = this;
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

        if (UnifiedSymbolTable.isAssignableTable(symbols) == false) {
            throw new IllegalArgumentException("you can only set a symbol table to a system or local table");
        }
        _symbols = symbols;
        if (child._context != this) {
            assert(child._context == null || child._context == this._owning_context);
            child._context = this;
        }
    }
}
