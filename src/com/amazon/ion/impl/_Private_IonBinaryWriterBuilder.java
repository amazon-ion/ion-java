// Copyright (c) 2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_Utils.initialSymtab;

import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.impl.BlockedBuffer.BufferedOutputStream;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.OutputStream;

/**
 * NOT FOR APPLICATION USE!
 */
@SuppressWarnings("deprecation")
public class _Private_IonBinaryWriterBuilder
    extends IonBinaryWriterBuilder
{
    private SymbolTable  myDefaultSystemSymtab;
    private ValueFactory mySymtabValueFactory;

    /** System or local */
    private SymbolTable  myInitialSymbolTable;
    private int          myInitialSymbolTableMaxId;


    private _Private_IonBinaryWriterBuilder()
    {
    }


    private
    _Private_IonBinaryWriterBuilder(_Private_IonBinaryWriterBuilder that)
    {
        super(that);

        this.myDefaultSystemSymtab     = that.myDefaultSystemSymtab;
        this.mySymtabValueFactory      = that.mySymtabValueFactory;
        this.myInitialSymbolTable      = that.myInitialSymbolTable;
        this.myInitialSymbolTableMaxId = that.myInitialSymbolTableMaxId;
    }


    /**
     * @return a new mutable builder.
     */
    public static _Private_IonBinaryWriterBuilder standard()
    {
        return new _Private_IonBinaryWriterBuilder.Mutable();
    }


    //=========================================================================


    @Override
    public final _Private_IonBinaryWriterBuilder copy()
    {
        return new Mutable(this);
    }

    @Override
    public _Private_IonBinaryWriterBuilder immutable()
    {
        return this;
    }

    @Override
    public _Private_IonBinaryWriterBuilder mutable()
    {
        return copy();
    }


    //=========================================================================

    // TODO The symtab value factory should not be needed.
    //      It's an artifact of how the binary writer gathers symtabs that
    //      are written through it.

    public ValueFactory getSymtabValueFactory()
    {
        return mySymtabValueFactory;
    }

    public void setSymtabValueFactory(ValueFactory factory)
    {
        mutationCheck();
        mySymtabValueFactory = factory;
    }

    public _Private_IonBinaryWriterBuilder
    withSymtabValueFactory(ValueFactory factory)
    {
        _Private_IonBinaryWriterBuilder b = mutable();
        b.setSymtabValueFactory(factory);
        return b;
    }


    //=========================================================================


    public SymbolTable getInitialSymtab()
    {
        return myInitialSymbolTable;
    }

    /**
     * Declares the symbol table to use for encoded data.
     * <p>
     * The given symbol table's max_id is recorded when this method is called.
     * When {@code build} is called, if any more symbols have been added, then
     * a new symbol table is created with the recorded max_id (and that many
     * symbols).  In other words: the effective symbol table will have the same
     * state as the one given to this method at the time it was called, and a
     * copy will be used if necessary.
     *
     * @param symtab must be a local or system symbol table.
     * May be null, in which case the initial symtab is that of
     * {@code $ion_1_0}.
     */
    public void setInitialSymtab(SymbolTable symtab)
    {
        mutationCheck();

        assert (symtab == null
                || symtab.isSystemTable()
                || symtab.isLocalTable());

        // TODO ensure there are no substitute imports

        myInitialSymbolTable      = symtab;
        myInitialSymbolTableMaxId = (symtab == null ? 0 : symtab.getMaxId());
    }

    /**
     * Defaults to $ion_1_0 if null.
     * @param symtab may be null.
     */
    _Private_IonBinaryWriterBuilder withInitialSymtab(SymbolTable symtab)
    {
        _Private_IonBinaryWriterBuilder b = mutable();
        b.setInitialSymtab(symtab);
        return b;
    }


    //=========================================================================


    /**
     * Fills all properties and returns an immutable builder.
     */
    private _Private_IonBinaryWriterBuilder fillDefaults()
    {
        // Ensure that we don't modify the user's builder.
        _Private_IonBinaryWriterBuilder b = copy();

        if (b.getSymtabValueFactory() == null)
        {
            IonSystem system = IonSystemBuilder.standard().build();
            b.setSymtabValueFactory(system);
        }

        SymbolTable initialSymtab = b.getInitialSymtab();
        if (initialSymtab == null)
        {
            initialSymtab = initialSymtab(b.getSymtabValueFactory(),
                                          _Private_Utils.systemSymtab(1),
                                          b.getImports());
            b.setInitialSymtab(initialSymtab);
        }
        else if (initialSymtab.isSystemTable())
        {
            initialSymtab = initialSymtab(b.getSymtabValueFactory(),
                                          initialSymtab,
                                          b.getImports());
            b.setInitialSymtab(initialSymtab);
        }

        return b.immutable();
    }


    private IonWriterSystemBinary buildSystemWriter(OutputStream out)
    {
        SymbolTable defaultSystemSymtab =
            myInitialSymbolTable.getSystemSymbolTable();

        return new IonWriterSystemBinary(defaultSystemSymtab,
                                         out,
                                         false /* autoFlush */,
                                         true /* ensureInitialIvm */);
    }


    /**
     * Returns a symtab usable in a local context.
     * This copies {@link #myInitialSymbolTable} if symbols have been added to
     * it since {@link #setInitialSymtab(SymbolTable)} was called.
     */
    SymbolTable buildContextSymbolTable()
    {
        int maxId = myInitialSymbolTableMaxId;
        if (myInitialSymbolTable.getMaxId() == maxId)
        {
            return myInitialSymbolTable;
        }

        return ((LocalSymbolTable) myInitialSymbolTable).makeCopy(maxId);
    }


    @Override
    public final IonWriterUserBinary build(OutputStream out)
    {
        _Private_IonBinaryWriterBuilder b = fillDefaults();

        IonWriterSystemBinary systemWriter = b.buildSystemWriter(out);

        return new IonWriterUserBinary(b, systemWriter);
    }


    @Deprecated
    public final IonBinaryWriter buildLegacy()
    {
        _Private_IonBinaryWriterBuilder b = fillDefaults();

        IonWriterSystemBinary systemWriter =
            b.buildSystemWriter(new BufferedOutputStream());

        return new _Private_IonBinaryWriterImpl(b, systemWriter);
    }


    //=========================================================================


    private static final class Mutable
        extends _Private_IonBinaryWriterBuilder
    {
        private Mutable() { }

        private Mutable(_Private_IonBinaryWriterBuilder that)
        {
            super(that);
        }

        @Override
        public _Private_IonBinaryWriterBuilder immutable()
        {
            return new _Private_IonBinaryWriterBuilder(this);
        }

        @Override
        public _Private_IonBinaryWriterBuilder mutable()
        {
            return this;
        }

        @Override
        protected void mutationCheck()
        {
        }
    }
}
