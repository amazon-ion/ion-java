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


    private _Private_IonBinaryWriterBuilder()
    {
    }


    private
    _Private_IonBinaryWriterBuilder(_Private_IonBinaryWriterBuilder that)
    {
        super(that);

        this.myDefaultSystemSymtab = that.myDefaultSystemSymtab;
        this.mySymtabValueFactory  = that.mySymtabValueFactory;
        this.myInitialSymbolTable  = that.myInitialSymbolTable;
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

    // TODO Should this set "local" symtab or "initial"?
    //      What about behavior after a finish?  Does it get reused?
    //      That doesn't really make sense for a local symtab, does it?

    public SymbolTable getInitialSymtab()
    {
        return myInitialSymbolTable;
    }

    /**
     * Defaults to $ion_1_0 if null.
     * @param symtab may be null.
     */
    public void setInitialSymtab(SymbolTable symtab)
    {
        mutationCheck();

        assert (symtab == null
                || symtab.isSystemTable()
                || symtab.isLocalTable());

        // TODO Should snapshot the LST here so later additions aren't used.
        //      That ensures predictable behavior in the face of mutation.
        //      Currently the copy happens when the writer is built, but it
        //      should happen here so the same data is used for every writer.
        // TODO Then again, maybe that behavior is desirable.
        // TODO But if the symtab is reused after finish, what data is used?

        // TODO ensure there are no substitute imports

        myInitialSymbolTable = symtab;
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
     * This copies {@link #myInitialSymbolTable} if it's mutable.
     */
    SymbolTable buildContextSymbolTable()
    {
        if (myInitialSymbolTable.isReadOnly())
        {
            return myInitialSymbolTable;
        }

        return ((LocalSymbolTable) myInitialSymbolTable).makeCopy();
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
