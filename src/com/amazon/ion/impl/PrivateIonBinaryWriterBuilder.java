// Copyright (c) 2014-2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SubstituteSymbolTableException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.impl.bin.PrivateIonManagedBinaryWriterBuilder;
import com.amazon.ion.impl.bin.PrivateIonManagedBinaryWriterBuilder.AllocatorMode;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public class PrivateIonBinaryWriterBuilder
    extends IonBinaryWriterBuilder
{
    // IONJAVA-467 expose configuration points properly and figure out deprecation path for the old writer.
    private final PrivateIonManagedBinaryWriterBuilder myBinaryWriterBuilder;
    private ValueFactory mySymtabValueFactory;

    /** System or local */
    private SymbolTable  myInitialSymbolTable;


    private PrivateIonBinaryWriterBuilder()
    {
        myBinaryWriterBuilder =
            PrivateIonManagedBinaryWriterBuilder
                .create(AllocatorMode.POOLED)
                .withPaddedLengthPreallocation(0)
                ;
    }


    private
    PrivateIonBinaryWriterBuilder(PrivateIonBinaryWriterBuilder that)
    {
        super(that);

        this.mySymtabValueFactory = that.mySymtabValueFactory;
        this.myInitialSymbolTable = that.myInitialSymbolTable;
        this.myBinaryWriterBuilder = that.myBinaryWriterBuilder.copy();
    }


    /**
     * @return a new mutable builder.
     */
    public static PrivateIonBinaryWriterBuilder standard()
    {
        return new PrivateIonBinaryWriterBuilder.Mutable();
    }


    //=========================================================================


    @Override
    public final PrivateIonBinaryWriterBuilder copy()
    {
        return new Mutable(this);
    }

    @Override
    public PrivateIonBinaryWriterBuilder immutable()
    {
        return this;
    }

    @Override
    public PrivateIonBinaryWriterBuilder mutable()
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

    public PrivateIonBinaryWriterBuilder
    withSymtabValueFactory(ValueFactory factory)
    {
        PrivateIonBinaryWriterBuilder b = mutable();
        b.setSymtabValueFactory(factory);
        return b;
    }


    //=========================================================================


    @Override
    public SymbolTable getInitialSymbolTable()
    {
        return myInitialSymbolTable;
    }

    /**
     * Declares the symbol table to use for encoded data.
     * To avoid conflicts between different data streams, if the given instance
     * is mutable, it will be copied when {@code build()} is called.
     *
     * @param symtab must be a local or system symbol table.
     * May be null, in which case the initial symtab is that of
     * {@code $ion_1_0}.
     *
     * @throws SubstituteSymbolTableException
     * if any imported table is a substitute (see {@link SymbolTable}).
     */
    @Override
    public void setInitialSymbolTable(SymbolTable symtab)
    {
        mutationCheck();

        if (symtab != null)
        {
            if (symtab.isLocalTable())
            {
                SymbolTable[] imports =
                    ((LocalSymbolTable) symtab).getImportedTablesNoCopy();
                for (SymbolTable imported : imports)
                {
                    if (imported.isSubstitute())
                    {
                        String message =
                            "Cannot encode with substitute symbol table: " +
                            imported.getName();
                        throw new SubstituteSymbolTableException(message);
                    }
                }
            }
            else if (! symtab.isSystemTable())
            {
                String message = "symtab must be local or system table";
                throw new IllegalArgumentException(message);
            }
        }

        myInitialSymbolTable = symtab;
        myBinaryWriterBuilder.withInitialSymbolTable(symtab);
    }

    /**
     * Defaults to $ion_1_0 if null.
     * @param symtab may be null.
     */
    @Override
    public
    PrivateIonBinaryWriterBuilder withInitialSymbolTable(SymbolTable symtab)
    {
        PrivateIonBinaryWriterBuilder b = mutable();
        b.setInitialSymbolTable(symtab);
        return b;
    }

    @Override
    public void setImports(final SymbolTable... imports)
    {
        super.setImports(imports);
        myBinaryWriterBuilder.withImports(imports);
    }

    @Override
    public void setCatalog(final IonCatalog catalog)
    {
        super.setCatalog(catalog);
        myBinaryWriterBuilder.withCatalog(catalog);
    }

    @Override
    public void setStreamCopyOptimized(final boolean optimized)
    {
        super.setStreamCopyOptimized(optimized);
        myBinaryWriterBuilder.withStreamCopyOptimization(optimized);
    }

    //=========================================================================


    /**
     * Fills all properties and returns an immutable builder.
     */
    private PrivateIonBinaryWriterBuilder fillDefaults()
    {
        // Ensure that we don't modify the user's builder.
        PrivateIonBinaryWriterBuilder b = copy();

        if (b.getSymtabValueFactory() == null)
        {
            IonSystem system = IonSystemBuilder.standard().build();
            b.setSymtabValueFactory(system);
        }

        return b.immutable();
    }

    /**
     * Returns a symtab usable in a local context.
     * This copies {@link #myInitialSymbolTable} if symbols have been added to
     * it since {@link #setInitialSymbolTable(SymbolTable)} was called.
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
    public final IonWriter build(OutputStream out)
    {
        PrivateIonBinaryWriterBuilder b = fillDefaults();
        try
        {
            return b.myBinaryWriterBuilder.newWriter(out);
        }
        catch (final IOException e)
        {
            throw new IonException("I/O Error", e);
        }
    }

    //=========================================================================


    private static final class Mutable
        extends PrivateIonBinaryWriterBuilder
    {
        private Mutable() { }

        private Mutable(PrivateIonBinaryWriterBuilder that)
        {
            super(that);
        }

        @Override
        public PrivateIonBinaryWriterBuilder immutable()
        {
            return new PrivateIonBinaryWriterBuilder(this);
        }

        @Override
        public PrivateIonBinaryWriterBuilder mutable()
        {
            return this;
        }

        @Override
        protected void mutationCheck()
        {
        }
    }
}
