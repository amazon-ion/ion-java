// Copyright (c) 2015-2016 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

import static com.amazon.ion.impl.bin.IonManagedBinaryWriter.ONLY_SYSTEM_IMPORTS;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SubstituteSymbolTableException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.impl.bin.AbstractIonWriter.WriteValueOptimization;
import com.amazon.ion.impl.bin.IonManagedBinaryWriter.ImportedSymbolContext;
import com.amazon.ion.impl.bin.IonManagedBinaryWriter.ImportedSymbolResolverMode;
import com.amazon.ion.impl.bin.IonRawBinaryWriter.PreallocationMode;
import com.amazon.ion.system.SimpleCatalog;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

// TODO unify this with the IonWriter builder APIs

/**
 * Constructs instances of binary {@link IonWriter}.
 * <p>
 * This class is thread-safe.
 */
@SuppressWarnings("deprecation")
public final class _Private_IonManagedBinaryWriterBuilder
{
    public enum AllocatorMode
    {
        POOLED
        {
            @Override
            BlockAllocatorProvider createAllocatorProvider()
            {
                return new PooledBlockAllocatorProvider();
            }
        },
        BASIC
        {
            @Override
            BlockAllocatorProvider createAllocatorProvider()
            {
                return BlockAllocatorProviders.basicProvider();
            }
        };

        /*package*/ abstract BlockAllocatorProvider createAllocatorProvider();
    }

    public static final int DEFAULT_BLOCK_SIZE = 32768;

    /*package*/ final    BlockAllocatorProvider provider;
    /*package*/ volatile int                    symbolsBlockSize;
    /*package*/ volatile int                    userBlockSize;
    /*package*/ volatile PreallocationMode      preallocationMode;
    /*package*/ volatile ImportedSymbolContext  imports;
    /*package*/ volatile IonCatalog             catalog;
    /*package*/ volatile WriteValueOptimization optimization;
    /*package*/ volatile SymbolTable            initialSymbolTable;

    private _Private_IonManagedBinaryWriterBuilder(final BlockAllocatorProvider provider)
    {
        this.provider = provider;
        this.symbolsBlockSize = DEFAULT_BLOCK_SIZE;
        this.userBlockSize = DEFAULT_BLOCK_SIZE;
        this.imports = ONLY_SYSTEM_IMPORTS;
        this.preallocationMode = PreallocationMode.PREALLOCATE_2;
        this.catalog = new SimpleCatalog();
        this.optimization = WriteValueOptimization.NONE;
    }

    private _Private_IonManagedBinaryWriterBuilder(final _Private_IonManagedBinaryWriterBuilder other)
    {
        this.provider           = other.provider;
        this.symbolsBlockSize   = other.symbolsBlockSize;
        this.userBlockSize      = other.userBlockSize;
        this.preallocationMode  = other.preallocationMode;
        this.imports            = other.imports;
        this.catalog            = other.catalog;
        this.optimization       = other.optimization;
        this.initialSymbolTable = other.initialSymbolTable;
    }

    public _Private_IonManagedBinaryWriterBuilder copy()
    {
        return new _Private_IonManagedBinaryWriterBuilder(this);
    }

    // Parameter Setting Methods

    public _Private_IonManagedBinaryWriterBuilder withSymbolsBlockSize(final int blockSize)
    {
        if (blockSize < 1)
        {
            throw new IllegalArgumentException("Block size cannot be less than 1: " + blockSize);
        }
        symbolsBlockSize = blockSize;
        return this;
    }

    public _Private_IonManagedBinaryWriterBuilder withUserBlockSize(final int blockSize)
    {
        if (blockSize < 1)
        {
            throw new IllegalArgumentException("Block size cannot be less than 1: " + blockSize);
        }
        userBlockSize = blockSize;
        return this;
    }

    public _Private_IonManagedBinaryWriterBuilder withImports(final SymbolTable... tables)
    {
        if (tables != null)
        {
            return withImports(Arrays.asList(tables));
        }
        return this;
    }

    public _Private_IonManagedBinaryWriterBuilder withImports(final List<SymbolTable> tables)
    {
        return withImports(ImportedSymbolResolverMode.DELEGATE, tables);
    }

    /**
     * Adds imports, flattening them to make lookup more efficient.  This is particularly useful
     * when a builder instance is long lived.
     */
    public _Private_IonManagedBinaryWriterBuilder withFlatImports(final SymbolTable... tables)
    {
        if (tables != null)
        {
            return withFlatImports(Arrays.asList(tables));
        }
        return this;
    }

    /** @see #withFlatImports(SymbolTable...) */
    public _Private_IonManagedBinaryWriterBuilder withFlatImports(final List<SymbolTable> tables)
    {
        return withImports(ImportedSymbolResolverMode.FLAT, tables);
    }

    /*package*/ _Private_IonManagedBinaryWriterBuilder withImports(final ImportedSymbolResolverMode mode, final List<SymbolTable> tables) {
        imports = new ImportedSymbolContext(mode, tables);
        return this;
    }

    /*package*/ _Private_IonManagedBinaryWriterBuilder withPreallocationMode(final PreallocationMode preallocationMode)
    {
        this.preallocationMode = preallocationMode;
        return this;
    }

    public _Private_IonManagedBinaryWriterBuilder withPaddedLengthPreallocation(final int pad)
    {
        this.preallocationMode = PreallocationMode.withPadSize(pad);
        return this;
    }

    public _Private_IonManagedBinaryWriterBuilder withCatalog(final IonCatalog catalog)
    {
        this.catalog = catalog;
        return this;
    }

    public _Private_IonManagedBinaryWriterBuilder withStreamCopyOptimization(boolean optimized)
    {
        this.optimization = optimized ? WriteValueOptimization.COPY_OPTIMIZED : WriteValueOptimization.NONE;
        return this;
    }

    public _Private_IonManagedBinaryWriterBuilder withInitialSymbolTable(SymbolTable symbolTable)
    {
        if (symbolTable != null)
        {
            if (!symbolTable.isLocalTable() && !symbolTable.isSystemTable())
            {
                throw new IllegalArgumentException("Initial symbol table must be local or system");
            }
            if (symbolTable.isSystemTable())
            {
                if (symbolTable.getMaxId() != SystemSymbols.ION_1_0_MAX_ID)
                {
                    throw new IllegalArgumentException("Unsupported system symbol table");
                }
                // don't need to set an explicit symbol table for system 1.0
                symbolTable = null;
            }
            else
            {
                // initial symbol table is local
                for (final SymbolTable st : symbolTable.getImportedTables())
                {
                    if (st.isSubstitute())
                    {
                        throw new SubstituteSymbolTableException(
                            "Cannot use initial symbol table with imported substitutes");
                    }
                }
            }
        }

        this.initialSymbolTable = symbolTable;
        return this;
    }

    // Construction

    public IonWriter newWriter(final OutputStream out) throws IOException
    {
        return new IonManagedBinaryWriter(this, out);
    }

    // Static Factories

    /**
     * Constructs a new builder.
     * <p>
     * Builders generally bind to an allocation pool as defined by {@link AllocatorMode}, so applications should reuse
     * them as much as possible.
     */
    public static _Private_IonManagedBinaryWriterBuilder create(final AllocatorMode allocatorMode)
    {
        return new _Private_IonManagedBinaryWriterBuilder(allocatorMode.createAllocatorProvider());
    }
}
