/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.bin;

import static software.amazon.ion.impl.bin.IonManagedBinaryWriter.ONLY_SYSTEM_IMPORTS;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SubstituteSymbolTableException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SystemSymbols;
import software.amazon.ion.impl.bin.AbstractIonWriter.WriteValueOptimization;
import software.amazon.ion.impl.bin.IonManagedBinaryWriter.ImportedSymbolContext;
import software.amazon.ion.impl.bin.IonManagedBinaryWriter.ImportedSymbolResolverMode;
import software.amazon.ion.impl.bin.IonRawBinaryWriter.PreallocationMode;
import software.amazon.ion.system.SimpleCatalog;

// TODO unify this with the IonWriter builder APIs

/**
 * Constructs instances of binary {@link IonWriter}.
 * <p>
 * This class is thread-safe.
 *
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public final class PrivateIonManagedBinaryWriterBuilder
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
    /*package*/ volatile boolean                isFloatBinary32Enabled;

    private PrivateIonManagedBinaryWriterBuilder(final BlockAllocatorProvider provider)
    {
        this.provider = provider;
        this.symbolsBlockSize = DEFAULT_BLOCK_SIZE;
        this.userBlockSize = DEFAULT_BLOCK_SIZE;
        this.imports = ONLY_SYSTEM_IMPORTS;
        this.preallocationMode = PreallocationMode.PREALLOCATE_2;
        this.catalog = new SimpleCatalog();
        this.optimization = WriteValueOptimization.NONE;
        this.isFloatBinary32Enabled = false;
    }

    private PrivateIonManagedBinaryWriterBuilder(final PrivateIonManagedBinaryWriterBuilder other)
    {
        this.provider           = other.provider;
        this.symbolsBlockSize   = other.symbolsBlockSize;
        this.userBlockSize      = other.userBlockSize;
        this.preallocationMode  = other.preallocationMode;
        this.imports            = other.imports;
        this.catalog            = other.catalog;
        this.optimization       = other.optimization;
        this.initialSymbolTable = other.initialSymbolTable;
        this.isFloatBinary32Enabled = other.isFloatBinary32Enabled;
    }

    public PrivateIonManagedBinaryWriterBuilder copy()
    {
        return new PrivateIonManagedBinaryWriterBuilder(this);
    }

    // Parameter Setting Methods

    public PrivateIonManagedBinaryWriterBuilder withSymbolsBlockSize(final int blockSize)
    {
        if (blockSize < 1)
        {
            throw new IllegalArgumentException("Block size cannot be less than 1: " + blockSize);
        }
        symbolsBlockSize = blockSize;
        return this;
    }

    public PrivateIonManagedBinaryWriterBuilder withUserBlockSize(final int blockSize)
    {
        if (blockSize < 1)
        {
            throw new IllegalArgumentException("Block size cannot be less than 1: " + blockSize);
        }
        userBlockSize = blockSize;
        return this;
    }

    public PrivateIonManagedBinaryWriterBuilder withImports(final SymbolTable... tables)
    {
        if (tables != null)
        {
            return withImports(Arrays.asList(tables));
        }
        return this;
    }

    public PrivateIonManagedBinaryWriterBuilder withImports(final List<SymbolTable> tables)
    {
        return withImports(ImportedSymbolResolverMode.DELEGATE, tables);
    }

    /**
     * Adds imports, flattening them to make lookup more efficient.  This is particularly useful
     * when a builder instance is long lived.
     */
    public PrivateIonManagedBinaryWriterBuilder withFlatImports(final SymbolTable... tables)
    {
        if (tables != null)
        {
            return withFlatImports(Arrays.asList(tables));
        }
        return this;
    }

    /** @see #withFlatImports(SymbolTable...) */
    public PrivateIonManagedBinaryWriterBuilder withFlatImports(final List<SymbolTable> tables)
    {
        return withImports(ImportedSymbolResolverMode.FLAT, tables);
    }

    /*package*/ PrivateIonManagedBinaryWriterBuilder withImports(final ImportedSymbolResolverMode mode, final List<SymbolTable> tables) {
        imports = new ImportedSymbolContext(mode, tables);
        return this;
    }

    /*package*/ PrivateIonManagedBinaryWriterBuilder withPreallocationMode(final PreallocationMode preallocationMode)
    {
        this.preallocationMode = preallocationMode;
        return this;
    }

    public PrivateIonManagedBinaryWriterBuilder withPaddedLengthPreallocation(final int pad)
    {
        this.preallocationMode = PreallocationMode.withPadSize(pad);
        return this;
    }

    public PrivateIonManagedBinaryWriterBuilder withCatalog(final IonCatalog catalog)
    {
        this.catalog = catalog;
        return this;
    }

    public PrivateIonManagedBinaryWriterBuilder withStreamCopyOptimization(boolean optimized)
    {
        this.optimization = optimized ? WriteValueOptimization.COPY_OPTIMIZED : WriteValueOptimization.NONE;
        return this;
    }

    public PrivateIonManagedBinaryWriterBuilder withFloatBinary32Enabled() {
        isFloatBinary32Enabled = true;
        return this;
    }

    public PrivateIonManagedBinaryWriterBuilder withFloatBinary32Disabled() {
        isFloatBinary32Enabled = false;
        return this;
    }

    public PrivateIonManagedBinaryWriterBuilder withInitialSymbolTable(SymbolTable symbolTable)
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
    public static PrivateIonManagedBinaryWriterBuilder create(final AllocatorMode allocatorMode)
    {
        return new PrivateIonManagedBinaryWriterBuilder(allocatorMode.createAllocatorProvider());
    }
}
