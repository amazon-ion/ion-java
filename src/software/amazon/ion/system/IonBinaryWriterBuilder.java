/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.system;

import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SubstituteSymbolTableException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.impl.PrivateIonBinaryWriterBuilder;


/**
 * The builder for creating {@link IonWriter}s emitting the Ion binary format.
 * <p>
 * <b>WARNING:</b> This class should not be extended by code outside of
 * this library.
 * <p>
 * Builders may be configured once and reused to construct multiple
 * objects.
 * <p>
 * <b>Instances of this class are not not safe for use by multiple threads
 * unless they are {@linkplain #immutable() immutable}.</b>
 */
public abstract class IonBinaryWriterBuilder
    extends IonWriterBuilderBase<IonBinaryWriterBuilder>
{
    private boolean myStreamCopyOptimized;


    /** NOT FOR APPLICATION USE! */
    protected IonBinaryWriterBuilder()
    {
    }


    /** NOT FOR APPLICATION USE! */
    protected IonBinaryWriterBuilder(IonBinaryWriterBuilder that)
    {
        super(that);

        this.myStreamCopyOptimized = that.myStreamCopyOptimized;
    }



    /**
     * The standard builder of binary writers, with all configuration
     * properties having their default values.
     *
     * @return a new, mutable builder instance.
     */
    public static IonBinaryWriterBuilder standard()
    {
        return PrivateIonBinaryWriterBuilder.standard();
    }


    //=========================================================================
    // Overrides to fix the return type in JavaDocs


    @Override
    public abstract IonBinaryWriterBuilder copy();

    @Override
    public abstract IonBinaryWriterBuilder immutable();

    @Override
    public abstract IonBinaryWriterBuilder mutable();


    @Override
    public final IonBinaryWriterBuilder withCatalog(IonCatalog catalog)
    {
        return super.withCatalog(catalog);
    }

    @Override
    public final IonBinaryWriterBuilder withImports(SymbolTable... imports)
    {
        return super.withImports(imports);
    }


    //=========================================================================


    /**
     * @return always {@link IonWriterBuilder.InitialIvmHandling#ENSURE}.
     */
    @Override
    public InitialIvmHandling getInitialIvmHandling()
    {
        return InitialIvmHandling.ENSURE;
    }


    // TODO Setting ivmMinimizing seems reasonable. Does the writer handle it?

    /**
     * @return always null.
     */
    @Override
    public IvmMinimizing getIvmMinimizing()
    {
        return null;
    }


    //=========================================================================


    /**
     * Gets the symbol table to use for encoded data.
     * To avoid conflicts between different data streams, if the given instance
     * is mutable, it will be copied when {@code build()} is called.
     *
     * @return a local or system symbol table.
     * May be null, in which case the initial symbol table is that of
     * {@code $ion_1_0}.
     *
     * @see #setInitialSymbolTable(SymbolTable)
     * @see #withInitialSymbolTable(SymbolTable)
     */
    public abstract SymbolTable getInitialSymbolTable();


    /**
     * Declares the symbol table to use for encoded data.
     * To avoid conflicts between different data streams, if the given instance
     * is mutable, it will be copied when {@code build()} is called.
     *
     * @param symtab must be a local or system symbol table.
     * May be null, in which case the initial symbol table is that of
     * {@code $ion_1_0}.
     *
     * @throws SubstituteSymbolTableException
     * if any imported table is a substitute (see {@link SymbolTable}).
     *
     * @see #getInitialSymbolTable()
     * @see #withInitialSymbolTable(SymbolTable)
     */
    public abstract void setInitialSymbolTable(SymbolTable symtab);


    /**
     * Declares the symbol table to use for encoded data.
     * To avoid conflicts between different data streams, if the given instance
     * is mutable, it will be copied when {@code build()} is called.
     *
     * @param symtab must be a local or system symbol table.
     * May be null, in which case the initial symbol table is that of
     * {@code $ion_1_0}.
     *
     * @throws SubstituteSymbolTableException
     * if any imported table is a substitute (see {@link SymbolTable}).
     */
    public abstract
    IonBinaryWriterBuilder withInitialSymbolTable(SymbolTable symtab);


    /**
     * Enables or disables writing Binary32 (4-byte, single precision,
     * IEEE-754) values for floats when there would be no loss in precision.
     * By default Binary32 support is disabled to ensure the broadest
     * compatibility with existing Ion implementations. Historically,
     * implementations were only able to read Binary64 values.
     * <p>
     * When enabled, floats are evaluated for a possible loss of data at single
     * precision. If the value can be represented in single precision without
     * data loss, it is written as a 4-byte, Binary32 value. Floats which cannot
     * be represented as single-precision values are  written as 8-byte,
     * Binary64 values (this is the legacy behavior for all  floats, regardless
     * of value).
     *
     * @param enabled {@code true} to enable writing 4-byte floats,
     * {@code false} to always write 8-byte floats.
     *
     * @see IonBinaryWriterBuilder#withFloatBinary32Enabled
     * @see IonBinaryWriterBuilder#withFloatBinary32Disabled
     */
    public abstract void setIsFloatBinary32Enabled(boolean enabled);

    /**
     * Enables writing Binary32 (4-byte, single precision,  IEEE-754) values
     * for floats when there would be no loss in precision. By default Binary32
     * support is disabled to ensure the broadest compatibility with existing
     * Ion implementations. Historically, implementations were only able to read
     * Binary64 values.
     * <p>
     * When enabled, floats are evaluated for a possible loss of data at single
     * precision. If the value can be represented in single precision without
     * data loss, it is written as a 4-byte, Binary32 value. Floats which
     * cannot be represented as single-precision values are written as 8-byte,
     * Binary64 values (this is the legacy behavior for all floats, regardless
     * of value).
     *
     * @see IonBinaryWriterBuilder#setIsFloatBinary32Enabled(boolean)
     * @see IonBinaryWriterBuilder#withFloatBinary32Disabled
     */
    public abstract IonBinaryWriterBuilder withFloatBinary32Enabled();

    /**
     * Disables writing Binary32 (4-byte, single precision, IEEE-754) values for
     * floats. This is the default behavior.
     * <p>
     * When disabled, floats are always written as 8-byte, Binary64 values
     * regardless of value. This is the legacy behavior for all Ion binary
     * writers and ensures the boarded compatibility with other Ion consumers.
     *
     * @param enabled {@code true} to enable writing 4-byte floats,
     * {@code false} to always write 8-byte floats.
     *
     * @see IonBinaryWriterBuilder#setIsFloatBinary32Enabled(boolean)
     * @see IonBinaryWriterBuilder#withFloatBinary32Enabled
     */
    public abstract IonBinaryWriterBuilder withFloatBinary32Disabled();

    //=========================================================================


    /**
     * Indicates whether built writers may attempt to optimize
     * {@link IonWriter#writeValue(IonReader)} by copying raw source data.
     * By default, this property is false.
     *
     * @see #setStreamCopyOptimized(boolean)
     * @see #withStreamCopyOptimized(boolean)
     */
    public boolean isStreamCopyOptimized()
    {
        return myStreamCopyOptimized;
    }

    /**
     * Declares whether built writers may attempt to optimize
     * {@link IonWriter#writeValue(IonReader)} by copying raw source data.
     * By default, this property is false.
     * <p>
     * <b>This feature is experimental! Please test thoroughly and report any
     * issues.</b>
     *
     * @throws UnsupportedOperationException if this is immutable.
     *
     * @see #isStreamCopyOptimized()
     * @see #withStreamCopyOptimized(boolean)
     */
    public void setStreamCopyOptimized(boolean optimized)
    {
        mutationCheck();
        myStreamCopyOptimized = optimized;
    }


    /**
     * Declares whether built writers may attempt to optimize
     * {@link IonWriter#writeValue(IonReader)} by copying raw source data,
     * returning a new mutable builder if this is immutable.
     * <p>
     * <b>This feature is experimental! Please test thoroughly and report any
     * issues.</b>
     *
     * @see #isStreamCopyOptimized()
     * @see #setStreamCopyOptimized(boolean)
     */
    public final
    IonBinaryWriterBuilder withStreamCopyOptimized(boolean optimized)
    {
        IonBinaryWriterBuilder b = mutable();
        b.setStreamCopyOptimized(optimized);
        return b;
    }
}
