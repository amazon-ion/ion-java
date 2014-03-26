// Copyright (c) 2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl._Private_IonBinaryWriterBuilder;
import java.io.OutputStream;


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
 *
 * @since R21
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
        return _Private_IonBinaryWriterBuilder.standard();
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


    //=========================================================================


    /**
     * Builds a new binary writer based on this builder's configuration
     * properties.
     *
     * @param out the stream that will receive Ion binary data.
     * Must not be null.
     *
     * @return a new {@link IonWriter} instance; not {@code null}.
     */
    public abstract IonWriter build(OutputStream out);
}
