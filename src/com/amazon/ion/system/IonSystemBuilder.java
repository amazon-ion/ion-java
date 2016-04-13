// Copyright (c) 2011-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import static com.amazon.ion.impl._Private_LazyDomTrampoline.newLazySystem;
import static com.amazon.ion.impl.lite._Private_LiteDomTrampoline.newLiteSystem;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl._Private_IonBinaryWriterBuilder;
import com.amazon.ion.impl._Private_Utils;

/*
 * IonValue DOM Implementations
 *
 * ============================== Lite ==============================
 *
 * Currently, systems built by this class construct {@link IonValue}s that use
 * a new, lightweight implementation (Lite) that fully materializes the input
 * into Java objects and does not retain a copy of the binary image of the
 * Ion data.
 *
 * This is suitable for the vast majority of applications and is the only
 * available DOM implementation for public use.
 *
 * ============================== Lazy ==============================
 *
 * In contrast, there is another DOM implementation that retains a copy
 * (essentially, a buffer) of the Ion binary encoding "underneath" the
 * materialized Ion value hierarchy, lazily materializing Java objects from
 * that buffer as needed. It incrementally updates the binary image on-demand,
 * without necessary re-encoding the entire Ion value hierarchy.
 *
 * This is more suitable for applications that take large binary data streams as
 * input, read or modify only selection portions, and then pass on the results.
 * Currently, this is not available for public use.
 */

/**
 * The builder for creating {@link IonSystem}s.
 * Most applications will only have one or two system instances;
 * see {@link IonSystem} for important constraints.
 * <p>
 * Builders may be configured once and reused to construct multiple
 * objects. They can be {@link #copy() copied} to create a mutable
 * copy of a prototype (presumably for altering some property).
 * <p>
 * <b>Instances of this class are not safe for use by multiple threads unless
 * they are {@linkplain #immutable() immutable}.</b>
 * <p>
 * The easiest way to get going is to use the {@link #standard()} builder:
 *<pre>
 *    IonSystem ion = IonSystemBuilder.standard().build();
 *</pre>
 * <p>
 * However, most long-lived applications will want to provide a custom
 * {@link IonCatalog} implementation rather than using the default
 * {@link SimpleCatalog}.  For example:
 *<pre>
 *    IonCatalog catalog = newCustomCatalog();
 *    IonSystemBuilder b = IonSystemBuilder.standard().copy();
 *    b.setCatalog(catalog);
 *    IonSystem ion = b.build();
 *</pre>
 * <p>
 * Configuration properties follow the standard JavaBeans idiom in order to be
 * friendly to dependency injection systems.  They also provide alternative
 * mutation methods that enable a more fluid style:
 *<pre>
 *    IonCatalog catalog = newCustomCatalog();
 *    IonSystem ion = IonSystemBuilder.standard()
 *                                    .withCatalog(catalog)
 *                                    .build();
 *</pre>
 *
 * <h2>Configuration Properties</h2>
 * <p>
 * This builder provides the following configurable properties:
 * <ul>
 *   <li>
 *     <b>catalog</b>: The {@link IonCatalog} used as a default when reading Ion
 *     data. If null, each system will be built with a new
 *     {@link SimpleCatalog}.
 *   </li>
 *   <li>
 *     <b>streamCopyOptimized</b>: When true, this enables optimizations when
 *     copying data between two Ion streams. For example, in some cases raw
 *     binary-encoded Ion can be copied directly from the input to the output.
 *     This can have significant performance benefits when the appropriate
 *     conditions are met. <b>This feature is experimental! Please test
 *     thoroughly and report any issues.</b>
 *   </li>
 * </ul>
 */
public class IonSystemBuilder
{
    /**
     * This is a back-door for allowing a JVM-level override of the default
     * DOM implementation.  The only reliable way to use this property is to
     * set via the command-line.
     */
    private static final String BINARY_BACKED_DOM_PROPERTY =
        "com.amazon.ion.system.IonSystemBuilder.useBinaryBackedDom";


    private static final IonSystemBuilder STANDARD = new IonSystemBuilder();

    /**
     * The standard builder of {@link IonSystem}s.
     * See the class documentation for the standard configuration.
     * <p>
     * The returned instance is immutable.
     */
    public static IonSystemBuilder standard()
    {
        return STANDARD;
    }


    //=========================================================================

    IonCatalog myCatalog;
    boolean myBinaryBacked = false;
    boolean myStreamCopyOptimized = false;


    /** You no touchy. */
    private IonSystemBuilder()
    {
        try
        {
            myBinaryBacked = Boolean.getBoolean(BINARY_BACKED_DOM_PROPERTY);
        }
        catch (final SecurityException e)
        {
            // NO-OP in the case where system properties are not accessible.
        }
    }

    private IonSystemBuilder(IonSystemBuilder that)
    {
        this.myCatalog      = that.myCatalog;
        this.myBinaryBacked = that.myBinaryBacked;
        this.myStreamCopyOptimized = that.myStreamCopyOptimized;
    }

    //=========================================================================

    /**
     * Creates a mutable copy of this builder.
     * @return a new builder with the same configuration as {@code this}.
     */
    public final IonSystemBuilder copy()
    {
        return new Mutable(this);
    }

    /**
     * Returns an immutable builder configured exactly like this one.
     *
     * @return this instance, if immutable;
     * otherwise an immutable copy of this instance.
     */
    public IonSystemBuilder immutable()
    {
        return this;
    }

    /**
     * Returns a mutable builder configured exactly like this one.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     */
    public IonSystemBuilder mutable()
    {
        return copy();
    }

    void mutationCheck()
    {
        throw new UnsupportedOperationException("This builder is immutable");
    }


    //=========================================================================
    // Properties

    /**
     * Gets the catalog to use when building an {@link IonSystem}.
     * By default, this property is null.
     *
     * @see #setCatalog(IonCatalog)
     * @see #withCatalog(IonCatalog)
     * @see IonSystem#getCatalog()
     */
    public final IonCatalog getCatalog()
    {
        return myCatalog;
    }

    /**
     * Sets the catalog to use when building an {@link IonSystem}.
     *
     * @param catalog the catalog to use in built systems.
     *  If null, each system will be built with a new {@link SimpleCatalog}.
     *
     * @see #getCatalog()
     * @see #withCatalog(IonCatalog)
     * @see IonSystem#getCatalog()
     *
     * @throws UnsupportedOperationException if this is immutable.
     */
    public final void setCatalog(IonCatalog catalog)
    {
        mutationCheck();
        myCatalog = catalog;
    }

    /**
     * Declares the catalog to use when building an {@link IonSystem},
     * returning a new mutable builder if this is immutable.
     *
     * @param catalog the catalog to use in built systems.
     *  If null, each system will be built with a new {@link SimpleCatalog}.
     *
     * @see #getCatalog()
     * @see #setCatalog(IonCatalog)
     * @see IonSystem#getCatalog()
     */
    public final IonSystemBuilder withCatalog(IonCatalog catalog)
    {
        IonSystemBuilder b = mutable();
        b.setCatalog(catalog);
        return b;
    }


    //=========================================================================


    /**
     * Indicates whether built systems will create binary-backed
     * {@link IonValue}s.
     * By default, this is false.
     */
    final boolean isBinaryBacked()
    {
        return myBinaryBacked;
    }

    /**
     * Indicates whether built systems will create binary-backed
     * {@link IonValue}s.
     * By default, this is false.
     *
     * @throws UnsupportedOperationException if this is immutable.
     */
    final void setBinaryBacked(boolean backed)
    {
        mutationCheck();
        myBinaryBacked = backed;
    }

    final IonSystemBuilder withBinaryBacked(boolean backed)
    {
        IonSystemBuilder b = mutable();
        b.setBinaryBacked(backed);
        return b;
    }


    //=========================================================================


    /**
     * Indicates whether built systems may attempt to optimize
     * {@link IonWriter#writeValue(IonReader)} by copying raw source data.
     * By default, this property is false.
     *
     * @see #setStreamCopyOptimized(boolean)
     * @see #withStreamCopyOptimized(boolean)
     */
    public final boolean isStreamCopyOptimized()
    {
        return myStreamCopyOptimized;
    }

    /**
     * Declares whether built systems may attempt to optimize
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
    public final void setStreamCopyOptimized(boolean optimized)
    {
        mutationCheck();
        myStreamCopyOptimized = optimized;
    }

    /**
     * Declares whether built systems may attempt to optimize
     * {@link IonWriter#writeValue(IonReader)} by copying raw source data,
     * returning a new mutable builder if this is immutable.
     * <p>
     * <b>This feature is experimental! Please test thoroughly and report any
     * issues.</b>
     *
     * @see #isStreamCopyOptimized()
     * @see #setStreamCopyOptimized(boolean)
     */
    public final IonSystemBuilder withStreamCopyOptimized(boolean optimized)
    {
        IonSystemBuilder b = mutable();
        b.setStreamCopyOptimized(optimized);
        return b;
    }



    //=========================================================================

    /**
     * Builds a new {@link IonSystem} instance based on this builder's
     * configuration properties.
     */
    public final IonSystem build()
    {
        IonCatalog catalog =
            (myCatalog != null ? myCatalog : new SimpleCatalog());

        IonTextWriterBuilder twb =
            IonTextWriterBuilder.standard().withCharsetAscii();
        twb.setCatalog(catalog);

        _Private_IonBinaryWriterBuilder bwb =
            _Private_IonBinaryWriterBuilder.standard();
        bwb.setCatalog(catalog);
        bwb.setStreamCopyOptimized(myStreamCopyOptimized);

        // TODO Would be nice to remove this since it's implied by the BWB.
        //      However that currently causes problems in the IonSystem
        //      constructors (which get a null initialSymtab).
        SymbolTable systemSymtab = _Private_Utils.systemSymtab(1);
        bwb.setInitialSymbolTable(systemSymtab);
        // This is what we need, more or less.
//        bwb = bwb.fillDefaults();

        IonSystem sys;
        if (isBinaryBacked())
        {
            sys = newLazySystem(twb, bwb);
        }
        else
        {
            sys = newLiteSystem(twb, bwb);
        }
        return sys;
    }

    //=========================================================================

    private static final class Mutable
        extends IonSystemBuilder
    {
        private Mutable(IonSystemBuilder that)
        {
            super(that);
        }

        @Override
        public IonSystemBuilder immutable()
        {
            return new IonSystemBuilder(this);
        }

        @Override
        public IonSystemBuilder mutable()
        {
            return this;
        }

        @Override
        void mutationCheck()
        {
        }
    }
}
