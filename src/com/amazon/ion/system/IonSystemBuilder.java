// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import static com.amazon.ion.impl._Private_LazyDomTrampoline.newLazySystem;
import static com.amazon.ion.impl.lite._Private_LiteDomTrampoline.newLiteSystem;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;

/**
 * The bootstrap builder for creating an {@link IonSystem}.
 * Most applications will only have one or two system instances;
 * see the documentation of that class for important constraints.
 * <p>
 * Builders may be configured once and reused to construct multiple
 * objects. They can be {@link #copy() copied} to create a mutable
 * copy of a prototype (presumably for altering some property).
 * Builder instances are <em>not</em> thread-safe unless they are immutable.
 * <p>
 * The easiest way to get going is to just use the {@link #standard()} builder:
 * <pre>
 *  IonSystem ion = IonSystemBuilder.standard().build();
 *</pre>
 * <p>
 * However, most long-lived applications will want to provide a custom
 * {@link IonCatalog} implementation rather than using the default
 * {@link SimpleCatalog}.  For example:
 * <pre>
 *  IonCatalog catalog = newCustomCatalog();
 *  IonSystemBuilder b = IonSystemBuilder.standard().copy();
 *  b.setCatalog(catalog);
 *  IonSystem ion = b.build();
 *</pre>
 * <p>
 * Configuration properties follow the standard JavaBeans idiom in order to be
 * friendly to dependency injection systems.  They also provide alternative
 * mutation methods that enable a more fluid style:
 * <pre>
 *  IonCatalog catalog = newCustomCatalog();
 *  IonSystem ion = IonSystemBuilder.standard()
 *                                  .withCatalog(catalog)
 *                                  .build();
 *</pre>
 * <!--
 * <h2>IonValue Implementation</h2>
 * Compared to the older {@link SystemFactory} API, systems built by this
 * class construct {@link IonValue}s using a new, lightweight implementation
 * that fully materializes the input into Java objects and does not retain a
 * copy of the Ion binary image.
 * This is suitable for the vast majority of applications.
 * <p>
 * In contrast, {@link SystemFactory}'s older implementation of
 * {@link IonValue} retains a copy of the Ion binary encoding "underneath" the
 * tree, lazily materializing Java objects from that buffer as needed.
 * It incrementally updates the binary image on demand, without necessarily
 * re-encoding the entire data set. This can be more suitable for applications
 * that accept large binary data streams, read or modify only selected portions,
 * and then pass on the results.
 * <p>
 * This class will eventually be expanded to support selection between these
 * implementations, but for now those applications that require the
 * binary-backed value tree should stick with {@link SystemFactory}.
 * -->
 * <h2>Configuration Properties</h2>
 *
 * <p>This builder provides the following configurable properties:
 * <dl>
 *   <dt>catalog
 *   <dd>The {@link IonCatalog} used as a default when reading Ion data.
 *     If null, each system will be built with a new {@link SimpleCatalog}.
 *
 *   <dt>streamCopyOptimized
 *   <dd>When true, this enables optimizations when copying data between two
 *     Ion streams. For example, in some cases raw binary-encoded Ion can be
 *     copied directly from the input to the output. This can have significant
 *     performance benefits when the appropriate conditions are met.
 *     <b>This feature is experimental! Please test thoroughly and report any
 *     issues.</b>
 * </dl>
 */
public class IonSystemBuilder
{
    /**
     * This is a back-door for allowing a JVM-level override of the default
     * DOM implementation.  The only reliable way to use this property is to
     * set via the command-line.
     * <p>
     * <b>DO NOT USE THIS WITHOUT APPROVAL FROM JONKER@AMAZON.COM!</b>
     * This private feature is subject to change without notice.
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
    boolean myBinaryBacked;
    boolean myStreamCopyOptimized;


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
        return new IonSystemBuilder.Mutable(this);
    }

    /**
     * Returns an immutable builder configured exactly like this one.
     *
     * @return this instance, if immutable;
     * otherwise an immutable copy of this instance.
     *
     * @since IonJava R15
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
     *
     * @since IonJava R15
     */
    public IonSystemBuilder mutable()
    {
        return copy();
    }

    private void mutationFailure()
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
    final public IonCatalog getCatalog()
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
    public void setCatalog(IonCatalog catalog)
    {
        mutationFailure();
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
    void setBinaryBacked(boolean backed)
    {
        mutationFailure();
    }

    final IonSystemBuilder withBinaryBacked(boolean backed)
    {
        IonSystemBuilder b = mutable();
        b.setBinaryBacked(backed);
        return b;
    }




    /**
     * Indicates whether built systems may attempt to optimize
     * {@link IonWriter#writeValue(IonReader)} by copying raw source data.
     * By default, this property is false.
     *
     * @see #setStreamCopyOptimized(boolean)
     * @see #withStreamCopyOptimized(boolean)
     *
     * @since R13
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
     *
     * @since R13
     */
    public void setStreamCopyOptimized(boolean optimized)
    {
        mutationFailure();
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
     *
     * @since R13
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

        IonSystem sys;
        if (isBinaryBacked())
        {
            sys = newLazySystem(catalog, myStreamCopyOptimized);
        }
        else
        {
            sys = newLiteSystem(catalog, myStreamCopyOptimized);
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
        public void setCatalog(IonCatalog catalog)
        {
            myCatalog = catalog;
        }

        @Override
        void setBinaryBacked(boolean backed)
        {
            myBinaryBacked = backed;
        }

        @Override
        public void setStreamCopyOptimized(boolean optimized)
        {
            myStreamCopyOptimized = optimized;
        }
    }
}
