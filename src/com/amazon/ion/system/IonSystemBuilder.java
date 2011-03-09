// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.impl.IonSystemImpl;
import com.amazon.ion.impl.lite.IonSystemLite;

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
 *
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
 *
 * <h2>Configuration Properties</h2>
 *
 * <p>This builder provides the following configurable properties:
 * <dl>
 *   <dt>catalog
 *   <dd>The {@link IonCatalog} used as a default when reading Ion data.
 *     If null, each system will be built with a new {@link SimpleCatalog}.
 * </dl>
 */
public class IonSystemBuilder
{
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

    protected IonCatalog myCatalog;
    protected boolean myBinaryBacked = false;


    /** You no touchy. */
    private IonSystemBuilder() { }

    private IonSystemBuilder(IonSystemBuilder that)
    {
        this.myCatalog      = that.myCatalog;
        this.myBinaryBacked = that.myBinaryBacked;
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

    IonSystemBuilder immutable()
    {
        return this;
    }

    IonSystemBuilder mutable()
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
     * By default, this is null.
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
    void setBinaryBacked(boolean bb)
    {
        mutationFailure();
    }

    final IonSystemBuilder withBinaryBacked(boolean bb)
    {
        IonSystemBuilder b = mutable();
        b.setBinaryBacked(bb);
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
            sys = new IonSystemImpl(catalog);
        }
        else
        {
            sys = new IonSystemLite(catalog);
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
        IonSystemBuilder immutable()
        {
            return new IonSystemBuilder(this);
        }

        @Override
        IonSystemBuilder mutable()
        {
            return this;
        }

        @Override
        public void setCatalog(IonCatalog catalog)
        {
            myCatalog = catalog;
        }

        @Override
        void setBinaryBacked(boolean bb)
        {
            myBinaryBacked = bb;
        }
    }
}
