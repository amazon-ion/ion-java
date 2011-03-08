// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;

/**
 * The bootstrap factory to create an application's {@link IonSystem}.
 * See the documentation of that class for important constraints.
 * <p>
 * Most long-lived applications will want to provide a custom
 * {@link IonCatalog} implementation rather than using the default
 * {@link SimpleCatalog}.
 *
 * @deprecated This class is being replaced by {@link IonSystemBuilder}, but
 * see its documentation for important limitations.
 */
@Deprecated
public final class SystemFactory
{
    private static SystemCapabilities DEFAULT_IMPLEMENTATION
                 = SystemCapabilities.LITE;

    /**
     * This enum lists the various IonSystem implementations
     * that are current known to this factory.  This can be
     * used to select the variant of IonSystem that is
     * returned by this factory.
     *
     * @deprecated This will be removed before R10 is released!
     */
    @Deprecated
    public enum SystemCapabilities {
        /**
         * returns the IonSystem implementation is currently
         * defined as the default (ORIGINAL at present).
         */
        DEFAULT,
        /**
         * Original buffer backed IonValue implementation.
         * This incrementally materializes the tree as
         * values are requested.  This may be faster
         * in cases where very few values are requested
         * and very few values, if any, are updated.
         * In general this will use more memory than the
         * LITE implementation.
         */
        ORIGINAL,
        /**
         * returns the IonSystem implementation that
         * uses the non-buffer backed IonValues.  This
         * minimizes the size of the fully materialized
         * tree but requires the tree to be fully
         * materialized at all times.
         */
        LITE
    }

    /**
     * Constructs a new system instance with a default configuration.
     * <p>
     * The catalog used by the new instance will be a {@link SimpleCatalog}
     * with no initial entries, so please be aware of the limitations of that
     * class.
     *
     * @return a new {@link IonSystem} instance; not null.
     *
     * @deprecated Use the more configurable {@link IonSystemBuilder} instead
     * (assuming your application can use the new lightweight {@link IonValue}
     * implementation).
     */
    @Deprecated
    public static IonSystem newSystem()
    {
        IonSystem sys = newSystem(DEFAULT_IMPLEMENTATION);
        return sys;
    }

    /**
     * Constructs a new system instance with the given catalog.
     *
     * @return a new {@link IonSystem} instance; not null.
     *
     * @throws NullPointerException if {@code catalog} is null.
     *
     * @deprecated Use the more configurable {@link IonSystemBuilder} instead
     * (assuming your application can use the new lightweight {@link IonValue}
     * implementation).
     */
    @Deprecated
    public static IonSystem newSystem(IonCatalog catalog)
    {
        IonSystem sys = newSystem(DEFAULT_IMPLEMENTATION, catalog);
        return sys;
    }

    /**
     * Constructs a new system instance with the given catalog.  This
     * method allows explicit control over which implementation of
     * IonSystem you want to create.  The SystemCapabilities enum
     * provides the current list.  A null implementation will return
     * the default.  The constructed IonSystem will have an
     * empty Catalog.
     *
     * @param implementation that has the desired capabilities
     *
     * @return a new {@link IonSystem} instance; not null.
     *
     * @throws NullPointerException if {@code catalog} is null.
     *
     * @deprecated This method will be removed before R10 is released!
     */
    @Deprecated
    public static IonSystem newSystem(SystemCapabilities implementation)
    {
        IonSystem sys = newSystem(implementation, new SimpleCatalog());
        return sys;
    }

    /**
     * Constructs a new system instance with the given catalog.  This
     * method allows explicit control over which implementation of
     * IonSystem you want to create.  The SystemCapabilities enum
     * provides the current list.  A null implementation will return
     * the default.
     *
     * @param implementation that has the desired capabilities
     * @param catalog non null default catalog for the system to use
     *
     * @return a new {@link IonSystem} instance; not null.
     *
     * @throws NullPointerException if {@code catalog} is null.
     *
     * @deprecated This method will be removed before R10 is released!
     */
    @Deprecated
    public static IonSystem newSystem(SystemCapabilities implementation, IonCatalog catalog)
    {
        if (catalog == null) {
            throw new NullPointerException("catalog is null");
        }

        if (implementation == null
            || implementation.equals(SystemCapabilities.DEFAULT)
           ) {
            implementation = DEFAULT_IMPLEMENTATION;
        }

        IonSystemBuilder builder = IonSystemBuilder.standard().copy();
        builder.setCatalog(catalog);
        builder.setBinaryBacked(implementation == SystemCapabilities.ORIGINAL);

        IonSystem sys = builder.build();
        return sys;
    }
}
