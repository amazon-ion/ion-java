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
        return newSystem(null);
    }

    /**
     * Constructs a new system instance with the given catalog.
     *
     * @param catalog the catalog to use in the new system.
     *   If null, a new {@link SimpleCatalog} will be used.
     * @return a new {@link IonSystem} instance; not null.
     *
     * @deprecated Use the more configurable {@link IonSystemBuilder} instead
     * (assuming your application can use the new lightweight {@link IonValue}
     * implementation).
     */
    @Deprecated
    public static IonSystem newSystem(IonCatalog catalog)
    {
        IonSystem sys = IonSystemBuilder.standard()
                                        .withCatalog(catalog)
                                        .withBinaryBacked(true)
                                        .build();
        return sys;
    }
}
