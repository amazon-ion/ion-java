// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;

/**
 * The bootstrap factory to create an application's {@link IonSystem}.
 * See the documentation of that class for important constraints.
 * <p>
 * Most long-lived applications will want to provide a custom
 * {@link IonCatalog} implementation rather than using the default
 * {@link SimpleCatalog}.
 *
 * @deprecated As of release R10, this class is replaced by
 * {@link IonSystemBuilder}.
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
     * @deprecated Use the more configurable {@link IonSystemBuilder} instead.
     * You can use your IDE to inline this method with the equivalent code:
     * <pre>IonSystemBuilder.standard().build()</pre>
     */
    @Deprecated
    public static IonSystem newSystem()
    {
        return IonSystemBuilder.standard().build();
    }

    /**
     * Constructs a new system instance with the given catalog.
     *
     * @param catalog the catalog to use in the new system.
     *   If null, a new {@link SimpleCatalog} will be used.
     * @return a new {@link IonSystem} instance; not null.
     *
     * @deprecated Use the more configurable {@link IonSystemBuilder} instead.
     * You can use your IDE to inline this method with the equivalent code:
     * <pre>IonSystemBuilder.standard().withCatalog(catalog).build()</pre>
     */
    @Deprecated
    public static IonSystem newSystem(IonCatalog catalog)
    {
        return IonSystemBuilder.standard().withCatalog(catalog).build();
    }
}
