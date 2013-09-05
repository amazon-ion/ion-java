// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;

/**
 * The factory for creating {@link IonSystem}s.
 * Most applications will only have one or two system instances;
 * see {@link IonSystem} for important constraints.
 * <p>
 * Most long-lived applications will want to provide a custom
 * {@link IonCatalog} implementation rather than using the default
 * {@link SimpleCatalog}.
 *
 * @deprecated Since IonJava R10. Use the more configurable
 * {@link IonSystemBuilder} instead.
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
     * @deprecated Since IonJava R10. Use
     * {@link IonSystemBuilder IonSystemBuilder.standard().build()} instead.
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
     * @deprecated Since IonJava R10. Use
     * {@link IonSystemBuilder IonSystemBuilder.standard().withCatalog(catalog).build()} instead.
     */
    @Deprecated
    public static IonSystem newSystem(IonCatalog catalog)
    {
        return IonSystemBuilder.standard().withCatalog(catalog).build();
    }
}
