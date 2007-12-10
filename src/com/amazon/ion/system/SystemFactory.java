/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.impl.IonSystemImpl;

/**
 * The bootstrap factory to create an application's {@link IonSystem}.
 * See the documentation of that class for important constraints.
 */
public final class SystemFactory
{
    /*
     * Potential configuration points:
     *
     * - default system version; could be lower than the latest supported.
     */

    /**
     * Constructs a new system instance with a default configuration.
     * <p>
     * The catalog used by the new instance will be a {@link SimpleCatalog}
     * with no initial entries.
     */
    public static IonSystem newSystem()
    {
        return new IonSystemImpl();
    }


    public static IonSystem newSystem(IonCatalog catalog)
    {
        return new IonSystemImpl(catalog);
    }
}
