// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;

/**
 * NOT FOR APPLICATION USE!
 * Isolates private APIs that are needed from other packages in this library.
 * The leading $ reduces the chance of somebody finding this via autocomplete.
 */
public final class _Private_LiteDomTrampoline
{
    public static IonSystem newLiteSystem(IonCatalog catalog,
                                          boolean streamCopyOptimized)
    {
        return new IonSystemLite(catalog, streamCopyOptimized);
    }


    public static boolean isLiteSystem(IonSystem system)
    {
        return (system instanceof IonSystemLite);
    }
}
