// Copyright (c) 2011-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.impl.lite.IonDatagramLite.REVERSE_BINARY_ENCODER_PROPERTY;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * Isolates private APIs that are needed from other packages in this library.
 * The leading _ reduces the chance of somebody finding this via autocomplete.
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

    public static void setReverseBinaryEncoder(boolean useReverseEncoder)
    {
        System.setProperty(REVERSE_BINARY_ENCODER_PROPERTY,
                           Boolean.toString(useReverseEncoder));
    }

    public static byte[] reverseEncode(int initialSize, SymbolTable symtab)
    {
        ReverseBinaryEncoder encoder = new ReverseBinaryEncoder(initialSize);
        encoder.serialize(symtab);
        return encoder.toNewByteArray();
    }
}
