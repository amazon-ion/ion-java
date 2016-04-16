// Copyright (c) 2011-2016 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.PrivateIonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;

/**
 * Isolates private APIs that are needed from other packages in this library.
 *
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public final class PrivateLiteDomTrampoline
{
    public static IonSystem newLiteSystem(IonTextWriterBuilder twb,
                                          PrivateIonBinaryWriterBuilder bwb)
    {
        return new IonSystemLite(twb, bwb);
    }

    public static boolean isLiteSystem(IonSystem system)
    {
        return (system instanceof IonSystemLite);
    }

    public static byte[] reverseEncode(int initialSize, SymbolTable symtab)
    {
        ReverseBinaryEncoder encoder = new ReverseBinaryEncoder(initialSize);
        encoder.serialize(symtab);
        return encoder.toNewByteArray();
    }
}
