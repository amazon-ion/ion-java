// Copyright (c) 2011-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonTextWriterBuilder;
import java.io.IOException;
import java.io.InputStream;

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * Isolates private APIs that are needed from other packages in this library.
 */
public final class _Private_LazyDomTrampoline
{
    public static
    _Private_IonSystem newLazySystem(IonTextWriterBuilder twb,
                                     _Private_IonBinaryWriterBuilder bwb)
    {
        return new IonSystemImpl(twb, bwb);
    }


    public static boolean isLazySystem(IonSystem system)
    {
        return (system instanceof IonSystemImpl);
    }


    public static void writeAsBase64(InputStream byteStream, Appendable out)
        throws IOException
    {
        Base64Encoder.TextStream ts = new Base64Encoder.TextStream(byteStream);

        for (;;) {
            int c = ts.read();
            if (c == -1) break;
            out.append((char) c);
        }
    }
}
