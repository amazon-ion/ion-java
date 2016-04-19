/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.lite;

import software.amazon.ion.IonSystem;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.impl.PrivateIonBinaryWriterBuilder;
import software.amazon.ion.system.IonTextWriterBuilder;

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
