/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonSystem;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl._Private_IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * Isolates private APIs that are needed from other packages in this library.
 * The leading _ reduces the chance of somebody finding this via autocomplete.
 */
public final class _Private_LiteDomTrampoline
{
    public static IonSystem newLiteSystem(IonTextWriterBuilder twb,
                                          _Private_IonBinaryWriterBuilder bwb,
                                          IonReaderBuilder rb)
    {
        return new IonSystemLite(twb, bwb, rb);
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
