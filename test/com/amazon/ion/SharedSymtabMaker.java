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

package com.amazon.ion;

import static com.amazon.ion.impl.Symtabs.sharedSymtabStruct;
import static junit.framework.Assert.assertSame;

import com.amazon.ion.impl._Private_IonSystem;

/**
 * Abstracts the various ways that a shared {@link SymbolTable} can be created
 * from an {@link IonSystem}, so test cases can cover all the APIs.
 */
public enum SharedSymtabMaker
{
    /**
     * Invokes {@link IonSystem#newSharedSymbolTable(IonReader)}.
     */
    FROM_READER
    {
        @Override
        public SymbolTable newSharedSymtab(_Private_IonSystem system,
                                           IonReader reader)
        {
            return system.newSharedSymbolTable(reader);
        }
    },

    /**
     * Invokes {@link IonSystem#newSharedSymbolTable(IonReader, boolean)}
     * with true.
     */
    FROM_READER_ON_STRUCT
    {
        @Override
        public SymbolTable newSharedSymtab(_Private_IonSystem system,
                                           IonReader reader)
        {
            assertSame(IonType.STRUCT, reader.next());
            return system.newSharedSymbolTable(reader, true);
        }
    },

    /**
     * Invokes {@link IonSystem#newSharedSymbolTable(IonReader, boolean)}
     * with false.
     */
    FROM_READER_NOT_ON_STRUCT
    {
        @Override
        public SymbolTable newSharedSymtab(_Private_IonSystem system,
                                           IonReader reader)
        {
            return system.newSharedSymbolTable(reader, false);
        }
    },

    /**
     * Invokes {@link _Private_IonSystem#newSharedSymbolTable(IonStruct)}.
     */
    FROM_STRUCT
    {
        @Override
        public SymbolTable newSharedSymtab(_Private_IonSystem system,
                                           IonStruct struct)
        {
            return system.newSharedSymbolTable(struct);
        }
    };


    public SymbolTable newSharedSymtab(_Private_IonSystem system,
                                       String name,
                                       int version,
                                       String... symbols)
    {
        IonStruct s = sharedSymtabStruct(system, name, version, symbols);
        return newSharedSymtab(system, s);
    }

    /**
     * @param reader must be positioned JUST BEFORE the symtab struct.
     */
    public SymbolTable newSharedSymtab(_Private_IonSystem system,
                                       IonReader reader)
    {
        assertSame(IonType.STRUCT, reader.next());
        IonStruct struct = (IonStruct) system.newValue(reader);
        return newSharedSymtab(system, struct);
    }

    public SymbolTable newSharedSymtab(_Private_IonSystem system,
                                       IonStruct struct)
    {
        IonReader reader = system.newReader(struct);
        return newSharedSymtab(system, reader);
    }
}
