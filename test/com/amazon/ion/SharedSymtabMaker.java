// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.Symtabs.sharedSymtabStruct;
import static junit.framework.Assert.assertSame;

import com.amazon.ion.impl.PrivateIonSystem;

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
        public SymbolTable newSharedSymtab(PrivateIonSystem system,
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
        public SymbolTable newSharedSymtab(PrivateIonSystem system,
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
        public SymbolTable newSharedSymtab(PrivateIonSystem system,
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
        public SymbolTable newSharedSymtab(PrivateIonSystem system,
                                           IonStruct struct)
        {
            return system.newSharedSymbolTable(struct);
        }
    };


    public SymbolTable newSharedSymtab(PrivateIonSystem system,
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
    public SymbolTable newSharedSymtab(PrivateIonSystem system,
                                       IonReader reader)
    {
        assertSame(IonType.STRUCT, reader.next());
        IonStruct struct = (IonStruct) system.newValue(reader);
        return newSharedSymtab(system, struct);
    }

    public SymbolTable newSharedSymtab(PrivateIonSystem system,
                                       IonStruct struct)
    {
        IonReader reader = system.newReader(struct);
        return newSharedSymtab(system, reader);
    }
}
