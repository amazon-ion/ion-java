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

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.IMPORTS;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.MAX_ID;
import static com.amazon.ion.SystemSymbols.NAME;
import static com.amazon.ion.SystemSymbols.SYMBOLS;
import static com.amazon.ion.SystemSymbols.VERSION;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * A LocalSymbolTable that memoizes its IonStruct representation.
 * @deprecated Should not be used in new code without data demonstrating its
 *             benefits. Instead, use {@link LocalSymbolTable}.
 */
@Deprecated
class LocalSymbolTableAsStruct
    extends LocalSymbolTable
{

    static class Factory implements _Private_LocalSymbolTableFactory
    {

        private final ValueFactory imageFactory;

        /**
         * @param imageFactory
         *          the factory to use when building a DOM image, not null
         */
        public Factory(ValueFactory imageFactory)
        {
            this.imageFactory = imageFactory;
        }

        public SymbolTable newLocalSymtab(IonCatalog catalog,
                                          IonReader reader,
                                          boolean alreadyInStruct)
        {
            List<String> symbolsList = new ArrayList<String>();
            LocalSymbolTableImports imports = readLocalSymbolTable(reader,
                                                                   catalog,
                                                                   alreadyInStruct,
                                                                   symbolsList,
                                                                   reader.getSymbolTable());
            return new LocalSymbolTableAsStruct(imageFactory, imports, symbolsList);
        }

        public SymbolTable newLocalSymtab(SymbolTable defaultSystemSymtab,
                                          SymbolTable... imports)
        {
            LocalSymbolTableImports unifiedSymtabImports =
                new LocalSymbolTableImports(defaultSystemSymtab, imports);

            return new LocalSymbolTableAsStruct(imageFactory,
                                                unifiedSymtabImports,
                                                null /* local symbols */);
        }

        /**
         * Constructs a new local symbol table represented by the passed in
         * {@link IonStruct}.
         *
         * @param catalog
         *          may be null
         * @param ionRep
         *          the struct represented the local symtab
         */
        // TODO this should die with the 'backed' DOM
        public SymbolTable newLocalSymtab(IonCatalog catalog,
                                          IonStruct ionRep)
        {
            assert imageFactory == ionRep.getSystem();
            IonReader reader = new IonReaderTreeSystem(ionRep);

            List<String> symbolsList = new ArrayList<String>();
            LocalSymbolTableImports imports = readLocalSymbolTable(reader,
                                                                   catalog,
                                                                   false,
                                                                   symbolsList,
                                                                   ionRep.getSymbolTable());

            LocalSymbolTableAsStruct table = new LocalSymbolTableAsStruct(imageFactory,
                                                                          imports,
                                                                          symbolsList);
            table.myImage = ionRep;

            return table;
        }

    }

    /**
     * The factory used to build the {@link #myImage} of a local symtab.
     * It's used by the datagram level to maintain the tree representation.
     * It cannot be changed since local symtabs can't be moved between trees.
     */
    private final ValueFactory myImageFactory;

    /**
     * Memoized result of {@link #getIonRepresentation()};
     * Once this is created, we maintain it as symbols are added.
     */
    private IonStruct myImage;

    /**
     * @param imageFactory      never null
     * @param imports           never null
     * @param symbolsList       may be null or empty
     */
    private LocalSymbolTableAsStruct(ValueFactory imageFactory,
                                     LocalSymbolTableImports imports,
                                     List<String> symbolsList)
    {
        super(imports, symbolsList);
        myImageFactory = imageFactory;
    }

    @Override
    int putSymbol(String symbolName)
    {
        int sid = super.putSymbol(symbolName);
        if (myImage != null)
        {
            recordLocalSymbolInIonRep(myImage, symbolName, sid);
        }
        return sid;
    }

    //
    // TODO: there needs to be a better way to associate a System with
    //       the symbol table, which is required if someone is to be
    //       able to generate an instance.  The other way to resolve
    //       this dependency would be for the IonSystem object to be
    //       able to take a SymbolTable and synthesize an Ion
    //       value from it, by using the public API's to see the useful
    //       contents.  But what about open content?  If the origin of
    //       the symbol table was an IonValue you could get the sys
    //       from it, and update it, thereby preserving any extra bits.
    //       If, OTOH, it was synthesized from scratch (a common case)
    //       then extra content doesn't matter.
    //

    /**
     * Only valid on local symtabs that already have an _image_factory set.
     *
     * @return Not null.
     */
    IonStruct getIonRepresentation()
    {
        synchronized (this)
        {
            IonStruct image = myImage;

            if (image == null)
            {
                // Start a new image from scratch
                myImage = image = makeIonRepresentation(myImageFactory);
            }

            return image;
        }
    }

    /**
     * NOT SYNCHRONIZED! Call only from a synch'd method.
     *
     * @return a new struct, not null.
     */
    private IonStruct makeIonRepresentation(ValueFactory factory)
    {
        IonStruct ionRep = factory.newEmptyStruct();

        ionRep.addTypeAnnotation(ION_SYMBOL_TABLE);

        SymbolTable[] importedTables = getImportedTablesNoCopy();

        if (importedTables.length > 1)
        {
            IonList importsList = factory.newEmptyList();
            for (int i = 1; i < importedTables.length; i++)
            {
                SymbolTable importedTable = importedTables[i];
                IonStruct importStruct = factory.newEmptyStruct();

                importStruct.add(NAME,
                                 factory.newString(importedTable.getName()));
                importStruct.add(VERSION,
                                 factory.newInt(importedTable.getVersion()));
                importStruct.add(MAX_ID,
                                 factory.newInt(importedTable.getMaxId()));

                importsList.add(importStruct);
            }
            ionRep.add(IMPORTS, importsList);
        }

        if (mySymbolsCount > 0)
        {
            int sid = myFirstLocalSid;
            for (int offset = 0; offset < mySymbolsCount; offset++, sid++)
            {
                String symbolName = mySymbolNames[offset];
                recordLocalSymbolInIonRep(ionRep, symbolName, sid);
            }
        }

        return ionRep;
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
     * @param symbolName can be null when there's a gap in the local symbols list.
     */
    private void recordLocalSymbolInIonRep(IonStruct ionRep,
                                           String symbolName,
                                           int sid)
    {
        assert sid >= myFirstLocalSid;

        ValueFactory sys = ionRep.getSystem();

        // TODO this is crazy inefficient and not as reliable as it looks
        // since it doesn't handle the case where's theres more than one list
        IonValue syms = ionRep.get(SYMBOLS);
        while (syms != null && syms.getType() != IonType.LIST)
        {
            ionRep.remove(syms);
            syms = ionRep.get(SYMBOLS);
        }
        if (syms == null)
        {
            syms = sys.newEmptyList();
            ionRep.put(SYMBOLS, syms);
        }

        int this_offset = sid - myFirstLocalSid;
        IonValue name = sys.newString(symbolName);
        ((IonList)syms).add(this_offset, name);
    }

}
