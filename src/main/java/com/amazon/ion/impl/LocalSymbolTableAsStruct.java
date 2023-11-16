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

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
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
    implements SymbolTableAsStruct
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
            SymbolTable currentSymbolTable = reader.getSymbolTable();
            LocalSymbolTableImports imports = readLocalSymbolTable(reader,
                                                                   catalog,
                                                                   alreadyInStruct,
                                                                   symbolsList,
                                                                   currentSymbolTable);
            if (imports == null) {
                // This was an LST append, so the existing symbol table was updated.
                return currentSymbolTable;
            }
            return new LocalSymbolTableAsStruct(imports, symbolsList, null);
        }

        public SymbolTable newLocalSymtab(SymbolTable defaultSystemSymtab,
                                          SymbolTable... imports)
        {
            LocalSymbolTableImports unifiedSymtabImports =
                new LocalSymbolTableImports(defaultSystemSymtab, imports);

            return new LocalSymbolTableAsStruct(unifiedSymtabImports, null /* local symbols */, null);
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

            return new LocalSymbolTableAsStruct(imports, symbolsList, ionRep);
        }

    }

    private final SymbolTableStructCache structCache;

    /**
     * @param imports           never null
     * @param symbolsList       may be null or empty
     */
    private LocalSymbolTableAsStruct(LocalSymbolTableImports imports,
                                     List<String> symbolsList,
                                     IonStruct image)
    {
        super(imports, symbolsList);
        structCache = new SymbolTableStructCache(this, imports.getImportedTablesNoCopy(), image);
    }

    @Override
    int putSymbol(String symbolName)
    {
        int sid = super.putSymbol(symbolName);
        if (structCache.hasStruct())
        {
            structCache.addSymbol(symbolName, sid);
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

    @Override
    public IonStruct getIonRepresentation(ValueFactory factory)
    {
        return structCache.getIonRepresentation(factory);
    }

}
