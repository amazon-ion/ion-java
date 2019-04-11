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
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;

/**
 * NOT FOR APPLICATION USE
 *
 * Implementations of this interface may be provided to IonReaders in order
 * to force them to construct LocalSymbolTables in a different way.
 *
 * In practice, this is used to construct a different LocalSymbolTable
 * implementation for use with the DOM than is used purely by readers
 * and writers.
 *
 * If {@link LocalSymbolTableAsStruct} is ever deleted, this can go away
 * too.
 */
@SuppressWarnings("javadoc")
public interface _Private_LocalSymbolTableFactory
{
    /**
     * Constructs a new local symbol table represented by the current value of
     * the passed in {@link IonReader}.
     * <p>
     * <b>NOTE:</b> It is assumed that the passed in reader is positioned
     * properly on/before a value that represents a local symtab semantically.
     * That is, no exception-checks are made on the {@link IonType}
     * and annotation, callers are responsible for checking this!
     *
     * @param catalog
     *          the catalog containing shared symtabs referenced by import
     *          declarations within the local symtab
     * @param reader
     *          the reader positioned on the local symbol table represented as
     *          a struct
     * @param alreadyInStruct
     *          denotes whether the reader is already positioned on the struct;
     *          false if it is positioned before the struct
     */
    public SymbolTable newLocalSymtab(IonCatalog catalog,
                                      IonReader reader,
                                      boolean alreadyInStruct);

    /**
     * Constructs a new local symtab with given imports and local symbols.
     *
     * @param defaultSystemSymtab
     *          the default system symtab, which will be used if the first
     *          import in {@code imports} isn't a system symtab, never null
     * @param imports
     *          the set of shared symbol tables to import; the first (and only
     *          the first) may be a system table, in which case the
     *          {@code defaultSystemSymtab} is ignored
     *
     * @throws IllegalArgumentException
     *          if any import is a local table, or if any but the first is a
     *          system table
     * @throws NullPointerException
     *          if any import is null
     */
    public SymbolTable newLocalSymtab(SymbolTable defaultSystemSymtab,
                                      SymbolTable... imports);
}