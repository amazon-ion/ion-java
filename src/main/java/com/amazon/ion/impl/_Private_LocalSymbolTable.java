package com.amazon.ion.impl;

import com.amazon.ion.SymbolTable;

import java.util.Collection;
import java.util.List;

public interface _Private_LocalSymbolTable extends SymbolTable {

    /**
     * @return a mutable copy of the symbol table.
     */
    _Private_LocalSymbolTable makeCopy();

    /**
     * Returns the imported symbol tables without making a copy.
     * <p>
     * <b>Note:</b> Callers must not modify the resulting SymbolTable array!
     * This will violate the immutability property of this class.
     *
     * @return
     *          the imported symtabs, as-is; the first element is a system
     *          symtab, the rest are non-system shared symtabs
     *
     * @see SymbolTable#getImportedTables()
     */
    SymbolTable[] getImportedTablesNoCopy();

    /**
     * Returns the imported symbol tables as a List without making a copy (if possible).
     * Like {@link #getImportedTables()}, the list does not include the system symbol table.
     *
     * @return the imported symbol tables. Does not include the system symbol table.
     *
     * @see SymbolTable#getImportedTables()
     */
    List<SymbolTable> getImportedTablesAsList();

    /**
     * Returns a collection containing the local symbols, without making a copy.
     * @return the local symbols.
     */
    Collection<String> getLocalSymbolsNoCopy();

    /**
     * @return the number of local symbols, which do not include the imported or system symbols.
     */
    int getNumberOfLocalSymbols();
}
