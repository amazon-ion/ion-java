package com.amazon.ion.impl;

import com.amazon.ion.SymbolTable;

interface _Private_LocalSymbolTable extends SymbolTable {

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
}
