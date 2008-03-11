package com.amazon.ion.stateless;

import com.amazon.ion.SymbolTable;

public abstract class StatelessSymbolTable implements SymbolTable {

    public StatelessSymbolTable(StatelessSymbolTable copy) {
        // TODO: make a copy of the passed in table.
    }

    /**
     * @param key
     *            The symbol to add to the table.
     * @return The fieldID that can be used in place of the symbol.
     */
    public abstract int add(String key);

    /**
     * @return A new StreamingSymbolTable that is the union of two compatible
     *         symbol tables.
     */
    public abstract StatelessSymbolTable combine(StatelessSymbolTable other);

    /**
     * @return A new compatible StreamingSymbolTable that just the elements that
     *         are in this table but that are not in the other table.
     */
    public abstract StatelessSymbolTable removeAll(StatelessSymbolTable other);
}
