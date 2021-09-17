package com.amazon.ion.impl;

import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;

import java.util.Iterator;

import static com.amazon.ion.SystemSymbols.IMPORTS;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.MAX_ID;
import static com.amazon.ion.SystemSymbols.NAME;
import static com.amazon.ion.SystemSymbols.SYMBOLS;
import static com.amazon.ion.SystemSymbols.VERSION;

/**
 * Caches the IonStruct representation of a {@link SymbolTable}.
 */
class SymbolTableStructCache {

    private final SymbolTable symbolTable;
    private final SymbolTable[] importedTables;
    private final int firstLocalSid;

    /**
     * Memoized result of {@link #getIonRepresentation(ValueFactory)};
     * Once this is created, it may be mutated as symbols are added using {@link #addSymbol(String, int)}.
     */
    private IonStruct image;

    /**
     * @param symbolTable the SymbolTable to represent as an IonStruct.
     * @param importedTables the symbol table's imported shared symbol tables.
     * @param image the IonStruct representation, if available. If null, an IonStruct will be created lazily.
     */
    SymbolTableStructCache(SymbolTable symbolTable, SymbolTable[] importedTables, IonStruct image) {
        this.symbolTable = symbolTable;
        this.importedTables = importedTables;
        this.firstLocalSid = symbolTable.getImportedMaxId() + 1;
        this.image = image;
    }

    /**
     * Returns the IonStruct representation of the symbol table. Creates and stores a new IonStruct on the first
     * invocation.
     * @param factory the {@link ValueFactory} from which to construct the IonStruct.
     * @return an IonStruct representing the symbol table.
     */
    public IonStruct getIonRepresentation(ValueFactory factory) {
        synchronized (this) {
            if (image == null) {
                makeIonRepresentation(factory);
            }
            return image;
        }
    }

    /**
     * @return true if an IonStruct has already been created for the symbol table; otherwise, false.
     */
    public boolean hasStruct() {
        return image != null;
    }

    /**
     * Create a new IonStruct representation of the symbol table.
     * @param factory the {@link ValueFactory} from which to construct the IonStruct.
     */
    private void makeIonRepresentation(ValueFactory factory) {
        image = factory.newEmptyStruct();

        image.addTypeAnnotation(ION_SYMBOL_TABLE);

        if (importedTables.length > 0) {
            // The system symbol table may be the first import. If it is, skip it.
            int i = importedTables[0].isSystemTable() ? 1 : 0;
            if (i < importedTables.length) {
                IonList importsList = factory.newEmptyList();
                while (i < importedTables.length) {
                    SymbolTable importedTable = importedTables[i];
                    IonStruct importStruct = factory.newEmptyStruct();

                    importStruct.add(NAME,
                        factory.newString(importedTable.getName()));
                    importStruct.add(VERSION,
                        factory.newInt(importedTable.getVersion()));
                    importStruct.add(MAX_ID,
                        factory.newInt(importedTable.getMaxId()));

                    importsList.add(importStruct);
                    i++;
                }
                image.add(IMPORTS, importsList);
            }
        }

        if (symbolTable.getMaxId() > symbolTable.getImportedMaxId()) {
            Iterator<String> localSymbolIterator = symbolTable.iterateDeclaredSymbolNames();
            int sid = symbolTable.getImportedMaxId() + 1;
            while (localSymbolIterator.hasNext()) {
                addSymbol(localSymbolIterator.next(), sid);
                sid++;
            }
        }
    }

    /**
     * Adds a new symbol table with the given symbol ID to the IonStruct's 'symbols' list.
     * @param symbolName can be null when there's a gap in the local symbols list.
     * @param sid the symbol ID to assign to the new symbol.
     */
    void addSymbol(String symbolName, int sid) {
        assert sid >= firstLocalSid;

        ValueFactory sys = image.getSystem();

        IonValue syms = image.get(SYMBOLS);
        // If the user has manually created a symbol table via an IonStruct, it may be malformed.
        // The following removes any "symbols" fields that aren't lists, then creates a list for
        // the symbols field if necessary.
        while (syms != null && syms.getType() != IonType.LIST) {
            image.remove(syms);
            syms = image.get(SYMBOLS);
        }
        if (syms == null) {
            syms = sys.newEmptyList();
            image.put(SYMBOLS, syms);
        }

        int thisOffset = sid - firstLocalSid;
        IonValue name = sys.newString(symbolName);
        ((IonList) syms).add(thisOffset, name);
    }
}
