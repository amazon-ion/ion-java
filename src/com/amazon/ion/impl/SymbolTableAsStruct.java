package com.amazon.ion.impl;

import com.amazon.ion.IonStruct;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;

/**
 * Identifies {@link SymbolTable} implementations capable of producing IonStruct representations of themselves.
 */
interface SymbolTableAsStruct {

    /**
     * Provides an IonStruct representation of the SymbolTable.
     * @param valueFactory the {@link ValueFactory} from which to construct the IonStruct.
     * @return an IonStruct representing the SymbolTable.
     */
    IonStruct getIonRepresentation(ValueFactory valueFactory);
}
