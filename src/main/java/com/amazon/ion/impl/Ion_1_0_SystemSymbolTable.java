// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl.bin.AbstractSymbolTable;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.amazon.ion.SystemSymbols.*;
import static com.amazon.ion.impl._Private_Utils.newSymbolToken;

public final class Ion_1_0_SystemSymbolTable extends AbstractSymbolTable {

    private Ion_1_0_SystemSymbolTable() {
        super(ION, 1);
    }

    public static final Ion_1_0_SystemSymbolTable INSTANCE = new Ion_1_0_SystemSymbolTable();

    // All Ion 1.0 System Symbol Tokens
    static final SymbolTokenImpl ION_TOKEN = newSymbolToken(ION, ION_SID);
    static final SymbolTokenImpl ION_1_0_TOKEN = newSymbolToken(ION_1_0, ION_1_0_SID);
    static final SymbolTokenImpl ION_SYMBOL_TABLE_TOKEN = newSymbolToken(ION_SYMBOL_TABLE, ION_SYMBOL_TABLE_SID);
    static final SymbolTokenImpl NAME_TOKEN = newSymbolToken(NAME, NAME_SID);
    static final SymbolTokenImpl VERSION_TOKEN = newSymbolToken(VERSION, VERSION_SID);
    static final SymbolTokenImpl IMPORTS_TOKEN = newSymbolToken(IMPORTS, IMPORTS_SID);
    static final SymbolTokenImpl SYMBOLS_TOKEN = newSymbolToken(SYMBOLS, SYMBOLS_SID);
    static final SymbolTokenImpl MAX_ID_TOKEN = newSymbolToken(MAX_ID, MAX_ID_SID);
    static final SymbolTokenImpl ION_SHARED_SYMBOL_TABLE_TOKEN = newSymbolToken(ION_SHARED_SYMBOL_TABLE, ION_SHARED_SYMBOL_TABLE_SID);

    // Hashes of all Ion 1.0 System Symbol Text
    private static final int ION_HASHCODE = ION.hashCode();
    private static final int ION_1_0_HASHCODE = ION_1_0.hashCode();
    private static final int ION_SYMBOL_TABLE_HASHCODE = ION_SYMBOL_TABLE.hashCode();
    private static final int NAME_HASHCODE = NAME.hashCode();
    private static final int VERSION_HASHCODE = VERSION.hashCode();
    private static final int IMPORTS_HASHCODE = IMPORTS.hashCode();
    private static final int SYMBOLS_HASHCODE = SYMBOLS.hashCode();
    private static final int MAX_ID_HASHCODE = MAX_ID.hashCode();
    private static final int ION_SHARED_SYMBOL_TABLE_HASHCODE = ION_SHARED_SYMBOL_TABLE.hashCode();

    // Trivial method implementations
    public SymbolTable[] getImportedTables() {
        return null;
    }

    public int getImportedMaxId() {
        return 0;
    }

    public boolean isSystemTable() {
        return true;
    }

    public boolean isSubstitute() {
        return false;
    }

    public boolean isSharedTable() {
        return true;
    }

    public boolean isReadOnly() {
        return true;
    }

    public boolean isLocalTable() {
        return false;
    }

    public SymbolTable getSystemSymbolTable() {
        return this;
    }

    public int getMaxId() {
        return ION_1_0_MAX_ID;
    }

    // Interesting method implementations

    public SymbolToken intern(final String text) {
        SymbolToken symbol = find(text);
        if (symbol == null) {
            throw new IonException("Cannot intern new symbol into system symbol table");
        }
        return symbol;
    }

    public String findKnownSymbol(final int id) {
        return staticFindKnownSymbol(id);
    }

    public static String staticFindKnownSymbol(final int id) {
        // This compiles into a jump table, which seems to be marginally faster than an array lookup based on
        // some informal performance testing most likely due to the fact that the array needs to be loaded from the heap.
        switch (id) {
            // TODO: It is unclear whether an exception should be thrown here. Existing implementation is inconsistent.
            // case 0: throw new IllegalArgumentException("SID cannot be less than 1: " + id);
            case ION_SID: return ION;
            case ION_1_0_SID: return ION_1_0;
            case ION_SYMBOL_TABLE_SID: return ION_SYMBOL_TABLE;
            case NAME_SID: return NAME;
            case VERSION_SID: return VERSION;
            case IMPORTS_SID: return IMPORTS;
            case SYMBOLS_SID: return SYMBOLS;
            case MAX_ID_SID: return MAX_ID;
            case ION_SHARED_SYMBOL_TABLE_SID: return ION_SHARED_SYMBOL_TABLE;
            default: return null;
        }
    }

    public static SymbolToken staticFindKnownSymbolToken(final int id) {
        // This compiles into a jump table, which seems to be marginally faster than an array lookup based on
        // some informal performance testing most likely due to the fact that the array needs to be loaded from the heap.
        switch (id) {
            case ION_SID: return ION_TOKEN;
            case ION_1_0_SID: return ION_1_0_TOKEN;
            case ION_SYMBOL_TABLE_SID: return ION_SYMBOL_TABLE_TOKEN;
            case NAME_SID: return NAME_TOKEN;
            case VERSION_SID: return VERSION_TOKEN;
            case IMPORTS_SID: return IMPORTS_TOKEN;
            case SYMBOLS_SID: return SYMBOLS_TOKEN;
            case MAX_ID_SID: return MAX_ID_TOKEN;
            case ION_SHARED_SYMBOL_TABLE_SID: return ION_SHARED_SYMBOL_TABLE_TOKEN;
            default: return null;
        }
    }

    public SymbolToken find(String text) {
        // Check all symbol hashes without branching!
        int hash = text.hashCode();
        long result = (long) (hash - ION_HASHCODE) *
                (hash - ION_1_0_HASHCODE) *
                (hash - ION_SYMBOL_TABLE_HASHCODE) *
                (hash - NAME_HASHCODE) *
                (hash - VERSION_HASHCODE) *
                (hash - IMPORTS_HASHCODE) *
                (hash - SYMBOLS_HASHCODE) *
                (hash - MAX_ID_HASHCODE) *
                (hash - ION_SHARED_SYMBOL_TABLE_HASHCODE);
        // If no hash collisions, then it's not in this symbol table
        if (result != 0) return null;

        // If there was any hash collision, we'll fall back to checking equality. In JDK 8, this compiles
        // to a lookup table based on the string's hashcode, and then checks for equality, so (unlike repeated
        // if/else) it doesn't result in a full string comparison for every one of the declared symbols.
        switch (text) {
            case ION:
                return ION_TOKEN;
            case ION_1_0:
                return ION_1_0_TOKEN;
            case ION_SYMBOL_TABLE:
                return ION_SYMBOL_TABLE_TOKEN;
            case NAME:
                return NAME_TOKEN;
            case VERSION:
                return VERSION_TOKEN;
            case IMPORTS:
                return IMPORTS_TOKEN;
            case SYMBOLS:
                return SYMBOLS_TOKEN;
            case MAX_ID:
                return MAX_ID_TOKEN;
            case ION_SHARED_SYMBOL_TABLE:
                return ION_SHARED_SYMBOL_TABLE_TOKEN;
            default:
                return null;
        }
    }

    public Iterator<String> iterateDeclaredSymbolNames() {
        return new Ion_1_0_SystemSymbolIterator();
    }

    /**
     * Rather than fetch an iterator for a list, we can define an iterator that is hard coded to iterate only the
     * declared symbols for Ion 1.0. This less indirection than iterating a List of SymbolTokens.
     */
    private static class Ion_1_0_SystemSymbolIterator implements Iterator<String> {
        private int i = 0;

        public boolean hasNext() {
            return i < ION_1_0_MAX_ID;
        }

        public String next() {
            if (i == ION_1_0_MAX_ID) throw new NoSuchElementException();
            return staticFindKnownSymbol(++i);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
