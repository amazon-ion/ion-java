/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.bin;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static software.amazon.ion.SystemSymbols.IMPORTS;
import static software.amazon.ion.SystemSymbols.IMPORTS_SID;
import static software.amazon.ion.SystemSymbols.ION;
import static software.amazon.ion.SystemSymbols.ION_1_0;
import static software.amazon.ion.SystemSymbols.ION_1_0_MAX_ID;
import static software.amazon.ion.SystemSymbols.ION_1_0_SID;
import static software.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE;
import static software.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE_SID;
import static software.amazon.ion.SystemSymbols.ION_SID;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static software.amazon.ion.SystemSymbols.MAX_ID;
import static software.amazon.ion.SystemSymbols.MAX_ID_SID;
import static software.amazon.ion.SystemSymbols.NAME;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.SystemSymbols.SYMBOLS;
import static software.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static software.amazon.ion.SystemSymbols.VERSION;
import static software.amazon.ion.SystemSymbols.VERSION_SID;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import software.amazon.ion.IonException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;

/**
 * Utilities for dealing with {@link SymbolToken} and {@link SymbolTable}.
 */
/*package*/ class Symbols
{
    private Symbols() {}

    /** Constructs a token with a non-null name and positive value. */
    public static SymbolToken symbol(final String name, final int val)
    {
        if (name == null) { throw new NullPointerException(); }
        if (val <= 0) { throw new IllegalArgumentException("Symbol value must be positive: " + val); }

        return new SymbolToken()
        {
            public String getText()
            {
                return name;
            }

            public String assumeText()
            {
                return name;
            }

            public int getSid()
            {
                return val;
            }

            @Override
            public String toString()
            {
                return "(symbol '" + getText() + "' " + getSid() + ")";
            }
        };
    }

    /** Lazy iterator over the symbol names of an iterator of symbol tokens. */
    public static Iterator<String> symbolNameIterator(final Iterator<SymbolToken> tokenIter)
    {
        return new Iterator<String>()
        {
            public boolean hasNext()
            {
                return tokenIter.hasNext();
            }

            public String next()
            {
                return tokenIter.next().getText();
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static final List<SymbolToken> SYSTEM_TOKENS = unmodifiableList(
        asList(
            symbol(ION,                      ION_SID)
          , symbol(ION_1_0,                  ION_1_0_SID)
          , symbol(ION_SYMBOL_TABLE,         ION_SYMBOL_TABLE_SID)
          , symbol(NAME,                     NAME_SID)
          , symbol(VERSION,                  VERSION_SID)
          , symbol(IMPORTS,                  IMPORTS_SID)
          , symbol(SYMBOLS,                  SYMBOLS_SID)
          , symbol(MAX_ID,                   MAX_ID_SID)
          , symbol(ION_SHARED_SYMBOL_TABLE,  ION_SHARED_SYMBOL_TABLE_SID)
        )
    );

    /** Returns a symbol token for a system SID. */
    public static SymbolToken systemSymbol(final int sid) {
        if (sid < 1 || sid > ION_1_0_MAX_ID)
        {
            throw new IllegalArgumentException("No such system SID: " + sid);
        }
        return SYSTEM_TOKENS.get(sid - 1);
    }

    private static final Map<String, SymbolToken> SYSTEM_TOKEN_MAP;
    static {
        final Map<String, SymbolToken> symbols = new HashMap<String, SymbolToken>();
        for (final SymbolToken token : SYSTEM_TOKENS)
        {
            symbols.put(token.getText(), token);
        }
        SYSTEM_TOKEN_MAP = unmodifiableMap(symbols);
    }

    private static SymbolTable SYSTEM_SYMBOL_TABLE = new AbstractSymbolTable(ION, 1)
    {
        public SymbolTable[] getImportedTables()
        {
            return null;
        }

        public int getImportedMaxId()
        {
            return 0;
        }

        public boolean isSystemTable()
        {
            return true;
        }

        public boolean isSubstitute()
        {
            return false;
        }

        public boolean isSharedTable()
        {
            return true;
        }

        public boolean isReadOnly()
        {
            return true;
        }

        public boolean isLocalTable()
        {
            return false;
        }

        public SymbolToken intern(final String text)
        {
            final SymbolToken token = SYSTEM_TOKEN_MAP.get(text);
            if (token == null)
            {
                throw new IonException("Cannot intern new symbol into system symbol table");
            }
            return token;
        }

        public String findKnownSymbol(int id)
        {
            if (id < 1)
            {
                throw new IllegalArgumentException("SID cannot be less than 1: " + id);
            }
            if (id > ION_1_0_MAX_ID)
            {
                return null;
            }

            return SYSTEM_TOKENS.get(id - 1).getText();
        }

        public SymbolToken find(String text)
        {
            return SYSTEM_TOKEN_MAP.get(text);
        }

        public SymbolTable getSystemSymbolTable()
        {
            return this;
        }

        public int getMaxId()
        {
            return ION_1_0_MAX_ID;
        }

        public Iterator<String> iterateDeclaredSymbolNames()
        {
            return symbolNameIterator(SYSTEM_TOKENS.iterator());
        }
    };

    /** Returns a representation of the system symbol table. */
    public static SymbolTable systemSymbolTable()
    {
        return SYSTEM_SYMBOL_TABLE;
    }

    /** Returns the system symbols as a collection. */
    public static Collection<SymbolToken> systemSymbols()
    {
        return SYSTEM_TOKENS;
    }

    /** Returns a substitute shared symbol table where none of the symbols are known. */
    public static SymbolTable unknownSharedSymbolTable(final String name,
                                                       final int version,
                                                       final int maxId)
    {
        return new AbstractSymbolTable(name, version)
        {

            public Iterator<String> iterateDeclaredSymbolNames()
            {
                return new Iterator<String>()
                {
                    int id = 1;

                    public boolean hasNext()
                    {
                        return id <= maxId;
                    }

                    public String next()
                    {
                        if (!hasNext())
                        {
                            throw new NoSuchElementException();
                        }
                        // all symbols are unknown
                        id++;
                        return null;
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            public boolean isSystemTable()
            {
                return false;
            }

            public boolean isSubstitute()
            {
                return true;
            }

            public boolean isSharedTable()
            {
                return true;
            }

            public boolean isReadOnly()
            {
                return true;
            }

            public boolean isLocalTable()
            {
                return false;
            }

            public SymbolToken intern(String text)
            {
                throw new UnsupportedOperationException(
                    "Cannot intern into substitute unknown shared symbol table: "
                    + name + " version " + version
                );
            }

            public SymbolTable getSystemSymbolTable()
            {
                return systemSymbolTable();
            }

            public int getMaxId()
            {
                return maxId;
            }

            public SymbolTable[] getImportedTables()
            {
                return null;
            }

            public int getImportedMaxId()
            {
                return 0;
            }

            public String findKnownSymbol(int id)
            {
                return null;
            }

            public SymbolToken find(String text)
            {
                return null;
            }
        };
    }
}
