/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl;

import static software.amazon.ion.SystemSymbols.ION;
import static software.amazon.ion.SystemSymbols.ION_1_0;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static software.amazon.ion.SystemSymbols.VERSION_SID;
import static software.amazon.ion.impl.PrivateUtils.getSidForSymbolTableField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import software.amazon.ion.EmptySymbolException;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.SystemSymbols;

/**
 * An <em>immutable</em> shared symbol table, supporting (non-system) shared
 * symbol tables and system symbol tables.
 * <p>
 * Instances of this class are safe for use by multiple threads.
 */
final class SharedSymbolTable
    implements SymbolTable
{
    /**
     * The array of system symbols as defined by Ion 1.0.
     */
    private static final String[] SYSTEM_SYMBOLS =
    {
        SystemSymbols.ION,
        SystemSymbols.ION_1_0,
        SystemSymbols.ION_SYMBOL_TABLE,
        SystemSymbols.NAME,
        SystemSymbols.VERSION,
        SystemSymbols.IMPORTS,
        SystemSymbols.SYMBOLS,
        SystemSymbols.MAX_ID,
        SystemSymbols.ION_SHARED_SYMBOL_TABLE
    };

    /**
     * The <b>singleton</b> instance of Ion 1.0 system symbol table.
     * <p>
     * TODO amznlabs/ion-java#34 Optimize system symtabs by using our custom backing impl.
     */
    private static final SymbolTable ION_1_0_SYSTEM_SYMTAB;
    static
    {
        Map<String, Integer> systemSymbolsMap = new HashMap<String, Integer>();

        for (int i = 0; i < SYSTEM_SYMBOLS.length; i++)
        {
            systemSymbolsMap.put(SYSTEM_SYMBOLS[i], i+1);
        }

        ION_1_0_SYSTEM_SYMTAB =
            new SharedSymbolTable(ION, 1, SYSTEM_SYMBOLS, systemSymbolsMap);
    }

    /**
     * The name of this shared symbol table. If this is a system symbol
     * table, it is {@link SystemSymbols#ION}.
     */
    private final String                        myName;

    /**
     * The version of this shared symbol table.
     */
    private final int                           myVersion;

    /**
     * The names (aka text) of declared symbols in this shared symbol
     * table (that are not imported from some other symbol table); never null.
     * The sid of the first element is 1.
     * <p>
     * Note that null references are valid elements within this member field,
     * denoting undefined symbol IDs ("gaps").
     */
    private final String[]                      mySymbolNames;

    /**
     * Map of symbol names to symbol ids of declared symbols.
     */
    private final Map<String, Integer>          mySymbolsMap;

    //==========================================================================
    // Private constructor(s) and static factory methods
    //==========================================================================

    private SharedSymbolTable(String name, int version,
                              List<String> symbolsList,
                              Map<String, Integer> symbolsMap)
    {
        myName          = name;
        myVersion       = version;
        mySymbolsMap    = symbolsMap;

        // Construct primitive fixed-length array from the passed-in List
        mySymbolNames   = symbolsList.toArray(new String[symbolsList.size()]);
    }

    private SharedSymbolTable(String name, int version,
                              String[] symbolNames,
                              Map<String, Integer> symbolsMap)
    {
        myName          = name;
        myVersion       = version;
        mySymbolsMap    = symbolsMap;

        mySymbolNames   = symbolNames;
    }

    /**
     * Constructs a new shared symbol table from the parameters.
     * <p>
     * As per {@link IonSystem#newSharedSymbolTable(String, int, Iterator, SymbolTable...)},
     * any duplicate or null symbol texts are skipped.
     * <p>
     * Therefore, <b>THIS METHOD IS NOT SUITABLE WHEN READING SERIALIZED
     * SHARED SYMBOL TABLES</b> since that scenario must preserve all sids.
     *
     * @param name              the name of the new shared symbol table
     * @param version           the version of the new shared symbol table
     * @param priorSymtab       may be null
     * @param symbols           never null
     */
    static SymbolTable newSharedSymbolTable(String name, int version,
                                            SymbolTable priorSymtab,
                                            Iterator<String> symbols)
    {
        if (name == null || name.length() < 1)
        {
            throw new IllegalArgumentException("name must be non-empty");
        }
        if (version < 1)
        {
            throw new IllegalArgumentException("version must be at least 1");
        }

        List<String> symbolsList = new ArrayList<String>();
        Map<String, Integer> symbolsMap = new HashMap<String, Integer>();

        assert version ==
            (priorSymtab == null ? 1 : priorSymtab.getVersion() + 1);

        prepSymbolsListAndMap(priorSymtab, symbols, symbolsList, symbolsMap);

        // We have all necessary data, pass it over to the private constructor.
        return new SharedSymbolTable(name, version, symbolsList, symbolsMap);
    }

    /**
     * Constructs a new shared symbol table represented by the passed in
     * {@link IonStruct}.
     *
     * @param ionRep
     *          the {@link IonStruct} representing the new shared symbol table
     * @return
     */
    static SymbolTable newSharedSymbolTable(IonStruct ionRep)
    {
        IonReader reader = new IonReaderTreeSystem(ionRep);
        return newSharedSymbolTable(reader, false);
    }

    /**
     * Constructs a new shared symbol table represented by the current value
     * of the passed in {@link IonReader}.
     *
     * @param reader
     *          the {@link IonReader} positioned on the shared symbol table
     *          represented as an {@link IonStruct}
     * @param isOnStruct
     *          denotes whether the {@link IonReader} is already positioned on
     *          the struct; false if it is positioned before the struct
     * @return
     */
    static SymbolTable newSharedSymbolTable(IonReader reader,
                                            boolean isOnStruct)
    {
        if (! isOnStruct)
        {
            IonType t = reader.next();
            if (t != IonType.STRUCT)
            {
                throw new IonException("invalid symbol table image passed " +
                                "into reader, " + t + " encountered when a " +
                                "struct was expected");
            }
        }

        String name = null;
        int version = -1;
        List<String> symbolsList = new ArrayList<String>();

        reader.stepIn();

        IonType fieldType = null;
        while ((fieldType = reader.next()) != null)
        {
            if (reader.isNullValue()) continue;

            SymbolToken symTok = reader.getFieldNameSymbol();
            int sid = symTok.getSid();
            if (sid == SymbolTable.UNKNOWN_SYMBOL_ID)
            {
                // This is a user-defined IonReader or a pure DOM, fall
                // back to text
                final String fieldName = reader.getFieldName();
                sid = getSidForSymbolTableField(fieldName);
            }

            // TODO amznlabs/ion-java#35 If there's more than one 'symbols' or 'imports'
            //      field, they will be merged together.
            // TODO amznlabs/ion-java#36 Switching over SIDs doesn't cover the case
            //      where the relevant field names are defined by a prev LST;
            //      the prev LST could have 'symbols' defined locally with a
            //      different SID!
            switch (sid)
            {
                case VERSION_SID:
                    if (fieldType == IonType.INT)
                    {
                        version = reader.intValue();
                    }
                    break;
                case NAME_SID:
                    if (fieldType == IonType.STRING)
                    {
                        name = reader.stringValue();
                    }
                    break;
                case SYMBOLS_SID:
                    // As per the Spec, other field types are treated as
                    // empty lists
                    if (fieldType == IonType.LIST)
                    {
                        reader.stepIn();
                        {
                            IonType t;
                            while ((t = reader.next()) != null)
                            {
                                String text = null;
                                if (t == IonType.STRING
                                    && ! reader.isNullValue())
                                {
                                    // As per the Spec, if any element of
                                    // the list is the empty string or any
                                    // other type, treat it as null
                                    text = reader.stringValue();
                                    if (text.length() == 0) text = null;
                                }
                                symbolsList.add(text);
                            }
                        }
                        reader.stepOut();
                    }
                    break;
                default:
                    break;
            }
        }

        reader.stepOut();

        if (name == null || name.length() == 0)
        {
            String message =
                "shared symbol table is malformed: field 'name' " +
                "must be a non-empty string.";
            throw new IonException(message);
        }

        // As per the Spec, if 'version' field is missing or not at
        // least 1, treat it as 1.
        version = (version < 1) ? 1 : version;

        Map<String, Integer> symbolsMap = null;
        if (! symbolsList.isEmpty())
        {
            symbolsMap = new HashMap<String, Integer>();
            transferNonExistingSymbols(symbolsList, symbolsMap);
        }
        else
        {
            // Empty Map is more efficient than an empty HashMap
            symbolsMap = Collections.emptyMap();
        }

        // We have all necessary data, pass it over to the private constructor.
        return new SharedSymbolTable(name, version, symbolsList, symbolsMap);
    }

    /**
     * Gets a specific version of the system symbol table.
     *
     * @param version
     *          the specified version of the system symbol table; currently,
     *          only version 1 (Ion 1.0) is supported
     * @return
     *
     * @throws IllegalArgumentException if the specified version isn't supported
     */
    static SymbolTable getSystemSymbolTable(int version)
    {
        if (version != 1)
        {
            throw new IllegalArgumentException("only Ion 1.0 system " +
                        "symbols are supported");
        }

        return ION_1_0_SYSTEM_SYMTAB;
    }

    //==========================================================================
    // Static methods relating to construction
    //==========================================================================

    private static void putToMapIfNotThere(Map<String, Integer> symbolsMap,
                                           String text, int sid)
    {
        // When there's a duplicate mapping for the symbol text, don't
        // replace the lower sid. This pattern avoids double-lookup in the
        // common scenario and only performs a second lookup when there's
        // a duplicate.
        Integer extantSid = symbolsMap.put(text, sid);
        if (extantSid != null)
        {
            // We always insert symbols with increasing sids
            assert extantSid < sid;
            symbolsMap.put(text, extantSid);
        }
    }

    /**
     * Collects the necessary symbols from {@code priorSymtab} and
     * {@code symbols}, and load them into the passed-in {@code symbolsList} and
     * {@code symbolsMap}.
     */
    private static void
    prepSymbolsListAndMap(SymbolTable priorSymtab, Iterator<String> symbols,
                          List<String> symbolsList,
                          Map<String, Integer> symbolsMap)
    {
        int sid = 1;

        // Collect from passed-in priorSymtab
        if (priorSymtab != null)
        {
            Iterator<String> priorSymbols =
                priorSymtab.iterateDeclaredSymbolNames();
            while (priorSymbols.hasNext())
            {
                String text = priorSymbols.next();
                if (text != null)
                {
                    assert text.length() > 0;
                    putToMapIfNotThere(symbolsMap, text, sid);
                }

                // NB: Null entries must be added in the sid sequence
                //     to retain compat. with the prior version.
                symbolsList.add(text);

                sid++;
            }
        }

        // Collect from passed-in symbols
        while (symbols.hasNext())
        {
            String text = symbols.next();
            // TODO amznlabs/ion-java#12 What about empty symbols?
            if (symbolsMap.get(text) == null)
            {
                putToMapIfNotThere(symbolsMap, text, sid);
                symbolsList.add(text);
                sid++;
            }
        }
    }

    /**
     * Transfer symbols from {@code symbolsList} to {@code symbolsMap} that
     * doesn't already exist in the map.
     */
    private static void
    transferNonExistingSymbols(List<String> symbolsList,
                               Map<String, Integer> symbolsMap)
    {
        int sid = 1;
        for (String text : symbolsList)
        {
            assert text == null || text.length() > 0;

            if (text != null)
            {
                putToMapIfNotThere(symbolsMap, text, sid);
            }

            sid++;
        }
    }

    //==========================================================================
    // Public methods
    //==========================================================================

    public String getName()
    {
        return myName;
    }

    public int getVersion()
    {
        return myVersion;
    }

    public boolean isLocalTable()
    {
        return false;
    }

    public boolean isSharedTable()
    {
        return true;
    }

    public boolean isSubstitute()
    {
        return false;
    }

    public boolean isSystemTable()
    {
        return ION.equals(myName);
    }

    public boolean isReadOnly()
    {
        return true;
    }

    public void makeReadOnly()
    {
        // No-op
    }

    public SymbolTable getSystemSymbolTable()
    {
        if (isSystemTable()) return this;

        return null; // non-system shared table
    }

    public String getIonVersionId()
    {
        if (isSystemTable())
        {
            int id = getVersion();
            if (id != 1)
            {
                throw new IonException("unrecognized system version " +
                		"encountered: " + id);
            }

            return ION_1_0;
        }

        return null; // non-system shared tables aren't tied to an Ion version
    }

    public SymbolTable[] getImportedTables()
    {
        return null;
    }

    public int getImportedMaxId()
    {
        return 0;
    }

    public int getMaxId()
    {
        return mySymbolNames.length;
    }

    public SymbolToken intern(String text)
    {
        SymbolToken symTok = find(text);
        if (symTok == null)
        {
            throw new ReadOnlyValueException(SymbolTable.class);
        }

        return symTok;
    }

    public SymbolToken find(String text)
    {
        if (text.length() < 1)
        {
            throw new EmptySymbolException();
        }

        Integer sid = mySymbolsMap.get(text);
        if (sid != null)
        {
            assert sid != UNKNOWN_SYMBOL_ID;

            int offset = sid - 1;
            String internedText = mySymbolNames[offset];

            assert internedText != null;

            return new SymbolTokenImpl(internedText, sid);
        }

        return null;
    }

    public int findSymbol(String name)
    {
        if (name.length() < 1)
        {
            throw new EmptySymbolException();
        }

        Integer sid = mySymbolsMap.get(name);
        if (sid != null)
        {
            return sid;
        }

        return UNKNOWN_SYMBOL_ID;
    }

    public String findKnownSymbol(int id)
    {
        if (id < 1)
        {
            throw new
                IllegalArgumentException("symbol IDs must be greater than 0");
        }

        int offset = id - 1;
        if (offset < mySymbolNames.length)
        {
            return mySymbolNames[offset];
        }

        return null;
    }

    public Iterator<String> iterateDeclaredSymbolNames()
    {
        return Collections
            .unmodifiableList(Arrays.asList(mySymbolNames)) // unsupported remove()
            .iterator();
    }

    public void writeTo(IonWriter writer) throws IOException
    {
        IonReader reader = new SymbolTableReader(this);
        writer.writeValues(reader);
    }

}
