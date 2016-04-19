/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static software.amazon.ion.SystemSymbols.IMPORTS;
import static software.amazon.ion.SystemSymbols.IMPORTS_SID;
import static software.amazon.ion.SystemSymbols.ION;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static software.amazon.ion.SystemSymbols.MAX_ID;
import static software.amazon.ion.SystemSymbols.MAX_ID_SID;
import static software.amazon.ion.SystemSymbols.NAME;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.SystemSymbols.SYMBOLS;
import static software.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static software.amazon.ion.SystemSymbols.VERSION;
import static software.amazon.ion.SystemSymbols.VERSION_SID;
import static software.amazon.ion.impl.PrivateUtils.copyOf;
import static software.amazon.ion.impl.PrivateUtils.getSidForSymbolTableField;
import static software.amazon.ion.impl.PrivateUtils.safeEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import software.amazon.ion.EmptySymbolException;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonException;
import software.amazon.ion.IonList;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.ValueFactory;
import software.amazon.ion.util.IonTextUtils;

/**
 * A local symbol table.
 * <p>
 * Instances of this class are safe for use by multiple threads.
 */
final class LocalSymbolTable
    implements SymbolTable
{
    /**
     * The initial length of {@link #mySymbolNames}.
     */
    private static final int DEFAULT_CAPACITY = 16;

    /**
     * The system and shared symtabs imported by this symtab. Never null.
     * <p>
     * Note: this member field is immutable and assigned only during
     * construction, hence no synchronization is needed for its method calls.
     */
    private final LocalSymbolTableImports myImportsList;

    /**
     * The factory used to build the {@link #myImage} of a local symtab.
     * It's used by the datagram level to maintain the tree representation.
     * It cannot be changed since local symtabs can't be moved between trees.
     */
    private final ValueFactory myImageFactory;

    /**
     * Map of symbol names to symbol ids of local symbols that are not in
     * imports.
     */
    private final Map<String, Integer> mySymbolsMap;

    /**
     * Whether this symbol table is read only, and thus, immutable.
     */
    private boolean isReadOnly;

    /**
     * Memoized result of {@link #getIonRepresentation(ValueFactory)};
     * Once this is created, we maintain it as symbols are added.
     */
    private IonStruct myImage;

    /**
     * The local symbol names declared in this symtab; never null.
     * The sid of the first element is {@link #myFirstLocalSid}.
     * Only the first {@link #mySymbolsCount} elements are valid.
     */
    private String[] mySymbolNames;

    /**
     * This is the number of symbols defined in this symbol table
     * locally, that is not imported from some other table.
     */
    private int mySymbolsCount;

    /**
     * The sid of the first local symbol, which is stored at
     * {@link #mySymbolNames}[0].
     */
    private final int myFirstLocalSid;

    //==========================================================================
    // Private constructor(s) and static factory methods
    //==========================================================================

    private void buildSymbolsMap()
    {
        int sid = myFirstLocalSid;
        for (int i = 0; i < mySymbolNames.length; i++, sid++)
        {
            String symbolText = mySymbolNames[i];
            if (symbolText != null)
            {
                putToMapIfNotThere(mySymbolsMap, symbolText, sid);
            }
        }
    }


    /**
     * @param imageFactory      never null
     * @param imports           never null
     * @param symbolsList       may be null or empty
     */
    private LocalSymbolTable(ValueFactory imageFactory,
                             LocalSymbolTableImports imports,
                             List<String> symbolsList)
    {
        if (symbolsList == null || symbolsList.isEmpty())
        {
            mySymbolsCount = 0;
            mySymbolNames  = PrivateUtils.EMPTY_STRING_ARRAY;
        }
        else
        {
            mySymbolsCount = symbolsList.size();
            mySymbolNames  = symbolsList.toArray(new String[mySymbolsCount]);
        }

        myImageFactory = imageFactory;
        myImportsList = imports;
        myFirstLocalSid = myImportsList.getMaxId() + 1;

        // Copy locally declared symbols to mySymbolsMap
        mySymbolsMap = new HashMap<String, Integer>();
        buildSymbolsMap();
    }

    /**
     * Copy-constructor, performs defensive copying of member fields where
     * necessary. The returned instance is mutable.
     */
    private LocalSymbolTable(LocalSymbolTable other, int maxId)
    {
        isReadOnly      = false;
        myFirstLocalSid = other.myFirstLocalSid;
        myImage         = null;
        myImageFactory  = other.myImageFactory;
        myImportsList   = other.myImportsList;
        mySymbolsCount  = maxId - myImportsList.getMaxId();

        mySymbolNames   = copyOf(other.mySymbolNames, mySymbolsCount);

        // Copy locally declared symbols to mySymbolsMap
        if (maxId == other.getMaxId())
        {
            // Shallow copy
            mySymbolsMap = new HashMap<String, Integer>(other.mySymbolsMap);
        }
        else
        {
            mySymbolsMap = new HashMap<String, Integer>(mySymbolsCount);
            buildSymbolsMap();
        }
    }

    /**
     * Constructs a new local symtab with given imports and local symbols.
     *
     * @param imageFactory
     *          the factory to use when building a DOM image, may be null
     * @param defaultSystemSymtab
     *          the default system symtab, which will be used if the first
     *          import in {@code imports} isn't a system symtab, never null
     * @param localSymbols
     *          the list of local symbols; may be null or empty to indicate
     *          no local symbols
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
    static LocalSymbolTable
    makeNewLocalSymbolTable(ValueFactory imageFactory,
                            SymbolTable defaultSystemSymtab,
                            List<String> localSymbols,
                            SymbolTable... imports)
    {
        LocalSymbolTableImports unifiedSymtabImports =
            new LocalSymbolTableImports(defaultSystemSymtab, imports);

        return new LocalSymbolTable(imageFactory,
                                    unifiedSymtabImports,
                                    localSymbols);
    }

    /**
     * Constructs a new local symbol table represented by the passed in
     * {@link IonStruct}.
     *
     * @param systemSymbolTable
     *          never null
     * @param catalog
     *          may be null
     * @param ionRep
     *          the struct represented the local symtab
     */
    static LocalSymbolTable
    makeNewLocalSymbolTable(SymbolTable systemSymbolTable,
                            IonCatalog catalog,
                            IonStruct ionRep)
    {
        ValueFactory imageFactory = ionRep.getSystem();
        IonReader reader = new IonReaderTreeSystem(ionRep);
        LocalSymbolTable table =
            makeNewLocalSymbolTable(imageFactory, systemSymbolTable,
                                    catalog, reader, false);

        table.myImage = ionRep;

        return table;
    }

    /**
     * Constructs a new local symbol table represented by the current value of
     * the passed in {@link IonReader}.
     * <p>
     * <b>NOTE:</b> It is assumed that the passed in reader is positioned
     * properly on/before a value that represents a local symtab semantically.
     * That is, no exception-checks are made on the {@link IonType}
     * and annotation, callers are responsible for checking this!
     *
     * @param imageFactory
     * @param systemSymbolTable
     * @param catalog
     *          the catalog containing shared symtabs referenced by import
     *          declarations within the local symtab
     * @param reader
     *          the reader positioned on the local symbol table represented as
     *          a struct
     * @param isOnStruct
     *          denotes whether the reader is already positioned on the struct;
     *          false if it is positioned before the struct
     */
    static LocalSymbolTable
    makeNewLocalSymbolTable(ValueFactory imageFactory,
                            SymbolTable systemSymbolTable,
                            IonCatalog catalog,
                            IonReader reader,
                            boolean isOnStruct)
    {
        if (! isOnStruct)
        {
            reader.next();
        }

        assert reader.getType() == IonType.STRUCT
            : "invalid symbol table image passed in reader " +
              reader.getType() + " encountered when a struct was expected";

        assert ION_SYMBOL_TABLE.equals(reader.getTypeAnnotations()[0])
            : "local symbol tables must be annotated by " + ION_SYMBOL_TABLE;

        reader.stepIn();

        List<String> symbolsList = new ArrayList<String>();
        List<SymbolTable> importsList = new ArrayList<SymbolTable>();
        importsList.add(systemSymbolTable);

        IonType fieldType;
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
                case SYMBOLS_SID:
                {
                    // As per the Spec, other field types are treated as
                    // empty lists
                    if (fieldType == IonType.LIST)
                    {
                        reader.stepIn();
                        IonType type;
                        while ((type = reader.next()) != null)
                        {
                            String text = null;
                            if (type == IonType.STRING)
                            {
                                // As per the Spec, if any element of
                                // the list is the empty string or any
                                // other type, treat it as null
                                text = reader.stringValue();
                                if (text != null && text.length() == 0)
                                {
                                    text = null;
                                }
                            }

                            symbolsList.add(text);
                        }
                        reader.stepOut();
                    }
                    break;
                }
                case IMPORTS_SID:
                {
                    if (fieldType == IonType.LIST)
                    {
                        prepImportsList(importsList, reader, catalog);
                    }
                    break;
                }
                default:
                {
                    // As per the Spec, any other field is ignored
                    break;
                }
            }
        }

        reader.stepOut();

        LocalSymbolTableImports imports =
            new LocalSymbolTableImports(importsList);

        // We have all necessary data, pass it over to the private constructor
        return new LocalSymbolTable(imageFactory, imports, symbolsList);
    }

    synchronized LocalSymbolTable makeCopy()
    {
        return new LocalSymbolTable(this, getMaxId());
    }

    synchronized LocalSymbolTable makeCopy(int maxId)
    {
        return new LocalSymbolTable(this, maxId);
    }

    public boolean isLocalTable()
    {
        return true;
    }

    public boolean isSharedTable()
    {
        return false;
    }

    public boolean isSystemTable()
    {
        return false;
    }

    public boolean isSubstitute()
    {
        return false;
    }

    public synchronized boolean isReadOnly()
    {
        return isReadOnly;
    }

    public synchronized void makeReadOnly()
    {
        isReadOnly = true;
    }

    public int getImportedMaxId()
    {
        return myImportsList.getMaxId();
    }

    public synchronized int getMaxId()
    {
        int maxid = mySymbolsCount + myImportsList.getMaxId();
        return maxid;
    }

    public int getVersion()
    {
        return 0;
    }

    public String getName()
    {
        return null;
    }

    public String getIonVersionId()
    {
        SymbolTable system_table = myImportsList.getSystemSymbolTable();
        return system_table.getIonVersionId();
    }

    public synchronized Iterator<String> iterateDeclaredSymbolNames()
    {
        return new SymbolIterator(mySymbolNames, mySymbolsCount);
    }

    public String findKnownSymbol(int id)
    {
        String name = null;

        if (id < 1)
        {
            String message = "symbol IDs must be greater than 0";
            throw new IllegalArgumentException(message);
        }
        else if (id < myFirstLocalSid)
        {
            name = myImportsList.findKnownSymbol(id);
        }
        else
        {
            int offset = id - myFirstLocalSid;

            String[] names;
            synchronized (this)
            {
                names = mySymbolNames;
            }

            if (offset < names.length)
            {
                name = names[offset];
            }
        }

        return name;
    }

    public int findSymbol(String name)
    {
        if (name.length() < 1) // throws NPE if null
        {
            throw new EmptySymbolException();
        }

        // Look in system then imports
        int sid = myImportsList.findSymbol(name);

        // Look in local symbols
        if (sid == UNKNOWN_SYMBOL_ID)
        {
            sid = findLocalSymbol(name);
        }

        return sid;
    }

    private int findLocalSymbol(String name)
    {
        assert(name.length() > 0);

        Integer isid;
        synchronized (this)
        {
            isid = mySymbolsMap.get(name);
        }

        if (isid != null)
        {
            assert isid != UNKNOWN_SYMBOL_ID;
            return isid;
        }
        return UNKNOWN_SYMBOL_ID;
    }


    public synchronized SymbolToken intern(String text)
    {
        SymbolToken is = find(text);
        if (is == null)
        {
            validateSymbol(text);
            int sid = putSymbol(text);
            is = new SymbolTokenImpl(text, sid);
        }
        return is;
    }

    public SymbolToken find(String text)
    {
        if (text.length() < 1)
        {
            throw new EmptySymbolException();
        }

        // Look in system then imports
        SymbolToken symTok = myImportsList.find(text);

        // Look in local symbols
        if (symTok == null)
        {
            Integer  sid;
            String[] names;
            synchronized (this)
            {
                sid = mySymbolsMap.get(text);
                names = mySymbolNames;
            }

            if (sid != null)
            {
                int offset = sid - myFirstLocalSid;
                String internedText = names[offset];
                assert internedText != null;
                symTok = new SymbolTokenImpl(internedText, sid);
            }
        }

        return symTok;
    }

    private static final void validateSymbol(String name)
    {
        if (name == null || name.length() < 1)
        {
            String message = "symbols must contain 1 or more characters";
            throw new IllegalArgumentException(message);
        }
        for (int i = 0; i < name.length(); ++i)
        {
            int c = name.charAt(i);
            if (c >= 0xD800 && c <= 0xDFFF)
            {
                if (c >= 0xDC00)
                {
                    String message = "unpaired trailing surrogate in symbol " +
                            "name at position " + i;
                    throw new IllegalArgumentException(message);
                }
                ++i;
                if (i == name.length())
                {
                    String message = "unmatched leading surrogate in symbol " +
                            "name at position " + i;
                    throw new IllegalArgumentException(message);
                }
                c = name.charAt(i);
                if (c < 0xDC00 || c > 0xDFFF)
                {
                    String message = "unmatched leading surrogate in symbol " +
                            "name at position " + i;
                    throw new IllegalArgumentException(message);
                }
            }
        }
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synch'd method.
     *
     * @param symbolName must be nonempty.
     */
    private int putSymbol(String symbolName)
    {
        assert symbolName.length() != 0;

        if (isReadOnly)
        {
            throw new ReadOnlyValueException(SymbolTable.class);
        }

        if (mySymbolsCount == mySymbolNames.length)
        {
            int newlen = mySymbolsCount * 2;
            if (newlen < DEFAULT_CAPACITY)
            {
                newlen = DEFAULT_CAPACITY;
            }
            String[] temp = new String[newlen];
            System.arraycopy(mySymbolNames, 0, temp, 0, mySymbolsCount);
            mySymbolNames = temp;
        }

        int sid = mySymbolsCount + myFirstLocalSid;
        assert sid == getMaxId() + 1;

        putToMapIfNotThere(mySymbolsMap, symbolName, sid);

        mySymbolNames[mySymbolsCount] = symbolName;
        mySymbolsCount++;

        if (myImage != null)
        {
            recordLocalSymbolInIonRep(myImage, symbolName, sid);
        }

        return sid;
    }

    private static void putToMapIfNotThere(Map<String, Integer> symbolsMap,
                                           String text,
                                           int sid)
    {
        // When there's a duplicate name, don't replace the lower sid.
        // This pattern avoids double-lookup in the normal happy case
        // and only requires a second lookup when there's a duplicate.
        Integer extantSid = symbolsMap.put(text, sid);
        if (extantSid != null)
        {
            // We always insert symbols with increasing sids
            assert extantSid < sid;
            symbolsMap.put(text, extantSid);
        }
    }

    public SymbolTable getSystemSymbolTable()
    {
        return myImportsList.getSystemSymbolTable();
    }

    public SymbolTable[] getImportedTables()
    {
        return myImportsList.getImportedTables();
    }

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
     * @see #getImportedTables()
     */
    SymbolTable[] getImportedTablesNoCopy()
    {
        return myImportsList.getImportedTablesNoCopy();
    }

    public void writeTo(IonWriter writer) throws IOException
    {
        IonReader reader = new SymbolTableReader(this);
        writer.writeValues(reader);
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

    /**
     * Only valid on local symtabs that already have an _image_factory set.
     *
     * @param imageFactory is used to check that the image uses the correct
     *   DOM implementation.
     *   It must be identical to the {@link #myImageFactory} and not null.
     * @return Not null.
     */
    IonStruct getIonRepresentation(ValueFactory imageFactory)
    {
        if (imageFactory == null)
        {
            throw new IonExceptionNoSystem("can't create representation without a system");
        }

        if (imageFactory != myImageFactory)
        {
            throw new IonException("wrong system");
        }

        IonStruct image;
        synchronized (this)
        {
            image = myImage;

            if (image == null)
            {
                // Start a new image from scratch
                myImage = image = makeIonRepresentation(myImageFactory);
            }
        }

        return image;
    }

    /**
     * NOT SYNCHRONIZED! Call only from a synch'd method.
     *
     * @return a new struct, not null.
     */
    private IonStruct makeIonRepresentation(ValueFactory factory)
    {
        IonStruct ionRep = factory.newEmptyStruct();

        ionRep.addTypeAnnotation(ION_SYMBOL_TABLE);

        SymbolTable[] importedTables = getImportedTablesNoCopy();

        if (importedTables.length > 1)
        {
            IonList importsList = factory.newEmptyList();
            for (int i = 1; i < importedTables.length; i++)
            {
                SymbolTable importedTable = importedTables[i];
                IonStruct importStruct = factory.newEmptyStruct();

                importStruct.add(NAME,
                                 factory.newString(importedTable.getName()));
                importStruct.add(VERSION,
                                 factory.newInt(importedTable.getVersion()));
                importStruct.add(MAX_ID,
                                 factory.newInt(importedTable.getMaxId()));

                importsList.add(importStruct);
            }
            ionRep.add(IMPORTS, importsList);
        }

        if (mySymbolsCount > 0)
        {
            int sid = myFirstLocalSid;
            for (int offset = 0; offset < mySymbolsCount; offset++, sid++)
            {
                String symbolName = mySymbolNames[offset];
                recordLocalSymbolInIonRep(ionRep, symbolName, sid);
            }
        }

        return ionRep;
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
     * @param symbolName can be null when there's a gap in the local symbols list.
     */
    private void recordLocalSymbolInIonRep(IonStruct ionRep,
                                           String symbolName,
                                           int sid)
    {
        assert sid >= myFirstLocalSid;

        ValueFactory sys = ionRep.getSystem();

        // TODO this is crazy inefficient and not as reliable as it looks
        // since it doesn't handle the case where's theres more than one list
        IonValue syms = ionRep.get(SYMBOLS);
        while (syms != null && syms.getType() != IonType.LIST)
        {
            ionRep.remove(syms);
            syms = ionRep.get(SYMBOLS);
        }
        if (syms == null)
        {
            syms = sys.newEmptyList();
            ionRep.put(SYMBOLS, syms);
        }

        int this_offset = sid - myFirstLocalSid;
        IonValue name = sys.newString(symbolName);
        ((IonList)syms).add(this_offset, name);
    }

    /**
     * Collects the necessary imports from the reader and catalog, and load
     * them into the passed-in {@code importsList}.
     */
    private static void prepImportsList(List<SymbolTable> importsList,
                                        IonReader reader,
                                        IonCatalog catalog)
    {
        assert IMPORTS.equals(reader.getFieldName());

        reader.stepIn();
        IonType t;
        while ((t = reader.next()) != null)
        {
            if (!reader.isNullValue() && t == IonType.STRUCT)
            {
                SymbolTable importedTable = readOneImport(reader, catalog);

                if (importedTable != null)
                {
                    importsList.add(importedTable);
                }
            }
        }
        reader.stepOut();
    }

    /**
     * Returns a {@link SymbolTable} representation of a single import
     * declaration from the passed-in reader and catalog.
     *
     * @return
     *          symbol table representation of the import; null if the import
     *          declaration is malformed
     */
    private static SymbolTable readOneImport(IonReader ionRep,
                                             IonCatalog catalog)
    {
        assert (ionRep.getType() == IonType.STRUCT);

        String name = null;
        int    version = -1;
        int    maxid = -1;

        ionRep.stepIn();
        IonType t;
        while ((t = ionRep.next()) != null)
        {
            if (ionRep.isNullValue()) continue;

            SymbolToken symTok = ionRep.getFieldNameSymbol();
            int field_id = symTok.getSid();
            if (field_id == UNKNOWN_SYMBOL_ID)
            {
                // this is a user defined reader or a pure DOM
                // we fall back to text here
                final String fieldName = ionRep.getFieldName();
                field_id = getSidForSymbolTableField(fieldName);
            }
            switch(field_id)
            {
                case NAME_SID:
                    if (t == IonType.STRING)
                    {
                        name = ionRep.stringValue();
                    }
                    break;
                case VERSION_SID:
                    if (t == IonType.INT)
                    {
                        version = ionRep.intValue();
                    }
                    break;
                case MAX_ID_SID:
                    if (t == IonType.INT)
                    {
                        maxid = ionRep.intValue();
                    }
                    break;
                default:
                    // we just ignore anything else as "open content"
                    break;
            }
        }
        ionRep.stepOut();

        // Ignore import clauses with malformed name field.
        if (name == null || name.length() == 0 || name.equals(ION))
        {
            return null;
        }

        if (version < 1)
        {
            version = 1;
        }

        SymbolTable itab = null;
        if (catalog != null)
        {
            itab = catalog.getTable(name, version);
        }
        if (maxid < 0)
        {
            if (itab == null || version != itab.getVersion())
            {
                String message =
                    "Import of shared table "
                        + IonTextUtils.printString(name)
                        + " lacks a valid max_id field, but an exact match was not"
                        + " found in the catalog";
                if (itab != null)
                {
                    message += " (found version " + itab.getVersion() + ")";
                }
                // TODO custom exception
                throw new IonException(message);
            }

            // Exact match is found, but max_id is undefined in import
            // declaration, set max_id to largest sid of shared symtab
            maxid = itab.getMaxId();
        }

        if (itab == null)
        {
            assert maxid >= 0;

            // Construct substitute table with max_id undefined symbols
            itab = new SubstituteSymbolTable(name, version, maxid);
        }
        else if (itab.getVersion() != version || itab.getMaxId() != maxid)
        {
            // A match was found BUT specs are not an exact match
            // Construct a substitute with correct specs, containing the
            // original import table that was found
            itab = new SubstituteSymbolTable(itab, version, maxid);
        }

        return itab;
    }

    /**
     * Generate the string representation of a symbol with an unknown id.
     * @param id must be a value greater than zero.
     * @return the symbol name, of the form <code>$NNN</code> where NNN is the
     * integer rendering of <code>id</code>
     */
    public static String unknownSymbolName(int id)
    {
        assert id > 0;
        return "$" + id;
    }

    @Override
    public String toString()
    {
        return "(LocalSymbolTable max_id:" + getMaxId() + ')';
    }

    static class IonExceptionNoSystem extends IonException
    {
        private static final long serialVersionUID = 1L;

        IonExceptionNoSystem(String msg)
        {
            super(msg);
        }
    }

    private static final class SymbolIterator
        implements Iterator<String>
    {
        private final String[] mySymbolNames;
        private final int      mySymbolsCount;
        private int            _idx = 0;

        SymbolIterator(String[] symbolNames, int count)
        {
            mySymbolNames = symbolNames;
            mySymbolsCount = count;
        }

        public boolean hasNext()
        {
            if (_idx < mySymbolsCount)
            {
                return true;
            }
            return false;
        }

        public String next()
        {
            if (_idx < mySymbolsCount)
            {
                return mySymbolNames[_idx++];
            }
            throw new NoSuchElementException();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * This method, and the context from which it is called, assumes that the
     * symtabs are not being mutated by another thread.
     * Therefore it doesn't use synchronization.
     */
    boolean symtabExtends(SymbolTable other)
    {
        // Throws ClassCastException if other isn't a local symtab
        LocalSymbolTable subset = (LocalSymbolTable) other;

        // Gather snapshots of each LST's data, so we don't

        // Superset must have same/more known symbols than subset.
        if (getMaxId() < subset.getMaxId()) return false;

        // TODO amznlabs/ion-java#18 Currently, we check imports by their refs. which
        //      might be overly strict; imports which are not the same ref.
        //      but have the same semantic states fails the extension check.
        if (! myImportsList.equalImports(subset.myImportsList))
            return false;

        int subLocalSymbolCount = subset.mySymbolsCount;

        // Superset extends subset if subset doesn't have any declared symbols.
        if (subLocalSymbolCount == 0) return true;

        // Superset must have same/more declared (local) symbols than subset.
        if (mySymbolsCount < subLocalSymbolCount) return false;

        String[] subsetSymbols = subset.mySymbolNames;

        // Before we go through the expensive iteration from the front,
        // check the last (largest) declared symbol in subset beforehand
        if (! safeEquals(mySymbolNames[subLocalSymbolCount- 1],
                                  subsetSymbols[subLocalSymbolCount- 1]))
        {
            return false;
        }

        // Now, we iterate from the first declared symbol, note that the
        // iteration below is O(n)!
        for (int i = 0; i < subLocalSymbolCount - 1; i++)
        {
            if (! safeEquals(mySymbolNames[i], subsetSymbols[i]))
                return false;
        }

        return true;
    }

}
