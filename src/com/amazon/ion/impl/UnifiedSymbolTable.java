// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.IonType.STRUCT;
import static com.amazon.ion.SystemSymbols.IMPORTS;
import static com.amazon.ion.SystemSymbols.IMPORTS_SID;
import static com.amazon.ion.SystemSymbols.ION;
import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.MAX_ID;
import static com.amazon.ion.SystemSymbols.MAX_ID_SID;
import static com.amazon.ion.SystemSymbols.NAME;
import static com.amazon.ion.SystemSymbols.NAME_SID;
import static com.amazon.ion.SystemSymbols.SYMBOLS;
import static com.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static com.amazon.ion.SystemSymbols.VERSION;
import static com.amazon.ion.SystemSymbols.VERSION_SID;
import static com.amazon.ion.util.IonTextUtils.printQuotedSymbol;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.util.IonTextUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A local symbol table.
 * <p>
 * symbol "inheritance" is":
 *  first load system symbol table
 *   then imported tables in order offset by prev _max_id
 *        and we remember their "max id" so they get a range
 *        and skip any names already present
 *   then local names from the new max id
 *
 */

final class UnifiedSymbolTable
    implements SymbolTable
{
    /** Our imports, never null. */
    private final UnifiedSymbolTableImports _import_list;

    private boolean                   _is_read_only;

    /**
     * Memoized result of {@link #getIonRepresentation(ValueFactory)};
     * only valid for local symtabs.
     * Once this is created, we maintain it as symbols are added.
     */
    private IonStruct                 _image;

    /**
     * The factory used to build the {@link #_image} of a local symtab.
     * It's used by the datagram level to maintain the tree representation.
     * It cannot be changed since local symtabs can't be moved between trees.
     * This is null for shared symbol tables, including system symtabs.
     */
    private final ValueFactory _image_factory;

    /**
     * The local symbol names declared in this symtab; never null.
     * The sid of the first element is {@link #_first_local_sid}.
     * Only the first {@link #_local_symbol_count} elements are valid.
     */
    private String[] _symbols;

    /** The initial length of {@link #_symbols}. */
    private static final int DEFAULT_CAPACITY = 16;

    /**
     * This is the number of symbols defined in this symbol table
     * locally, that is not imported from some other table.
     */
    private int _local_symbol_count;

    /**
     * The sid of the first local symbol, which is stored at
     * {@link #_symbols}[0].
     */
    private int _first_local_sid = 1;

    /**
     * Map from symbol name to SID of local symbols that are not in imports.
     */
    private final Map<String, Integer> _id_map;


    //========================================================================


    private UnifiedSymbolTable(ValueFactory imageFactory,
                               UnifiedSymbolTableImports imports,
                               Map<String, Integer> idMap)
    {
        _image_factory = imageFactory;
        _import_list = imports;
        _id_map = idMap;
        _symbols = _Private_Utils.EMPTY_STRING_ARRAY;
    }


    /**
     * Constructs an empty local symbol table.
     *
     * @param systemSymbolTable must be a system symbol table, not null.
     */
    private UnifiedSymbolTable(ValueFactory imageFactory,
                               SymbolTable systemSymbolTable)
    {
        this(imageFactory,
             new UnifiedSymbolTableImports(systemSymbolTable),
             new HashMap<String, Integer>(DEFAULT_CAPACITY));

        if (!systemSymbolTable.isSystemTable()) {
            throw new IllegalArgumentException();
        }

        _first_local_sid = _import_list.getMaxId() + 1;
    }


    //========================================================================

    /*
     * load helpers that fill in the contents of the symbol table
     * from various sources
     */

    /**
     * @param reader must not be null.
     * @param catalog may be null.
     */
    private void loadLocalSymbolTable(IonReader reader, IonCatalog catalog,
                                      boolean isOnStruct)
    {
        if (!isOnStruct) {
            IonType t = reader.next();
            if (t != IonType.STRUCT) {
                String message = "invalid symbol table image passed in reader "
                               + t
                               + " encountered when a struct was expected";
                throw new IonException(message);
            }
        }

        reader.stepIn();
        readIonRep(reader, catalog);
        reader.stepOut();
    }


    /*
     * public methods for constructing symbol tables most tailored to assure
     * the SymbolTable is properly initialized.
     *
     */

    /**
     * Helper method for APIs that take {@code SymbolTable...} parameters;
     * returns a minimal symtab, either system or local depending an the
     * given values. If the imports are empty, the default system symtab is
     * returned.
     *
     * @param imageFactory The factory to use when building a DOM image.
     *  May be null.
     * @param defaultSystemSymtab is used if a system symtab isn't the first
     *  import.
     * @param imports the set of shared symbol tables to import.
     * The first (and only the first) may be a system table, in which case the
     * default is ignored. May be null or empty.
     *
     * @return not null.
     */
    static SymbolTable initialSymbolTable(ValueFactory imageFactory,
                                          SymbolTable defaultSystemSymtab,
                                          SymbolTable... imports)
    {
        if (imports == null || imports.length == 0)
        {
            return defaultSystemSymtab;
        }

        if (imports.length == 1 && imports[0].isSystemTable())
        {
            return imports[0];
        }

        return makeNewLocalSymbolTable(imageFactory,
                                       defaultSystemSymtab,
                                       imports);
    }


    /**
     * Creates a new local symtab with given imports.
     *
     * @param imageFactory The factory to use when building a DOM image.
     *  May be null.
     * @param systemSymbolTable the default system symtab, if one doesn't
     * exist in the imports. Must not be null.
     * @param imports the set of shared symbol tables to import.
     * The first (and only the first) may be a system table, in which case the
     * default is ignored.
     *
     * @throws IllegalArgumentException if any import is a local table,
     * or if any but the first is a system table.
     * @throws NullPointerException if any import is null.
     */
    static UnifiedSymbolTable
    makeNewLocalSymbolTable(ValueFactory imageFactory,
                            SymbolTable systemSymbolTable,
                            SymbolTable... imports)
    {
        UnifiedSymbolTable table =
            new UnifiedSymbolTable(imageFactory, systemSymbolTable);

        if (imports != null && imports.length > 0 && imports[0].isSystemTable()) {
            systemSymbolTable = imports[0];

            if (imports.length != 0)
            {
                SymbolTable[] others = new SymbolTable[imports.length - 1];
                System.arraycopy(imports, 1, others, 0, imports.length - 1);
                imports = others;
            }
            else
            {
                imports = null;
            }
        }

        if (imports != null) {
            for (SymbolTable symbolTable : imports) {
                table._import_list.addImport(symbolTable);
            }
            table._first_local_sid = table._import_list.getMaxId() + 1;
        }

        return table;
    }


    /**
     * @param systemSymbolTable must not be null.
     * @param catalog may be null.
     */
    static UnifiedSymbolTable
    makeNewLocalSymbolTable(SymbolTable systemSymbolTable, IonCatalog catalog,
                            IonStruct ionRep)
    {
        ValueFactory sys = ionRep.getSystem();
        IonReader reader = new IonReaderTreeSystem(ionRep);
        UnifiedSymbolTable table =
            makeNewLocalSymbolTable(sys, systemSymbolTable, catalog, reader,
                                    false);

        table.set_image(ionRep);

        return table;
    }


    /**
     * @param systemSymbolTable must not be null.
     * @param catalog may be null.
     */
    static UnifiedSymbolTable
    makeNewLocalSymbolTable(ValueFactory imageFactory,
                            SymbolTable systemSymbolTable,
                            IonCatalog catalog, IonReader reader,
                            boolean alreadyInStruct)
    {
        UnifiedSymbolTable table =
            new UnifiedSymbolTable(imageFactory, systemSymbolTable);
        table.loadLocalSymbolTable(reader, catalog, alreadyInStruct);
        return table;
    }

    public boolean isLocalTable() {
        return true;
    }

    public boolean isSharedTable() {
        return false;
    }

    public boolean isSystemTable() {
        return false;
    }

    public boolean isSubstitute()
    {
        return false;
    }

    /**
     * checks the _is_read_only flag and if the flag is set
     * this throws an error.  This is used by the various
     * methods that may modify a value.
     */
    private void verify_not_read_only() {
        if (_is_read_only) {
            throw new ReadOnlyValueException(SymbolTable.class);
        }
    }

    public boolean isReadOnly() {
        return _is_read_only;
    }

    public synchronized void makeReadOnly() {
        if (! _is_read_only) {
            _import_list.makeReadOnly(); // TODO should always be read-only
            _is_read_only = true;
        }
    }


    private void set_image(IonStruct ionRep)
    {
        verify_not_read_only();
        assert ionRep.getSystem() == _image_factory;
        _image = ionRep;
    }


    public synchronized int getImportedMaxId()
    {
        return _import_list.getMaxId();
    }

    public synchronized int getMaxId()
    {
        int maxid = _local_symbol_count + _import_list.getMaxId();
        return maxid;
    }

    public int getVersion()
    {
        return 1;
    }

    public String getName()
    {
        return null;
    }

    public String getIonVersionId()
    {
        SymbolTable system_table = _import_list.getSystemSymbolTable();
        int id = system_table.getVersion();
        if (id != 1) {
            throw new IonException("unrecognized system version encountered: "+id);
        }
        return ION_1_0;
    }


    public synchronized Iterator<String> iterateDeclaredSymbolNames()
    {
        return new SymbolIterator();
    }


    public synchronized String findKnownSymbol(int id)
    {
        String name = null;

        if (id < 1) {
            throw new IllegalArgumentException("symbol IDs are greater than 0");
        }
        else if (id < _first_local_sid) {
            name = _import_list.findKnownSymbol(id);
        }
        else  {
            int offset = id - _first_local_sid;
            if (offset < _symbols.length) {
                name = _symbols[offset];
            }
        }

        return name;
    }

    public synchronized int findSymbol(String name)
    {
        // not name == null test, we let Java throw a NullPointerException
        if (name.length() < 1) {
            throw new EmptySymbolException();
        }

        int sid = findLocalSymbol(name);

        if (sid == UNKNOWN_SYMBOL_ID) {
            sid = _import_list.findSymbol(name);
        }
        return sid;
    }

    int findLocalSymbol(String name)
    {
        assert(name.length() > 0);

        Integer isid = _id_map.get(name);
        if (isid != null) {
            assert isid != UNKNOWN_SYMBOL_ID;
            return isid;
        }
        return UNKNOWN_SYMBOL_ID;
    }


    public SymbolToken intern(String text)
    {
        SymbolToken is = find(text);
        if (is == null)
        {
            validateSymbol(text);
            int sid = getMaxId() + 1;
            putSymbol(text, sid);
            is = new SymbolTokenImpl(text, sid);
        }
        return is;
    }


    public SymbolToken find(String text)
    {
        if (text.length() < 1) {
            throw new EmptySymbolException();
        }

        int sid = findLocalSymbol(text);
        if (sid != UNKNOWN_SYMBOL_ID)
        {
            int offset = sid - _first_local_sid;
            String internedText = _symbols[offset];
            assert internedText != null;
            return new SymbolTokenImpl(internedText, sid);
        }

        return _import_list.find(text);
    }

    private static final void validateSymbol(String name)
    {
        // not synchronized since this is local to the string which is immutable
        if (name == null || name.length() < 1) {
            throw new IllegalArgumentException("symbols must contain 1 or more characters");
        }
        for (int i = 0; i < name.length(); ++i) {
            int c = name.charAt(i);
            if (c >= 0xD800 && c <= 0xDFFF) {
                if (c >= 0xDC00) {
                    throw new IllegalArgumentException("unpaired trailing surrogate in symbol name at position " + i);
                }
                ++i;
                if (i == name.length()) {
                    throw new IllegalArgumentException("unmatched leading surrogate in symbol name at position " + i);
                }
                c = name.charAt(i);
                if (c < 0xDC00 || c > 0xDFFF) {
                    throw new IllegalArgumentException("unmatched leading surrogate in symbol name at position " + i);
                }
            }
        }
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synch'd method.
     *
     * TODO streamline: we no longer accept arbitrary sid, its always > max_id
     *
     * @param symbolName may be null, indicating a gap in the symbol table.
     */
    private void putSymbol(String symbolName, int sid)
    {
        verify_not_read_only();

        assert _symbols != null;

        int idx = sid - _first_local_sid;
        assert idx >= 0;

        if (idx >= _local_symbol_count) {
            int oldlen = _local_symbol_count;
            if (oldlen >= _symbols.length) {
                int newlen = oldlen * 2;
                if (newlen < DEFAULT_CAPACITY) {
                    newlen = DEFAULT_CAPACITY;
                }
                while (newlen < idx) {
                    newlen *= 2;
                }
                String[] temp = new String[newlen];
                if (oldlen > 0) {
                    System.arraycopy(_symbols, 0, temp, 0, oldlen);
                }
                _symbols = temp;
            }
        }
        else if (_symbols[idx] != null) {
            String message =
                "Cannot redefine $" + sid + " from "
                + printQuotedSymbol(_symbols[idx])
                + " to " + printQuotedSymbol(symbolName);
            throw new IonException(message);
        }

        // Don't add to _id_map if the text is covered by an import
        if (symbolName != null && _import_list.findSymbol(symbolName) < 0)
        {
            putToIdMapIfNotThere(symbolName, sid);
        }

        _symbols[idx] = symbolName;

        if (idx >= _local_symbol_count) {
            _local_symbol_count = idx + 1;
        }

        if (_image != null) {
            assert _local_symbol_count > 0;
            recordLocalSymbolInIonRep(_image, symbolName, sid);
        }
    }

    private void putToIdMapIfNotThere(String text, int sid)
    {
        // When there's a duplicate name, don't replace the lower sid.
        // This pattern avoids double-lookup in the normal happy case
        // and only requires a second lookup when there's a duplicate.
        Integer extantSid = _id_map.put(text, sid);
        if (extantSid != null)
        {
            // We always insert symbols with increasing sids
            assert extantSid < sid;
            _id_map.put(text, extantSid);
        }
    }

    public SymbolTable getSystemSymbolTable()
    {
        // Not synchronized since this member never changes after construction.
        return _import_list.getSystemSymbolTable();
    }

    // TODO add to interface to let caller avoid getImports which makes a copy

    public SymbolTable[] getImportedTables()
    {
        // this does not need to be synchronized since the import
        // list is fixed at construction time.  Once a caller has
        // a symtab the import list is immutable
        // was: synchronized (this) {

        int count = _import_list.getImportCount();
        SymbolTable[] imports = new SymbolTable[count];
        if (count > 0) {
            _import_list.getImports(imports, count);
        }
        return imports;

        //}
    }

    /**
     * Synchronized to ensure that this symtab isn't changed while being
     * written.
     */
    public synchronized void writeTo(IonWriter writer) throws IOException
    {
        IonReader reader = new UnifiedSymbolTableReader(this);
        writer.writeValues(reader);
    }


    //
    // TODO: there needs to be a better way to associate a System with
    //       the symbol table, which is required if someone is to be
    //       able to generate an instance.  The other way to resolve
    //       this dependency would be for the IonSystem object to be
    //       able to take a UnifiedSymbolTable and synthesize an Ion
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
     *   It must be identical to the {@link #_image_factory} and not null.
     * @return Not null.
     */
    synchronized IonStruct getIonRepresentation(ValueFactory imageFactory)
    {
        if (imageFactory == null) {
            throw new IonExceptionNoSystem("can't create representation without a system");
        }

        if (imageFactory != _image_factory) {
            throw new IonException("wrong system");
        }

        if (_image == null)
        {
            // Start a new image from scratch
            _image = makeIonRepresentation(_image_factory);
        }

        return _image;
    }


    IonReader getReader()
    {
        IonReader reader = new UnifiedSymbolTableReader(this);
        return reader;
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

        SymbolTable[] imports = this.getImportedTables();

        if (imports.length > 0) {
            IonList imports_as_ion = factory.newEmptyList();
            for (int ii=0; ii<imports.length; ii++) {
                SymbolTable imptable = imports[ii];
                IonStruct imp = factory.newEmptyStruct();
                imp.add(NAME, factory.newString(imptable.getName()));
                imp.add(VERSION, factory.newInt(imptable.getVersion()));
                int max_id = imptable.getMaxId();
                imp.add(MAX_ID, factory.newInt(max_id));
                imports_as_ion.add(imp);
            }
            ionRep.add(IMPORTS, imports_as_ion);
        }

        if (_local_symbol_count > 0)
        {
            replaceSymbols(ionRep);
        }

        return ionRep;
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
     */
    private void replaceSymbols(IonStruct ionRep)
    {
        assert ionRep != null;

        int sid = _first_local_sid;
        for (int offset = 0; offset < _local_symbol_count; offset++, sid++) {
            String symbolName = _symbols[offset];
            recordLocalSymbolInIonRep(ionRep, symbolName, sid);
        }
    }


    /**
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
     * @param symbolName can be null when there's a gap in the local symbols list.
     */
    private void recordLocalSymbolInIonRep(IonStruct ionRep,
                                           String symbolName, int sid)
    {
        assert sid >= _first_local_sid;

        ValueFactory sys = ionRep.getSystem();

        // TODO this is crazy inefficient and not as reliable as it looks
        // since it doesn't handle the case where's theres more than one list
        IonValue syms = ionRep.get(SYMBOLS);
        while (syms != null && syms.getType() != IonType.LIST) {
            ionRep.remove(syms);
            syms = ionRep.get(SYMBOLS);
        }
        if (syms == null) {
            syms = sys.newEmptyList();
            ionRep.put(SYMBOLS, syms);
        }

        int this_offset = sid - _first_local_sid;
        IonValue name = sys.newString(symbolName);
        ((IonList)syms).add(this_offset, name);
    }

    /**
     * Read a symtab, assuming we're inside the struct already.
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
     *
     * @param catalog may be null.
     */
    private void readIonRep(IonReader reader, IonCatalog catalog)
    {
        if (!reader.isInStruct()) {
            throw new IllegalArgumentException("symbol tables must be contained in structs");
        }

        ArrayList<String> symbols = null;

        IonType fieldType;
        while ((fieldType = reader.next()) != null)
        {
            if (reader.isNullValue()) continue;

            int fieldId = reader.getFieldId();
            if (fieldId < 0) {
                // this is a user defined reader or a pure DOM
                // we fall back to text here
                final String fieldName = reader.getFieldName();
                fieldId = get_symbol_sid_helper(fieldName);
            }

            switch (fieldId) {
            case SYMBOLS_SID:
                if (fieldType == IonType.LIST) {
                    // Other types treated as empty-list
                    symbols = new ArrayList<String>();

                    reader.stepIn();
                    IonType type;
                    while ((type = reader.next()) != null) {
                        String text = null;
                        if (type == IonType.STRING && !reader.isNullValue()) {
                            text = reader.stringValue();
                            if (text.length() == 0) text = null;
                        }

                        symbols.add(text);
                    }
                    reader.stepOut();
                }
                break;
            case IMPORTS_SID:
                if (fieldType == IonType.LIST) {
                    readImportList(reader, catalog);
                }
                break;
            default:
                break;
            }
        }

        // Imports have already been processed above.
        if (symbols != null)
        {
            int sid = _first_local_sid;
            for (String symbolText : symbols) {
                putSymbol(symbolText, sid);
                sid++;
            }
        }
    }

    static final int get_symbol_sid_helper(String fieldName)
    {
        final int shortestFieldNameLength = 4; // 'name'

        if (fieldName != null && fieldName.length() >= shortestFieldNameLength)
        {
            int c = fieldName.charAt(0);
            switch (c) {
            case 'v':
                if (VERSION.equals(fieldName)) {
                    return VERSION_SID;
                }
                break;
            case 'n':
                if (NAME.equals(fieldName)) {
                    return NAME_SID;
                }
                break;
            case 's':
                if (SYMBOLS.equals(fieldName)) {
                    return  SYMBOLS_SID;
                }
                break;

            case 'i':
                if (IMPORTS.equals(fieldName)) {
                    return IMPORTS_SID;
                }
                break;
            case 'm':
                if (MAX_ID.equals(fieldName)) {
                    return MAX_ID_SID;
                }
                break;
            default:
                break;
            }
        }
        return UNKNOWN_SYMBOL_ID;
    }


    /**
     * NOT SYNCHRONIZED! Call within constructor or from synch'd method.
     *
     * @param catalog may be null.
     */
    private void readImportList(IonReader reader, IonCatalog catalog)
    {
        assert (reader.getFieldId() == IMPORTS_SID || IMPORTS.equals(reader.getFieldName()));
        assert (reader.getType() == IonType.LIST);

        reader.stepIn();
        IonType t;
        while ((t = reader.next()) != null) {
            if (t == STRUCT) {
                readOneImport(reader, catalog);
            }
        }
        reader.stepOut();
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synch'd method.
     *
     * @param catalog may be null.
     */
    private void readOneImport(IonReader ionRep, IonCatalog catalog)
    {
        assert (ionRep.getType() == IonType.STRUCT);

        if (_local_symbol_count > 0) {
            throw new IllegalStateException("importing tables is not valid once user symbols have been added");
        }

        String name = null;
        int    version = -1;
        int    maxid = -1;

        ionRep.stepIn();
        IonType t;
        while ((t = ionRep.next()) != null)
        {
            if (ionRep.isNullValue()) continue;

            int field_id = ionRep.getFieldId();
            if (field_id == -1) {
                // this is a user defined reader or a pure DOM
                // we fall back to text here
                final String fieldName = ionRep.getFieldName();
                field_id = get_symbol_sid_helper(fieldName);
            }
            switch(field_id) {
                case NAME_SID:
                    if (t == IonType.STRING) {
                        name = ionRep.stringValue();
                    }
                    break;
                case VERSION_SID:
                    if (t == IonType.INT) {
                        version = ionRep.intValue();
                    }
                    break;
                case MAX_ID_SID:
                    if (t == IonType.INT) {
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
        if (name == null || name.length() == 0 || name.equals(ION)) {
            return;
        }

        if (version < 1) {
            version = 1;
        }

        SymbolTable itab = null;
        if (catalog != null) {
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
                if (itab != null) {
                    message += " (found version " + itab.getVersion() + ")";
                }
                // TODO custom exception
                throw new IonException(message);
            }

            // Exact match is found, but max_id is undefined in import
            // declaration, set max_id to largest sid of shared symtab
            maxid = itab.getMaxId();
        }

        if (itab == null) {
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

        _import_list.addImport(itab);
        _first_local_sid = _import_list.getMaxId() + 1;
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
        StringBuilder buf = new StringBuilder("(UnifiedSymbolTable ");
        buf.append("local");
        buf.append(" max_id::"+this.getMaxId());
        buf.append(')');
        return buf.toString();
    }

    static class IonExceptionNoSystem extends IonException
    {
        private static final long serialVersionUID = 1L;

        IonExceptionNoSystem(String msg)
        {
            super(msg);
        }

    }

    Iterator<SymbolTable> getImportIterator()
    {
        return _import_list.getImportIterator();
    }


    private final class SymbolIterator implements Iterator<String>
    {
        int _idx = 0;

        public boolean hasNext()
        {
            if (_idx < _local_symbol_count) {
                return true;
            }
            return false;
        }

        public String next()
        {
            // TODO bad failure mode if next() called beyond end
            if (_idx < _local_symbol_count) {
                String name = (_idx < _symbols.length) ? _symbols[_idx] : null;
                _idx++;
                return name;
            }
            return null;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

}
