// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.IonType.STRUCT;
import static com.amazon.ion.SystemSymbols.IMPORTS;
import static com.amazon.ion.SystemSymbols.IMPORTS_SID;
import static com.amazon.ion.SystemSymbols.ION;
import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE;
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
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.util.IonTextUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Common implementation of {@link SymbolTable}, supporting system, shared,
 * and local symtabs.
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
    private enum SymbolTableType {
        SYSTEM,
        LOCAL,
        SHARED,
        INVALID
    }


    /**
     * The set of system symbols as defined by Ion 1.0.
     * We keep this private to (help) prevent it from being modified.
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

    static final SymbolTable ION_1_0_SYMTAB;
    static {
        UnifiedSymbolTable table = new UnifiedSymbolTable(null);
        table.loadSharedSymbolTableContents(ION, 1, SYSTEM_SYMBOLS);
        ION_1_0_SYMTAB = table;
    }


    /** The name of shared (or system) table, null if local, "$ion" if system. */
    private String _name;

    /** The version of the shared table. */
    private int _version;

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
    private static final int DEFAULT_CAPACITY = 10;

    /**
     * This is the number of symbols defined in this symbol table
     * locally, that is not imported from some other table.
     */
    private int _local_symbol_count;

    /**
     * The sid of the first local symbol, which is stored at
     * {@link #_symbols}[0].
     *
     * @see #convertSidToLocalOffset
     * @see #convertLocalOffsetToSid
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
     * Constructs a shared symtab with no imports.
     * Caller must also invoke {@link #share(String, int)}.
     * @param imageFactory
     */
    private UnifiedSymbolTable(ValueFactory imageFactory)
    {
        this(imageFactory, UnifiedSymbolTableImports.emptyImportList,
             new HashMap<String, Integer>(DEFAULT_CAPACITY));
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
     * @param symbols will be retained by this symtab (but not modified).
     * It must not contain any empty strings.
     */
    private void loadSharedSymbolTableContents(String name,
                                               int version,
                                               String[] symbols)
    {
        assert symbols != null;

        _symbols = symbols;
        _local_symbol_count = symbols.length;
        for (int ii=0, sid= 1; ii < _local_symbol_count; ii++, sid++)
        {
            String symName = symbols[ii];
            assert symName == null || symName.length() > 0;

            if (symName != null)
            {
                putToIdMapIfNotThere(symName, sid);
            }
        }

        share(name, version);
    }

    private void loadSharedSymbolTableContents(String name, int version,
                                               SymbolTable priorSymtab,
                                               Iterator<String> symbols)
    {
        assert symbols != null;
        assert version == (priorSymtab == null
                            ? 1
                            : priorSymtab.getVersion() + 1);
        assert _first_local_sid == 1;

        int sid = 1;

        if (priorSymtab != null)
        {
            Iterator<String> priorSymbols =
                priorSymtab.iterateDeclaredSymbolNames();
            while (priorSymbols.hasNext())
            {
                String symName = priorSymbols.next();
                if (symName != null)
                {
                    assert symName.length() > 0;
                    putSymbol(symName, sid);
                }

                // Null entries must leave a gap in the sid sequence
                // so we retain compatability with the prior version.
                sid++;
            }
        }

        while (symbols.hasNext()) {
            String symName = symbols.next();
            // FIXME what about empty?  ION-189
            if (findSymbol(symName) == UNKNOWN_SYMBOL_ID) {
                putSymbol(symName, sid);
                sid++;
            }
        }

        share(name, version);
    }

    private void loadSharedSymbolTableContents(IonReader reader,
                                               boolean isOnStruct)
    {
        if (!isOnStruct) {
            IonType t = reader.next();
            if (t != IonType.STRUCT) {
                throw new IonException("invalid symbol table image passed in reader "+t+" encountered when a struct was expected");
            }
       }

        reader.stepIn();
        readIonRep(SymbolTableType.SHARED, reader, null);
        reader.stepOut();

        assert _name != null;
    }

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
        readIonRep(SymbolTableType.LOCAL, reader, catalog);
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


    //
    //  system symbol table constructor
    //      we get a version just to remind users there will
    //      be choices for this later
    //
    static SymbolTable systemSymbolTable(int version)
    {
        if (version != 1) {
            throw new IllegalArgumentException("only version 1 system symbols are supported currently");
        }

        return ION_1_0_SYMTAB;
    }

    //
    //   shared symbol table factories
    //       they don't take a catalog since they don't use a catalog
    //       the local symbol table uses the catalog for import resolution
    //

    /**
     * As per {@link IonSystem#newSharedSymbolTable(String, int, Iterator, SymbolTable...)},
     * any duplicate or null symbol texts are skipped.
     * Therefore, <b>THIS METHOD IS NOT SUITABLE WHEN READING SERIALIZED
     * SHARED SYMBOL TABLES</b> since that scenario must preserve all sids.
     *
     * @param priorSymtab may be null.
     */
    static UnifiedSymbolTable
    makeNewSharedSymbolTable(String name, int version,
                             SymbolTable priorSymtab,
                             Iterator<String> symbols)
    {
        if (name == null || name.length() < 1) {
            throw new IllegalArgumentException("invalid name for shared symbol table");
        }
        if (version < 1) {
            throw new IllegalArgumentException("invalid version for shared symbol table");
        }
        UnifiedSymbolTable table = new UnifiedSymbolTable(null);
        table.loadSharedSymbolTableContents(name, version, priorSymtab, symbols);
        return table;
    }

    static UnifiedSymbolTable
    makeNewSharedSymbolTable(IonStruct ionRep)
    {
        IonReader reader = new IonReaderTreeSystem(ionRep);
        return makeNewSharedSymbolTable(reader, false);
    }

    static UnifiedSymbolTable
    makeNewSharedSymbolTable(IonReader reader, boolean alreadyInStruct)
    {
        UnifiedSymbolTable table = new UnifiedSymbolTable(null);
        table.loadSharedSymbolTableContents(reader, alreadyInStruct);
        return table;
    }


    /**
     * Creates a new local symtab with no imports.
     *
     * @param systemSymbolTable the system symtab. Must not be null.
     */
    static UnifiedSymbolTable
    makeNewLocalSymbolTable(ValueFactory imageFactory,
                            SymbolTable systemSymbolTable)
    {
        UnifiedSymbolTable table =
            new UnifiedSymbolTable(imageFactory, systemSymbolTable);
        return table;
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

    /**
     * This method is not synchronized because it's only called during
     * construction.
     * Also calls {@link #makeReadOnly()}.
     */
    private void share(String name, int version)
    {
        verify_not_read_only();

        if (name == null || name.length() == 0) {
            throw new IllegalArgumentException("name must be non-empty");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be at least 1");
        }
        if (_name != null) {
            throw new IllegalStateException("already shared");
        }

        _name = name;
        _version = version;
        assert _import_list.getMaxId() == 0;
        makeReadOnly();
    }


    public boolean isLocalTable() {
        // Not synchronized since this member never changes after construction.
        return _name == null;
    }

    /**
     * {@inheritDoc}
     * Not synchronized since this member never changes after construction.
     */
    public boolean isSharedTable() {
        return _name != null;
    }

    public boolean isSystemTable() {
        // Not synchronized since these members never change after construction.
        // the is locked test is a short cut since most tables are local and
        // locked, therefore the bool gets us out of here in a hurry
        return (_name != null && ION.equals(_name));
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation always returns false.
     */
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


    public
    synchronized
    int getImportedMaxId()
    {
        return _import_list.getMaxId();
    }

    public
    synchronized
    int getMaxId()
    {
        int maxid = _local_symbol_count + _import_list.getMaxId();
        return maxid;
    }

    public int getVersion()
    {
        // Not synchronized since this member never changes after construction.
        return _version;
    }

    public String getName()
    {
        // Not synchronized since this member never changes after construction.
        return _name;
    }

    public String getIonVersionId()
    {
        SymbolTable system_table = this;
        if (!system_table.isSystemTable()) {
            if (isSharedTable()) {
                // As per spec, shared tables aren't tied to an Ion version.
                return null;
            }
            // else this is a local table, which always has a system symtab
            system_table = _import_list.getSystemSymbolTable();
            assert system_table != null;
        }
        int id = system_table.getVersion();
        if (id != 1) {
            throw new IonException("unrecognized system version encountered: "+id);
        }
        return ION_1_0;
    }


    public
    synchronized
    Iterator<String> iterateDeclaredSymbolNames()
    {
        return new SymbolIterator();
    }


    public
    synchronized
    String findKnownSymbol(int id)
    {
        String name = null;

        if (id < 1) {
            throw new IllegalArgumentException("symbol IDs are greater than 0");
        }
        else if (id < _first_local_sid) {
            name = _import_list.findKnownSymbol(id);
        }
        else  {
            int offset = convertSidToLocalOffset(id);
            if (offset < _symbols.length) {
                name = _symbols[offset];
            }
        }

        return name;
    }

    public
    synchronized
    int findSymbol(String name)
    {
        // not name == null test, we let Java throw a NullPointerException
        if (name.length() < 1) {
            throw new EmptySymbolException();
        }

        int sid = findLocalSymbol(name);

        // TODO why magic for system tables?
        if (sid == UNKNOWN_SYMBOL_ID && isSystemTable() == false) {
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
            int offset = convertSidToLocalOffset(sid);
            String internedText = _symbols[offset];
            assert internedText != null;
            return new SymbolTokenImpl(internedText, sid);
        }

        // Don't search imports on shared symtabs, they are "open content"
        // TODO clarify whether _import_list is even filled in that case.
        if (isSharedTable()) return null;

        return _import_list.find(text);
    }

    @Deprecated
    public synchronized String findSymbol(int id)
    {
        String name = findKnownSymbol(id);
        if (name == null) {
            throw new UnknownSymbolException(id);
        }
        return name;
    }

    public synchronized int addSymbol(String name)
    {
        int sid = this.findSymbol(name);
        if (sid == UNKNOWN_SYMBOL_ID) {
            validateSymbol(name);
            sid = getMaxId() + 1;
            putSymbol(name, sid);
        }
        return sid;
    }

    private static final void validateSymbol(String name)
    {
        // not synchronized since this is local to the string which is immutable
        if (name == null || name.length() < 1) {
            throw new IllegalArgumentException("symbols must contain 1 or more characters");
        }
        boolean pending_high_surrogate = false;
        int ii=0;
        for (; ii<name.length(); ii++) {
            char c = name.charAt(ii);
            if (c >= 0xD800 && c <= 0xDFFF) {
                if (_Private_IonConstants.isHighSurrogate(c)) {
                    if (pending_high_surrogate) {
                        break; // our check at the end will throw for us
                    }
                    pending_high_surrogate = true;
                }
                else if (_Private_IonConstants.isLowSurrogate(c)) {
                    if (!pending_high_surrogate) {
                        throw new IllegalArgumentException("unparied low surrogate in symbol name at position "+ii);
                    }
                    pending_high_surrogate = false;
                }
            }
            if (c > 0x10FFFF) {
                throw new IllegalArgumentException("illegal unicode codepoint " + (int)c);
            }
        }
        if (pending_high_surrogate) {
            throw new IllegalArgumentException("unmatched high surrogate in symbol name at position "+ii);
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

        assert _name == null && _symbols != null;

        int idx = convertSidToLocalOffset(sid);
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
        if (isSystemTable()) return this;

        // Not synchronized since this member never changes after construction.
        return _import_list.getSystemSymbolTable();
    }

    // TODO add to interface to let caller avoid getImports which makes a copy

    /**
     * this checks the import list for symbol tabls other than the
     * system symbol table, which is doesn't count.  If any non system
     * imports exist it return true.  Otherwise it return false.
     */
    public boolean hasImports()
    {
        // Not synchronized since this member never changes after construction.
        return _import_list.getImportCount() > 0;
    }


    public SymbolTable[] getImportedTables()
    {
        if (isSharedTable()) return null;

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
    synchronized
    IonStruct getIonRepresentation(ValueFactory imageFactory)
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

        if (this.isSharedTable()) {
            assert getVersion() > 0;
            ionRep.addTypeAnnotation(ION_SHARED_SYMBOL_TABLE);
            ionRep.add(NAME, factory.newString(this.getName()));
            ionRep.add(VERSION, factory.newInt(this.getVersion()));
        }
        else {
            ionRep.addTypeAnnotation(ION_SYMBOL_TABLE);
        }

        SymbolTable[] imports = this.getImportedTables();
        assert (this.isLocalTable() || imports == null);

        if (imports != null && imports.length > 0) {
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

        int this_offset = convertSidToLocalOffset(sid);
        IonValue name = sys.newString(symbolName);
        ((IonList)syms).add(this_offset, name);
    }

    private final int convertSidToLocalOffset(int sid) {
        int offset = sid - _first_local_sid;
        return offset;
    }
    private final int convertLocalOffsetToSid(int local_offset) {
        int sid = local_offset + _first_local_sid;
        return sid;
    }

    /**
     * Read a symtab, assuming we're inside the struct already.
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
     *
     * @param catalog may be null.
     */
    private void readIonRep(SymbolTableType symtabType,
                            IonReader reader,
                            IonCatalog catalog)
    {
        if (!reader.isInStruct()) {
            throw new IllegalArgumentException("symbol tables must be contained in structs");
        }

        String name = null;
        int version = -1;

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
            case VERSION_SID:
                if (symtabType == SymbolTableType.SHARED && fieldType == IonType.INT) {
                    version = reader.intValue();
                }
                break;
            case NAME_SID:
                if (symtabType == SymbolTableType.SHARED && fieldType == IonType.STRING) {
                    name = reader.stringValue();
                }
                break;
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
                if (symtabType == SymbolTableType.LOCAL && fieldType == IonType.LIST) {
                    readImportList(reader, catalog);
                }
                break;
            default:
                break;
            }
        }

        if (symtabType == SymbolTableType.SHARED) {
            if (name == null || name.length() == 0) {
                String message =
                    "Shared symbol table is malformed: field 'name' " +
                    "must be a non-empty string.";
                throw new IonException(message);
            }

            if (version < 1) {
                version = 1;
            }

            assert _symbols == _Private_Utils.EMPTY_STRING_ARRAY;
            assert _id_map.isEmpty();

            if (symbols != null && symbols.size() != 0)
            {
                String[] syms = symbols.toArray(new String[symbols.size()]);
                loadSharedSymbolTableContents(name, version, syms);
            }
            else
            {
                share(name, version);
            }
        }
        else {
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
    }

    static
    private
    final
    int get_symbol_sid_helper(String fieldName)
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

            maxid = itab.getMaxId();
        }

        if (itab == null) {
            assert maxid >= 0;

            // Construct substitute table with max_id undefined symbols
            itab = new SubstituteSymbolTable(name, version, maxid);
        }
        else if (itab.getVersion() != version || itab.getMaxId() != maxid)
        {
            // Construct a substitute with correct specs
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
        if (isSharedTable()) {
            buf.append(_name);
            buf.append(" ");
            buf.append(_version);
        }
        else {
            buf.append("local");
        }
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
