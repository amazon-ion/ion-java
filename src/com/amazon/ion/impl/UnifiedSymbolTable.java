// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.SymbolTableType.LOCAL;
import static com.amazon.ion.impl.SymbolTableType.SHARED;
import static com.amazon.ion.util.IonTextUtils.printQuotedSymbol;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.InvalidSystemSymbolException;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.util.IonTextUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 *
 * UnifiedSymbolTable supports all the current symbol table
 * interfaces.  It is a static symbol table when the table
 * has been locked.  Is is a local symbol table when the it
 * is unlocked and has no more than 1 imported table. It is
 * a system symbol table when the name is the ion magic name
 * system symbol table name.
 *
 * symbol "inheritance" is":
 *  first load system symbol table
 *   then imported tables in order offset by prev _max_id
 *        and we remember their "max id" so they get a range
 *        and skip any names already present
 *   then local names from the new max id
 *
 */

public final class UnifiedSymbolTable
    implements SymbolTable
{

    public static final int UNKNOWN_SID = -1;

    public static final int DEFAULT_ION_VERSION = 1;

    /**
     * The system symbol <tt>'$ion'</tt>, as defined by Ion 1.0.
     */
    public static final String ION = "$ion";
    public static final int    ION_SID = 1;

    /**
     * The system symbol <tt>'$ion_1_0'</tt>, as defined by Ion 1.0.
     */
    public static final String ION_1_0 = "$ion_1_0";
    public static final int    ION_1_0_SID = 2;

    /**
     * The system symbol <tt>'$ion_symbol_table'</tt>, as defined by Ion 1.0.
     */
    public static final String ION_SYMBOL_TABLE = "$ion_symbol_table";
    public static final int    ION_SYMBOL_TABLE_SID = 3;

    /**
     * The system symbol <tt>'name'</tt>, as defined by Ion 1.0.
     */
    public static final String NAME = "name";
    public static final int    NAME_SID = 4;

    /**
     * The system symbol <tt>'version'</tt>, as defined by Ion 1.0.
     */
    public static final String VERSION = "version";
    public static final int    VERSION_SID = 5;

    /**
     * The system symbol <tt>'imports'</tt>, as defined by Ion 1.0.
     */
    public static final String IMPORTS = "imports";
    public static final int    IMPORTS_SID = 6;

    /**
     * The system symbol <tt>'symbols'</tt>, as defined by Ion 1.0.
     */
    public static final String SYMBOLS = "symbols";
    public static final int    SYMBOLS_SID = 7;

    /**
     * The system symbol <tt>'max_id'</tt>, as defined by Ion 1.0.
     */
    public static final String MAX_ID = "max_id";
    public static final int    MAX_ID_SID = 8;

    /**
     * The system symbol <tt>'$ion_embedded_value'</tt>, as defined by Ion 1.0.
     */
    @Deprecated
    public static final String ION_EMBEDDED_VALUE = "$ion_embedded_value";
    @Deprecated
    public static final int    ION_EMBEDDED_VALUE_SID = 9;

    /**
     * The system symbol <tt>'$ion_shared_symbol_table'</tt>, as defined by Ion 1.0.
     */
    public static final String ION_SHARED_SYMBOL_TABLE = "$ion_shared_symbol_table";
    public static final int    ION_SHARED_SYMBOL_TABLE_SID = 9;

    public static final int    ION_SYSTEM_SYMBOL_TABLE_MAX_ID = 9;

    /**
     * The set of system symbols as defined by Ion 1.0.
     * We keep this private to (help) prevent it from being modified.
     */
    private static final String[] SYSTEM_SYMBOLS =
    {
        UnifiedSymbolTable.ION,
        UnifiedSymbolTable.ION_1_0,
        UnifiedSymbolTable.ION_SYMBOL_TABLE,
        UnifiedSymbolTable.NAME,
        UnifiedSymbolTable.VERSION,
        UnifiedSymbolTable.IMPORTS,
        UnifiedSymbolTable.SYMBOLS,
        UnifiedSymbolTable.MAX_ID,
        UnifiedSymbolTable.ION_SHARED_SYMBOL_TABLE
    };


    private String                    _name;              // name of shared (or system) table, null if local, "$ion" if system
    private int                       _version;           // version of the shared table
    private UnifiedSymbolTableImports _import_list;       // list of imported tables

    private IonStruct                 _image;             // previous image of a symbol table
    private IonSystem                 _image_system;      // Ion system of the previous image
    private boolean                   _is_changed;        // set if anything changed, cleared at image set time
    private boolean                   _is_read_only;      // set to mark this symbol table as being (now) immutable
    private boolean                   _maintain_image;    // set if we want to keep the image up to date as we add symbols

    /**
     * The local symbol names declared in this symtab; never null.
     * The sid of the first element is {@link #_first_local_sid}.
     * Only the first {@link #_local_symbol_count} elements are valid.
     */
    private String[] _symbols;

    /** The initial length of {@link #_symbols}. */
    private static final int DEFAULT_SYMBOL_LENGTH = 10;

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
    private int _first_local_sid;

    /** Map from symbol name to SID of local symbols only. */
    private final HashMap<String, Integer> _id_map;


    /*
     * SymbolTable constructors that are mostly intended to be used
     * internally.
     *
     */

    private UnifiedSymbolTable(IonSystem sys)
    {
        assert sys != null;
        _sys_holder = sys;

        _name = null;
        _version = 0;
        _first_local_sid = 1;
        _symbols = IonImplUtils.EMPTY_STRING_ARRAY;
        _local_symbol_count = 0;
        _id_map = new HashMap<String, Integer>(10);
        _import_list = UnifiedSymbolTableImports.emptyImportList;

        _image = null;
        _image_system = sys;
        _is_changed = true;
    }
    private void init(SymbolTable systemSymbols) {
        _import_list = new UnifiedSymbolTableImports(systemSymbols);
        _first_local_sid = _import_list.getMaxId() + 1;
    }

    /**
     * Constructs an empty local symbol table.
     *
     * @param systemSymbols must be a system symbol table or null.
     */
    private UnifiedSymbolTable(IonSystem sys, SymbolTable systemSymbols)
    {
        this(sys);
        init(systemSymbols);

        if (systemSymbols != null) {
            if (!systemSymbols.isSystemTable()) {
                throw new IllegalArgumentException();
            }
            if (! (systemSymbols instanceof UnifiedSymbolTable)) {
                throw new IllegalArgumentException();
            }
        }
    }

    /*
     * load helpers that fill in the contents of the symbol table
     * from various sources
     */

    /**
     * @param symbols will be retained by this symtab (but not modified).
     * It must not contain any empty strings.
     */
    private void loadSharedSymbolTableContents(String name, int version, String[] symbols)
    {
        assert symbols != null;

        _symbols = symbols;
        _local_symbol_count = symbols.length;
        for (int ii=0, sid= 1; ii < _local_symbol_count; ii++, sid++) {
            String symName = symbols[ii];
            assert symName == null || symName.length() > 0;
            // When there's a duplicate name, don't replace the lower sid.
            if (! _id_map.containsKey(symName)) {
                _id_map.put(symName, sid);
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

        int sid = convertLocalOffsetToSid(0);

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
            if (findSymbol(symName) == UnifiedSymbolTable.UNKNOWN_SID) {
                putSymbol(symName, sid);
                sid++;
            }
        }

        share(name, version);
    }

    private void loadSharedSymbolTableContents(IonReader reader, boolean isOnStruct)
    {
        if (!isOnStruct) {
            if (!IonImplUtils.READER_HASNEXT_REMOVED && !reader.hasNext()) {
                throw new IonException("invalid symbol table image passed in reader");
            }
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
            if (!IonImplUtils.READER_HASNEXT_REMOVED && !reader.hasNext()) {
                throw new IonException("invalid symbol table image passed in reader");
            }
            IonType t = reader.next();
            if (t != IonType.STRUCT) {
                String message = "invalid symbol table image passed in reader "
                               + t.toString()
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

    //
    //  system symbol table constructor
    //      we get a version just to remind users there will
    //      be choices for this later
    //
    static public UnifiedSymbolTable makeSystemSymbolTable(IonSystem sys, int version)
    {
        if (version != 1) {
            throw new IllegalArgumentException("only version 1 system symbols are supported currently");
        }

        UnifiedSymbolTable table = new UnifiedSymbolTable(sys);
        table.loadSharedSymbolTableContents(SystemSymbolTable.ION, version, SYSTEM_SYMBOLS);
        table.makeReadOnly();
        return table;
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
     */
    static public UnifiedSymbolTable
    makeNewSharedSymbolTable(IonSystem sys, String name, int version,
                             SymbolTable priorSymtab,
                             Iterator<String> symbols)
    {
        if (name == null || name.length() < 1) {
            throw new IllegalArgumentException("invalid name for shared symbol table");
        }
        if (version < 1) {
            throw new IllegalArgumentException("invalid version for shared symbol table");
        }
        UnifiedSymbolTable table = new UnifiedSymbolTable(sys);
        table.loadSharedSymbolTableContents(name, version, priorSymtab, symbols);
        table.makeReadOnly();
        return table;
    }
    static public UnifiedSymbolTable makeNewSharedSymbolTable(IonStruct ionRep)
    {
        IonReader reader = new IonReaderTreeSystem(ionRep);
        IonSystem sys    = ionRep.getSystem();

        UnifiedSymbolTable table = makeNewSharedSymbolTable_helper(sys, reader, false);
        table.set_image(sys, ionRep);
        table.makeReadOnly();  // public APIs force shared tables to read only

        return table;
    }

    static public UnifiedSymbolTable
    makeNewSharedSymbolTable(IonSystem sys, IonReader reader,
                             boolean alreadyInStruct)
    {
        UnifiedSymbolTable table = makeNewSharedSymbolTable_helper(sys, reader,
                                        alreadyInStruct);
        table.makeReadOnly();  // public APIs force shared tables to read only
        return table;
    }


    static private UnifiedSymbolTable
    makeNewSharedSymbolTable_helper(IonSystem sys, IonReader reader,
                             boolean alreadyInStruct)
    {
        UnifiedSymbolTable table = new UnifiedSymbolTable(sys);
        table.loadSharedSymbolTableContents(reader, alreadyInStruct);
        return table;
    }

    //
    //  local symbol tables
    //     local symbol tables need both a system and a catalog
    //     which we use to resolve imports
    static public UnifiedSymbolTable makeNewLocalSymbolTable(IonSystem sys, int version)
    {
        if (version != 1) {
            throw new IllegalArgumentException("only Ion version 1 is supported currently");
        }

        SymbolTable sys_table = sys.getSystemSymbolTable();
        UnifiedSymbolTable table = makeNewLocalSymbolTable(sys, sys_table);

        return table;
    }

    /**
     * Creates a new local symtab with no imports.
     *
     * @param systemSymbolTable the system symtab. Must not be null.
     */
    static public UnifiedSymbolTable
    makeNewLocalSymbolTable(IonSystem sys, SymbolTable systemSymbolTable)
    {
        if (!systemSymbolTable.isSystemTable()) {
            throw new IllegalArgumentException();
        }
        if (! (systemSymbolTable instanceof UnifiedSymbolTable)) {
            throw new IllegalArgumentException();
        }

        UnifiedSymbolTable table = new UnifiedSymbolTable(sys, systemSymbolTable);
        return table;
    }


    /**
     * Creates a new local symtab with given imports.
     *
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
    static public UnifiedSymbolTable
    makeNewLocalSymbolTable(IonSystem system,
                            SymbolTable systemSymbolTable,
                            SymbolTable... imports)
    {
        if (!systemSymbolTable.isSystemTable()) {
            throw new IllegalArgumentException();
        }

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

        // TODO check all imports?
        if (! (systemSymbolTable instanceof UnifiedSymbolTable)) {
            throw new IllegalArgumentException("Symbol tables must be UnifiedSymbolTable");
        }

        UnifiedSymbolTable table = new UnifiedSymbolTable(system, systemSymbolTable);

        if (imports != null) {
            for (int ii=0; ii<imports.length; ii++) {
                UnifiedSymbolTable symbolTable = (UnifiedSymbolTable) imports[ii];
                table._import_list.addImport(symbolTable, symbolTable.getMaxId());
            }
            table._first_local_sid = table._import_list.getMaxId() + 1;
        }

        return table;
    }

    static public UnifiedSymbolTable makeNewLocalSymbolTable(IonStruct ionRep)
    {
        IonSystem  sys        = ionRep.getSystem();
        IonCatalog catalog    = sys.getCatalog();
        SymbolTable sys_table = sys.getSystemSymbolTable();

        UnifiedSymbolTable table = makeNewLocalSymbolTable(sys_table, catalog, ionRep);

        table.set_image(ionRep.getSystem(), ionRep);

        return table;
    }

    /**
     * @param catalog may be null.
     */
    static public UnifiedSymbolTable
    makeNewLocalSymbolTable(IonSystem sys, IonCatalog catalog, IonStruct ionRep)
    {
        SymbolTable sys_table = sys.getSystemSymbolTable();
        IonReader reader = new IonReaderTreeSystem(ionRep);
        UnifiedSymbolTable table =
            makeNewLocalSymbolTable(sys, sys_table, catalog, reader, false);

        table.set_image(sys, ionRep);

        return table;
    }

    /**
     * @param catalog may be null.
     */
    static public UnifiedSymbolTable
    makeNewLocalSymbolTable(SymbolTable systemSymbolTable, IonCatalog catalog,
                            IonStruct ionRep)
    {
        IonSystem sys = ionRep.getSystem();
        IonReader reader = new IonReaderTreeSystem(ionRep);
        UnifiedSymbolTable table =
            makeNewLocalSymbolTable(sys, systemSymbolTable, catalog, reader,
                                    false);

        table.set_image(sys, ionRep);

        return table;
    }

    static public UnifiedSymbolTable
    makeNewLocalSymbolTable(IonSystem sys, IonReader reader,
                            boolean alreadyInStruct)
    {
        IonCatalog  catalog = sys.getCatalog();
        SymbolTable sys_table = sys.getSystemSymbolTable();
        UnifiedSymbolTable table =
            makeNewLocalSymbolTable(sys, sys_table, catalog, reader,
                                    alreadyInStruct);
        return table;
    }

    /**
     * @param catalog may be null.
     */
    static public UnifiedSymbolTable
    makeNewLocalSymbolTable(IonSystem sys, IonCatalog catalog,
                            IonReader reader, boolean alreadyInStruct)
    {
        SymbolTable sys_table = sys.getSystemSymbolTable();
        UnifiedSymbolTable table =
            makeNewLocalSymbolTable(sys, sys_table, catalog, reader,
                                    alreadyInStruct);
        return table;
    }


    /**
     * @param catalog may be null.
     */
    static public UnifiedSymbolTable
    makeNewLocalSymbolTable(IonSystem sys, SymbolTable systemSymbolTable,
                            IonCatalog catalog, IonReader reader,
                            boolean alreadyInStruct)
    {
        UnifiedSymbolTable table = new UnifiedSymbolTable(sys, systemSymbolTable);
        table.loadLocalSymbolTable(reader, catalog, alreadyInStruct);
        return table;
    }

    /**
     * This method is not synchronized because it's only called during
     * construction.
     *
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
        assert this._import_list == null || this._import_list.getMaxId() == 0;
    }


    public static final boolean isRealLocalTable(SymbolTable table)
    {
        return (table != null && table.isLocalTable());
    }

    public static final boolean isAssignableTable(SymbolTable table)
    {
        if (table == null)         return true;
        if (table.isSystemTable()) return true;
        if (table.isLocalTable())  return true;
        return false;
    }

    public static boolean isTrivialTable(SymbolTable table)
    {
        if (table == null)         return true;
        if (table.isSystemTable()) return true;
        if (table.isLocalTable()) {
            // this is only true when there are no local
            // symbols defined
            // and there are no imports with any symbols
            if (table.getMaxId() == ION_SYSTEM_SYMBOL_TABLE_MAX_ID) {
                return true;
            }
        }
        return false;
    }

    public static boolean isLocalTable(SymbolTable table)
    {
        if (table == null) return false;
        return table.isLocalTable();
    }

    public static boolean isSharedTable(SymbolTable table)
    {
        if (table == null) return false;
        return table.isSharedTable();
    }

    public static boolean isSystemTable(SymbolTable table)
    {
        if (table == null) return false;
        return table.isSystemTable();
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
        return (_name != null && SystemSymbolTable.ION.equals(_name));
    }

    /**
     * checks the _is_read_only flag and if the flag is set
     * this throws an error.  This is used by the various
     * methods that may modify a value.
     */
    private void verify_not_read_only() {
        if (_is_read_only) {
            throw new IonException("modifications to read only symbol table not allowed");
        }
    }

    /**
     * public accessor to determine if this symbol table
     * has been marked as immutable.
     *
     * @return boolean true if this table is locked, false if editing is allowed
     */
    public boolean isReadOnly() {
        return _is_read_only;
    }

    public synchronized void makeReadOnly() {
        if (isReadOnly()) {
            return;
        }
        if (_import_list != null) {
            _import_list.makeReadOnly();
        }
        _is_read_only = true;
    }

    /**
     * Checks the passed in value and returns whether or not
     * the value could be a local symbol table.  It does this
     * by checking the type, the annotations and verifying that
     * the value does not contain a name or version.
     *
     * @param v
     * @return boolean true if v can be a local symbol table otherwise false
     */
    public static boolean valueIsLocalSymbolTable(IonValue v)
    {
        if (v instanceof IonStruct) {
            if (v.hasTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE)) {
                IonStruct s = (IonStruct)v;
                if (s.containsKey(UnifiedSymbolTable.NAME)) {
                    return false;
                }
                if (s.containsKey(UnifiedSymbolTable.VERSION)) {
                    return false;
                }
                return true;
            }
        }
        return false;
    }
    // was:
    //public static final boolean valueIsLocalSymbolTable(IonValue value)
    //{
    //    return (value instanceof IonStruct
    //            && value.hasTypeAnnotation(ION_SYMBOL_TABLE));
    //}

    public final boolean valueIsSharedSymbolTable(IonValue value)
    {
        return (value instanceof IonStruct
                && value.hasTypeAnnotation(ION_SHARED_SYMBOL_TABLE));
    }

    public static boolean isLocalAndNonTrivial(SymbolTable symbolTable)
    {
        if (symbolTable == null) return false;
        if (!symbolTable.isLocalTable()) return false;

        // If symtab has imports we must retain it.
        // Note that I chose to retain imports even in the degenerate case
        // where the imports have no symbols.
        if (symbolTable.getImportedTables().length > 0) {
            return true;
        }

        // there are no imports so now we check if there are any local
        // symbols - if this is one of our instances it's easy otherwise
        // it's more expensive
        if (symbolTable instanceof UnifiedSymbolTable) {
            if (((UnifiedSymbolTable)symbolTable)._local_symbol_count > 0) {
                return true;
            }
        }
        else {
            Iterator<String> names = symbolTable.iterateDeclaredSymbolNames();
            if (names.hasNext()) {
                return true;
            }
        }
        return false;
    }

    private void set_image(IonSystem sys, IonStruct ionRep)
    {
        verify_not_read_only();

        _image = ionRep;
        _image_system = sys;
        _is_changed = false;
        _maintain_image = (_image != null);
    }
    private void clear_image()
    {
        verify_not_read_only();

        _image = null;
        _image_system = null; // TODO: or should we keep this around until the system-symboltable binding hack is cleaned up?
        _is_changed = true;
        _maintain_image = false;
    }

    @Deprecated
    public
    synchronized
    int size()
    {
        return getMaxId();
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
        int maxid = _local_symbol_count;
        maxid += _import_list.getMaxId();
        return maxid;
    }

    public
    synchronized
    int getLocalSymbolCount()
    {
        return _local_symbol_count;
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

    @Deprecated
    public String getSystemId()
    {
        return getIonVersionId();
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
        return SystemSymbolTable.ION_1_0;
    }


    public
    synchronized
    Iterator<String> iterateDeclaredSymbolNames()
    {
        List<String> slice =
            Arrays.asList(_symbols).subList(0, _local_symbol_count);
        return Collections.unmodifiableList(slice).iterator();
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

        if (sid == UNKNOWN_SYMBOL_ID && isSystemTable() == false) {
            sid = _import_list.findSymbol(name);
            if (sid < 1 && name.charAt(0) == '$') {
                if (name.length() > 1 && Character.isDigit(name.charAt(1))) {
                    String sidText = name.substring(1);
                    try {
                        sid = Integer.parseInt(sidText);
                        if (sid <= 0) {
                            sid = UNKNOWN_SYMBOL_ID;
                        }
                        // else fall through
                    }
                    catch (NumberFormatException e) {
                        if (name.startsWith(SystemSymbolTable.ION_RESERVED_PREFIX)) {
                            throw new InvalidSystemSymbolException(name);
                        }
                        // else symbol is unknown and then fall through to return
                        sid = UNKNOWN_SYMBOL_ID;
                    }
                }
            }
        }
        return sid;
    }

    int findLocalSymbol(String name)
    {
        assert(name.length() > 0);

        Integer isid = _id_map.get(name);
        if (isid != null) {
            return isid.intValue();
        }
        return UNKNOWN_SYMBOL_ID;
    }

    public static int decodeIntegerSymbol(String name)
    {
        if (name == null) return UNKNOWN_SYMBOL_ID;
        if (name.length() < 2) return UNKNOWN_SYMBOL_ID;
        if (name.startsWith("$") == false) return UNKNOWN_SYMBOL_ID;
        int id = 0;
        for (int ii=1; ii<name.length(); ii++) {
            char c = name.charAt(ii);
            if (Character.isDigit(c) == false) {
                return UNKNOWN_SYMBOL_ID;
            }
            id *= 10;
            id += c - '0';
        }
        return id;
    }

    public
    synchronized
    String findSymbol(int id)
    {
        String name = findKnownSymbol(id);
        if (name == null) {
            name = unknownSymbolName(id);
        }
        return name;
    }

    public
    synchronized
    int addSymbol(String name)
    {
        int sid = this.findSymbol(name);
        if (sid == UNKNOWN_SID) {
            if (_name != null) {
                throw new UnsupportedOperationException("can't change shared symbol table");
            }
            validateSymbol(name);
            sid = getMaxId() + 1;
            putSymbol(name, sid);
        }
        return sid;
    }

    public
    static
    final
    void validateSymbol(String name)
    {
        // not synchronized since this is local to the string which is immutable
        if (name == null || name.length() < 1) {
            throw new IllegalArgumentException("symbols must contain 1 or more characters");
        }
        boolean pending_high_surrogate = false;
        int ii=0;
        for (; ii<name.length(); ii++) {
            char c = name.charAt(ii);
            if (IonConstants.isHighSurrogate(c)) {
                if (pending_high_surrogate) {
                    break; // our check at the end will throw for us
                }
                pending_high_surrogate = true;
            }
            else if (IonConstants.isLowSurrogate(c)) {
                if (!pending_high_surrogate) {
                    throw new IllegalArgumentException("unparied low surrogate in symbol name at position "+ii);
                }
                pending_high_surrogate = false;
            }
        }
        if (pending_high_surrogate) {
            throw new IllegalArgumentException("unmatched high surrogate in symbol name at position "+ii);
        }
    }

    @Deprecated // internal only
    public
    synchronized
    void defineSymbol(String name, int id)
    {
        if (_name != null) {
            throw new UnsupportedOperationException("can't change shared symbol table");
        }
        if (name == null || name.length() < 1 || id < 1) {
            throw new IllegalArgumentException("invalid symbol definition");
        }
        if (id < _first_local_sid) {
            throw new IllegalArgumentException("invalid symbol definition");
        }

        int sid = this.findSymbol(name);
        if (sid != UNKNOWN_SID && sid != id) {
            throw new IllegalArgumentException("it's not valid to change a symbols id");
        }
        else if (sid == UNKNOWN_SID) {
            putSymbol(name, id);
        }

        // TODO disallow using sid within imports range
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synch'd method.
     *
     * TODO streamline: we no longer accept arbitrary sid, its always > max_id
     * (After we remove {@link #defineSymbol(String, int)} at least)
     *
     * @param symbolName may be null, indicating a gap in the symbol table.
     */
    private void putSymbol(String symbolName, int sid)
    {
        assert _name == null && _symbols != null;

        verify_not_read_only();

        int idx = convertSidToLocalOffset(sid);
        assert idx >= 0;

        if (idx >= _local_symbol_count) {
            int oldlen = _local_symbol_count;
            if (oldlen >= _symbols.length) {
                int newlen = oldlen * 2;
                if (newlen < DEFAULT_SYMBOL_LENGTH) {
                    newlen = DEFAULT_SYMBOL_LENGTH;
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

        _id_map.put(symbolName, sid);
        _symbols[idx] = symbolName;

        if (idx >= _local_symbol_count) {
            _local_symbol_count = idx + 1;
        }

        if (_maintain_image) {
            assert _local_symbol_count > 0;
            recordLocalSymbolInIonRep(_image_system, _image, symbolName, sid);
        }
    }

    public SymbolTable getSystemSymbolTable()
    {
        // Not synchronized since this member never changes after construction.
        SymbolTable system_table = _import_list.getSystemSymbolTable();
        if (system_table == null && _sys_holder != null) {
            system_table = _sys_holder.getSystemSymbolTable();
        }
        assert (system_table != null && system_table.isSystemTable());
        return system_table;
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
    private IonSystem _sys_holder = null;

    void setSystem(IonSystem sys) {
        // Not synchronized since this member is only called from system when
        // the symtab is created.
        verify_not_read_only();
        if (_sys_holder != null) {
            // Changing system will cause problems!
            assert _sys_holder == sys;
        }
        _sys_holder = sys;
    }

    public UnifiedSymbolTable[] getImportedTables()
    {
        if (isSharedTable()) return null;

        // this does not need to be synchronized since the import
        // list is fixed at construction time.  Once a caller has
        // a symtab the import list is immutable
        // was: synchronized (this) {

        int count = _import_list.getImportCount();
        UnifiedSymbolTable[] imports =
            new UnifiedSymbolTable[count];
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

    // FIXME don't create a new system.
    // this whole construct is a terrible hack
    // it should be replace by a custom reader
    // that "reads" the current state of a symbol table
    private synchronized IonStruct safeGetIonRepresentation()
    {
        IonStruct rep;
        try {
            rep = getIonRepresentation();
        }
        catch (IonExceptionNoSystem e) {
            IonSystem sys = IonSystemBuilder.standard().build();
            rep = makeIonRepresentation(sys);
            clear_image(); // don't keep this one around
        }
        return rep;
    }

    @Deprecated
    public synchronized IonStruct getIonRepresentation()
    {
        if (_image != null && !_is_changed) return _image;

        if (this._image_system == null) {
            if (this._sys_holder == null) {
                throw new IonExceptionNoSystem("can't create representation without a system");
            }
            verify_not_read_only();
            _image_system = _sys_holder;
        }

        return getIonRepresentation(_image_system);
    }

    public
    synchronized
    IonStruct getIonRepresentation(IonSystem sys)
    {
        if (_is_changed == false && sys == _image_system) {
            return _image;
        }

        if (sys == null) {
            throw new IonExceptionNoSystem("can't create representation without a system");
        }

        verify_not_read_only();

        if (_image == null || sys != _image_system) {
            _image = makeIonRepresentation(sys);
            _image_system = sys;
            _maintain_image = true;
        }
        else {
            replaceSymbols(sys, _image);
        }

        _is_changed = false;

        return _image;
    }

    // TODO: optimize this.  this is a quick and dirty version
    //       to provide users the "correct" alternative.  Later
    //       we should make a real synthetic read that does this
    //       without requiring the underlying struct.
    @Deprecated
    public
    IonReader getReader()
    {
        IonStruct rep = safeGetIonRepresentation();
        IonReader reader = new IonReaderTreeSystem(rep);
// cas FIXME:        IonReader reader = new USTReader(this);
        return reader;
    }

    // TODO: optimize this.  this is a quick and dirty version
    //       to provide users the "correct" alternative.  Later
    //       we should make a real synthetic read that does this
    //       without requiring the underlying struct.
    protected
    IonReader getReader(IonSystem sys)
    {
        IonStruct rep = this.getIonRepresentation(sys);
        IonReader reader = new IonReaderTreeSystem(rep);
// cas FIXME:        IonReader reader = new USTReader(this);
        return reader;
    }


    /**
     * NOT SYNCHRONIZED! Call only from a synch'd method.
     */
    private IonStruct makeIonRepresentation(IonSystem sys)
    {
        IonStruct ionRep = sys.newEmptyStruct();

        if (this.isSharedTable()) {
            assert getVersion() > 0;
            ionRep.addTypeAnnotation(UnifiedSymbolTable.ION_SHARED_SYMBOL_TABLE);
            ionRep.add(UnifiedSymbolTable.NAME, sys.newString(this.getName()));
            ionRep.add(UnifiedSymbolTable.VERSION, sys.newInt(this.getVersion()));
        }
        else {
            ionRep.addTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE);
        }

        SymbolTable[] imports = this.getImportedTables();
        assert (this.isLocalTable() || imports == null);

        if (imports != null && imports.length > 0) {
            IonList imports_as_ion = sys.newEmptyList();
            for (int ii=0; ii<imports.length; ii++) {
                SymbolTable imptable = imports[ii];
                IonStruct imp = sys.newEmptyStruct();
                imp.add(UnifiedSymbolTable.NAME, sys.newString(imptable.getName()));
                imp.add(UnifiedSymbolTable.VERSION, sys.newInt(imptable.getVersion()));
                int max_id = _import_list.getMaxIdForExportAdjusted(ii);
                if (max_id > 0) {
                    imp.add(UnifiedSymbolTable.MAX_ID, sys.newInt(max_id));
                }
                imports_as_ion.add(imp);
            }
            ionRep.add(IMPORTS, imports_as_ion);
        }

        if (_local_symbol_count > 0)
        {
            replaceSymbols(sys, ionRep);
        }

        return ionRep;
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
     */
    private void replaceSymbols(IonSystem sys, IonStruct ionRep)
    {
        assert ionRep != null;
        assert sys == ionRep.getSystem();

        // TODO Why can't we just append to the end of the existing ionRep?
        IonList symbolsList = sys.newEmptyList();

        ionRep.remove(UnifiedSymbolTable.SYMBOLS);  // to cover the "replace existing" case
        ionRep.add(UnifiedSymbolTable.SYMBOLS, symbolsList);

        int sid = _first_local_sid;
        for (int offset = 0; offset < _local_symbol_count; offset++, sid++) {
            String symbolName = _symbols[offset];
            recordLocalSymbolInIonRep(sys, ionRep, symbolName, sid);
        }
    }


    /**
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
     * @param symbolName can be null when there's a gap in the local symbols list.
     */
    private void recordLocalSymbolInIonRep(IonSystem sys, IonStruct ionRep,
                                           String symbolName, int sid)
    {
        assert sid >= _first_local_sid;
        assert ionRep != null;
        assert ionRep.getSystem() == sys;

        verify_not_read_only();

        // TODO this is crazy inefficient and not as reliable as it looks
        // since it doesn't handle the case where's theres more than one list
        IonValue syms = ionRep.get(UnifiedSymbolTable.SYMBOLS);
        while (syms != null && syms.getType() != IonType.LIST) {
            ionRep.remove(syms);
            syms = ionRep.get(UnifiedSymbolTable.SYMBOLS);
        }
        if (syms == null) {
            syms = sys.newEmptyList();
            ionRep.put(UnifiedSymbolTable.SYMBOLS, syms);
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

        while(reader.hasNext()) {
            IonType fieldType = reader.next();

            if (reader.isNullValue()) continue;

            int fieldId = reader.getFieldId();
            if (fieldId < 0) {
                // this is a user defined reader or a pure DOM
                // we fall back to text here
                final String fieldName = reader.getFieldName();
                fieldId = get_symbol_sid_helper(fieldName);
            }

            switch (fieldId) {
            case UnifiedSymbolTable.VERSION_SID:
                if (symtabType == SHARED && fieldType == IonType.INT) {
                    version = reader.intValue();
                }
                break;
            case UnifiedSymbolTable.NAME_SID:
                if (symtabType == SHARED && fieldType == IonType.STRING) {
                    name = reader.stringValue();
                }
                break;
            case UnifiedSymbolTable.SYMBOLS_SID:
                if (fieldType == IonType.LIST) {
                    // Other types treated as empty-list
                    symbols = new ArrayList<String>();

                    reader.stepIn();
                    while (reader.hasNext()) {
                        IonType type = reader.next();
                        if (type == null) {
                            break;
                        }

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
            case UnifiedSymbolTable.IMPORTS_SID:
                if (symtabType == LOCAL && fieldType == IonType.LIST) {
                    readImportList(reader, catalog);
                }
                break;
            default:
                break;
            }
        }

        if (symtabType == SHARED) {
            if (name == null || name.length() == 0) {
                String message =
                    "Shared symbol table is malformed: field 'name' " +
                    "must be a non-empty string.";
                throw new IonException(message);
            }

            if (version < 1) {
                version = 1;
            }

            assert _symbols == IonImplUtils.EMPTY_STRING_ARRAY;
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
                    // Allocate a sid even when the symbols's text is malformed
                    // FIXME ION-189 this needs to retain the text in case
                    // there's a reference to the corresponding sid.
                    if (symbolText != null && findSymbol(symbolText) != UNKNOWN_SID) {
                        symbolText = null;
                    }
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
        if (fieldName != null && fieldName.length() > 0) {
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
            default:
                break;
            }
        }
        return UNKNOWN_SID;
    }


    /**
     * NOT SYNCHRONIZED! Call within constructor or from synch'd method.
     *
     * @param catalog may be null.
     */
    private void readImportList(IonReader reader, IonCatalog catalog)
    {
        assert (reader.getFieldId() == SystemSymbolTable.IMPORTS_SID || SystemSymbolTable.IMPORTS.equals(reader.getFieldName()));
        assert (reader.getType() == IonType.LIST);

        reader.stepIn();
        while (reader.hasNext()) {
            IonType t = reader.next();
            if (t == null) {
                break;
            }
            if (IonType.STRUCT.equals(t)) {
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
        while (ionRep.hasNext()) {
            IonType t = ionRep.next();
            if (t == null) {
                break;
            }

            if (ionRep.isNullValue()) continue;

            int field_id = ionRep.getFieldId();
            if (field_id == -1) {
                // this is a user defined reader or a pure DOM
                // we fall back to text here
                final String fieldName = ionRep.getFieldName();
                field_id = get_symbol_sid_helper(fieldName);
            }
            switch(field_id) {
                case UnifiedSymbolTable.NAME_SID:
                    if (t == IonType.STRING) {
                        name = ionRep.stringValue();
                    }
                    break;
                case UnifiedSymbolTable.VERSION_SID:
                    if (t == IonType.INT) {
                        version = ionRep.intValue();
                    }
                    break;
                case UnifiedSymbolTable.MAX_ID_SID:
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

        UnifiedSymbolTable itab = null;
        if (catalog != null) {
            itab = (UnifiedSymbolTable) catalog.getTable(name, version);
        }
        if (maxid < 0
            && (itab == null || version != itab.getVersion()))
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

        if (itab == null) {
            assert maxid >= 0;

            // Construct dummy table with max_id undefined symbols
            itab = new UnifiedSymbolTable(_sys_holder);
            itab._local_symbol_count = maxid;
            itab.share(name, version);
        }

        _import_list.addImport(itab, maxid);
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

    @SuppressWarnings("unchecked")
    Iterator<String> getLocalSymbolIterator()
    {
        return new SymbolIterator();
    }

    Iterator<UnifiedSymbolTable> getImportIterator()
    {
        return _import_list.getImportIterator();
    }

    final class SymbolIterator implements Iterator
    {
        int _idx = 0;

        public boolean hasNext()
        {
            if (_idx < _local_symbol_count) {
                return true;
            }
            return false;
        }

        public Object next()
        {
            if (_idx < _local_symbol_count) {
                String name = _symbols[_idx];
                _idx++;
                if (name == null) {
                    name = "";
                }
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
