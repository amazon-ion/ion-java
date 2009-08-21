// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.SymbolTableType.LOCAL;
import static com.amazon.ion.impl.SymbolTableType.SHARED;
import static com.amazon.ion.util.IonTextUtils.printQuotedSymbol;

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
import com.amazon.ion.system.SystemFactory;
import com.amazon.ion.util.IonTextUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

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

public final

 class UnifiedSymbolTable
    implements SymbolTable
{

    public static final int UNKNOWN_SID = -1;

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
     */
    public static final String[] SYSTEM_SYMBOLS =
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

    private enum SYSTEM_ONLY { SYSTEM_SYMBOL_TABLE }

    private final static UnifiedSymbolTable _system_1_0_symbols =
                     new UnifiedSymbolTable(SYSTEM_ONLY.SYSTEM_SYMBOL_TABLE);

    private UnifiedSymbolTable(SYSTEM_ONLY make_unique_constructor)
    {
        this();

        _symbols = new UnifiedSymbolTableSymbol[SYSTEM_SYMBOLS.length];

        for (int ii=0; ii<SYSTEM_SYMBOLS.length; ii++) {
            int sid = convertLocalOffsetToSid(ii);
            UnifiedSymbolTableSymbol sym = new UnifiedSymbolTableSymbol(SYSTEM_SYMBOLS[ii], sid, this);
            defineSymbol(sym);
        }

        share(SystemSymbolTable.ION, 1);
    }



    private static final UnifiedSymbolTableSymbol[] NO_SYMBOLS = new UnifiedSymbolTableSymbol[0];

    private final int DEFAULT_SYMBOL_LENGTH = 10;

    private String                    _name;              // name of shared (or system) table, null if local, "$ion" if system
    private int                       _version;           // version of the shared table
    private UnifiedSymbolTableImports _import_list;       // list of imported tables

    /**
     * All symbols from the system symtab, imported tables, and locals.
     * The index in the array is the sid.
     * Can't be private (yet) since its used by the binary writer.
     */
    /**
     * just the symbols that are defined locally.
     */
    UnifiedSymbolTableSymbol[]      _symbols;

    /**
     * This is the number of symbols defined in this symbol table
     * locally, that is not imported from some other table.
     *
     */
    private int _local_symbol_count;

    /**
     * the sid of the first local symbol which will be stored at
     * offset 0 in the local symbol list.  See convertSidToLocalOffset
     * and convertLocalOffsetToSid.
     */
    private int _sid_base;

    private final HashMap<String, Integer> _id_map;  // map from symbol name to SID of local symbols only

    private IonStructImpl            _ion_rep;         // Ion DOM representation of this symbol table
    private IonList                  _ion_symbols_rep; // list that mimics the _symbols list of local symbols as an IonList

    /*
     * SymbolTable constructors that are mostly intended to be used
     * internally.
     *
     */

    private UnifiedSymbolTable() {
        _name = null;
        _version = 0;
        _sid_base = 0;
        _symbols = NO_SYMBOLS;
        _local_symbol_count = 0;
        _id_map = new HashMap<String, Integer>(10);
        _import_list = UnifiedSymbolTableImports.emptyImportList;
    }
    private void init(SymbolTable systemSymbols) {
        _import_list = new UnifiedSymbolTableImports(systemSymbols);
        _sid_base = _import_list.getMaxId();
    }

    /**
     * Constructs an empty local symbol table.
     *
     * @param systemSymbols must be a system symbol table or null.
     */
    private UnifiedSymbolTable(SymbolTable systemSymbols)
    {
        this();
        init(systemSymbols);

        if (systemSymbols != null) {
            if (!systemSymbols.isSystemTable()) {
                throw new IllegalArgumentException();
            }
            if (! (systemSymbols instanceof UnifiedSymbolTable)) {
                throw new IllegalArgumentException();
            }
            _sid_base = _import_list.getMaxId();
        }
    }
    /*
     * load helpers that fill in the contents of the symbol table
     * from various sources
     */
    private void loadSharedSymbolTableContents(IonReader reader, boolean isOnStruct)
    {
        if (!isOnStruct) {
            if (!reader.hasNext()) {
                throw new IonException("invalid symbol table image passed in reader");
            }
            IonType t = reader.next();
            if (t != IonType.STRUCT) {
                throw new IonException("invalid symbol table image passed in reader");
            }
        }

        reader.stepIn();
        readIonRep(SymbolTableType.SHARED, reader, null);
        reader.stepOut();

        assert _name != null;
    }
    private void loadSharedSymbolTableContents(String name, int version, String[] symbols)
    {
        assert name != null;
        assert version >= 0;
        assert symbols != null;

        _symbols = new UnifiedSymbolTableSymbol[symbols.length];
        for (int ii=0; ii<symbols.length; ii++) {
            String symName = symbols[ii];
            if (findSymbol(symName) == UnifiedSymbolTable.UNKNOWN_SID) {
                int sid = convertLocalOffsetToSid(ii);
                UnifiedSymbolTableSymbol sym = new UnifiedSymbolTableSymbol(symName, sid, this);
                defineSymbol(sym);
            }
        }

        share(name, version);
    }
    private void loadSharedSymbolTableContents(String name, int version, Iterator<String> symbols)
    {
        assert name != null;
        assert version >= 0;
        assert symbols != null;

        int sid = convertLocalOffsetToSid(0);
        while (symbols.hasNext()) {
            String symName = symbols.next();
            if (findSymbol(symName) == UnifiedSymbolTable.UNKNOWN_SID) {
                UnifiedSymbolTableSymbol sym = new UnifiedSymbolTableSymbol(symName, sid, this);
                defineSymbol(sym);
                sid++;
            }
        }

        share(name, version);
    }

    /**
     * Loads a local symbol table from an IonReader and makes it ready
     * for use.
     * @param reader positioned after hasNext on a struct with a local symbol table annotation
     * @param catalog user catalog
     * @return local symbol table
     */
    public static
    synchronized  // TODO: why does this need to be synchronized? Are we protecting the reader?
    UnifiedSymbolTable loadLocalSymbolTable(IonSystem system, IonReader reader, IonCatalog catalog)
    {
        SymbolTable        system_symbols = system.getSystemSymbolTable();
        UnifiedSymbolTable symtab = UnifiedSymbolTable.makeNewLocalSymbolTable(system_symbols);

        reader.next();
        reader.stepIn();
        symtab.readIonRep(SymbolTableType.LOCAL, reader, catalog);
        reader.stepOut();

        return symtab;
    }


    /*
     * public methods for constructing symbol tables most tailored to assure
     * the SymbolTable is properly initialized.
     *
     */
    static public UnifiedSymbolTable makeSystemSymbolTable(int version)
    {
        if (version != 1) {
            throw new IllegalArgumentException("only version 1 system symbols are supported curretnly");
        }

        UnifiedSymbolTable table = new UnifiedSymbolTable();
        table.loadSharedSymbolTableContents(SystemSymbolTable.ION, version, SYSTEM_SYMBOLS);

        return table;
    }
    static public UnifiedSymbolTable makeNewSharedSymbolTable(String name, int version, Iterator<String> symbols)
    {
        if (name == null || name.length() < 1) {
            throw new IllegalArgumentException("invalid name for shared symbol table");
        }
        if (version < 1) {
            throw new IllegalArgumentException("invalid version for shared symbol table");
        }
        UnifiedSymbolTable table = new UnifiedSymbolTable();
        table.loadSharedSymbolTableContents(name, version, symbols);

        return table;
    }
    static public UnifiedSymbolTable makeNewSharedSymbolTable(IonReader reader, boolean alreadyInStruct)
    {
        UnifiedSymbolTable table = new UnifiedSymbolTable();
        table.loadSharedSymbolTableContents(reader, alreadyInStruct);

        return table;
    }
    static public UnifiedSymbolTable makeNewSharedSymbolTable(IonStruct ionRep)
    {
        UnifiedSymbolTable table = new UnifiedSymbolTable();
        IonReader reader = new IonTreeReader(ionRep);

        table.loadSharedSymbolTableContents(reader, false);
        if (ionRep instanceof IonStructImpl) {
            table._ion_rep = (IonStructImpl) ionRep;
        }

        return table;
    }
    static public UnifiedSymbolTable makeNewLocalSymbolTable(int version)
    {
        if (version != 1) {
            throw new IllegalArgumentException("only Ion version 1 is supported currently");
        }
        return makeNewLocalSymbolTable(UnifiedSymbolTable.getSystemSymbolTableInstance());
    }

    static public UnifiedSymbolTable makeNewLocalSymbolTable(SymbolTable systemSymbolTable)
    {
        int base = 0;

        if (systemSymbolTable != null) {
            if (!systemSymbolTable.isSystemTable()) {
                throw new IllegalArgumentException();
            }
            if (! (systemSymbolTable instanceof UnifiedSymbolTable)) {
                throw new IllegalArgumentException();
            }
            base = systemSymbolTable.getMaxId();
        }

        UnifiedSymbolTable table = new UnifiedSymbolTable();
        table.init(systemSymbolTable);

        table._sid_base = base;

        return table;
    }

    static public UnifiedSymbolTable makeNewLocalSymbolTable(IonSystem system, SymbolTable systemSymbolTable, SymbolTable... imports)
    {
        if (systemSymbolTable != null) {
            if (!systemSymbolTable.isSystemTable()) {
                throw new IllegalArgumentException();
            }
            if (! (systemSymbolTable instanceof UnifiedSymbolTable)) {
                throw new IllegalArgumentException();
            }
        }

        UnifiedSymbolTable table = new UnifiedSymbolTable(systemSymbolTable);
        table.setSystem(system);

        for (int ii=0; ii<imports.length; ii++) {
            UnifiedSymbolTable symbolTable = (UnifiedSymbolTable) imports[ii];
            table._import_list.addImport(symbolTable, symbolTable.getMaxId());
        }

        table._sid_base = table._import_list.getMaxId();

        return table;
    }

    static public UnifiedSymbolTable makeNewLocalSymbolTable(SymbolTable systemSymbolTable, IonStruct ionRep, IonCatalog catalog)
    {
        UnifiedSymbolTable table = new UnifiedSymbolTable(systemSymbolTable);

        IonReader reader = new IonTreeReader(ionRep);
        reader.next();
        reader.stepIn();
        table.readIonRep(SymbolTableType.LOCAL, reader, catalog);

        table._ion_rep = (IonStructImpl) ionRep;

        return table;
    }

    static public UnifiedSymbolTable makeNewLocalSymbolTable(IonSystem system, IonReader reader, boolean alreadyInStruct)
    {
        SymbolTable systemSymbolTable = system.getSystemSymbolTable();
        IonCatalog  catalog = system.getCatalog();


        if (!alreadyInStruct) {
            reader.next();
            reader.stepIn();
        }
        UnifiedSymbolTable table = makeNewLocalSymbolTable(systemSymbolTable, catalog, reader);

        if (!alreadyInStruct) {
            reader.stepOut();
        }

        return table;
    }

    static public UnifiedSymbolTable makeNewLocalSymbolTable(SymbolTable systemSymbolTable, IonCatalog catalog, IonReader reader)
    {
        UnifiedSymbolTable table = new UnifiedSymbolTable(systemSymbolTable);

        table.readIonRep(SymbolTableType.LOCAL, reader, catalog);

        return table;
    }

    // TODO get rid of this nasty static, it needs to come from the system.
    public static UnifiedSymbolTable getSystemSymbolTableInstance()
    {
        return _system_1_0_symbols;
    }

    /**
     * This method is not synchronized because it's only called during
     * construction.
     *
     */
    private void share(String name, int version)
    {
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

    public boolean isLocalTable() {
        // Not synchronized since this member never changes after construction.
        return _name == null;
    }

    /**
     * {@inheritDoc}
     * Not synchonized since this member never changes after construction.
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
        // Not synchonized since this member never changes after construction.
        SymbolTable system = _import_list.getSystemSymbolTable();
        if (system == null && _sys_holder != null) {
            system = _sys_holder.getSystemSymbolTable();
        }
        if (system != null) {
            return system.getIonVersionId();
        }
        assert isSystemTable();
        return SystemSymbolTable.ION_1_0;
    }


    public
    synchronized
    Iterator<String> iterateDeclaredSymbolNames()
    {
        return new Iterator<String>()
        {
            int myCurrentId =  _sid_base;
            final int myMaxId = _sid_base + _local_symbol_count;

            public boolean hasNext()
            {
                return myCurrentId < myMaxId;
            }

            public String next()
            {
                // TODO inline and optimize
                return findKnownSymbol(++myCurrentId);
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }


    public
    synchronized
    String findKnownSymbol(int id)
    {
        String name = null;

        if (id < 1) {
            throw new IllegalArgumentException("symbol IDs are greater than 0");
        }
        else if (id <= _sid_base) {
            name = _import_list.findKnownSymbol(id);
        }
        else  {
            int offset = convertSidToLocalOffset(id);
            if (offset < _symbols.length) {
                UnifiedSymbolTableSymbol sym = _symbols[offset];
                if (sym != null) {
                    name = sym.name;
                }
            }
        }

        return name;
    }

    public
    synchronized
    int findSymbol(String name)
    {
        if (name == null || name.length() < 1) {
            throw new IllegalArgumentException("a symbol name must have something in it");
        }
        int sid = UNKNOWN_SYMBOL_ID;

        Integer isid = _id_map.get(name);
        if (isid != null) {
            sid = isid;
        }
        else if (isSystemTable() == false) {
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
            defineSymbol(new UnifiedSymbolTableSymbol(name, sid, this));
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
        if (id <= _sid_base) {
            throw new IllegalArgumentException("invalid symbol definition");
        }

        int sid = this.findSymbol(name);
        if (sid != UNKNOWN_SID && sid != id) {
            throw new IllegalArgumentException("it's not valid to change a symbols id");
        }
        else if (sid == UNKNOWN_SID) {
            defineSymbol(new UnifiedSymbolTableSymbol(name, id, this));
        }

        // TODO disallow using sid within imports range
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synch'd method.
     *
     * TODO streamline: we no longer accept arbitrary sid, its always > max_id
     * (After we remove {@link #defineSymbol(String, int)} at least)
     */
    private void defineSymbol(UnifiedSymbolTableSymbol sym)
    {
        assert _name == null;

        int idx = convertSidToLocalOffset(sym.sid); // sym.sid - _sid_base - 1;
        int oldlen, newlen;

        if (idx < 0 || _symbols == null) {
            assert idx >= 0 && _symbols != null;
        }
        if (idx >= _local_symbol_count) {
            oldlen = _local_symbol_count;
            if (oldlen >= _symbols.length) {
                newlen = oldlen * 2;
                if (newlen < DEFAULT_SYMBOL_LENGTH) {
                    newlen = DEFAULT_SYMBOL_LENGTH;
                }
                while (newlen < idx) {
                    newlen *= 2;
                }
                UnifiedSymbolTableSymbol[] temp = new UnifiedSymbolTableSymbol[newlen];
                if (oldlen > 0) {
                    System.arraycopy(_symbols, 0, temp, 0, oldlen);
                }
                _symbols = temp;
            }
        }
        else if (_symbols[idx] != null) {
            String message =
                "Cannot redefine $" + sym.sid + " from "
                + printQuotedSymbol(_symbols[idx].name)
                + " to " + printQuotedSymbol(sym.name);
            throw new IonException(message);
        }

        _id_map.put(sym.name, sym.sid);
        _symbols[idx] = sym;

        if (idx >= _local_symbol_count) {
            _local_symbol_count = idx + 1;
        }
        if (sym.source == this) {
            assert _local_symbol_count > 0;
            if (_ion_rep != null) {
                recordLocalSymbolInIonRep(sym, sym.sid);
            }
        }
        return;
    }

    public SymbolTable getSystemSymbolTable()
    {
        // Not synchronized since this member never changes after construction.
        SymbolTable system_table = _import_list.getSystemSymbolTable();
        if (system_table == null && _sys_holder != null) {
            system_table = _sys_holder.getSystemSymbolTable();
        }
        if (system_table == null) {
            // TODO: this is a HACK since we don't seem to have the IonSystem everywhere we need it
            system_table = UnifiedSymbolTable.getSystemSymbolTableInstance();
        }
        return system_table;
    }

    // TODO add to interface to let caller avoid getImports which makes a copy
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


    public
    synchronized   // TODO: why is this synchronized?  are we just protecting the writer?  might we only synchoronize if this is a local sym tab?
    void writeTo(IonWriter writer) throws IOException
    {
        IonStruct rep = _ion_rep;
        if (rep == null)
        {
            IonSystem sys = _sys_holder;
            if (sys == null) {
                // FIXME don't create a new system.
                sys = SystemFactory.newSystem();
            }
            rep = makeIonRepresentation(sys);
        }

        writer.writeValue(rep);
    }

    public
    synchronized
    IonStruct getIonRepresentation()
    {
        if (_ion_rep != null) return _ion_rep;

        if (this._sys_holder == null) {
            throw new IonException("can't create representation without a system");
        }

        _ion_rep = makeIonRepresentation(_sys_holder);
        _ion_symbols_rep = (IonList) _ion_rep.get(SYMBOLS);
        return _ion_rep;
    }

    /**
     * NOT SYNCHRONIZED! Call only from a synch'd method.
     */
    private IonStructImpl makeIonRepresentation(IonSystem sys)
    {
        IonStructImpl ionRep = (IonStructImpl) sys.newEmptyStruct();

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
                imp.add(UnifiedSymbolTable.MAX_ID, sys.newInt(imptable.getMaxId()));
                imports_as_ion.add(imp);
            }
            ionRep.add(IMPORTS, imports_as_ion);
        }

        if (_local_symbol_count > 0)
        {
            _ion_rep = ionRep;  // FIXME ugly hack to enable recordLocal... below
            IonList symbolsList = sys.newEmptyList();
            ionRep.add(UnifiedSymbolTable.SYMBOLS, symbolsList);

            for (int offset = 0; offset < _local_symbol_count; offset++) {
                UnifiedSymbolTableSymbol sym = _symbols[offset];
                int    sid;
                if (sym != null) {
                    assert sym.source == this;
                    sid = sym.sid;
                }
                else {
                    sid = this.convertLocalOffsetToSid(offset);
                }
                recordLocalSymbolInIonRep(sym, sid);
            }
            _ion_rep = null;
            _ion_symbols_rep = null;
        }

        return ionRep;
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
     */
    private void recordLocalSymbolInIonRep(UnifiedSymbolTableSymbol sym, int sid)
    {
        assert sym == null || sym.source == this; // sym may be null
        assert sym == null || sym.sid == sid;
        assert sid > _sid_base;

        IonSystem system = _ion_rep.getSystem();

        if (_ion_symbols_rep == null) {
            IonValue syms = _ion_rep.get(UnifiedSymbolTable.SYMBOLS);
            if (syms == null || syms.getType() != IonType.LIST) {
                syms = system.newEmptyList();
                _ion_rep.put(UnifiedSymbolTable.SYMBOLS, syms);
            }
            _ion_symbols_rep = (IonList) syms;
        }

        int this_offset = convertSidToLocalOffset(sid);
        IonValue name;
        if (sym == null) {
            name = system.newNull(IonType.STRING);
        }
        else {
            name = system.newString(sym.name);
        }
        _ion_symbols_rep.add(this_offset, name);

    }

    private final int convertSidToLocalOffset(int sid) {
        int offset = sid;
        offset -= _sid_base;
        offset--;
        return offset;
    }
    private final int convertLocalOffsetToSid(int local_offset) {
        int sid = local_offset;
        sid += _sid_base;
        sid++;
        return sid;
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synched method.
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

            assert _id_map.isEmpty();

            if (symbols != null)
            {
                int sid = 0;
                for (String symbolText : symbols) {
                    // Allocate a sid even when the symbols's text is malformed
                    sid++;
                    if (symbolText != null && findSymbol(symbolText) != UNKNOWN_SID) {
                        // if the symbol is already defined elsewhere we don't
                        // want a second copy (that would be invalid)
                        // but we do need to preserve the SID position
                        symbolText = null;
                    }
                    UnifiedSymbolTableSymbol sym = new UnifiedSymbolTableSymbol(symbolText, sid, this);
                    defineSymbol(sym);
                }
            }

            if (version < 1) {
                version = 1;
            }

            share(name, version);
        }
        else {
            // Imports have already been processed above.
            if (symbols != null)
            {
                int sid = _sid_base;
                for (String symbolText : symbols) {
                    // Allocate a sid even when the symbols's text is malformed
                    if (symbolText != null && findSymbol(symbolText) != UNKNOWN_SID) {
                        symbolText = null;
                    }
                    sid++;
                    UnifiedSymbolTableSymbol sym = new UnifiedSymbolTableSymbol(symbolText, sid, this);
                    defineSymbol(sym);
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
     */
    private void readImportList(IonReader reader, IonCatalog catalog)
    {
        assert (reader.getFieldId() == SystemSymbolTable.IMPORTS_SID);
        assert (reader.getType() == IonType.LIST);

        reader.stepIn();
        while (reader.hasNext()) {
            IonType t = reader.next();
            if (t == IonType.STRUCT) {
                readOneImport(reader, catalog);
            }
        }
        reader.stepOut();
    }

    /**
     * NOT SYNCHRONIZED! Call within constructor or from synch'd method.
     *
     * @param catalog may be null
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

            if (ionRep.isNullValue()) continue;

            switch(ionRep.getFieldId()) {
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
            itab = new UnifiedSymbolTable();
            itab._local_symbol_count = maxid;
            itab.share(name, version);
        }

        _import_list.addImport(itab, maxid);
        _sid_base = _import_list.getMaxId();
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
}
