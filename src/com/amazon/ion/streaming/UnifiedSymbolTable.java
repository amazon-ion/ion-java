/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.InvalidSystemSymbolException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.StaticSymbolTable;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.impl.IonBinary;
import com.amazon.ion.impl.IonConstants;
import java.util.HashMap;

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
    implements SymbolTable, LocalSymbolTable, StaticSymbolTable,
    SystemSymbolTable
{

    public final int UNKNOWN_SID = 0;
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
    public static final String ION_EMBEDDED_VALUE = "$ion_embedded_value";
    public static final int    ION_EMBEDDED_VALUE_SID = 9;

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
        UnifiedSymbolTable.ION_EMBEDDED_VALUE
    };

    private final static UnifiedSymbolTable _system_1_0_symbols;
    static {
        UnifiedSymbolTable systab = new UnifiedSymbolTable();

        systab.setSystemSymbolTable(systab);
        systab.setName(SystemSymbolTable.ION_1_0);
        systab.setVersion(1);

        for (int ii=0; ii<SYSTEM_SYMBOLS.length; ii++) {
            systab.defineSymbol(new Symbol(SYSTEM_SYMBOLS[ii], ii+1, systab));
        }

        // we lock it because no one should be adding symbols to
        // the system symbol table.
        systab.lock();
        _system_1_0_symbols = systab;
    }

    public static class Symbol {
        public int                sid;
        public String             name;
        public int                name_len, sid_len, td_len;
        public UnifiedSymbolTable source;
        public Symbol() {}
        public Symbol(String symbolName, int symbolId, UnifiedSymbolTable table) {
            name   = symbolName;
            sid    = symbolId;
            source = table;
            name_len = IonBinary.lenIonString(symbolName);
            sid_len  = IonBinary.lenVarUInt7(sid);
            td_len   = IonBinary.lenLenFieldWithOptionalNibble(name_len);
            td_len  += IonConstants.BB_TOKEN_LEN;
        }
        public String toString() {
            return "Symbol:"+sid+(name != null ? "-"+name : "");
        }
    }

    String                  _name;
    int                     _version;
    UnifiedSymbolTable      _system_symbols;
    UnifiedSymbolTable[]    _imports;
    int                     _import_count;
    Symbol[]                _symbols;
    int                     _symbol_count;
    int                     _max_id;
    int                     _symbol_offset;
    boolean                 _has_user_symbols;
    boolean                 _is_locked;

    HashMap<String, Integer> _id_map;

    private UnifiedSymbolTable() {
        _name = null;
        _version = 0;
        _system_symbols = null;
        _imports = null;
        _import_count = 0;
        _symbols = new Symbol[10];
        _symbol_count = 0;
        _max_id = 0;
        _symbol_offset = 0;
        _has_user_symbols = false;
        _is_locked = false;
        _id_map = new HashMap<String, Integer>(10);
    }

    public UnifiedSymbolTable(UnifiedSymbolTable systemSymbols) {
        this();
        if (!systemSymbols.isSystemTable()) {
            throw new IllegalArgumentException();
        }
        _system_symbols = systemSymbols;
        _max_id = importSymbols(systemSymbols, 0, 0);
        _symbol_offset = _max_id + 1;
    }

    // TODO this should be removed during integration !!
    UnifiedSymbolTable(SymbolTable symboltable) {
        this(getSystemSymbolTableInstance());

        if (symboltable instanceof StaticSymbolTable)
        {
            StaticSymbolTable sst = (StaticSymbolTable) symboltable;
            _name = sst.getName();
            _version = sst.getVersion();
        }

        int minid = this._system_symbols.getMaxId();
        int maxid = symboltable.getMaxId();
        for (int ii=minid + 1; ii <= maxid; ii++) {
            String name = symboltable.findKnownSymbol(ii);
            // FIXME shouldn't happen, we decided to not allow removing symbols
            if (name == null) continue;
            this.defineSymbol(name, ii);
        }
    }

    public static UnifiedSymbolTable getSystemSymbolTableInstance()
    {
        return _system_1_0_symbols;
    }

    public void lock() {
        _is_locked = true;
        // TODO validate that name and version (and maxId?) are legal
    }
    public void unlock() {
        _is_locked = false;
    }
    public void setIsLocked(boolean isLocked) {
        if (isLocked) {
            lock();
        }
        else {
            unlock();
        }
    }
    public boolean isLocked() {
        return _is_locked;
    }
    public boolean isSystemTable() {
        // the is locked test is a short cut since most tables are local and
        // locked, therefore the bool gets us out of here in a hurry
        return (_is_locked
             && this._name != null
             && this._name.equals(SystemSymbolTable.ION_1_0)
             );
    }

    public int size()
    {
        // TODO: what is the size?  is it the number of symbols?
        //       number of locally defined symbols? maximum symbol
        //       id (which counts "null symbols")?  We'll start
        //       with max id, which is the largest and think about it.
        return _symbol_count;
    }
    public int getMaxId()
    {
        return _max_id;
    }
    public void setMaxId(int maxId)
    {
        if (maxId < 0) {
            throw new IllegalArgumentException("symbol id's are greater than 0, and maxId must be at least 0");
        }
        if (_is_locked) {
            throw new IllegalStateException("can't change locked symbol table");
        }

    }
    public void setVersion(int version)
    {
        if (version < 0) {
            throw new IllegalArgumentException("versions must be integers of value 1 or higher, or 0 for 'no version'");
        }
        if (_is_locked) {
            throw new IllegalStateException("can't change locked symbol table");
        }
        _version = version;
        return;
    }
    public int getVersion()
    {
        return _version;
    }
    public String getName()
    {
        return _name;
    }
    public void setName(String name)
    {
        if (name != null && name.length() < 1) {
            throw new IllegalArgumentException("name must have content (length > 0, null for 'no name')");
        }
        if (_is_locked) {
            throw new IllegalStateException("can't change locked symbol table");
        }
        _name = name;
    }
    public String getSystemId()
    {
        if (this._system_symbols != null && this._system_symbols != this) {
            return this._system_symbols.getSystemId();
        }
        return SystemSymbolTable.ION_1_0;
    }
    public String findKnownSymbol(int id)
    {
        String name = null;
        if (id < 1) {
            throw new IllegalArgumentException("symbol id's are greater than 0");
        }
        if (id <= _max_id) {
            if (_system_symbols != null && _system_symbols != this && id <= _system_symbols.getMaxId()) {
                name = _system_symbols.findKnownSymbol(id);
            }
            if (name == null) {
                Symbol sym = _symbols[id - _symbol_offset];
                if (sym != null) {
                    name = sym.name;
                }
            }
        }
        return name;
    }

    public int findSymbol(String name)
    {
        if (name == null || name.length() < 1) {
            throw new IllegalArgumentException("a symbol name must have something in it");
        }
        int sid = 0;
        if (_system_symbols != null && _system_symbols != this) {
            sid = _system_symbols.findSymbol(name);
        }
        if (sid == 0) {
            Integer isid = _id_map.get(name);
            if (isid != null) {
                sid = isid;
            }
            else {
                if (name.charAt(0) == '$') {
                    String sidText = name.substring(1);
                    try {
                        sid = Integer.parseInt(sidText);
                        if (sid < 0) {
                            sid = IonSymbol.UNKNOWN_SYMBOL_ID;
                        }
                        // else fall through
                    }
                    catch (NumberFormatException e)
                    {
                        if (name.startsWith(ION_RESERVED_PREFIX)) {
                            throw new InvalidSystemSymbolException(name);
                        }
                        // else fall through
                    }
                }
            }
        }
        return sid;
    }

    public String findSymbol(int id)
    {
        if (id < 1) {
            throw new IllegalArgumentException("symbol id's are greater than 0");
        }
        String name = findKnownSymbol(id);
        if (name == null) {
            name = unknownSymbolName(id);
        }
        return name;
    }

    public int addSymbol(String name)
    {
        if (_is_locked) {
            throw new IllegalStateException("can't change locked symbol table");
        }
        int sid = this.findSymbol(name);
        if (sid == UNKNOWN_SID) {
            sid = _max_id + 1;
            defineSymbol(new Symbol(name, sid, this));
        }
        return sid;
    }
    public void defineSymbol(String name, int id)
    {
        if (_is_locked) {
            throw new IllegalStateException("can't change locked symbol table");
        }
        if (name == null || name.length() < 1 || id < 1) {
            throw new IllegalArgumentException("invalid symbol definition");
        }
        int sid = this.findSymbol(name);
        if (sid != UNKNOWN_SID && sid != id) {
            throw new IllegalArgumentException("it's not valid to change a symbols id");
        }
        else if (sid == UNKNOWN_SID) {
            defineSymbol(new Symbol(name, id, this));
        }
    }
    private void defineSymbol(Symbol sym)
    {
        assert !_is_locked;

        if (sym.sid > _max_id) _max_id = sym.sid;
        int offset = sym.sid - _symbol_offset;

        if (offset >= _symbols.length) {
            int newlen = _symbol_count > 0 ? _symbol_count* 2 : 10;
            while (newlen < offset) {
                newlen *= 2;
            }
            Symbol[] temp = new Symbol[newlen];
            if (_symbol_count > 0) {
                System.arraycopy(_symbols, 0, temp, 0, _symbol_count);
            }
            _symbols = temp;
        }

        _symbols[offset] = sym;
        _id_map.put(sym.name, sym.sid);

        if (offset >= _symbol_count) _symbol_count = offset + 1;
        if (sym.source == this) _has_user_symbols = true;
    }
    public void removeSymbol(String name, int id)
    {
        if (_is_locked) {
            throw new IllegalStateException("can't change locked symbol table");
        }
        int sid = this.findSymbol(name);
        if (sid != id) {
            throw new IllegalArgumentException("id doesn't match existing id");
        }
        removeSymbol(name, id, this);
    }
    public void removeSymbol(String name)
    {
        if (_is_locked) {
            throw new IllegalStateException("can't change locked symbol table");
        }
        int sid = this.findSymbol(name);
        if (sid == UNKNOWN_SID) return;

        removeSymbol(name, sid, this);
    }
    private void removeSymbol(String name, int sid, UnifiedSymbolTable table)
    {
        assert !_is_locked;
        assert sid > 0 && sid <= _symbol_count && sid <= _max_id;

        if (_system_symbols != null && sid <= _system_symbols.getMaxId()) {
            throw new IllegalArgumentException("you can't remove system symbols");
        }

        assert _symbols[sid - _symbol_offset].name.equals(name);
        assert _symbols[sid - _symbol_offset].source == table;

        _symbols[sid - _symbol_offset] = null;
        //if (_id_map != null) {
            _id_map.remove(name);
        //}
    }

    public UnifiedSymbolTable getImportedTable(String name)
    {
        if (name == null || name.length() < 1) {
            throw new IllegalArgumentException("name must have content (non null, length > 0)");
        }
        UnifiedSymbolTable table = null;
        if (_import_count > 0) {
            for (int ii=0; ii<_import_count; ii++) {
                if (_imports[ii].getName().equals(name)) {
                    table = _imports[ii];
                    break;
                }
            }
        }
        return table;
    }

    /**
     *
     * @param newTable
     * @param maxId
     *   the largest symbol ID to import; if zero, import all symbols.
     */
    public void addImportedTable(UnifiedSymbolTable newTable, int maxId)
    {
        if (_has_user_symbols) {
            throw new IllegalStateException("importing tables is not valid once user symbols have been added");
        }
        if (_is_locked) {
            throw new IllegalStateException("importing tables is not valid on a locked table");
        }
        if (_system_symbols == null) {
            throw new IllegalStateException("a system table must be defined before importing other tables");
        }
        if (newTable == null || newTable.getName() == null) {
            throw new IllegalArgumentException("imported symbol tables must be named");
        }
        if (!newTable.isLocked()) {
            throw new IllegalArgumentException("only locked tables can be imported");
        }
        if (!_system_symbols.getSystemId().equals(newTable.getSystemId())) {
            throw new IllegalArgumentException("you can only import tables based on the same system symbols");
        }

        String name = newTable.getName();
        if (_import_count > 0) {
            for (int ii=0; ii<_import_count; ii++) {
                if (_imports[ii].getName().equals(name)) {
                    throw new IllegalArgumentException("imported symbol tables already exists");
                }
            }
        }

        if (_imports == null || _import_count >= _imports.length) {
            int newlen = _import_count > 0 ? _import_count * 2 : 10;
            UnifiedSymbolTable[] temp = new UnifiedSymbolTable[newlen];
            if (_import_count > 0) {
                System.arraycopy(_imports, 0, temp, 0, _import_count);
            }
            _imports = temp;
        }
        _imports[_import_count++] = newTable;

        int offset = _max_id - newTable.getSystemSymbolTable().getMaxId();
        int newmax = importSymbols(newTable, offset, maxId);
        if (newmax > _max_id) {
            _max_id = newmax;
            _symbol_offset = _max_id + 1;
        }

        return;
    }

    /**
     *
     * @param newTable
     * @param sidOffset
     * @param maxId
     *   the largest symbol ID to import; if zero, import all symbols.
     * @return
     */
    int importSymbols(UnifiedSymbolTable newTable, int sidOffset, int maxId)
    {
        int maxsid = -1;

        for (int ii=0; ii< newTable._symbols.length; ii++) {
            Symbol sym = newTable._symbols[ii];
            if (sym == null) continue;
            if (maxId > 0 && sym.sid > maxId ) continue;
            int sid = sym.sid + sidOffset;
            defineSymbol(new Symbol(sym.name, sid, newTable));
            if (sid > maxsid) maxsid = sid;
        }
        return maxsid;
    }
    public UnifiedSymbolTable getSystemSymbolTable()
    {
        return _system_symbols;
    }
    public void setSystemSymbolTable(SymbolTable systemSymbols)
    {
        if (_is_locked) {
            throw new IllegalStateException("can't change locked symbol table");
        }
        if (systemSymbols != null && !(systemSymbols instanceof UnifiedSymbolTable)) {
            throw new IllegalArgumentException("sorry, but even system symbol tables must be UnifiedSymbolTable's (we'll fix this type decl Real Soon Now)");
        }
        if (_system_symbols != null) {
            throw new IllegalArgumentException("sorry, you can only specify the system symbol table once");
        }
        UnifiedSymbolTable usystemSymbols = null;
        if (systemSymbols != null) {
            usystemSymbols = (UnifiedSymbolTable)systemSymbols;
            for (int ii=0; ii<usystemSymbols._symbols.length; ii++) {
                Symbol sym = usystemSymbols._symbols[ii];
                if (sym == null) continue;
                defineSymbol(new Symbol(sym.name, sym.sid, usystemSymbols));
            }
        }
        _system_symbols = usystemSymbols;
    }

    public boolean hasImports()
    {
        return (_import_count > 0);
    }

    //
    // TODO: there needs to be a better way to associate a System with
    //       the symbol table, which is required if someone is to be
    //       able to generate an instance.  The other way to resolve
    //       this dependancy would be for the IonSystem object to be
    //       able to take a UnifiedSymbolTable and synthisize an Ion
    //       value from it, by using the public API's to see the useful
    //       contents.  But what about open content?  If the origin of
    //         the symbol table was an IonValue you could get the sys
    //         from it, and update it, thereby preserving any extra bits.
    //         If, OTOH, it was sythesized from scratch (a common case)
    //         then extra content doesn't matter.
    //
    IonSystem _sys_holder = null;
    public void setSystem(IonSystem sys) {
        _sys_holder = sys;
    }
    public IonSystem getSystem() {
        return _sys_holder;
    }
    public UnifiedSymbolTable[] getImportedTables()
    {
        UnifiedSymbolTable[] imports = null;
        if (this._import_count > 0) {
            imports = new UnifiedSymbolTable[this._import_count];
            for (int ii=0; ii<_import_count; ii++) {
                imports[ii] = _imports[ii];
            }
        }
        return imports;
    }

    public IonStruct getIonRepresentation() {
        return getIonRepresentation(_sys_holder);
    }
    public IonStruct getIonRepresentation(IonSystem sys)
    {
        IonStruct itable = sys.newEmptyStruct();
        itable.addTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE);
        if (this.getName() != null) {
            if (this.getVersion() < 1) this.setVersion(1);
            itable.add(UnifiedSymbolTable.NAME, sys.newString(this.getName()));
            itable.add(UnifiedSymbolTable.VERSION, sys.newInt(this.getVersion()));
        }
        itable.add(UnifiedSymbolTable.MAX_ID, sys.newInt(this.getMaxId()));
        UnifiedSymbolTable[] imports = this.getImportedTables();
        if (imports != null && imports.length > 0) {
            IonList imports_as_ion = sys.newEmptyList();
            for (int ii=0; ii<_import_count; ii++) {
                UnifiedSymbolTable imptable = _imports[ii];
                IonStruct imp = sys.newEmptyStruct();
                imp.add(UnifiedSymbolTable.NAME, sys.newString(imptable.getName()));
                imp.add(UnifiedSymbolTable.VERSION, sys.newInt(imptable.getVersion()));
                imp.add(UnifiedSymbolTable.MAX_ID, sys.newInt(imptable.getMaxId()));
                imports_as_ion.add(imp);
            }
        }
        IonStruct symlist = null;
        for (int ii=0; ii<this._symbols.length; ii++) {
            Symbol sym = this._symbols[ii];
            if (sym == null || sym.source != this) continue;
            if (symlist == null) {
                symlist = sys.newEmptyStruct();
            }
            symlist.add("$"+sym.sid, sys.newString(sym.name));
        }
        if (symlist != null) {
            itable.add(UnifiedSymbolTable.SYMBOLS, symlist);
        }
        return itable;
    }
    public boolean isCompatible(SymbolTable other)
    {
        UnifiedSymbolTable master;
        UnifiedSymbolTable candidate;

        if (!(other instanceof UnifiedSymbolTable)) {
            throw new IllegalArgumentException("sorry, both instances must be UnifiedSymbolTable's");
        }

        master = this;
        candidate = (UnifiedSymbolTable)other;

        for (int ii=0; ii<candidate._symbols.length; ii++) {
            Symbol sym = candidate._symbols[ii];
            if (sym == null) continue;
            int id = master.findSymbol(sym.name);
            if (id != sym.sid) {
                return false;
            }
        }

        return true;
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

}
