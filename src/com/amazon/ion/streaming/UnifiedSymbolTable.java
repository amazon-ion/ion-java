/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.InvalidSystemSymbolException;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.impl.IonBinary;
import com.amazon.ion.impl.IonConstants;
import com.amazon.ion.impl.IonStructImpl;
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
    implements SymbolTable
{

    public final int UNKNOWN_SID = -1;
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

        systab._system_symbols = systab;

        systab.setName(SystemSymbolTable.ION);
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
        public Symbol(String symbolName, int symbolId,
                      UnifiedSymbolTable sourceTable)
        {
            name   = symbolName;
            sid    = symbolId;
            source = sourceTable;
            name_len = IonBinary.lenIonString(symbolName);
            sid_len  = IonBinary.lenVarUInt7(sid);
            td_len   = IonBinary.lenLenFieldWithOptionalNibble(name_len);
            td_len  += IonConstants.BB_TOKEN_LEN;
        }
        @Override
        public String toString() {
            return "Symbol:"+sid+(name != null ? "-"+name : "");
        }
    }

    String                  _name;
    int                     _version;
    UnifiedSymbolTable      _system_symbols;
    UnifiedSymbolTable[]    _imports;
    int                     _import_count;

    /**
     * All symbols from the system symtab, imported tables, and locals.
     * The index in the array is the sid.
     */
    Symbol[]                _symbols;
    int                     _max_id;
    boolean                 _has_user_symbols;
    boolean                 _is_locked;

    HashMap<String, Integer> _id_map;

    IonStructImpl            _ion_rep;
    IonStruct                _ion_symbols_rep;


    private UnifiedSymbolTable() {
        _name = null;
        _version = 0;
        _system_symbols = null;
        _imports = null;
        _import_count = 0;
        _symbols = new Symbol[10];
        _max_id = 0;
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
    }

/* TODO not quite ready for this...
    public UnifiedSymbolTable(UnifiedSymbolTable systemSymbols,
                              IonStruct ionRep,
                              IonCatalog catalog)
    {
        this(systemSymbols);

        IonReader reader = new IonTreeIterator(ionRep);
//        reader.next();
//        reader.stepInto();
        readIonRep(reader, catalog);

        _ion_rep = (IonStructImpl) ionRep;
    }
*/

    /**
     * @param reader must be positioned on the first field of the struct.
     */
    public UnifiedSymbolTable(UnifiedSymbolTable systemSymbols,
                              IonReader reader,
                              IonCatalog catalog)
    {
        this(systemSymbols);
        readIonRep(reader, catalog);
    }

    // TODO this should be removed during integration !!
    private UnifiedSymbolTable(SymbolTable symboltable, boolean dummy) {
        this(getSystemSymbolTableInstance());

        String name = symboltable.getName();
        if (name != null)
        {
            _name = name;
            _version = symboltable.getVersion();
            assert _version > 0;
        }

        int minid = this._system_symbols.getMaxId();
        int maxid = symboltable.getMaxId();
        for (int ii=minid + 1; ii <= maxid; ii++) {
            String symbolText = symboltable.findKnownSymbol(ii);
            // FIXME shouldn't happen, we decided to not allow removing symbols
            if (symbolText == null) continue;
            this.defineSymbol(symbolText, ii);
        }
    }


    /**
     * Avoids a scary and bug-prone overload of constructor on
     * UnifiedSymbolTable versus SymbolTable.
     * @deprecated TODO remove after integrating symtab implementations
     */
    @Deprecated
    public static UnifiedSymbolTable copyFrom(SymbolTable symbolTable)
    {
        return new UnifiedSymbolTable(symbolTable, true);
    }

    public static UnifiedSymbolTable getSystemSymbolTableInstance()
    {
        return _system_1_0_symbols;
    }

    public void lock() {
        if (_name == null) {
            throw new IllegalStateException("Symbol table has no name");
        }
        if (_version < 1) {
            throw new IllegalStateException("Symbol table has no version");
        }
        _is_locked = true;
        // TODO validate that maxId is legal?
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

    public boolean isLocalTable() {
        return ! _is_locked;
    }

    public boolean isSharedTable() {
        return _is_locked;
    }

    public boolean isSystemTable() {
        // the is locked test is a short cut since most tables are local and
        // locked, therefore the bool gets us out of here in a hurry
        return (_is_locked && SystemSymbolTable.ION.equals(_name));
    }

    public int size()
    {
        // TODO: what is the size?  is it the number of symbols?
        //       number of locally defined symbols? maximum symbol
        //       id (which counts "null symbols")?  We'll start
        //       with max id, which is the largest and think about it.
        int lowBound = _system_symbols.getMaxId();
        for (int i = 0; i < _import_count; i++)
        {
            UnifiedSymbolTable table = _imports[i];

            // FIXME this is wrong: we need the declared max_id
            lowBound += table.getMaxId();
            throw new UnsupportedOperationException();
        }
        return _max_id - lowBound;
    }
    public int getMaxId()
    {
        return _max_id;
    }
    public void setMaxId(int maxId)  // FIXME in-use but nonfunctional
    {
        // TODO when can maxId==0 ?
        if (maxId < 0) {
            throw new IllegalArgumentException("symbol id's are greater than 0, and maxId must be at least 0");
        }
        if (_is_locked) {
            throw new IllegalStateException("can't change shared symbol table");
        }

    }
    public void setVersion(int version)
    {
        if (version < 0) {
            throw new IllegalArgumentException("versions must be integers of value 1 or higher, or 0 for 'no version'");
        }
        if (_is_locked) {
            throw new IllegalStateException("can't change shared symbol table");
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
            throw new IllegalStateException("can't change shared symbol table");
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
                Symbol sym = _symbols[id];
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
        int sid = IonSymbol.UNKNOWN_SYMBOL_ID;
        if (_system_symbols != null && _system_symbols != this) {
            sid = _system_symbols.findSymbol(name);
        }
        if (sid == IonSymbol.UNKNOWN_SYMBOL_ID) {
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
                        if (name.startsWith(SystemSymbolTable.ION_RESERVED_PREFIX)) {
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
            throw new IllegalStateException("can't change shared symbol table");
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
            throw new IllegalStateException("can't change shared symbol table");
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

        final int sid = sym.sid;

        if (sid >= _symbols.length) {
            int newlen = _max_id > 0 ? _max_id * 2 : 10;
            while (newlen < sid) {
                newlen *= 2;
            }
            Symbol[] temp = new Symbol[newlen];
            if (_max_id > 0) {
                System.arraycopy(_symbols, 0, temp, 0, _max_id + 1);
            }
            _symbols = temp;
        }

        _symbols[sid] = sym;
        _id_map.put(sym.name, sid);

        if (sid > _max_id) _max_id = sid;
        if (sym.source == this) {
            _has_user_symbols = true;
            if (_ion_rep != null) {
                recordLocalSymbolInIonRep(sym);
            }
        }
    }

    public void removeSymbol(String name, int id)
    {
        if (_is_locked) {
            throw new IllegalStateException("can't change shared symbol table");
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
            throw new IllegalStateException("can't change shared symbol table");
        }
        int sid = this.findSymbol(name);
        if (sid == UNKNOWN_SID) return;

        removeSymbol(name, sid, this);
    }
    private void removeSymbol(String name, int sid, UnifiedSymbolTable table)
    {
        assert !_is_locked;
        assert sid > 0 && sid <= _max_id;

        if (_system_symbols != null && sid <= _system_symbols.getMaxId()) {
            throw new IllegalArgumentException("you can't remove system symbols");
        }

        assert _symbols[sid].name.equals(name);
        assert _symbols[sid].source == table;

        _symbols[sid] = null;
        //if (_id_map != null) {
            _id_map.remove(name);
        //}
        if (table == this && _ion_symbols_rep != null) {
            _ion_symbols_rep.removeAll("$" + sid);
        }
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
     * @param declaredMaxId
     *   the largest symbol ID to import; if zero, import all symbols.
     */
    public void addImportedTable(UnifiedSymbolTable newTable, int declaredMaxId)
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
        if (!newTable.isSharedTable()) {
            throw new IllegalArgumentException("only shared tables can be imported");
        }
        // FIXME this precondition will make it hard to change system symtab
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

        // FIXME this could break when system symtab is updated.
        // The shared table isn't serialized with any systemid, so there's no
        // way to know where the defined symbols start.
        // If newtable has sid=10 (right beyond the 1.0 symtab) defined
        // but the current system 1.1 has 10 defined to something else, then
        // the offset will be incorrect.

        int offset = _max_id - newTable.getSystemSymbolTable().getMaxId();
        int newmax = importSymbols(newTable, offset, declaredMaxId);
        if (newmax > _max_id) {
            _max_id = newmax;
        }
    }

    /**
     *
     * @param newTable
     * @param sidOffset
     *   will be added to each sid in newTable to derive the sid in this table.
     * @param declaredMaxId
     *   the largest symbol ID to import; if zero, import all symbols.
     *   This is the declared max_id on the import declaration.
     * @return
     */
    int importSymbols(UnifiedSymbolTable newTable, int sidOffset, int declaredMaxId)
    {
        int maxsid = -1;

        // TODO limit by newTable._max_id instead?
        for (int ii=0; ii< newTable._symbols.length; ii++) {
            Symbol sym = newTable._symbols[ii];
            if (sym == null) continue;
            if (declaredMaxId > 0 && sym.sid > declaredMaxId ) continue;
              // TODO break above instead of continue?
            int sid = sym.sid + sidOffset;
            defineSymbol(new Symbol(sym.name, sid, newTable));
            if (sid > maxsid) maxsid = sid;
        }
        return maxsid;
    }
    public SymbolTable getSystemSymbolTable()
    {
        return _system_symbols;
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
        if (isSharedTable()) return null;

        UnifiedSymbolTable[] imports =
            new UnifiedSymbolTable[this._import_count];
        for (int ii=0; ii<_import_count; ii++) {
            imports[ii] = _imports[ii];
        }
        return imports;
    }

    public IonStruct getIonRepresentation() {
        return getIonRepresentation(_sys_holder);
    }

    public IonStruct getIonRepresentation(IonSystem sys)
    {
        if (_ion_rep != null) return _ion_rep;

        _ion_rep = (IonStructImpl) sys.newEmptyStruct();

        // TODO this replicates LocalSymbolTableImpl behavior, but its creepy.
        _ion_rep.setSymbolTable(this);

        _ion_rep.addTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE);
        if (this.isSharedTable()) {
            if (this.getVersion() < 1) this.setVersion(1);
            _ion_rep.add(UnifiedSymbolTable.NAME, sys.newString(this.getName()));
            _ion_rep.add(UnifiedSymbolTable.VERSION, sys.newInt(this.getVersion()));
            _ion_rep.add(UnifiedSymbolTable.MAX_ID, sys.newInt(this.getMaxId()));
            // TODO do we need to save max_id for local tables?
        }

        UnifiedSymbolTable[] imports = this.getImportedTables();
        assert (this.isLocalTable() || imports == null);

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

        // TODO improve compression by avoiding empty/null symbols field
        _ion_symbols_rep = sys.newNullStruct();
        _ion_rep.add(UnifiedSymbolTable.SYMBOLS, _ion_symbols_rep);
        for (int ii=0; ii<this._symbols.length; ii++) {
            Symbol sym = this._symbols[ii];

            // Ignore imported symbols
            if (sym == null || sym.source != this) continue;

            recordLocalSymbolInIonRep(sym);
        }

        return _ion_rep;
    }


    private void recordLocalSymbolInIonRep(Symbol sym)
    {
        assert sym.source == this;

        IonSystem system = _ion_rep.getSystem();
        _ion_symbols_rep.add("$"+sym.sid, system.newString(sym.name));
    }


    private void readIonRep(IonReader reader, IonCatalog catalog)
    {
        assert reader.isInStruct();

        String name = null;
        int version = 1;

        while(reader.hasNext()) {
            reader.next();
            switch (reader.getFieldId()) {
            case UnifiedSymbolTable.VERSION_SID:
                 version = reader.intValue();
                break;
            case UnifiedSymbolTable.MAX_ID_SID:
                setMaxId(reader.intValue());  // FIXME this does nothing
                break;
            case UnifiedSymbolTable.NAME_SID:
                name = reader.stringValue();
                break;
            case UnifiedSymbolTable.SYMBOLS_SID:
                boolean manual_sid;
                switch (reader.getType()) {
                case STRUCT:
                    manual_sid = true;
                    break;
                case LIST:
                    manual_sid = false;
                    break;
                default:
                    throw new IonException("the symbols field of a symbol table must be a list or a struct value, not a "+reader.getType());
                }
                reader.stepInto();
                while (reader.hasNext()) {
                    if (reader.next() != IonType.STRING) {
                        continue; // we could error here, but open content says don't bother
                    }
                    int sid;
                    if (manual_sid) {
                        sid = reader.getFieldId();
                    }
                    else {
                        sid = getMaxId() + 1;
                    }
                    String symbol = reader.stringValue();
                    defineSymbol(symbol, sid);
                }
                reader.stepOut();
                break;
            case UnifiedSymbolTable.IMPORTS_SID:
                readImportList(reader, catalog);
                break;
            default:
                break;
            }
        }

        // set name/version and lock
        if (name != null) {
            // TODO throw if version < 1
            setName(name);
            setVersion(version);
            lock();
        }
    }


    private void readImportList(IonReader reader, IonCatalog catalog)
    {
        assert (reader.getFieldId() == SystemSymbolTable.IMPORTS_SID);
        assert (reader.getType().equals(IonType.LIST));

        reader.stepInto();
        while (reader.hasNext()) {
            IonType t = reader.next();
            if (IonType.STRUCT.equals(t)) {
                readOneImport(reader, catalog);
            }
        }
        reader.stepOut();
    }

    private void readOneImport(IonReader ionRep, IonCatalog catalog)
    {
        // assert (this.getFieldId() == SystemSymbolTable.IMPORTS_SID);
        assert (ionRep.getType().equals(IonType.STRUCT));

        String name = null;
        int    version = -1;
        int    maxid = -1;

        ionRep.stepInto();
        while (ionRep.hasNext()) {
            IonType t = ionRep.next();
            switch(ionRep.getFieldId()) {
                case UnifiedSymbolTable.NAME_SID:
                    if (IonType.STRING.equals(t) || IonType.SYMBOL.equals(t)) {
                        name = ionRep.stringValue();
                    }
                    else throw new IonException("Symbol Table Import Name is not a string, it's a "+t.toString());
                    break;
                case UnifiedSymbolTable.VERSION_SID:
                    if (IonType.INT.equals(t)) {
                        version = ionRep.intValue();
                    }
                    else throw new IonException("Symbol Table Import Version is not an int, it's a "+t.toString());
                    break;
                case UnifiedSymbolTable.MAX_ID_SID:
                    if (IonType.INT.equals(t)) {
                        maxid = ionRep.intValue();
                    }
                    else throw new IonException("Symbol Table Import Max ID is not an int, it's a "+t.toString());
                    break;
                default:
                    // we just ignore anything else as "open content"
                    break;
            }
        }
        ionRep.stepOut();

        // TODO throw if name == null
        UnifiedSymbolTable itab = null;
        if (version != -1) {
            itab = (UnifiedSymbolTable) catalog.getTable(name, version);
        }
        else {
            itab = (UnifiedSymbolTable) catalog.getTable(name);
        }
        if (itab == null) {
            throw new IonException("Import Symbol Table not found: "
                                   + name
                                   + ( (version == -1) ? "" : " version: "+version)
            );
        }
        addImportedTable(itab, (maxid == -1) ? itab.getMaxId() : maxid);
    }


    public boolean isCompatible(SymbolTable other)
    {
        UnifiedSymbolTable master;
        UnifiedSymbolTable candidate;

        if (!(other instanceof UnifiedSymbolTable)) {
            return false;
//          throw new IllegalArgumentException("sorry, both instances must be UnifiedSymbolTable's");
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
