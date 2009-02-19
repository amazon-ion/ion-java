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
final class UnifiedSymbolTable
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

    private final static UnifiedSymbolTable _system_1_0_symbols;
    static {
        UnifiedSymbolTable systab = new UnifiedSymbolTable();

        for (int ii=0; ii<SYSTEM_SYMBOLS.length; ii++) {
            systab.defineSymbol(new Symbol(SYSTEM_SYMBOLS[ii], ii+1, systab));
        }

        systab.share(SystemSymbolTable.ION, 1);
        systab._system_symbols = systab;
        _system_1_0_symbols = systab;
    }

    public static class Symbol {
        public int                sid;
        public String             name;
        public int                name_len, td_len;
        public UnifiedSymbolTable source;
        public Symbol() {}
        public Symbol(String symbolName, int symbolId,
                      UnifiedSymbolTable sourceTable)
        {
            name   = symbolName;
            sid    = symbolId;
            source = sourceTable;
            name_len = IonBinary.lenIonString(symbolName);
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

    /**
     * The largest sid allocated to imported symbols.
     * Local symbols start at the next highest value.
     */
    private int _import_max_id;

    boolean                 _has_user_symbols;
    boolean                 _is_locked;

    HashMap<String, Integer> _id_map;

    IonStructImpl            _ion_rep;
    IonList                  _ion_symbols_rep;


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

    /**
     * Constructs an empty local symbol table.
     *
     * @param systemSymbols must be a system symbol table.
     */
    public UnifiedSymbolTable(SymbolTable systemSymbols) {
        this();
        if (!systemSymbols.isSystemTable()) {
            throw new IllegalArgumentException();
        }
        if (! (systemSymbols instanceof UnifiedSymbolTable)) {
            throw new IllegalArgumentException();
        }
        _system_symbols = (UnifiedSymbolTable) systemSymbols;
        importSymbols(_system_symbols, 0, -1);
        assert _max_id == systemSymbols.getMaxId();
    }


    /**
     * Constructs a local symbol table.
     *
     * @param ionRep
     * @param catalog
     */
    public UnifiedSymbolTable(IonStruct ionRep,
                              IonCatalog catalog)
    {
        this(ionRep.getSymbolTable().getSystemSymbolTable());

        IonReader reader = new IonTreeReader(ionRep);
        reader.next();
        reader.stepIn();
        readIonRep(SymbolTableType.LOCAL, reader, catalog);

        _ion_rep = (IonStructImpl) ionRep;
    }

    /**
     * Constructs a shared symbol table.
     * @param ionRep
     */
    public UnifiedSymbolTable(IonStruct ionRep)
    {
        this();

        IonReader reader = new IonTreeReader(ionRep);
        reader.next();
        reader.stepIn();
        readIonRep(SymbolTableType.SHARED, reader, null);

        assert _is_locked;

        _ion_rep = (IonStructImpl) ionRep;
    }


    /**
     * Constructs a local symbol table.
     * @param reader must be positioned on the first field of the struct.
     * That's a bit odd but it's currently necessary due to state management
     * in the binary reader.
     */
    public UnifiedSymbolTable(SymbolTable systemSymbols,
                              IonReader reader,
                              IonCatalog catalog)
    {
        this(systemSymbols);
        readIonRep(SymbolTableType.LOCAL, reader, catalog);
    }

    /**
     * Constructs a shared symbol table with given symbols.
     * Null values returned by the iterator cause undefined sids.
     */
    UnifiedSymbolTable(String name, int version, Iterator<String> symbols)
    {
        this();

        while (symbols.hasNext())
        {
            String text = symbols.next();
            if (findSymbol(text) == UNKNOWN_SID)
            {
                int sid = _max_id + 1;
                defineSymbol(new Symbol(text, sid, this));
            }
        }

        share(name, version);
    }

    public static UnifiedSymbolTable getSystemSymbolTableInstance()
    {
        return _system_1_0_symbols;
    }

    public void share(String name, int version)
    {
        if (name == null || name.length() == 0) {
            throw new IllegalArgumentException("name must be non-empty");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be at least 1");
        }
        if (_is_locked) {
            throw new IllegalStateException("already shared");
        }

        _name = name;
        _version = version;
        _system_symbols = null;
        _is_locked = true;
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

    public boolean isTrivial()
    {
        return (_is_locked
                ? _max_id == 0
                : (!_has_user_symbols && _import_count == 0));
    }

    @Deprecated
    public int size()
    {
        int lowBound =
            (_system_symbols == null ? 0 :_system_symbols.getMaxId());

        for (int i = 0; i < _import_count; i++)
        {
            UnifiedSymbolTable table = _imports[i];

            // FIXME this is wrong: we need the declared max_id
            lowBound += table.getMaxId();
            throw new UnsupportedOperationException();
        }
        return _max_id - lowBound;
    }

    int getImportMaxId()
    {
        return _import_max_id;
    }

    public int getMaxId()
    {
        return _max_id;
    }

    public int getVersion()
    {
        return _version;
    }

    public String getName()
    {
        return _name;
    }

    @Deprecated
    public String getSystemId()
    {
        return getIonVersionId();
    }

    public String getIonVersionId()
    {
        if (this._system_symbols == null) return null;

        if (this._system_symbols != this) {
            return this._system_symbols.getIonVersionId();
        }
        assert isSystemTable();
        return SystemSymbolTable.ION_1_0;
    }


    public synchronized Iterator<String> iterateDeclaredSymbols()
    {
        return new Iterator<String>()
        {
            int myCurrentId = _import_max_id;
            final int myMaxId = _max_id;

            public boolean hasNext()
            {
                return myCurrentId < myMaxId;
            }

            public String next()
            {
                return findKnownSymbol(++myCurrentId);
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
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
        int sid = UNKNOWN_SYMBOL_ID;
        if (_system_symbols != null && _system_symbols != this) {
            sid = _system_symbols.findSymbol(name);
        }
        if (sid == UNKNOWN_SYMBOL_ID) {
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
                            sid = UNKNOWN_SYMBOL_ID;
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
        int sid = this.findSymbol(name);
        if (sid == UNKNOWN_SID) {
            if (_is_locked) {
                throw new UnsupportedOperationException("can't change shared symbol table");
            }
            sid = _max_id + 1;
            defineSymbol(new Symbol(name, sid, this));
        }
        return sid;
    }
    public void defineSymbol(String name, int id)
    {
        if (_is_locked) {
            throw new UnsupportedOperationException("can't change shared symbol table");
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

        // TODO disallow using sid within imports range
    }

// TODO streamline: we no longer accept arbirtrary sid, its always max+1
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
                int length = Math.min(_max_id + 1, _symbols.length);
                System.arraycopy(_symbols, 0, temp, 0, length);
            }
            _symbols = temp;
        }
        else if (_symbols[sid] != null) {
            String message =
                "Cannot redefine $" + sid + " from "
                + printQuotedSymbol(_symbols[sid].name)
                + " to " + printQuotedSymbol(sym.name);
            throw new IonException(message);
        }

        _symbols[sid] = sym;
        Integer priorSid = _id_map.put(sym.name, sid);
        if (priorSid != null) {
            if (priorSid < sid) {
                // Ignore this attempted re-definition
                _id_map.put(sym.name, priorSid);
                _symbols[sid] = null;
            }
            else {
                // Replace existing definition with higher sid
                // TODO test this!
                _symbols[priorSid] = null;
            }
        }

        // FIXME lexicographic selection if sid is duplicated
        // Be sure to only sort within one import!  We can

        if (sid > _max_id) _max_id = sid;
        if (sym.source == this) {
            _has_user_symbols = true;
            if (_ion_rep != null) {
                recordLocalSymbolInIonRep(sym);
            }
        }
    }

    private void defineLocalSymbol(Symbol sym/*, int firstLocalSid*/)
    {
        assert !_is_locked;

        final int sid = sym.sid;

        if (sid <= _import_max_id) {
            // Attempted override of imported symbol
            // FIXME still need to insert pad in local rep
            //   for the case where this is being called on load
            return;
        }

        defineSymbol(sym);
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
     *   the largest symbol ID to import; if less than zero, import all symbols.
     */
    public void addImportedTable(UnifiedSymbolTable newTable, int declaredMaxId)
    {
        if (_has_user_symbols) {
            throw new IllegalStateException("importing tables is not valid once user symbols have been added");
        }
        if (_is_locked) {
            throw new UnsupportedOperationException("importing tables is not valid on a shared table");
        }
        if (_system_symbols == null) {
            throw new IllegalStateException("a system table must be defined before importing other tables");
        }
        if (newTable == null || newTable.getName() == null) {
            throw new IllegalArgumentException("imported symbol tables must be named");
        }
        if (newTable.isLocalTable() || newTable.isSystemTable()) {
            throw new IllegalArgumentException("only non-system shared tables can be imported");
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

        importSymbols(newTable, _max_id, declaredMaxId);
    }

    /**
     *
     * @param newTable
     * @param sidOffset
     *   will be added to each sid in newTable to derive the sid in this table.
     * @param declaredMaxId
     *   the largest symbol ID to import; if less than zero, import all symbols.
     *   This is the declared max_id on the import declaration.
     */
    private void importSymbols(UnifiedSymbolTable newTable, int sidOffset,
                               int declaredMaxId)
    {
        // Always use the declaredMaxId so sid computations are the same even
        // when the imported table cannot be found or when we only have an
        // older version that's missing some symbols.

        if (declaredMaxId < 0) {
            declaredMaxId = newTable.getMaxId();
            assert declaredMaxId >= 0;
        }

        int priorMaxId = _max_id;

        assert newTable._max_id < newTable._symbols.length;
        assert newTable._symbols[0] == null;

        int limitId = Math.min(newTable._max_id, declaredMaxId);
        for (int ii = 1; ii <= limitId; ii++) {
            Symbol sym = newTable._symbols[ii];
            if (sym == null) continue;
            assert sym.sid == ii;
            int sid = ii + sidOffset;
            defineSymbol(new Symbol(sym.name, sid, newTable));
        }

        int newMaxId = priorMaxId + declaredMaxId;
        assert _max_id <= newMaxId;
        assert _import_max_id <= newMaxId;
        _max_id = newMaxId;
        _import_max_id = newMaxId;
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


    public void writeTo(IonWriter writer) throws IOException
    {
        IonStruct rep = _ion_rep;
        if (rep == null)
        {
            // FIXME don't create a new system.
            rep = makeIonRepresentation(SystemFactory.newSystem());
        }

        writer.writeValue(rep);
    }

    public IonStruct getIonRepresentation() {
        return getIonRepresentation(_sys_holder);
    }

    @Deprecated
    public IonStruct getIonRepresentation(IonSystem sys)
    {
        if (_ion_rep != null) return _ion_rep;

        _ion_rep = makeIonRepresentation(sys);
        _ion_symbols_rep = (IonList) _ion_rep.get(SYMBOLS);
        return _ion_rep;
    }

    IonStructImpl makeIonRepresentation(IonSystem sys)
    {
        IonStructImpl ionRep = (IonStructImpl) sys.newEmptyStruct();
        ionRep.addTypeAnnotation(UnifiedSymbolTable.ION_SYMBOL_TABLE);

        if (this.isSharedTable()) {
            assert getVersion() > 0;
            ionRep.add(UnifiedSymbolTable.NAME, sys.newString(this.getName()));
            ionRep.add(UnifiedSymbolTable.VERSION, sys.newInt(this.getVersion()));
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
            ionRep.add(IMPORTS, imports_as_ion);
        }

        if (_max_id > _import_max_id)
        {
            _ion_rep = ionRep;  // FIXME ugly hack to enable recordLocal... below
            IonList symbolsList = sys.newEmptyList();
            ionRep.add(UnifiedSymbolTable.SYMBOLS, symbolsList);
            for (int sid = _import_max_id+1; sid <= _max_id; sid++) {
                Symbol sym = this._symbols[sid];

                if (sym == null) continue;
                assert sym.source == this;

                recordLocalSymbolInIonRep(sym);
            }
            _ion_rep = null;
        }

        return ionRep;
    }


    private void recordLocalSymbolInIonRep(Symbol sym)
    {
        assert sym.source == this;
        assert sym.sid > _import_max_id;

        IonSystem system = _ion_rep.getSystem();

        if (_ion_symbols_rep == null) {
            IonValue syms = _ion_rep.get(UnifiedSymbolTable.SYMBOLS);
            if (syms == null || syms.getType() != IonType.LIST) {
                syms = system.newEmptyList();
                _ion_rep.put(UnifiedSymbolTable.SYMBOLS, syms);
            }
            _ion_symbols_rep = (IonList) syms;
        }

        int index = sym.sid - _import_max_id - 1;

        while (_ion_symbols_rep.size() < index) {
            _ion_symbols_rep.add(system.newNull());
        }

        _ion_symbols_rep.add(system.newString(sym.name));
    }


    private void readIonRep(SymbolTableType symtabType,
                            IonReader reader,
                            IonCatalog catalog)
    {
        assert reader.isInStruct();

        String name = null;
        int version = -1;

        ArrayList<String> symbols = null;

        while(reader.hasNext()) {
            IonType fieldType = reader.next();

            if (reader.isNullValue()) continue;

            int fieldId = reader.getFieldId();
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
                // FIXME fail if field ID < 1
//                if (fieldId < 1) throw new IllegalStateException();
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

            assert _max_id == 0;
            assert _system_symbols == null;
            assert _id_map.isEmpty();

            if (symbols != null)
            {
                int sid = 0;
                for (String symbolText : symbols) {
                    // Allocate a sid even when the symbols's text is malformed
                    sid++;
                    Symbol sym = new Symbol(symbolText, sid, this);
                    defineSymbol(sym);
                }
            }

            if (version < 1) {
                version = 1;
            }

            share(name, version);
        }
        else {
            assert _import_max_id >= SystemSymbolTable.ION_1_0_MAX_ID;
            if (symbols != null)
            {
                int sid = _max_id;
                for (String symbolText : symbols) {
                    // Allocate a sid even when the symbols's text is malformed
                    sid++;
                    Symbol sym = new Symbol(symbolText, sid, this);
                    defineLocalSymbol(sym);
                }
            }
        }
    }


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
     * @param catalog may be null
     */
    private void readOneImport(IonReader ionRep, IonCatalog catalog)
    {
        assert (ionRep.getType() == IonType.STRUCT);

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
            itab._max_id = maxid;
            itab.share(name, version);
        }

        addImportedTable(itab, maxid);
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
        StringBuilder buf = new StringBuilder("[UnifiedSymbolTable ");
        if (isSharedTable()) {
            buf.append(_name);
            buf.append(' ');
            buf.append(_version);
        }
        else {
            buf.append("local");
        }
        buf.append(']');
        return buf.toString();
    }
}
