// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.impl.IonImplUtils.EMPTY_STRING_ARRAY;
import static com.amazon.ion.impl.UnifiedSymbolTable.isNonSystemSharedTable;
import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;
import static com.amazon.ion.util.Equivalence.ionEquals;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonBinary.Reader;
import com.amazon.ion.impl.IonBinary.Writer;
import com.amazon.ion.util.Printer;
import java.io.IOException;

/**
 *
 */
public abstract class IonValueImpl
    implements IonValuePrivate
{
    /**
     * We could multiplex this with member id, but it adds way more complexity
     * than it saves space.
     * <p>
     * WARNING: This member can change even when the value is read-only, since
     * the container may be mutable and have values added or removed.
     */
    protected int    _elementid;
    private String   _fieldName;
    private String[] _annotations;

    //
    //      | td w vlen | value |
    //      | td | vlen | value |
    //      | an w tlen | alen | ann | td w vlen | value |
    //      | an w tlen | alen | ann | td | vlen | value |
    //      | an | tlen | alen | ann | td w vlen | value |
    //      | an | tlen | alen | ann | td | vlen | value |
    //
    //      | mid | td w vlen | value |
    //      | mid | td | vlen | value |
    //      | mid | an w tlen | alen | ann | td w vlen | value |
    //      | mid | an w tlen | alen | ann | td | vlen | value |
    //      | mid | an | tlen | alen | ann | td w vlen | value |
    //      | mid | an | tlen | alen | ann | td | vlen | value |
    //      |     |                        |                   |
    //       ^ entry_start                  ^ value_td_start
    //             ^ [+ len(mid)]   value_content_start ^
    //                                               next_start ^
    //
    private int _fieldSid;        // field symbol id in buffer

    /**
     * The actual TD byte of the value.
     * This is always a positive value between 0x00 and 0xFF.
     * <p>
     * TODO document when the low-nibble is known to be correct.
     */
    private int _type_desc;

    /**
     * Offset of the inital TD, possibly an annotation wrapper TD.
     * May be -1 to indicate that the real position is unknown; the other
     * position fields will then be relative to that faux position.
     */
    private int _entry_start;
    private int _value_td_start;    // offset of td of the actual value
    private int _value_content_start;// offset of td of the actual value

    // TODO should be private but I needed to change it in DatagramImpl ctor
    protected int _next_start;        // offset of the next value (end + 1)

    /**
     * states for a value:
     *
     * materialized - value has been loaded from the buffer into the native
     *                  value, positionLoaded is a precondition for a value
     *                  to be materialized. This is false if there is no buffer.
     *
     * positionLoaded - the type descriptor information has been loaded from
     *                  the buffer.  This is always true if the value is
     *                  materialized.  And always false if there is no buffer.
     *
     * hasNativeValue - the Java value has been set, either directly or
     *                  through materialization.
     *
     * isDirty - the value in the buffer does not match the value in the
     *                  native reference.  This is always true when there is
     *                  no buffer.
     */

    /**
     * this hold all the various boolean flags we have
     * in a single int.  Use set_flag(), clear_flag(), is_true()
     * and the associated int flag to check the various flags.
     * This is to avoid the overhead java seems to impose
     * for a boolean value - it should be a bit, but it seems
     * to be an int (4 bytes for 1 bit seems excessive).
     */
    private short _flags;

    private static final int IS_MATERIALIZED    = 0x01;
    private static final int IS_POSITION_LOADED = 0x02;
    private static final int HAS_NATIVE_VALUE   = 0x04;
    private static final int IS_DIRTY           = 0x08;
    private static final int IS_LOCKED          = 0x10;
    private static final int IS_SYSTEM_VALUE    = 0x20;
    private static final int IS_NULL_VALUE      = 0x40;
    private static final int IS_BOOL_TRUE       = 0x80;


    private final boolean is_true(int flag_bit)
    {
        // FIXME: removed, dead code for debugging
        if (flag_bit == IS_BOOL_TRUE) {
            flag_bit = IS_BOOL_TRUE;
        }
        return ((_flags & flag_bit) != 0);
    }
    private final void set_flag(int flag_bit)
    {
        // FIXME: removed, dead code for debugging
        if (flag_bit == IS_BOOL_TRUE) {
            flag_bit = IS_BOOL_TRUE;
        }
        assert(flag_bit != 0);
        _flags |= flag_bit;
    }
    private final void clear_flag(int flag_bit)
    {
        // FIXME: removed, dead code for debugging
        if (flag_bit == IS_BOOL_TRUE) {
            flag_bit = IS_BOOL_TRUE;
        }
        assert(flag_bit != 0);
        _flags &= ~flag_bit;
    }


    /**
     * Indicates whether or not a value has been loaded from its buffer.
     * If the value does not have backing buffer this is always false;
     * If this value is true the _isPositionLoaded must also be true;
     */
    protected final boolean _isMaterialized() { return is_true(IS_MATERIALIZED); }
    protected final boolean _isMaterialized(boolean flag)
    {
        if (flag) {
            set_flag(IS_MATERIALIZED);
        }
        else {
            clear_flag(IS_MATERIALIZED);
        }
        return flag;
    }

    /**
     * Indicates whether or not the position information has been loaded.
     * If the value does not have backing buffer this is always false;
     */
    protected final boolean _isPositionLoaded() { return is_true(IS_POSITION_LOADED); }
    protected final boolean _isPositionLoaded(boolean flag)
    {
        if (flag) {
            set_flag(IS_POSITION_LOADED);
        }
        else {
            clear_flag(IS_POSITION_LOADED);
        }
        return flag;
    }

    /**
     * Indicates whether our nativeValue (stored by concrete subclasses)
     * has been determined.
     */
    protected final boolean _hasNativeValue() { return is_true(HAS_NATIVE_VALUE); }
    protected final boolean _hasNativeValue(boolean flag)
    {
        if (flag) {
            set_flag(HAS_NATIVE_VALUE);
        }
        else {
            clear_flag(HAS_NATIVE_VALUE);
        }
        return flag;
    }


    /**
     * Indicates whether our {@link #_hasNativeValue} has been updated and is
     * is currently out of synch with the underlying byte buffer. Initially
     * a non-buffer backed value is dirty, and a buffer backed value (even
     * if it has not been materialized) is clean.  Don't modify this directly,
     * go through {@link #setDirty()}.  If this flag is true, then our parent
     * must be dirty, too.  If this flag is true, then {@link #_hasNativeValue}
     * must be true too.
     * <p>
     * Note that a value with no buffer is always dirty.
     */
    protected final boolean _isDirty() { return is_true(IS_DIRTY); }
    private final boolean _isDirty(boolean flag)
    {
        if (flag) {
            set_flag(IS_DIRTY);
        }
        else {
            clear_flag(IS_DIRTY);
        }
        return flag;
    }


    /**
     * Tracks whether or not this instance is locked.  Locked values
     * may not be mutated and must be thread safe for reading.
     */
    protected final boolean _isLocked() { return is_true(IS_LOCKED); }
    protected final boolean _isLocked(boolean flag)
    {
        if (flag) {
            set_flag(IS_LOCKED);
        }
        else {
            clear_flag(IS_LOCKED);
        }
        return flag;
    }


    protected final boolean _isSystemValue() { return is_true(IS_SYSTEM_VALUE); }
    protected final boolean _isSystemValue(boolean flag)
    {
        if (flag) {
            set_flag(IS_SYSTEM_VALUE);
        }
        else {
            clear_flag(IS_SYSTEM_VALUE);
        }
        return flag;
    }

    /**
     * used by IonIntImpl and IonBoolImpl (so far)
     */
    protected final boolean _isNullValue() { return is_true(IS_NULL_VALUE); }
    protected final boolean _isNullValue(boolean flag)
    {
        if (flag) {
            set_flag(IS_NULL_VALUE);
        }
        else {
            clear_flag(IS_NULL_VALUE);
        }
        return flag;
    }

    /**
     * since an IonBool only has true or false, it's value
     * can live in one of the flags bit
     */
    protected final boolean _isBoolTrue() { return is_true(IS_BOOL_TRUE); }
    protected final boolean _isBoolTrue(boolean flag)
    {
        if (flag) {
            set_flag(IS_BOOL_TRUE);
        }
        else {
            clear_flag(IS_BOOL_TRUE);
        }
        return flag;
    }



    /**
     * This is the containing value, if there is one.  The container
     * may hold the symbol table and/or the buffer.  If the container
     * changes the buffer and symbol table need to be checked for
     * equality.  If these change then the value will need to be
     * materialized before the change is made as the "old" buffer or
     * symbol table may be needed for the value to be understood.
     */
    protected IonContainerImpl _container;

    /**
     * The buffer with this element's binary-encoded data.  May be non-null
     * even when {@link #_container} is null.
     */
    protected BufferManager _buffer;

    /**
     * The local symbol table is an updatable (?) copy of the
     * symbol table, if one exists for this value.  This may be
     * stored in at a parent container.
     */
    protected SymbolTable _symboltable;

    /**
     * The instance maintains a reference back to the system that
     * created it so that it can create a symbol table when it
     * is forced to, and not before.  Generally the symbol table
     * is created as high on the container tree as possible.  However
     * a localsymboltable may need to be created on any value if
     * the symbol id is being requested for a fieldname, the int
     * value of an IonSymbol or an annotation.
     */
    final protected IonSystemImpl _system;


    /**
     * Constructs a value with the given native value and high-nibble, and zero
     * low-nibble. This instance is dirty by default.
     *
     * @param system must not be null.
     */
    protected IonValueImpl(IonSystemImpl system, int typedesc)
    {
        _system = system;

        pos_init();
        _isMaterialized(false);
        _isPositionLoaded(false);
        _hasNativeValue(false);
        _isDirty(true);
        pos_setTypeDescriptorByte(typedesc);
        boolean isnull = (IonConstants.getLowNibble(typedesc) == IonConstants.lnIsNull);
        _isNullValue(isnull);
    }


    public final IonSystemImpl getSystem()
    {
        return _system;
    }

    /**
     * the base classes in IonValue(Impl) should not be called for
     * cloning directly. The user should be calling clone on the
     * actualy (leaf) instances class (such as IonIntImpl).  The
     * shared clone work is handled in copyFrom(src) in any base
     * classes that need to support this - including IonValueImpl,
     * IonContainerImpl, IonTextImpl and IonLobImpl.
     */
    @Override
    public abstract IonValue clone();

    /**
     * Since {@link #equals(Object)} is overridden, each concrete class must provide
     * an implementation of {@link Object#hashCode()}
     * @return hash code for instance consistent with equals().
     */
    @Override
    public abstract int hashCode();

    /**
     * this copies the annotations and the field name if
     * either of these exists from the passed in instance.
     * It overwrites these values on the current instance.
     * Since these will be the string representations it
     * is unnecessary to update the symbol table ... yet.
     * <p>
     * This method will materialize this instance and the source.
     *
     * @param source instance to copy from
     */
    protected void copyAnnotationsFrom(IonValueImpl source)
    {
        // first this instance has to be ready
        // (although it probably is)
        makeReady();

        // getting the type annotations will also force the
        // this instance to be "ready" (i.e. it will call
        // MakeReady()) which we'll want.
        String[] a = source.getTypeAnnotations();
        // underlying code relies on null for empty
        _annotations = a.length == 0 ? null : a;
    }


    // this is split to make it more likely
    // that it will be inlined. This is
    // very frequently called.
    // It doesn't look like you can have
    // a materialized value which doesn't
    // have a native value, so the first
    // test should be enough.  But the
    // _isMaterialized test was there
    // before and there's no need, at this
    // time, to change this.
    protected final void makeReady()
    {
        if (_hasNativeValue()) return;
        makeReadyHelper();
    }
    private final void makeReadyHelper()
    {
        if (_isMaterialized()) return;
        if (_entry_start != -1) {
            assert _isPositionLoaded() == true;
            try {
                materialize();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }
    }

    /**
     * @param symboltable must be local, not shared.
     *
     * @return not null.
     */
    public static IonValueImpl makeValueFromReader(int fieldSID,
                                                   IonBinary.Reader reader,
                                                   BufferManager buffer,
                                                   SymbolTable symboltable,
                                                   IonContainerImpl container,
                                                   IonSystemImpl system)
        throws IOException
    {
        IonValueImpl value;
        int pos = reader.position();
        int tdb = reader.readActualTypeDesc();

        value = makeValue(tdb, system);
        value._fieldSid    = fieldSID;
        value._buffer      = buffer;
        value._container   = container;
        value._symboltable = symboltable;
        reader.sync();
        reader.setPosition(pos);
        value.pos_load(fieldSID, reader);

        return value;
    }

    static IonValueImpl makeValue(int typedesc, IonSystemImpl system)
    {
        assert system != null;

        IonValueImpl value = null;
        int typeId = IonConstants.getTypeCode(typedesc);

        switch (typeId) {
        case IonConstants.tidNull: // null(0)
            value = new IonNullImpl(system, typedesc);
            break;
        case IonConstants.tidBoolean: // boolean(1)
            value = new IonBoolImpl(system, typedesc);
            break;
        case IonConstants.tidPosInt: // 2
        case IonConstants.tidNegInt: // 3
            value = new IonIntImpl(system, typedesc);
            break;
        case IonConstants.tidFloat: // float(4)
            value = new IonFloatImpl(system, typedesc);
            break;
        case IonConstants.tidDecimal: // decimal(5)
            value = new IonDecimalImpl(system, typedesc);
            break;
        case IonConstants.tidTimestamp: // timestamp(6)
            value = new IonTimestampImpl(system, typedesc);
            break;
        case IonConstants.tidSymbol: // symbol(7)
            value = new IonSymbolImpl(system, typedesc);
            break;
        case IonConstants.tidString: // string (8)
            value = new IonStringImpl(system, typedesc);
            break;
        case IonConstants.tidClob: // clob(9)
            value = new IonClobImpl(system, typedesc);
            break;
        case IonConstants.tidBlob: // blob(10)
            value = new IonBlobImpl(system, typedesc);
            break;
        case IonConstants.tidList: // list(11)
            value = new IonListImpl(system, typedesc);
            break;
        case IonConstants.tidSexp: // 12
            value = new IonSexpImpl(system, typedesc);
            break;
        case IonConstants.tidStruct: // 13
            value = new IonStructImpl(system, typedesc);
            break;

        case IonConstants.tidTypedecl: // 14
            // the only case where this is valid is if this is
            // really an IonVersionMaker
            assert IonConstants.getLowNibble(typedesc) == 0;
            value = system.newSystemIdSymbol(ION_1_0);
            break;

        default:
            throw new IonException("invalid type "+typeId+" ("+typedesc+") encountered");
        }

        return value;
    }

    public final synchronized boolean isNullValue()
    {
        return _isNullValue();
    }

    /**
     * Ensures that this value is not an Ion null.  Used as a helper for
     * methods that have that precondition.
     * @throws NullValueException if <code>this.isNullValue()</code>
     */
    protected final void validateThisNotNull()
        throws NullValueException
    {
        if (isNullValue())
        {
            throw new NullValueException();
        }
    }

    public String getFieldName()
    {
        if (this._fieldName != null) return this._fieldName;
        if (this._fieldSid < 1) return null;
        _fieldName = this.getSymbolTable().findSymbol(this._fieldSid);
        assert _fieldName != null;
        return _fieldName;
    }

    public int getFieldId()
    {
        if (this._fieldSid == 0 && this._fieldName != null)
        {
            this._fieldSid = this.resolveSymbol(this._fieldName);
            if (this._fieldSid == SymbolTable.UNKNOWN_SYMBOL_ID) {
                this._fieldSid = this.addSymbol(this._fieldName);
            }
        }
        return this._fieldSid;
    }

    /**
     * variant for symbol table resolution
     * @return boolean if an add symbol was done
     */
    private SymbolTable getFieldId(SymbolTable symtab)
    {
        assert symtab == this.getSymbolTable();

        if (this._fieldSid == 0 && this._fieldName != null)
        {
            this._fieldSid = this.resolveSymbol(this._fieldName);
            if (this._fieldSid == SymbolTable.UNKNOWN_SYMBOL_ID) {
                checkForLock();
                symtab = this.addSymbol(this._fieldName, symtab);
                this._fieldSid = symtab.findSymbol(this._fieldName);
                assert( this._fieldSid != SymbolTable.UNKNOWN_SYMBOL_ID);
            }
        }
        return symtab;
    }

    @Deprecated
    public final int getFieldNameId()
    {
        return getFieldId();
    }

    public IonValueImpl topLevelValue()
    {
        assert ! (this instanceof IonDatagram);

        IonValue value = this;
        for (;;) {
            IonValue parent = value.getContainer();
            if (parent == null || parent instanceof IonDatagram) {
                break;
            }
            value = parent;
        }
        return (IonValueImpl) value;
    }

    public final IonContainer getContainer()
    {
        if (_container == null) {
            return null;
        }

        // TODO why is this here?  (See below as well)
        // I think this is vestigial embedded value logic that's now wrong.
        if (_container._isSystemValue()) {
            return _container.getContainer();
        }
        return _container;
    }

    public final boolean removeFromContainer()
    {
        // TODO how does this interact with the strange system value note above?
        IonContainer c = getContainer();
        if (c == null) return false;

        boolean removed = c.remove(this);
        assert removed;
        return true;
    }


    public void doNothing()
    {
        // used for timing things (to simulate a method call, but not do any work)
    }

    public final boolean isDirty()
    {
        return _isDirty();
    }

    public void makeReadOnly()
    {
        // NOTE: it sure seems to me this should be true - but it seems not. hmmm.
        // IonValuePrivate parent = this.get_symbol_table_root();
        // if (parent != this) {
        //     throw new IonException("child members can't be make read only by themselves");
        // }
        make_read_only_helper(true);
    }
    void make_read_only_helper(boolean is_root) {
        if (_isLocked()) return;
        synchronized (this) {
            deepMaterialize();
            if (is_root) {
                this.populateSymbolValues(this._symboltable);
            }
            _isLocked(true);
        }
    }

    public final boolean isReadOnly()
    {
        return _isLocked();
    }

    /**
     * Verifies that this value is not read-only.
     *
     * @throws ReadOnlyValueException
     *   if this value {@link #isReadOnly()}.
     */
    protected final void checkForLock()
        throws ReadOnlyValueException
    {
        if (_isLocked()) {
            throwReadOnlyException();
        }
    }
    private final void throwReadOnlyException()
        throws ReadOnlyValueException
    {
        throw new ReadOnlyValueException();
    }

    /**
     * Marks this element, and it's container, dirty.  This forces the binary
     * buffer to be updated when it's next needed.
     */
    protected void setDirty()
    {
        checkForLock();
        if (this._isDirty() == false) {
            this._isDirty(true);
            if (this._container != null) {
                this._container.setDirty();
            }
        }
        else {
            assert this._container == null || this._container.isDirty();
        }
    }

    protected void setClean()
    {
        assert _buffer != null;
        this._isDirty(false);
    }


    public SymbolTable getSymbolTable()
    {
        assert ! (this instanceof IonDatagram);

        if (this._symboltable != null)  return this._symboltable;
        if (this._container != null)    return this._container.getSymbolTable();

        return this._symboltable;
    }

    public SymbolTable getAssignedSymbolTable()
    {
        return this._symboltable;
    }

    /**
     * this returns the current values symbol table,
     * which is typically
     * owned by the top level value, if the current
     * symbol table is a local symbol table.  Otherwise
     * this replaces the current symbol table with a
     * new local symbol table based on the current Ion system version.
     * @return SymbolTable that is updatable (i.e. a local symbol table)
     */
    public SymbolTable getUpdatableSymbolTable()
    {
        IonValueImpl parent = topLevelValue();
        SymbolTable symbols = parent.getSymbolTable();

        if (UnifiedSymbolTable.isLocalTable(symbols)) {
            return symbols;
        }

        if (symbols == null) {
            symbols = _system.getSystemSymbolTable();
        }

        symbols = makeNewLocalSymbolTable(_system, symbols);
        parent.setSymbolTable(symbols);

        return symbols;
    }

    /**
     * checks in the current symbol table for this
     * symbol (name) and returns the symbol id if
     * this symbol is defined.
     *
     * @param name text for the symbol of interest
     * @return int symbol id if found or
     *          UnifiedSymbolTable.UNKNOWN_SID if
     *          it is not already defined
     */
    public int resolveSymbol(String name)
    {
        SymbolTable symbols = getSymbolTable();
        if (symbols == null) {
            return SymbolTable.UNKNOWN_SYMBOL_ID;
        }
        int sid = symbols.findSymbol(name);
        return sid;
    }

    /**
     * checks in the current symbol table for this
     * symbol id (sid) and returns the symbol text if
     * this symbol is defined.
     *
     * @param sid symbol id of interest
     * @return String symbol text if found or
     *          null if it is not already defined
     */
    public String resolveSymbol(int sid)
    {
        SymbolTable symbols = getSymbolTable();
        if (symbols == null) {
            return null;
        }
        String name = symbols.findKnownSymbol(sid);
        return name;
    }

    /**
     * adds a symbol name to the current symbol
     * table.  This may change the current symbol
     * table if the current symbol table is either
     * null or not updatable.
     * @param name symbol text to be added
     * @return int symbol id of the existing, or
     *             newly defined, symbol
     */
    public int addSymbol(String name)
    {
        int sid = resolveSymbol(name);
        if (sid != SymbolTable.UNKNOWN_SYMBOL_ID) {
            return sid;
        }
        checkForLock();

        SymbolTable symbols = getUpdatableSymbolTable();
        sid = symbols.addSymbol(name);
        return sid;
    }

    /**
     * variant of addSymbol that returns the symbol
     * table for symbol resolution instead of the
     * symbol id which users generally want.
     * @param name new symbol
     * @return SymbolTable after the add
     */
    protected SymbolTable addSymbol(String name, SymbolTable symbols)
    {
        assert resolveSymbol(name) == SymbolTable.UNKNOWN_SYMBOL_ID;
        assert symbols == this.getSymbolTable();

        if (UnifiedSymbolTable.isLocalTable(symbols) == false) {
            symbols = getUpdatableSymbolTable();
        }
        checkForLock();
        symbols.addSymbol(name);

        return symbols;
    }

    /**
     *
     * @param symtab must be local, system, or null.
     */
    public void setSymbolTable(SymbolTable symtab) {
        if (isNonSystemSharedTable(symtab)) {
            throw new IllegalArgumentException("symbol table must be local or system");
        }

        IonValueImpl parent = topLevelValue();
        SymbolTable  currentSymtab = parent.getSymbolTable();
        if (currentSymtab != symtab) {
            checkForLock();
            if (UnifiedSymbolTable.isTrivialTable(currentSymtab) == false) {
                parent.detachFromSymbolTable(); // Calls setDirty
            }
            else {
                parent.makeReady();
                this.setDirty();
            }
            parent._symboltable = symtab;
            assert ((parent == this) || (this._symboltable == null));
        }
    }

    /**
     * Recursively materialize all symbol text and detach from any symtab.
     * Calls {@link #setDirty()}.
     * <p>
     * <b>PRECONDITION:</b> this value must be deep materialized.
     */
    void detachFromSymbolTable()
    {
        // Force reading of annotation text
        getTypeAnnotations();

        if (this._fieldSid > 0) {
            if (this._fieldName == null) {
                this._fieldName = this.resolveSymbol(this._fieldSid);
            }
            this._fieldSid = 0; // FIXME should be UNKNOWN_SYMBOL_ID
        }

        this._symboltable = null;

        setDirty();
    }


    /**
     * Gets the index of this value within its container.
     * @return zero if this is not within a container.
     * @deprecated Use {@link IonSequence#indexOf(Object)}
     */
    @Deprecated
    public int getElementId()
    {
        return this._elementid;
    }

    /**
     * @deprecated Use {@link #getTypeAnnotations()} instead
     */
    @Deprecated
    public String[] getTypeAnnotationStrings()
    {
        return getTypeAnnotations();
    }

    public String[] getTypeAnnotations()
    {
        // if we have a list, then it's a good one
        if (this._annotations != null) return this._annotations;

        // if this has been materialized (and we clearly don't
        // have a list) then there is no annotations
        makeReady();

        return this._annotations == null ? EMPTY_STRING_ARRAY : this._annotations;
    }

    public void setTypeAnnotations(String... annotations)
    {
        checkForLock();
        makeReady();

        if (annotations == null || annotations.length == 0)
        {
            // Normalize all empty lists to the same instance.
            _annotations = EMPTY_STRING_ARRAY;
        }
        else
        {
            IonImplUtils.ensureNonEmptySymbols(annotations);
            _annotations = annotations.clone();
        }
        setDirty();
    }

    public void clearTypeAnnotations()
    {
        checkForLock();
        makeReady();
        this._annotations = null;
        this.setDirty();
    }

    public boolean hasTypeAnnotation(String annotation)
    {
        makeReady();
        if (_annotations != null) {
            for (String s : _annotations) {
                if (s.equals(annotation)) return true;
            }
        }
        return false;
    }

    public void removeTypeAnnotation(String annotation)
    {
        checkForLock();
        if (!this.hasTypeAnnotation(annotation)) return;

        String[] temp = (_annotations.length == 1) ? null : new String[_annotations.length - 1];
        for (int ii=0, jj=0; ii < _annotations.length; ii++) {
            if (ii != jj || !_annotations[ii].equals(annotation)) {
                temp[jj++] = _annotations[ii];
            }
        }
        _annotations = temp;
    }

    public void addTypeAnnotation(String annotation)
    {
        checkForLock();
        if (hasTypeAnnotation(annotation)) return;

        // allocate a larger array and copy if necessary
        int oldlen = (_annotations == null) ? 0 : _annotations.length;
        int newlen = oldlen + 1;
        String[] temp = new String[newlen];
        if (_annotations != null) {
            System.arraycopy(this._annotations, 0, temp, 0, oldlen);
        }
        // load the new sid
        temp[newlen - 1] = annotation;
        this._annotations = temp;

        setDirty();
    }

    void pos_init()
    {
        _fieldSid           =  0;
        _entry_start        = -1;
        _value_td_start     = -1;
        _value_content_start= -1;
        _next_start         = -1;

    }
    void pos_init(int fieldNameSymbol)
    {
        pos_init();
        pos_setFieldId(fieldNameSymbol);
    }
    void pos_init(int fieldNameSymbol, Reader valueReader) throws IOException
    {
        pos_init(fieldNameSymbol);
        pos_load(fieldNameSymbol, valueReader);
    }

    // overridden in container to call for clearing the children
    // and the container version calls this to clear its local values
    void clear_position_and_buffer()
    {
        // these calls force this ion value to be fully reified
        makeReady();
        this.getFieldName();
        this.getTypeAnnotations();

        _buffer = null;
        // cas removed (1 apr 2008): _symboltable = null;
        // TODO: should this be if (!(_container instanceof IonDatagram)) _symboltable = null;
        //       that is push all symbol tables up to the immediate datagram child members?
        //       since there's no buffer there's no binary ...
        _isMaterialized(false);   // because there's no buffer
        _isPositionLoaded(false);
        _isDirty(true);

        _fieldSid           =  0;
        _entry_start        = -1;
        _value_td_start     = -1;
        _value_content_start= -1;
        _next_start         = -1;
    }

    void pos_initDatagram(int typeDesc, int length)
    {
        _isMaterialized(false);
        _isPositionLoaded(true);
        _hasNativeValue(false);

        // if we have a buffer, and it's not loaded, it has to be clean
        _isDirty(false);

        _fieldSid           = 0;
        _type_desc          = typeDesc;
        // FIXME verify or remove - cas: 22 apr 2008
        _entry_start        = 0; // BINARY_VERSION_MARKER_SIZE;
        _value_td_start     = 0; // BINARY_VERSION_MARKER_SIZE;
        _value_content_start= 0; // BINARY_VERSION_MARKER_SIZE;
        _next_start         = length;
    }

    void pos_clear()
    {
        _isMaterialized(false);
        _isPositionLoaded(false);
        _isDirty(true);

        _fieldSid           =  0;
        _type_desc          =  0;
        _entry_start        = -1;
        _value_td_start     = -1;
        _value_content_start= -1;
        _next_start         = -1;
    }

    void pos_load(int fieldId, Reader valueReader) throws IOException
    {
        _isMaterialized(false);
        _isPositionLoaded(true);
        _hasNativeValue(false);
        _isDirty(false); // if we have a buffer, and it's not loaded, it has to be clean

        pos_setFieldId(fieldId);

        // we expect to be positioned at the initial typedesc byte
        int start = this._value_td_start = valueReader.position();

        // our _entry_start needs to be back filled to precede the field id
        this._entry_start = start - pos_getFieldIdLen();

        // read and remember our type descriptor byte
        this._type_desc = valueReader.readToken();

        // this first length is our overall length
        // whether we have annotations or not
        int type = this.pos_getType();

        // vlen might get reset later if this is a IonVersionMarker
        int vlen = valueReader.readLength(type, this.pos_getLowNibble());

        this._value_content_start = valueReader.position();
        this._next_start = this._value_content_start + vlen;

        if (type == IonConstants.tidTypedecl) {
            // check for the special case of the typedecl that is a binary version marker
            if (this._type_desc == (IonConstants.BINARY_VERSION_MARKER_1_0[0] & 0xff)) {
                // read the remaining (3) bytes of the IonVersionMarker
                for (int ii=1; ii<IonConstants.BINARY_VERSION_MARKER_SIZE; ii++) {
                    int b = valueReader.read();
                    if ((b & 0xff) != (IonConstants.BINARY_VERSION_MARKER_1_0[ii] & 0xff)) {
                        throw new IonException("illegal value encoded at "+this._value_content_start);
                    }
                }
                // fixup the "next start" position, since when we set this
                // the value length was "wrong"
                vlen = IonConstants.BINARY_VERSION_MARKER_SIZE - 1;
                this._next_start = this._value_content_start + vlen;
                this._isSystemValue(true);
            }
            else {
                // read past the annotation list
                int annotationListLength = valueReader.readVarUIntAsInt();
                assert annotationListLength > 0;  // TODO throw if bad
                this._value_td_start = valueReader.position() + annotationListLength;
                valueReader.setPosition(this._value_td_start);
                // read the actual values type desc
                this._type_desc = valueReader.readToken();
                // TODO check that td != annotation (illegal nested annotation)

                int ln = pos_getLowNibble();
                if ((ln == IonConstants.lnIsVarLen)
                    || (this._type_desc == IonStructImpl.ORDERED_STRUCT_TYPEDESC))
                {
                    // Skip over the extended length to find the content start.
                    valueReader.readVarUIntAsInt();
                }
                this._value_content_start = valueReader.position();
            }
        }
    }

    public final byte pos_getTypeDescriptorByte()
    {
        return (byte) this._type_desc;
    }
    public final void pos_setTypeDescriptorByte(int td)
    {
        assert td >= 0 && td <= 0xFF;
        this._type_desc = td;
    }
    public final int pos_getType()
    {
        return IonConstants.getTypeCode(this._type_desc);
    }

    public final int pos_getLowNibble()
    {
        return IonConstants.getLowNibble(this._type_desc);
    }

    public final int pos_getFieldId()
    {
        return _fieldSid;
    }
    public final int pos_getFieldIdLen()
    {
        return IonBinary.lenVarUInt(_fieldSid);
    }
    public final void pos_setFieldId(int fieldNameSid)
    {
        assert fieldNameSid >= 0;
        int old_field_len = pos_getFieldIdLen();
        if (fieldNameSid == 0) {
            _fieldSid = 0;
        }
        else {
            _fieldSid = fieldNameSid;
        }
        int new_field_len = pos_getFieldIdLen();
        int delta = (new_field_len - old_field_len);
        // with more space taken by the field id, the actual
        // value and the next value must also move down
        _value_td_start += delta;
        _value_content_start += delta;
        _next_start  += delta;
    }

    public final int pos_getOffsetAtFieldId()
    {
        return this._entry_start;
    }
    public final int pos_getOffsetInitialTD()
    {
        return this._entry_start + this.pos_getFieldIdLen();
    }
    public final boolean pos_isAnnotated()
    {
        return (this._entry_start + IonBinary.lenVarUInt(this._fieldSid) != this._value_td_start);
    }
    public final int pos_getOffsetAtAnnotationTD()
    {
        if (this.pos_isAnnotated()) {
            return this.pos_getOffsetInitialTD();
        }
        return -1;
    }
    public final int pos_getOffsetAtValueTD()
    {
        return this._value_td_start;
    }
    public final int pos_getOffsetAtActualValue()
    {
        return this._value_content_start;
    }
    public final int pos_getOffsetofNextValue()
    {
        return this._next_start;
    }
    public final int  pos_getValueOnlyLength()
    {
        return this._next_start - this._value_content_start;
    }

    public void pos_setEntryStart(int entryStart)
    {
        int delta = entryStart - this._entry_start;
        pos_moveAll(delta);
    }

    public void pos_moveAll(int delta)
    {
        this._entry_start += delta;
        this._value_td_start += delta;
        this._value_content_start += delta;
        this._next_start += delta;
    }

    static int getFieldLength(int td, IonBinary.Reader reader) throws IOException
    {
        int ln = IonConstants.getLowNibble(td);
        int hn = IonConstants.getTypeCode(td);
        int len = reader.readLength(hn, ln);
        return len;
    }


    /**
     * Decodes the content of this element into Java values for access via
     * this object.  If this is a container, the children are not necessarily
     * materialized.  Real work is in materialize_helper() which has the
     * synchronization is overridden in IonContainerImpl.  The split should
     * allow materialize to be in-lined.
     * <p/>
     * Postcondition: <code>this._hasNativeValue == true </code>
     * <p/>
     * this method is split to help the JIT inline the base version.
     */
    protected final void materialize() throws IOException
    {
        if ( this._isMaterialized() ) return;
        materialize_helper();
    }
    protected synchronized void materialize_helper()
        throws IOException
    {
        if ( this._isPositionLoaded() == false ) {
            if (this._buffer != null) {
                throw new IonException("invalid value state - buffer but not loaded!");
            }
            // No buffer, so no work to do.
            return;
        }

        if (this._buffer == null) {
            throw new IonException("invalid value state - loaded but no buffer!");
        }

        assert ! this._isLocked();

        IonBinary.Reader reader = _buffer.reader();
        reader.sync();
        if ( ! this.pos_isAnnotated() ) {
            // if there aren't annontations, just position the reader
            reader.setPosition(this.pos_getOffsetAtValueTD());
        }
        else {
            materializeAnnotations(reader);
        }

        // now materialize the value itself (this is done in the
        // specialized sub classes)
        assert reader.position() == this.pos_getOffsetAtValueTD();

        // TODO doMaterializeValue should precondition !_hasNativeValue and
        // then set _hasNativeValue here, OnceAndOnlyOnce.
        this.doMaterializeValue(reader);
        assert _hasNativeValue();
        this._isMaterialized(true);
    }

    protected SymbolTable materializeSymbolTable()
    {
        SymbolTable symtab = _symboltable;
        if (symtab == null && _container != null) {
            symtab = _container.materializeSymbolTable();
        }
        return symtab;
    }

    // TODO this is likely to overlap with getUpdateableSymbolTable logic
    protected SymbolTable materializeUpdateableSymbolTable()
    {
        SymbolTable symtab = _symboltable;
        if (symtab != null && symtab.isLocalTable()) {
            return symtab;
        }
        if (_container != null) {
            symtab = _container.materializeUpdateableSymbolTable();
        }
        else if (!this.isReadOnly()) {
            if (symtab == null) {
                symtab = _system.getSystemSymbolTable();
            }
            assert symtab.isSystemTable();
            synchronized (this) {
                symtab = makeNewLocalSymbolTable(_system, symtab);
                this.setSymbolTable(symtab);
            }
        }
        // else we return whatever we already have
        return symtab;
    }

    protected synchronized void materializeAnnotations(IonBinary.Reader reader) throws IOException
    {
        assert this._isMaterialized() == false;

        if (!this.pos_isAnnotated()) return;

        //  so we read any annotations that are present
        reader.setPosition(this.pos_getOffsetAtAnnotationTD());

        // skip over the annoation td and total length
        int td = reader.read();
        if (IonConstants.getTypeCode(td) != IonConstants.tidTypedecl) {
            throw new IonException("invalid user type annotation");
        }
        if (IonConstants.getLowNibble(td) == IonConstants.lnIsVarLen) {
            // skip past the overall length, which we don't care about here
            reader.readVarIntAsInt();
        }

        // now we load the annotation SID list from the buffer
        int[] sids = reader.readAnnotations();
        assert reader.position() == this.pos_getOffsetAtValueTD();

        // and convert them to strings
        int len = sids.length;
        this._annotations = new String[len];
        SymbolTable symtab = getSymbolTable();
        assert symtab != null || len < 1 ;
        for (int ii=0; ii<len; ii++) {
            int id = sids[ii];
            this._annotations[ii] = symtab.findSymbol(id);
        }
    }

    /**
     * Postcondition: <code>this._hasNativeValue == true</code>
     *
     * @param reader is not <code>null</code>.
     * @throws IOException
     */
    abstract void doMaterializeValue(IonBinary.Reader reader)
        throws IOException;


    public void deepMaterialize()
    {
        try
        {
            materialize();
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }


    protected void detachFromBuffer()
        throws IOException
    {
        // Containers override this method, doing deep materialization on the
        // way down. This avoids two recursive traversals.
        materialize();
        _buffer = null;
    }

    /**
     * Removes this value from its container, ensuring that all data stays
     * available.  Dirties this value and it's original container.
     *
     * @throws IOException
     *   if there's a problem materializing this value from its binary buffer.
     *   When this is thrown, no values are dirtied!
     */
    protected final void detachFromContainer() throws IOException
    {
        checkForLock();
        // TODO this should really copy the buffer to avoid materialization.
        // Note: that forces extraction and reconstruction of the local symtab.
        detachFromBuffer();

        // Requires prior deep-materialization.
        // Calls setDirty() which also dirties container.
        detachFromSymbolTable();
        assert _isDirty() && _container.isDirty();

        _container = null;
        _fieldName = null;
        assert _fieldSid == 0;
        _elementid = 0;
    }

    protected void setFieldName(String name)
    {
        assert this._fieldName == null;

        // First materialize, because we're gonna mark ourselves dirty.
        makeReady();
        String oldname = this._fieldName;
        this._fieldName = name;
        this._fieldSid  = 0;
        IonContainer container = this._container;
        if (container != null) {
            if (IonType.STRUCT.equals(container.getType())) {
                assert(container instanceof IonStructImpl);
                ((IonStructImpl)container).updateFieldName(oldname, name, this);
            }
        }
        this.setDirty();
    }


    @Override
    public String toString()
    {
        Printer p = new Printer();
        StringBuilder builder = new StringBuilder();
        try {
            p.print(this, builder);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return builder.toString();
    }


    // TODO rename to getContentLength
    protected int getNakedValueLength() throws IOException
    {
        // container overrides this method.
        assert !(this instanceof IonContainer);

        int len;
        if (this._isDirty()) {
            assert _hasNativeValue() == true || _isPositionLoaded() == false;

            if (isNullValue()) return 0;

            len = getNativeValueLength();
        }
        else {
            assert _isPositionLoaded();
            len = pos_getValueOnlyLength();
        }
        return len;
    }


    // TODO rename to computeContentLengthFromNativeValue
    /**
     * Computes the content length based on the materialized view.
     * <p>
     * PRECONDITION: {@code _hasNativeValue == true}
     */
    abstract protected int getNativeValueLength();


    /**
     *
     * @return zero if there's no annotation on this value, else the length
     * of the annotation header:
     *      typedesc + overall value length + symbol list length + symbol ids.
     */
    public int getAnnotationOverheadLength(int valueLen)
    {
        int len = 0;
        String[] annotationStrings = this.getTypeAnnotations();
        assert annotationStrings != null;

        if (annotationStrings.length != 0)
        {
            len = getAnnotationLength();

            // finally add the option length of the overall value, which include
            // the length passed in of the actual data in this value and the length
            // of the annotation list we just added up
            len += IonBinary.lenLenFieldWithOptionalNibble(valueLen + len);

            // this add 1 for the typedesc itself
            len += IonBinary._ib_TOKEN_LEN;
        }

        return len;
    }

    int getAnnotationLength()
    {
        int len = 0;
        assert (this._annotations != null);

        // first add up the length of the annotations symbol id lengths
        for (int ii=0; ii<_annotations.length; ii++) {
            int symId = resolveSymbol(_annotations[ii]);
            if (symId <= 0) {
                symId = addSymbol(_annotations[ii]);
            }
            if (symId <= 0) {
                throw new IllegalStateException("the annotation must be in the symbol table");
            }
            int vlen = IonBinary.lenVarUInt(symId);
            len += vlen;
        }
        // then add the length of the symbol list which will preceed the symbols
        len += IonBinary.lenVarUInt(len);

        return len;
    }

    /**
     * Gets the size of the encoded field name (a single VarUInt).
     * @return the size, or zero if there's no field name.
     */
    public int getFieldNameOverheadLength()
    {
        int len = 0;

        // CAS: 14 jan 2010 changed fieldSid==0 to fieldSid<=0
        if (this._fieldSid <= 0 && this._fieldName != null)
        {
            // We haven't interned the symbol, do so now.
            this._fieldSid = this.resolveSymbol(this._fieldName);
            if (this._fieldSid <= 0) {
                throw new IonException("the field name must be in the symbol table, if you're using a name table");
            }
            // we will be using the symbol id in just bit (down below)
        }

        if (this._fieldSid > 0) {
            len = IonBinary.lenVarUInt(this._fieldSid);
            assert len > 0;
        }

        return len;
    }

    /**
     * Length of the core header.
     * @param contentLength length of the core value.
     * @return at least one.
     */
    public int getTypeDescriptorAndLengthOverhead(int contentLength)
    {
        int len = IonConstants.BB_TOKEN_LEN;

        if (isNullValue()) return len;

        switch (this.pos_getType()) {
        case IonConstants.tidNull: // null(0)
        case IonConstants.tidBoolean: // boolean(1)
            break;
        case IonConstants.tidPosInt: // 2
        case IonConstants.tidNegInt: // 3
        case IonConstants.tidFloat: // float(4)
        case IonConstants.tidDecimal: // decimal(5)
        case IonConstants.tidTimestamp: // timestamp(6)
        case IonConstants.tidSymbol: // symbol(7)
        case IonConstants.tidString: // string (8)
        case IonConstants.tidClob:   // 9
        case IonConstants.tidBlob:   // 10
        case IonConstants.tidList:   // 11
        case IonConstants.tidSexp:   // 12
            len += IonBinary.lenLenFieldWithOptionalNibble(contentLength);
            break;
        case IonConstants.tidStruct: // Overridden
        default:
            throw new IonException("this value has an illegal type descriptor id "+this.pos_getTypeDescriptorByte());
        }
        return len;
    }

    /**
     * Includes: header, field name, annotations, value.
     * @return > 0
     * @throws IOException
     */
    int getFullEncodedSize() throws IOException
    {

        int len = 0; // getTotalValueLength();

        if (this.isNullValue()) {
            // the raw non-annotated, non-fieldnamed, length
            // is 1 for a null container
            len = IonConstants.BB_TOKEN_LEN;
        }
        else {
            // for non-null we start with the length of the
            // contents
            len  = this.getNakedValueLength();

            // we need the value length to know whether the len
            // will overflow out of the low nibble
            len += this.getTypeDescriptorAndLengthOverhead(len);
        }
        // we add the length of the annotations (and the ensuing
        // overhead) and the field name length to the total
        // even when it's null

        // we need to know the aggregate length of the value
        // WITH its typedesc and optional length to know
        // how long the length will be for the the annotation
        // typedesc (since it wraps the values td and len and value)
        len += this.getAnnotationOverheadLength(len);

        // and the field precedes them all and doesn't get caught
        // up in all that "length" business.
        len += this.getFieldNameOverheadLength();

        assert len > 0;
        return len;
    }

    /**
     * @return the number of bytes by which this element's total length has
     * changed.
     * @throws IOException
     */
    protected final int updateToken() throws IOException
    {
        if (!this._isDirty()) return 0;

        int old_next_start = _next_start;

        //      | fid | an | tlen | alen | ann | td | vlen | value |
        //      |     |                        |                   |
        //       ^ entry_start                  ^ value_start
        //             ^ + len(fid)                      next_start ^

        int fieldSidLen = 0;
        int newFieldSid = 0;
        if (_fieldName != null) {
            assert this._container != null;
            assert this._container.pos_getType() == IonConstants.tidStruct;

            newFieldSid  = this.resolveSymbol(_fieldName);
            if (newFieldSid < 1) {
                newFieldSid = this.addSymbol(_fieldName);
            }
            fieldSidLen  = IonBinary.lenVarUInt(newFieldSid);
        }

        int valuelen      = this.getNakedValueLength();
        int tdwithvlenlen = this.getTypeDescriptorAndLengthOverhead(valuelen);

        //_entry_start - doesn't change

        // adjust the _value_start to include the fieldsid and annotations
        _value_td_start = _entry_start + fieldSidLen;
        if (_annotations != null) {
            // the annotation overhead needs to know the total value length
            // as it effects how long the annotation type length is
            int annotationlen = this.getAnnotationOverheadLength(valuelen + tdwithvlenlen);
            _value_td_start += annotationlen;
        }

        _value_content_start = _value_td_start + tdwithvlenlen;

        // the next start include the value and it's header as well
        _next_start = _value_td_start + tdwithvlenlen + valuelen;

        _type_desc = computeTypeDesc(valuelen);

        // overwrite the sid as everything is computed on the new value
        _fieldSid = newFieldSid;

        _isPositionLoaded(true);

        // and the delta is how far the end moved
        return _next_start - old_next_start;
    }


    /**
     * Precondition:  _isDirty && _hasNativeValue
     */
    protected int computeTypeDesc(int valuelen)
        throws IOException
    {
        int hn = this.pos_getType();
        int ln = this.computeLowNibble(valuelen);
        return IonConstants.makeTypeDescriptor(hn, ln);
    }


    /**
     * Precondition:  _isDirty && _hasNativeValue
     *
     * @param valuelen
     * @return the current type descriptor low nibble for this value.
     *
     * @throws IOException if there's a problem reading binary data while
     * making this computation.
     */
    abstract protected int computeLowNibble(int valuelen) throws IOException;


    protected void shiftTokenAndChildren(int delta)
    {
        assert (!this.isDirty());

        pos_moveAll(delta);
    }

    /**
     * Adds all of our annotations into the symbol table.
     */
    public SymbolTable populateSymbolValues(SymbolTable symtab)
    {
        // TODO can any of this be short-circuited?

        if (this._annotations != null) {
            for (String s : this._annotations) {
                int sid = this.resolveSymbol(s);
                if (sid < 1) {
                    checkForLock();
                    symtab = this.addSymbol(s, symtab);
                }
            }
        }

        if (this._fieldName != null) {
            symtab = this.getFieldId(symtab);
        }

        return symtab;
    }

    /**
     * Brings the binary buffer in sync with the materialized view.
     * <p>
     * The cumulativePositionDelta counts how many bytes have been
     * inserted/removed from the byte buffer "to the left" of this value.
     * If this value has data in the buffer, its current position must be
     * offset by this amount to find where the data is upon entry to this
     * method.  This method may do further inserts and/or deletes, and it must
     * return the accumulated delta.  For example, if this method (and/or any
     * methods it calls) causes N bytes to be inserted into the buffer, it
     * must return <code>(cumulativePositionDelta + N)</code>.  If M bytes are
     * removed, it must return <code>(cumulativePositionDelta - M)</code>.
     * <p>
     * Note that "inserted into the buffer" is different from "written into the
     * buffer".  Writes don't affect the delta, but calls like
     * {@link Writer#insert(int)} and {@link Writer#remove(int)} do.
     * <p>
     * Preconditions: If we've been encoded before, the existing data is at a
     * position after newPosition.  If we need more space we must insert it.
     * If we're dirty, our token has not been updated (so it still points to
     * the old data.
     * <p>
     * Postconditions: Our token is fully up to date.  isDirty() == false
     *
     * @return the updated cumulativePositionDelta.
     * @throws IOException
     */
    protected final int updateBuffer2(IonBinary.Writer writer, int newPosition,
                                      int cumulativePositionDelta)
        throws IOException
    {
        assert writer != null;
        assert writer.position() == newPosition;

        if (!this._isDirty())
        {
            // We don't need to re-encode, but we may need to move our data
            // and we must update our positions to deal with buffer movement.
            int outdatedPosition = pos_getOffsetAtFieldId();
            int realPosition = outdatedPosition + cumulativePositionDelta;

            int gapSize = realPosition - newPosition;
            assert 0 <= gapSize;
            if (0 < gapSize)
            {
                // Remove the gap to our left
                writer.setPosition(newPosition);  // TODO shouldn't be needed
                writer.remove(gapSize);
                cumulativePositionDelta -= gapSize;
            }

            // Update the position in our token.
            // TODO short-circuit on zero delta.
            if (cumulativePositionDelta != 0) {
                shiftTokenAndChildren(cumulativePositionDelta);
            }
            writer.setPosition(this.pos_getOffsetofNextValue());
        }
        else if (_entry_start == -1)
        {
            cumulativePositionDelta =
                updateNewValue(writer, newPosition, cumulativePositionDelta);
            setClean();
        }
        else  // We already have data but it's dirty.
        {
            cumulativePositionDelta =
                updateOldValue(writer, newPosition, cumulativePositionDelta);
            setClean();
        }

        return cumulativePositionDelta;
    }

    protected int updateNewValue(IonBinary.Writer writer, int newPosition,
                                 int cumulativePositionDelta)
        throws IOException
    {
        assert writer.position() == newPosition;

        updateToken();

        if (_buffer == null) {
            _buffer = _container._buffer;
            assert _buffer != null;
        }
        else {
            assert _buffer == _container._buffer;
        }

        assert pos_getOffsetAtFieldId() < 0;

        // since the start is at -1 for a new value, the next value
        // offset will be 1 shy of the actual total length (including
        // the field sid, if that's present)
        int size = this.pos_getOffsetofNextValue() + 1;

        // to change the -1 initial position to become newPosition
        this.pos_setEntryStart(newPosition);

        assert newPosition == pos_getOffsetAtFieldId();

        // Create space for me myself and I.
        writer.insert(size);
        cumulativePositionDelta += size;

        cumulativePositionDelta =
            writeElement(writer, cumulativePositionDelta);

        return cumulativePositionDelta;
    }

    protected int updateOldValue(IonBinary.Writer writer, int newPosition,
                                 int cumulativePositionDelta)
        throws IOException
    {
        assert !(this instanceof IonContainer);
        assert writer.position() == newPosition;

        int oldEndOfValue = pos_getOffsetofNextValue();


        pos_moveAll(cumulativePositionDelta);

        assert newPosition <= pos_getOffsetAtFieldId();

        updateToken();

        int newEndOfValue = pos_getOffsetofNextValue();
        if (newEndOfValue > oldEndOfValue) {
            // We'll be writing beyond the previous limit, so make more space.
            writer.insert(newEndOfValue - oldEndOfValue);
            cumulativePositionDelta += newEndOfValue - oldEndOfValue;
        }

        int cumulativePositionDelta2 =
            writeElement(writer, cumulativePositionDelta);

        assert cumulativePositionDelta2 == cumulativePositionDelta;

        return cumulativePositionDelta;
    }


    public final void writeTo(IonWriter writer)
    {
        IonReader valueReader = getSystem().newReader(this);
        try
        {
            writer.writeValues(valueReader);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }


    /**
     * Precondition: the token is up to date, the buffer is positioned properly,
     * and enough space is available.
     * @throws IOException
     */
    final int writeElement(IonBinary.Writer writer, int cumulativePositionDelta) throws IOException
    {
        writeFieldName(writer);

        assert writer.position() == this.pos_getOffsetInitialTD();
        writeAnnotations(writer);

        assert writer.position() == this.pos_getOffsetAtValueTD();
        cumulativePositionDelta = writeValue(writer, cumulativePositionDelta);
        assert writer.position() == this.pos_getOffsetofNextValue();

        return cumulativePositionDelta;
    }

    /**
     * Called from {@link #updateBuffer()}.
     * <p>
     * Precondition: the token is up to date, the buffer is positioned properly,
     * and enough space is available.
     * @throws IOException

     */
    void writeFieldName(IonBinary.Writer writer) throws IOException
    {
        if (_container instanceof IonStruct)
        {
            int fieldSid = this.getFieldId();
            assert fieldSid > 0;
            assert writer.position() == this.pos_getOffsetAtFieldId();

            writer.writeVarUIntValue(fieldSid, true);
        }
    }

    /**
     * Precondition: the token is up to date, the buffer is positioned properly,
     * and enough space is available.
     * @throws IOException
     */
    final void writeAnnotations(IonBinary.Writer writer) throws IOException
    {
        if (this._annotations != null) {
            int valuelen      = this.getNakedValueLength();
            int tdwithvlenlen = this.getTypeDescriptorAndLengthOverhead(valuelen);
            int annotationlen = getAnnotationLength();
            int wrappedLength = valuelen + tdwithvlenlen + annotationlen;

            writer.writeCommonHeader(IonConstants.tidTypedecl,
                                                wrappedLength);
            // this is depending on getAnnotationLength() to have
            // added all the symbols as necessary so that writeAnnotations
            // doesn't have to update the symbol table
            writer.writeAnnotations(_annotations, this.getSymbolTable());
        }
    }

    protected abstract void doWriteNakedValue(IonBinary.Writer writer,
                                              int valueLen)
        throws IOException;

    /**
     * Precondition: the token is up to date, the buffer is positioned properly,
     * and enough space is available.
     *
     * @return the cumulative position delta at the end of this value.
     * @throws IOException
     */
    protected int writeValue(IonBinary.Writer writer,
                             int cumulativePositionDelta)
        throws IOException
    {
        // overridden by container.
        assert !(this instanceof IonContainer);

        // everyone gets a type descriptor byte (which was set
        // properly during updateToken which needs to have been
        // called before this routine!

        writer.write(this.pos_getTypeDescriptorByte());

        // now we write any data bytes - unless it's null
        int vlen = this.getNativeValueLength();
        if (vlen > 0) {
            switch (this.pos_getLowNibble()) {
            case IonConstants.lnIsNullAtom:
            case IonConstants.lnNumericZero:
                // we don't need to do anything here
                break;
            case IonConstants.lnIsVarLen:
                writer.writeVarUIntValue(vlen, true);
            default:
                this.doWriteNakedValue(writer, vlen);
                break;
            }
        }
        return cumulativePositionDelta;
    }

    /**
     * Implements equality over values.
     * This is currently defined using the Equivalence class.
     *
     * @see com.amazon.ion.util.Equivalence
     *
     * @param   other   The value to compare with.
     *
     * @return  A boolean, true if the other value is an Ion Value that is the same
     *          content and annotations.
     */
    @Override
    public boolean equals(final Object other)
    {
        boolean same = false;
        if (other == this) {
            // we shouldn't make 3 deep method calls for this common case
            return true;
        }
        if (other instanceof IonValue)
        {
            same = ionEquals(this, (IonValue) other);
        }
        return same;
    }
}
