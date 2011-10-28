// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.impl.IonImplUtils.EMPTY_STRING_ARRAY;
import static com.amazon.ion.impl.SystemValueIteratorImpl.makeSystemReader;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonBinary.BufferManager;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;

/**
 *
 */
public final class IonDatagramImpl
    extends IonSequenceImpl
    implements IonDatagram
{
    static private final int DATAGRAM_TYPEDESC  =
        IonConstants.makeTypeDescriptor(IonConstants.tidSexp,
                                        IonConstants.lnIsEmptyContainer);

    private static final int HASH_SIGNATURE =
        IonType.DATAGRAM.toString().hashCode();

    /** Underlying catalog */
    private final IonCatalog _catalog;

    /**
     * Used while constructing, then set to null.
     */
    private SystemValueIterator _rawStream;

    /**
     * Superset of {@link #_contents}; contains only user values.
     */
    private ArrayList<IonValue> _userContents;

    private static BufferManager make_empty_buffer()
    {
        BufferManager buffer = new BufferManager();

        try {
            // TODO is this necessary? Should wait to see what the user does.
            buffer.writer().write(IonConstants.BINARY_VERSION_MARKER_1_0);
        }
        catch (IOException e) {
             throw new IonException(e);
        }
        return buffer;
    }


    //=========================================================================

    public IonDatagramImpl(IonSystemImpl system, IonCatalog catalog) {
        this(system, catalog, make_empty_buffer());
    }


    /**
     * Creates a datagram wrapping bytes containing Ion text or binary data.
     *
     * @param ionData may be either Ion binary data, or UTF-8 Ion text.
     * <em>This method assumes ownership of the array</em> and may modify it at
     * will.
     *
     * @throws NullPointerException if any parameter is null.
     * @throws IonException if there's a syntax error in the Ion content.
     */
    public IonDatagramImpl(IonSystemImpl system, IonCatalog catalog, byte[] ionData)
    {
        this(system, system.newLegacySystemReader(catalog, ionData));
    }


    /**
     * Materializes the top-level values in the buffer.
     *
     * @param buffer is filled with Ion data.
     *
     * @throws NullPointerException if any parameter is null.
     */
    public IonDatagramImpl(IonSystemImpl system, IonCatalog catalog, BufferManager buffer)
    {
        this(system, makeSystemReader(system, catalog, buffer));
    }


    /**
     * Loads the whole stream into memory.
     */
    public IonDatagramImpl(IonSystemImpl system, IonCatalog catalog, Reader ionText)
    {
        this(system
            ,catalog
            ,(SymbolTable)null
            ,ionText);
    }


    @Override
    public IonDatagramImpl clone()
    {
        byte[] data = this.getBytes();

        IonDatagramImpl clone = new IonDatagramImpl(this._system, _catalog, data);

        return clone;
    }

    /**
     * Implements {@link Object#hashCode()} consistent with equals.
     *
     * @return  An int, consistent with the contracts for
     *          {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    @Override
    public int hashCode() {
        return sequenceHashCode(HASH_SIGNATURE);
    }

    /**
     * this copies the annotations and the field name if
     * either of these exists from the passed in instance.
     * It overwrites these values on the current instance.
     * Since these will be the string representations it
     * is unnecessary to update the symbol table ... yet.
     * @param source instance to copy from
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws NullPointerException
     */
    @Override
    protected void copyFrom(IonContainerImpl source)
        throws NullPointerException, IllegalArgumentException, IOException
    {
        throw new UnsupportedOperationException("this work is done in clone()");
    }

    /**
     * Loads the whole stream into memory.
     *
     * @param initialSymbolTable must be local, not shared.
     *
     * @throws NullPointerException if any parameter is null.
     */
    public IonDatagramImpl(IonSystemImpl system,
                           IonCatalog catalog,
                           SymbolTable initialSymbolTable,
                           Reader ionText)
    {
        this(system,
             makeSystemReader(system,
                              catalog,
                              initialSymbolTable, ionText));
    }

    /**
     * Workhorse constructor this loads the whole stream into memory and
     * materializes all top-level values.
     *
     * @throws NullPointerException if any parameter is null.
     */
    IonDatagramImpl(IonSystemImpl system,
                    SystemValueIterator rawStream)
    {
        super(system, DATAGRAM_TYPEDESC, false);

        assert system == rawStream.getSystem();

        _userContents = new ArrayList<IonValue>();

        // This is only used during construction.
        _catalog = rawStream.getCatalog();
        _rawStream = rawStream;
        _buffer = _rawStream.getBuffer();

        // Force reading of the first element, so we get the buffer bootstrapped
        // including the IVM.
        _rawStream.hasNext();

        // fake up the pos values as this is an odd case
        // Note that size is incorrect; we fix it below.
        pos_initDatagram(DATAGRAM_TYPEDESC, _buffer.buffer().size());


        // Force materialization and update of the buffer, since we're
        // injecting the initial symbol table.
        try {
            materialize(); // This ends up calling doMaterializeValue below.
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        finally {
            try {
                rawStream.close();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            _rawStream = null;
        }

        // TODO this touches privates (testBinaryDataWithNegInt)
        _next_start = _buffer.buffer().size();
    }

    /**
     * @param ionData must be in system mode
     */
    public IonDatagramImpl(IonSystemImpl system, IonCatalog catalog,
                           IonReader ionData)
        throws IOException
    {
        super(system, DATAGRAM_TYPEDESC, false);

        _catalog = catalog;
        _userContents = new ArrayList<IonValue>();
        _buffer = new BufferManager();

        // fake up the pos values as this is an odd case
        // Note that size is incorrect; we fix it below.
        pos_initDatagram(DATAGRAM_TYPEDESC, _buffer.buffer().size());
        _isMaterialized(true);
        _hasNativeValue(true);

        if (ionData != null)  // FIXME refactor, we don't throw in this case
        {
            IonWriter treeWriter = system.newTreeSystemWriter(this);

            treeWriter.writeValues(ionData);

            populateSymbolValues(null);
        }
    }

    public IonType getType()
    {
        return IonType.DATAGRAM;
    }

    @Override
    public IonValueImpl topLevelValue()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(IonValue element)
        throws ContainedValueException, NullPointerException
    {
        boolean isSystem = false;
        if (_system.valueIsSystemId(element)) {
            // Ensure systemId has proper symtab and flags
            _system.blessSystemIdSymbol((IonSymbolImpl) element);
            isSystem = true;
        }
        else {
            if (this.get_child_count() == 0) {
                this.injectInitialIVM();
            }
            isSystem = UnifiedSymbolTable.valueIsLocalSymbolTable(element);
        }

        int systemPos = this.get_child_count();
        int userPos = isSystem ? -1 : this._userContents.size();
        try {
            this.add( element, systemPos, userPos );
            return true;
        } catch (IOException e) {
            throw new IonException(e);
        }
    }

    /**
     * @param userPos  if <0 then the element is a system value.
     */
    private void add(IonValue element, int systemPos, int userPos)
        throws ContainedValueException, NullPointerException, IOException
    {
        checkForLock(); // before we start modifying anything
        validateNewChild(element);

        // here we are going to FORCE this member to have a symbol
        // table, whether it likes it or not.

        SymbolTable existing_symtab = element.getSymbolTable();

        if (UnifiedSymbolTable.isTrivialTable(existing_symtab)) {
            SymbolTable new_symtab = null;
            if (_system.valueIsSystemId(element)) {
                new_symtab = _system.getSystemSymbolTable();
            }
            else if (_symboltable != null) {
                new_symtab = _symboltable;
            }
            else {
                new_symtab = getCurrentSymbolTable(systemPos);
            }
            if (existing_symtab == null
             || UnifiedSymbolTable.isTrivialTable(new_symtab) == false
            ) {
                if (new_symtab == null) {
                    // FIXME this uses an unpredictable system symtab
                    // TODO can we delay this until later?
                    new_symtab = _system.newLocalSymbolTable();
                }
                // NOTE: this doesn't reset any extant encoded data
                ((IonValueImpl)element).setSymbolTable(new_symtab);
                if (new_symtab == _symboltable) {
                    _symboltable = null;
                }
            }
        }
        assert element.getSymbolTable() != null;

        add(systemPos, element, true);

        if (userPos >= 0) {
            _userContents.add(userPos, element);
        }
    }

    SymbolTable getCurrentSymbolTable(int systemPos)
    {
        SymbolTable symtab = this._symboltable;

        if (UnifiedSymbolTable.isLocalAndNonTrivial(symtab)) {
            this._symboltable = null;
            return symtab;
        }

        // if the systemPos is 0 (or less) then there's no room
        // for a local symbol table, so there's no need to look
        if (systemPos > 0)
        {
            IonValueImpl v = (IonValueImpl)get_child(systemPos - 1);
            if (UnifiedSymbolTable.valueIsLocalSymbolTable(v))
            {
                IonStruct symtabStruct = (IonStruct) v;
                symtab = UnifiedSymbolTable.makeNewLocalSymbolTable(
                             _system.getSystemSymbolTable()
                             ,_catalog
                             , symtabStruct
                         );
            }
            else if (v._isSystemValue()) {
                assert (_system.valueIsSystemId(v) == true);
                symtab = _system.getSystemSymbolTable();
            }
            else {
                // if the preceding value isn't a symbol table in
                // it's own right, we can just use it's symbol table
                // (it should have one)
                symtab = v._symboltable;
                assert symtab != null;
            }
        }

        return symtab;
    }


    @Override
    public void add(int index, IonValue element)
        throws ContainedValueException, NullPointerException
    {
        add(index, element, true);

//        // TODO JIRA ION-84
//        throw new UnsupportedOperationException("JIRA issue ION-84");
    }

    @Override
    public ValueFactory add(int index)
        throws ContainedValueException, NullPointerException
    {
        // TODO JIRA ION-84
        throw new UnsupportedOperationException("JIRA issue ION-84");
    }

    @Override
    public boolean addAll(int index, Collection<? extends IonValue> c)
    {
        // TODO JIRA ION-83
        throw new UnsupportedOperationException("JIRA issue ION-83");
    }

    @Override
    public IonValue set(int index, IonValue element)
    {
        // TODO JIRA ION-90
        throw new UnsupportedOperationException("JIRA issue ION-90");

    }

    @Override
    public IonValue get(int index)
        throws NullValueException, IndexOutOfBoundsException
    {
        return _userContents.get(index);
    }

    public IonValue systemGet(int index)
        throws IndexOutOfBoundsException
    {
        return super.get(index);
    }

    @Override
    public Iterator<IonValue> iterator() {
        return new UserContentsIterator(_userContents.listIterator(0),
                                        this.isReadOnly());
    }

    @Override
    public ListIterator<IonValue> listIterator(int index)
    {
        // TODO implement remove, add, set on UserDatagram.listIterator()
        // Tricky bit is properly updating both user and system contents.
        return new UserContentsIterator(_userContents.listIterator(index),
                                        true);
    }

    public ListIterator<IonValue> systemIterator()
    {
        // Modification is *not* disabled.
        // What if a system value is removed or inserted?
        // the iterator repositions itself on the "current"
        return new SequenceContentIterator(0, this.isReadOnly());
    }

    @Override
    public void clear()
    {
        super.clear();
        _userContents.clear();
    }

    @Override
    public void makeReadOnly() {
        // we know this is the root, so there's no helper here: make_read_only_helper(true);
        if (_isLocked()) return;
        synchronized (this) { // TODO why is this needed?
            deepMaterialize();
            updateBuffer();
            if (_children != null) {
                for (int ii=0; ii<_child_count; ii++) {
                    IonValue child = _children[ii];
                    //child.makeReadOnly();
                    ((IonValueImpl)child).make_read_only_helper(false);
                }
            }
            _isLocked(true);
        }

    }

    @Override
    public boolean isEmpty() throws NullValueException
    {
        return _userContents.isEmpty();
    }


    @Override
    public int indexOf(Object o)
    {
        IonValueImpl v = (IonValueImpl) o;
        if (this != v.getContainer()) return -1;

        // elementId is index in system _contents
        int i = Math.min(v.getElementId(), _userContents.size() - 1);
        for (; i >= 0; i--)
        {
            if (_userContents.get(i) == v) return i;
        }
        throw new RuntimeException("Inconsistent state in datagram.");
    }


    @Override
    public boolean remove(IonValue element) throws NullValueException
    {
        checkForLock();

        if (element.getContainer() != this) return false;

        // TODO may leave dead symbol tables (and/or symbols) in the datagram
        for (Iterator<IonValue> i = _userContents.iterator(); i.hasNext();)
        {
            IonValue child = i.next();
            if (child == element) // Yes, instance identity.
            {
                super.remove(element);
                i.remove();
                return true;
            }
        }
        return false;
    }


    @Override
    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public int size() throws NullValueException
    {
        return _userContents.size();
    }

    public int systemSize()
    {
        return super.size();
    }


    @Override
    public void addTypeAnnotation(String annotation)
    {
        String message = "Datagrams do not have annotations";
        throw new UnsupportedOperationException(message);
    }


    @Override
    public SymbolTable getSymbolTable()
    {
        // FIXME this is incompatible with the documentation in IonDatagram
        return null;
    }


    @Override
    public final SymbolTable getAssignedSymbolTable()
    {
        throw new UnsupportedOperationException();
    }


    @Override
    public void makeNull()
    {
        throw new UnsupportedOperationException("Cannot make a null datagram");
    }


    public void accept(ValueVisitor visitor)
        throws Exception
    {
        visitor.visit(this);
    }


    @Override
    public void clearTypeAnnotations()
    {
        // No annotations, nothing to do.
        // Except complain about misuse of this api.
        checkForLock();
    }


//    public IonContainer getContainer()
//    {
//        return null;
//    }


    /**
     * @return null
     */
    @Override
    public String getFieldName()
    {
        return null;
    }


    /**
     * @return an empty array.
     */
    @Override
    public String[] getTypeAnnotationStrings()
    {
        return EMPTY_STRING_ARRAY;
    }


    /**
     * @return an empty array.
     */
    @Override
    public String[] getTypeAnnotations()
    {
        return EMPTY_STRING_ARRAY;
    }


    /**
     * @return false
     */
    @Override
    public boolean hasTypeAnnotation(String annotation)
    {
        return false;
    }

    /**
     * @return false
     */
    //@Override
    //public boolean isNullValue()
    //{
    //    assert(this._isNullValue() == false);
    //    return false;
    //}

    /**
     * Does nothing since datagrams have no annotations.
     */
    @Override
    public void removeTypeAnnotation(String annotation)
    {
        // Nothing to do, no annotations here.
        // Except check to see is someone is trying to misuse this instance.
        checkForLock();
    }


    //=========================================================================
    // IonValueImpl overrides

    /**
     * This keeps the datagram from blowing up during updates.
     */
    @Override
    public int getTypeDescriptorAndLengthOverhead(int valuelen)
    {
        return 0;
    }

    /**
     * Overrides IonValueImpl to NOT create a symbol table. At
     * the datagram the symbol table isn't a valid concept for
     * ownership.  This returning null will cause the child to
     * create a symbol table instead.
     */
    @Override
    protected SymbolTable materializeSymbolTable()
    {
        return null;
    }

    /**
     * Overrides IonValueImpl.container copy to add code to
     * read the field name symbol ids (fieldSid)
     * @throws IOException
     */
    @Override
    protected void doMaterializeValue(IonBinary.Reader reader) throws IOException
    {
        assert get_child_count() == 0;
        assert _userContents !=  null && _userContents.size() == 0;

        SymbolTable previous_symtab = null;
        IonValue    previous_value = null;

        while (_rawStream.hasNext())
        {
            IonValueImpl child = (IonValueImpl)_rawStream.next();
            SymbolTable child_symtab = child.getSymbolTable();

            // as we insert the first value we need to check and make sure there
            // is an IonVersionMarker in place (which is required for a datagram
            // but only if we didn't get one out of the input stream
            if (previous_value == null && !_system.valueIsSystemId(child)) {
                IonSymbolImpl ivm = this.injectInitialIVM();
                previous_value = ivm;
            }

            // CAS: 14 jan 2010: reorg this logic to always check
            // to see if a local symbol table is needed, even on
            // system values, since a local symbol table can have
            // open content that needs it's own symbol table
            // So now we always do this block of work

            // for user values we have to check for symbol table fixups
            if (child_symtab != previous_symtab
                && isNeededLocalSymbolTable(child_symtab))
            {
                IonStruct sym =
                    ((UnifiedSymbolTable)child_symtab).getIonRepresentation(_system);

                // this value symbol table might already be present
                // in which case it will be the value just before us
                // let's check ...
                if (sym != previous_value) {
                    // it's not, then it better be an unattached value
                    assert sym.getContainer() == null;

                    // We'll fix things up when/if we updateBuffer.
                    setDirty();
                }
            }

            addToContents(child);


            if (!_rawStream.currentIsHidden())
            {
                // for not hidden values we add them to the user
                // visible contents
                _userContents.add(child);
            }
            else {
                // for system values these generally ARE a symbol table
                // or imply a shift to the system symbol table so we only
                // have to add it to the system contents

                if (_system.valueIsSystemId(child)) {
                    // Not sure why this is the case, but it forces the whole
                    // datagram to be marked dirty (see below).
                    assert child.isDirty();
                }

                child._isSystemValue(true);
                // already done: addToContents(child);
            }

            // TODO it would be better if this didn't happen, but IVMs come out
            // dirty already. See assertion above.
            if (child.isDirty()) {
                setDirty();
            }

            previous_value = child;

            // this symbol table has been added to the datagram (if it needed to be)
            previous_symtab = child_symtab;
        }
    }
    private void addToContents(IonValueImpl v) {
        v._container = this;
        v._elementid = this.get_child_count();
        this.add_child(v._elementid, v);
    }


    /**
     * FIXME this can modify state, even when read-only
     * I'm not confident that synchronization is sufficient.
     */
    private synchronized int updateBuffer() throws IonException
    {
        int oldSize = 0;

        try
        {
            _buffer.reader().sync();   // TODO is this necessary?

            oldSize = _buffer.buffer().size();

            // a datagram has to start with an Ion Version Marker
            // so here we check and if it doesn't - we fix that
            if ( get_child_count() == 0
             || _system.valueIsSystemId(get_child(0)) == false
            ) {
                injectInitialIVM();
            }

            if (this.isDirty())
            {
                // Ensure correct symbol table data is in place.
                // Iterate _userContents and inject local symtabs
                // as needed.
                populateSymbolValues(null);

                updateBufferInsertLocalSymbolTables();
            }

            // now that we've fixed up all the system values
            // we can actually update the buffer itself
            updateBuffer2(_buffer.writer(0), 0, 0);

            // we should always have a size, since we inject
            // an IVM into the datagram even if nothing else
            // is present (one can ask whether that's a good
            // idea or not, but we'll have a size here)
            int size = systemSize();
            if (size > 0) {
                int idx = size - 1;
                IonValueImpl lastChild = (IonValueImpl)
                    systemGet(idx);

                assert _buffer.writer().position() ==
                    lastChild.pos_getOffsetofNextValue();
            }
            else {
                assert _buffer.writer().position() == 0;
            }
            _buffer.writer().truncate();
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }

        return _buffer.buffer().size() - oldSize;
    }

    private void updateBufferInsertLocalSymbolTables() throws ContainedValueException, NullPointerException, IOException
    {
        // PASS TWO - insert any needed symbol tables (if they aren't in the buffer)
        for (int ii=0; ii<_userContents.size(); ii++)
        {
            IonValueImpl ichild = (IonValueImpl)_userContents.get(ii);
            SymbolTable symtab = ichild.getSymbolTable();

            // jonker 2008-12-11: We assert this because we (currently)
            // force symtab creation when elements are added.
            // That should probably be delayed until necessary here.
            assert symtab != null;

            if (symtab == null) {
                // FIXME we should use the symtab of the prior child
                symtab = ichild.materializeSymbolTable();
            }
            if (symtab != null) {
                // now that this symbol table is up to date let's make sure
                // that (if it's got any useful symbols in it) it's serialized
                // in the datagram *before* the value that needs it.
                if (isNeededLocalSymbolTable(symtab)) {
                    IonValue ionsymtab =
                        ((UnifiedSymbolTable)symtab).getIonRepresentation(this._system);
                    if (ionsymtab.getContainer() == null) {
//                        assert ionsysmtab.getSymbolTable() == null;

                        int systemPos = ichild._elementid;
                        assert get_child(systemPos) == ichild;

                        // TODO this is more agressive than necessary
                        // assuming we are allow local symtab chaining.
                        // Here we always inject a new systemId.
                        if (systemPos < 1
                         || !_system.valueIsSystemId(get_child(systemPos - 1))
                         ) {
                            IonSymbolImpl ivm = makeIonVersionMarker();
                            assert ivm.getSymbolTable().isSystemTable();
                            this.add(ivm, systemPos, -1);
                            systemPos++;
                        }
                        this.add(ionsymtab, systemPos, -1);
//FIXME: we need to recurse here if the local symbol table needs its own local symbol table which may not have been added
                    }
                    else {
                        assert ionsymtab.getContainer() == this;
                        assert ionsymtab.getSymbolTable() != null;
                    }
                }
            }
        }
    }

    static boolean isNeededLocalSymbolTable(SymbolTable symtab) {
        if (symtab.isLocalTable()) {
            if (symtab.getImportedTables().length > 0) return true;
            if (symtab.getMaxId() > symtab.getSystemSymbolTable().getMaxId()) return true;
        }
        return false;
    }

    @Override
    protected int computeLowNibble(int valuelen)
        throws IOException
    {
        return 0;
    }

    /**
     * Preconditions: isDirty().  Children's tokens not updated nor shifted.
     * @throws IOException
     */
    @Override
    protected int writeValue(IonBinary.Writer writer,
                             int cumulativePositionDelta) throws IOException
    {
        assert _hasNativeValue();

        cumulativePositionDelta = doWriteContainerContents(writer, cumulativePositionDelta);

        return cumulativePositionDelta;
    }

    @Override
    protected int doWriteContainerContents(IonBinary.Writer writer,
                                           int cumulativePositionDelta)
        throws IOException
    {
        for (int ii = 0; ii<get_child_count(); ii++) {
            IonValueImpl child;
            child = (IonValueImpl) get_child(ii);

            cumulativePositionDelta =
            child.updateBuffer2(writer, writer.position(),
                                cumulativePositionDelta);
        }
        this._next_start = writer.position();
        return cumulativePositionDelta;
    }



    public int byteSize()
        throws IonException
    {
        updateBuffer();
        return _buffer.buffer().size();
    }

    @Deprecated
    public byte[] toBytes() throws IonException
    {
        return getBytes();
    }

    public byte[] getBytes() throws IonException
    {
        int len = byteSize();
        byte[] bytes = new byte[len];

        doGetBytes(bytes, 0, len);

        return bytes;
    }

    public int getBytes(byte[] dst)
        throws IonException
    {
        return getBytes(dst, 0);
    }

    public int getBytes(byte[] dst, int offset)
        throws IonException
    {
        return doGetBytes(dst, offset, byteSize());
    }

    private int doGetBytes(byte[] dst, int offset, int byteSize)
        throws IonException
    {
        try
        {
            IonBinary.Reader reader = _buffer.reader();
            reader.sync();
            reader.setPosition(0);
            return reader.read(dst, offset, byteSize);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }

    public int getBytes(OutputStream out)
        throws IOException
    {
        int len = byteSize();  // Throws IonException if there's breakage.

        // Allow IOException to propagate from here.
        IonBinary.Reader reader = _buffer.reader();
        reader.sync();
        reader.setPosition(0);
        int len2 = reader.writeTo(out, len);
        assert len == len2;

        return len2;
    }

    /** Encapsulates an iterator and implements a custom remove method */
    protected final class UserContentsIterator
        implements ListIterator<IonValue>
    {
        private final ListIterator<IonValue> it;

        private final boolean readOnly;

        private IonValue current;

        public UserContentsIterator(ListIterator<IonValue> it,
                                    boolean readOnly)
        {
            this.it = it;
            this.readOnly = readOnly;
        }


        public void add(IonValue element)
        {
            throw new UnsupportedOperationException();
        }

        public boolean hasNext()
        {
            return it.hasNext();
        }

        public boolean hasPrevious()
        {
            return it.hasPrevious();
        }

        public IonValue next()
        {
            current = it.next();
            return current;
        }

        public int nextIndex()
        {
            return it.nextIndex();
        }

        public IonValue previous()
        {
            current = it.previous();
            return current;
        }

        public int previousIndex()
        {
            return it.previousIndex();
        }

        /**
         * Sets the container to dirty after calling {@link Iterator#remove()}
         * on the encapsulated iterator
         */
        public void remove()
        {
            if (readOnly) {
                throw new UnsupportedOperationException();
            }

            IonValueImpl concrete = (IonValueImpl) current;

            it.remove();
            try
            {
                concrete.detachFromContainer();
            }
            catch (IOException e)
            {
                throw new IonException(e);
            }
            finally
            {
                updateElementIds(concrete.getElementId());
                setDirty();
            }
        }

        public void set(IonValue element)
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public IonValue[] toArray()
    {
        if (get_child_count() < 1) return EMPTY_VALUE_ARRAY;
        IonValue[] array = new IonValue[this._userContents.size()];
        _userContents.toArray(array);
        return array;
    }

    @Override
    public <T> T[] toArray(T[] a)
    {
        return _userContents.toArray(a);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends IonValue> T[] extract(Class<T> type)
    {
        if (isNullValue()) return null;
        T[] array = (T[]) Array.newInstance(type, size());
        toArray(array);
        clear();
        return array;
    }

    @Override
    public SymbolTable populateSymbolValues(SymbolTable nullSymtab)
    {
        // when called on the datagram there should
        // be no defined symbol table yet
        assert(nullSymtab == null);

        // FIXME stuff will break if user adds a local symtab struct.
        // We won't detect that anywhere.

        // PASS ONE: Make sure all user values have a local symtab
        // filled with all the necessary local symbols.
        SymbolTable currentSymtab = null;
        if (get_child_count() > 0) {
            IonValueImpl first = (IonValueImpl)get_child(0);
            currentSymtab = first.getSymbolTable();
            if (!_system.valueIsSystemId(first)) {
                IonValue ivm = injectInitialIVM();
                currentSymtab = ivm.getSymbolTable();
            }
        }
        assert UnifiedSymbolTable.isTrivialTable(currentSymtab); // was: currentSymtab.isSystemTable();
        boolean priorIsLocalSymtab = false;

        // this starts at 1 since we forced the 0th entry to be an IVM
        for (int ii = 1; ii < this.get_child_count(); ii++)
        {
            IonValueImpl ichild = (IonValueImpl)get_child(ii);
            if (_system.valueIsSystemId(ichild))
            {
                currentSymtab = ichild.getSymbolTable();
                assert UnifiedSymbolTable.isSystemTable(currentSymtab);
                continue;
            }

            SymbolTable symtab = ichild.getSymbolTable();
            if (UnifiedSymbolTable.isLocalTable(symtab)) {
                currentSymtab = symtab;
            }
            else
            {
                symtab = currentSymtab;
                ichild.setSymbolTable(symtab);
            }
            // TODO why would this change currentSymtab?
            currentSymtab = ichild.populateSymbolValues(currentSymtab);

            priorIsLocalSymtab = IonSystemImpl.valueIsLocalSymbolTable(ichild);
            if (priorIsLocalSymtab)
            {
                currentSymtab =
                    UnifiedSymbolTable.makeNewLocalSymbolTable(
                        _system.getSystemSymbolTable()
                      ,_catalog
                      ,(IonStruct)ichild
                    );
                assert UnifiedSymbolTable.isLocalTable(currentSymtab);
            }
        }

        return currentSymtab;
    }

    private IonSymbolImpl injectInitialIVM()
    {
        IonSymbolImpl ivm = _system.newSystemIdSymbol(ION_1_0);
        SymbolTable symbols;

        symbols = _system.getSystemSymbolTable();
        ivm.setSymbolTable(symbols);
        ivm.setDirty();

        // we save the user set current symbol table
        // since add may fiddle with this and try to
        // put the user supplied symbol table in ivm
        symbols = this._symboltable;
        this._symboltable = null;

        // now we do the add with some confidence
        // the symbol table won't be stepped on.
        add(0, ivm);

        // and now we restore it
        this._symboltable = symbols;

        return ivm;
    }

    private IonSymbolImpl makeIonVersionMarker()
    {
        IonSymbolImpl ivm =
            _system.newSystemIdSymbol(ION_1_0);

        // TODO why is this needed?
        ivm._buffer = this._buffer;

        return ivm;
    }

}
