// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonImplUtils.EMPTY_STRING_ARRAY;

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
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonBinary.BufferManager;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
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

    /** Underlying catalog */
    private final IonCatalog _catalog;

    /**
     * Used while constructing, then set to null.
     */
    private SystemReader _rawStream;

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
     *
     * @param buffer is filled with Ion data.
     *
     * @throws NullPointerException if any parameter is null.
     */
    public IonDatagramImpl(IonSystemImpl system, IonCatalog catalog, BufferManager buffer)
    {
        this(system, new SystemReader(system, catalog, buffer));
    }


    public IonDatagramImpl(IonSystemImpl system, IonCatalog catalog, Reader ionText)
    {
        this(system
            ,catalog
            ,system.newLocalSymbolTable()
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
        // FIXME this method is unused, since our clone() does its own thing.

        // first copy the annotations and such, which
        // will materialize the value as needed.
        // This will materialize the field name and
        // annotations if present.  And it will instanciate
        // the immediate children (but it is not
        // a deep materialization, so we'll live with
        // it for now).
        super.copyFrom(source);

        // now we can copy the contents

        // first see if this value is null (and we're really
        // done here)
        if (source.isNullValue()) {
            makeNull();
        }
        else {
            // it's not null so there better be something there
            // at least 0 children :)
            assert source._contents != null;

            // and we'll need a contents array to hold at least 0
            // children
            if (this._contents == null) {
                int len = source._contents.size();
                if (len < 1) len = 10;
                this._contents = new ArrayList<IonValue>(len);
            }
            // we should have an empty content list at this point
            assert this._contents.size() == 0;

            if (false && source._buffer != null && !source.isDirty()) {
                // if this is buffer backed, and not dirty
                // then we can do a binary copy

                // TODO: offer this optimized path, however this requires
                //       a variety of general purpose binary buffer handling
                //       and should probably be done along with the lazy
                //       "copy on write" reference/copy optimizations
                //       - which is really a project in its own right
            }
            else {
                // if this is not buffer backed, we just have to
                // do a deep copy
                for (IonValue child : source._contents) {
                    IonValue copy = child.clone();
                    this.add(copy);
                }
            }
        }
    }

    /**
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
             new SystemReader(system,
                              catalog,
                              initialSymbolTable, ionText));
    }

    /**
     * Workhorse constructor this does the actual work.
     *
     * @throws NullPointerException if any parameter is null.
     */
    IonDatagramImpl(IonSystemImpl system,
                    SystemReader rawStream)
    {
        super(system, DATAGRAM_TYPEDESC, true);

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
            rawStream.close();
            _rawStream = null;
        }

        // TODO this touches privates (testBinaryDataWithNegInt)
        _next_start = _buffer.buffer().size();
    }

    /**
     * @param ionData must be in system mode
     */
    public IonDatagramImpl(IonSystemImpl system, IonCatalog catalog, IonReader ionData)
        throws IOException
    {
        super(system, DATAGRAM_TYPEDESC, false);

        _catalog = catalog;
        _userContents = new ArrayList<IonValue>();
        _buffer = new BufferManager();

        // fake up the pos values as this is an odd case
        // Note that size is incorrect; we fix it below.
        pos_initDatagram(DATAGRAM_TYPEDESC, _buffer.buffer().size());
        _isMaterialized = true;
        _hasNativeValue = true;

        if (ionData != null)  // FIXME refactor, we don't throw in this case
        {
            IonWriter treeWriter = system.newWriter(this);
            treeWriter.writeValues(ionData);
        }
    }

    public IonType getType()
    {
        return IonType.DATAGRAM;
    }


    @Override
    protected ArrayList<IonValue> userContents()
    {
        makeReady();
        return _userContents;
    }

    private IonSymbolImpl makeIonVersionMarker()
    {
        IonSymbolImpl ivm =
            _system.newSystemIdSymbol(SystemSymbolTable.ION_1_0);

        // TODO why is this needed?
        ivm._buffer = this._buffer;

        return ivm;
    }

    // FIXME need to make add more solid, maintain symbol tables etc.

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
            isSystem = _system.valueIsLocalSymbolTable(element);
        }

        int systemPos = this._contents.size();
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

        SymbolTable symtab = element.getSymbolTable();

        if (symtab == null) {
            symtab = getCurrentSymbolTable(userPos, systemPos);
            if (symtab == null) {
                // FIXME this uses an unpredictable system symtab
                // TODO can we delay this until later?
                symtab = _system.newLocalSymbolTable();
                IonStructImpl ionsymtab = (IonStructImpl)symtab.getIonRepresentation();
                ionsymtab.setSymbolTable(symtab);  // XXX wrong
            }
            // NOTE: this doesn't reset any extant encoded data
            ((IonValueImpl)element).setSymbolTable(symtab);
        }
        assert symtab != null;

        add(systemPos, element, true);
        //  removed again 22 apr 2009 - assert element.getSymbolTable() == null; // cas removed 1 apr 2008, restored 19 apr 2008, ta da!

        if (userPos >= 0) {
            _userContents.add(userPos, element);
        }
    }

    SymbolTable getCurrentSymbolTable(int userPos, int systemPos)
    {
        SymbolTable symtab = null;

        // if the systemPos is 0 (or less) then there's no room
        // for a local symbol table, so there's no need to look
        if (systemPos > 0) {
            IonValueImpl v = (IonValueImpl)_contents.get(systemPos - 1);
            if (false // FIXME
                && _system.valueIsLocalSymbolTable(v))
            {
                // FIXME the appropriate symtab may already be attached to the
                // child AFTER the insertion point (at userPos)

                // 2008-12-12 Currently this only works because this method is
                // only called when appending to the end of this datagram.
                assert systemPos == _contents.size();
                assert userPos == -1 || userPos == _userContents.size();

                // We always inject the systemId, so we always have a system table
                IonValue prior = _contents.get(systemPos - 2);

//                if (false) {
                IonStruct symtabStruct = (IonStruct) v;
                symtab = UnifiedSymbolTable.makeNewLocalSymbolTable(
                             _system.getSystemSymbolTable()
                             , symtabStruct
                             ,_catalog
                         );
//                }
//                else {
//                    symtab = v.getSymbolTable();
//                }
            }
            else {
                // if the preceeding value isn't a symbol table in
                // it's own right, we can just use it's symbol table
                // (it should have one)
                symtab = v._symboltable;
                assert symtab != null;
            }
        }
// FIXME I have no idea what this stuff is doing:
        if (symtab == null) {
            int userSize = (userPos != -1) ? userPos : _userContents.size();
            int fullsize = systemPos - 1;

            if (userSize > 0) {
                symtab = _userContents.get(userSize - 1).getSymbolTable();
            }
            else if (fullsize > 0) {
                symtab = _contents.get(fullsize - 1).getSymbolTable();
            }
        }
        return symtab;
    }

    @Override
    public void add(int index, IonValue element)
        throws ContainedValueException, NullPointerException
    {
        // TODO JIRA ION-84
        throw new UnsupportedOperationException("JIRA issue ION-84");
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
    public ListIterator<IonValue> listIterator(int index)
    {
        // TODO implement remove, add, set on UserDatagram.listIterator()
        // Tricky bit is properly updating both user and system contents.
        return new IonContainerIterator(_userContents.listIterator(index),
                                        true);
    }

    public ListIterator<IonValue> systemIterator()
    {
        // Modification is disabled.
        // What if a system value is removed or inserted?
        return new IonContainerIterator(_contents.listIterator(), true);
    }

    @Override
    public void clear()
    {
        super.clear();
        _userContents.clear();
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
    @Override
    public boolean isNullValue()
    {
        return false;
    }


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
        assert _contents !=  null && _contents.size() == 0;
        assert _userContents !=  null && _userContents.size() == 0;

        SymbolTable previous_symtab = null;
        IonValue    previous_value = null;

        while (_rawStream.hasNext())
        {
            IonValueImpl child = _rawStream.next();
            SymbolTable child_symtab = child.getSymbolTable();

            // as we insert the first value we need to check and make sure there
            // is an IonVersionMarker in place (which is required for a datagram
            // but only if we didn't get one out of the input stream
            if (previous_value == null && !_system.valueIsSystemId(child)) {
                IonSymbolImpl ivm = this.makeIonVersionMarker();

                this.addToContents(ivm);
                setDirty();

                previous_value = ivm;
            }


            if (_rawStream.currentIsHidden()) {
                // for system values these generally ARE a symbol table
                // or imply a shift to the system symbol table so we only
                // have to add it to the system contents

                if (_system.valueIsSystemId(child)) {
                    // Not sure why this is the case, but it forces the whole
                    // datagram to be marked dirty (see below).
                    assert child.isDirty();
                }

                child._isSystemValue = true;
                addToContents(child);
            }
            else {
                // for user values we have to check for symbol table fixups
                if (child_symtab != previous_symtab
                    && isNeededLocalSymbolTable(child_symtab))
                {
                    IonStruct sym = child_symtab.getIonRepresentation();

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

                _userContents.add(child);
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
        v._elementid = this._contents.size();
        this._contents.add(v);
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
            _buffer.reader().sync();   // FIXME is this correct?

            oldSize = _buffer.buffer().size();

            // a datagram is not a datagram unless it has an
            // IonVersionMarker (at least)
            if (_contents.size() == 0) {
                IonSymbolImpl ivm = makeIonVersionMarker();
                this.add(ivm, 0, -1);
                setDirty();
            }

            if (this.isDirty()) {
                // a datagram has to start with an Ion Version Marker
                // so here we check and if it doesn't - we fix that
                {
                    IonValue first = _contents.get(0);
                    if (!_system.valueIsSystemId(first)) {
                        IonSymbolImpl ivm = makeIonVersionMarker();
                        this.add(ivm, 0, -1);
                    }
                }

                // Ensure correct symbol table data is in place.
                // Iterate _userContents and inject local symtabs as needed.

                // FIXME stuff will break if user adds a local symtab struct.
                // We won't detect that anywhere.

                // PASS ONE: Make sure all user values have a local symtab
                // filled with all the necessary local symbols.
                SymbolTable currentSymtab = _contents.get(0).getSymbolTable();
                assert currentSymtab.isSystemTable();
                boolean priorIsLocalSymtab = false;
                for (int ii = 1; ii < _contents.size(); ii++)
                {
                    IonValueImpl ichild = (IonValueImpl)_contents.get(ii);

                    if (_system.valueIsSystemId(ichild))
                    {
                        currentSymtab = ichild.getSymbolTable();
                        assert currentSymtab.isSystemTable();
                        continue;
                    }


                    SymbolTable symtab = ichild.getSymbolTable();
                    if (symtab == null || symtab.isSystemTable())
                    {
                        if (priorIsLocalSymtab)
                        {
                            currentSymtab =
                                UnifiedSymbolTable.makeNewLocalSymbolTable(
                                    _system.getSystemSymbolTable()
                                  , (IonStruct) _contents.get(ii - 1)
                                  ,_catalog
                                );
                        }
                        else if (currentSymtab.isSystemTable())
                        {
                            currentSymtab = _system.newLocalSymbolTable(currentSymtab);
                        }
                        else
                        {
                            assert currentSymtab.isLocalTable();
                        }

                        symtab = currentSymtab;

                        // TODO this shouldn't happen if ichild is local symtab
                        ichild.setSymbolTable(symtab);
                    }

                    ichild.updateSymbolTable(symtab);

                    priorIsLocalSymtab = _system.valueIsLocalSymbolTable(ichild);
                }

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
                        // TODO we shouldn't always require a local symtab
                        // When values are inserted they get the prior symtab,
                        // which may be system.
//                        if (symtab.isSystemTable()) {
//                            symtab = _system.newLocalSymbolTable(symtab);
//                            ichild.setSymbolTable(symtab);
//                        }
//                        ichild.updateSymbolTable(symtab);

                        // now that this symbol table is up to date let's make sure
                        // that (if it's got any useful symbols in it) it's serialized
                        // in the datagram *before* the value that needs it.
                        if (this.isNeededLocalSymbolTable(symtab)) {
                            IonValue ionsymtab = symtab.getIonRepresentation();
                            if (ionsymtab.getContainer() == null) {
//                                assert ionsysmtab.getSymbolTable() == null;

                                int systemPos = ichild._elementid;
                                assert _contents.get(systemPos) == ichild;

                                // TODO this is more agressive than necessary
                                // assuming we are allow local symtab chaining.
                                // Here we always inject a new systemId.
                                if (systemPos < 1
                                 || !_system.valueIsSystemId(this._contents.get(systemPos - 1))
                                 ) {
                                    IonSymbolImpl ivm = makeIonVersionMarker();
                                    assert ivm.getSymbolTable().isSystemTable();
                                    this.add(ivm, systemPos, -1);
                                    systemPos++;
                                }
                                this.add(ionsymtab, systemPos, -1);
                            }
                            else {
                                assert ionsymtab.getContainer() == this;
                                assert ionsymtab.getSymbolTable() != null;
                            }
                        }
                    }
                }
            }

            // now that we've fixed up all the system values we can
            // actually update the buffer itself
            updateBuffer2(_buffer.writer(0), 0, 0);

            // cas 22 apr 2008: was ...
            //updateBuffer2(_buffer.writer(BINARY_VERSION_MARKER_SIZE),
            //              BINARY_VERSION_MARKER_SIZE,
            //              0);

            if (systemSize() == 0) {
                // Nothing should've been written.
                assert _buffer.writer().position() == 0; // cas 22 apr 2008: was: BINARY_VERSION_MARKER_SIZE;
            }
            else {
                IonValueImpl lastChild = (IonValueImpl)
                    systemGet(systemSize() - 1);
                assert _buffer.writer().position() ==
                    lastChild.pos_getOffsetofNextValue();
            }
            _buffer.writer().truncate();
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }

        return _buffer.buffer().size() - oldSize;
    }

    boolean isNeededLocalSymbolTable(SymbolTable symtab) {
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
        assert _hasNativeValue;

        cumulativePositionDelta = doWriteContainerContents(writer, cumulativePositionDelta);

        return cumulativePositionDelta;
    }

    @Override
    protected int doWriteContainerContents(IonBinary.Writer writer,
                                           int cumulativePositionDelta)
        throws IOException
    {
        if (_contents != null)
        {
            int ii = 0;
            IonValueImpl child;

            while (ii < _contents.size())
            {
                child = (IonValueImpl) _contents.get(ii);

                cumulativePositionDelta =
                child.updateBuffer2(writer, writer.position(),
                                    cumulativePositionDelta);

                ii++;
            }
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
}
