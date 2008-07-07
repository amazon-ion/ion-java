/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SystemSymbolTable;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonBinary.BufferManager;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;

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

    private final static String[] EMPTY_STRING_ARRAY = new String[0];

   // CAS symtab: moved _system to IonValueImpl
   // /**
   //  * The system that created this datagram.
   //  */
   // private IonSystem _system;

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
            buffer.writer().write(IonConstants.BINARY_VERSION_MARKER_1_0);
        }
        catch (IOException e) {
             throw new IonException(e);
        }
        return buffer;
    }


    //=========================================================================

    public IonDatagramImpl(IonSystemImpl system) {
        this(system, make_empty_buffer());
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
    public IonDatagramImpl(IonSystemImpl system, byte[] ionData)
    {
        this(system.newSystemReader(ionData));
    }


    /**
     *
     * @param buffer is filled with Ion data.
     *
     * @throws NullPointerException if any parameter is null.
     */
    public IonDatagramImpl(IonSystemImpl system, BufferManager buffer)
    {
        this(new SystemReader(system, buffer));
    }


    public IonDatagramImpl(IonSystemImpl system, Reader ionText)
    {
        this(system
            ,system.newLocalSymbolTable()
            ,ionText);
    }

    /**
     * throws a CloneNotSupportedException as this is a
     * parent type that should not be directly created.
     * Instances should constructed of either IonSexpImpl
     * IonListImpl or IonStructImpl as needed.
     */
    @Override
    public IonDatagramImpl clone() throws CloneNotSupportedException
    {
        byte[] data = this.toBytes();

        IonDatagramImpl clone = new IonDatagramImpl(this._system, data);

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
     * @throws
     */
    @Override
    protected void copyFrom(IonContainerImpl source) throws CloneNotSupportedException, NullPointerException, IllegalArgumentException, IOException
    {
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
     * @throws NullPointerException if any parameter is null.
     */
    public IonDatagramImpl(IonSystemImpl system,
                           LocalSymbolTable initialSymbolTable,
                           Reader ionText)
    {
        this(new SystemReader(system,
                              system.getCatalog(),
                              initialSymbolTable, ionText));
    }

    /**
     * Workhorse constructor this does the actual work.
     *
     * @throws NullPointerException if any parameter is null.
     */
    IonDatagramImpl(SystemReader rawStream)
    {
        super(DATAGRAM_TYPEDESC, true);

        _userContents = new ArrayList<IonValue>();

        // This is only used during construction.
        _rawStream = rawStream;
        _system = _rawStream.getSystem();
        _buffer = _rawStream.getBuffer();

        // Force reading of the first element, so we get the buffer bootstrapped
        // including the header.
        _rawStream.hasNext();

        // fake up the pos values as this is an odd case
        // Note that size is incorrect; we fix it below.
        pos_initDatagram(DATAGRAM_TYPEDESC, _buffer.buffer().size());


        // Force materialization and update of the buffer, since we're
        // injecting the initial symbol table.
        try {
            materialize(); // This ends up calling doMaterializeValue below.
            // NOT NEEDED: FIXME: place_symbol_table(rawStream.getLocalSymbolTable());  // cas
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

    public IonType getType()
    {
        return IonType.DATAGRAM;
    }

    final boolean isSystemValue(IonValueImpl element) {
        boolean is_system_value = false;
        if (element instanceof IonSymbolImpl) {
            IonSymbolImpl symbol = (IonSymbolImpl)element;
            is_system_value = (symbol.isNullValue() == false)
                    && (SystemSymbolTable.ION_1_0.equals(symbol.getValue()));
        }
        else if (element._annotations != null) {
            if (element instanceof IonStructImpl) {
                is_system_value = element.hasTypeAnnotation(SystemSymbolTable.ION_SYMBOL_TABLE);
            }
            else if (element instanceof IonSexpImpl) {
                is_system_value = element.hasTypeAnnotation(SystemSymbolTable.ION_EMBEDDED_VALUE);
            }
        }
        return is_system_value;
    }
    private IonSymbolImpl makeIonVersionMarker()
    {
        IonSymbolImpl ivm = (IonSymbolImpl)_system.newSymbol(SystemSymbolTableImpl.ION_1_0);

        ivm.setIsIonVersionMarker(true);
        assert ivm.getSymbolTable() == null;

        ivm._buffer = this._buffer;

        return ivm;
    }

    // FIXME need to make add more solid, maintain symbol tables etc.

    @Override
    public void add(IonValue element)
        throws ContainedValueException, NullPointerException
    {
        boolean isSystem = isSystemValue((IonValueImpl)element);
        add(element, isSystem);
    }

    public void add(IonValue element, boolean isSystem)
        throws ContainedValueException, NullPointerException
    {
        int systemPos = this._contents.size();
        int userPos = isSystem ? -1 : this._userContents.size();
        try {
        this.add( element, systemPos, userPos );
		} catch (IOException e) {
			throw new IonException(e);
		}
    }

    private void add(IonValue element, int systemPos, int userPos)
        throws ContainedValueException, NullPointerException, IOException
    {
        checkForLock(); // before we start modifying anything
        validateNewChild(element);

        // here we are going to FORCE this member to have a symbol
        // table, whether it likes it or not.

        LocalSymbolTable symtab = element.getSymbolTable();

        if (symtab == null) {
            symtab = getCurrentSymbolTable(userPos, systemPos);
            if (symtab == null) {
                symtab = _system.newLocalSymbolTable();
                IonStructImpl ionsymtab = (IonStructImpl)symtab.getIonRepresentation();
                ionsymtab.setSymbolTable(symtab);
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
    LocalSymbolTable getCurrentSymbolTable(int userPos, int systemPos)
    {
        LocalSymbolTable symtab = null;

        // if the systemPos is 0 (or less) then there's no room
        // for a local symbol table, so there's no need to look
        if (systemPos > 0) {
            IonValueImpl v = (IonValueImpl)_contents.get(systemPos - 1);
            if (v instanceof IonStructImpl
             && v._annotations != null
             && v.hasTypeAnnotation(SystemSymbolTable.ION_SYMBOL_TABLE))
            {
                if (((IonStructImpl)v).get(SystemSymbolTable.NAME) == null) {
                    // it's a local symbol table alright
                    symtab = new LocalSymbolTableImpl(
                            _system,
                            _system.getCatalog(),
                            (IonStruct)v,
                            _system.getSystemSymbolTable()
                    );
                }
            }
            else {
                // if the preceeding value isn't a symbol table in
                // it's own right, we can just use it's symbol table
                // (it should have one)
                symtab = v._symboltable;
            }
        }

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
        throw new UnsupportedOperationException();
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
    public Iterator<IonValue> iterator() throws NullValueException
    {
        // TODO implement remove on UserDatagram.iterator()
        // Tricky bit is properly removing from both user and system contents.
        return new IonContainerIterator(_userContents.iterator(), false);
    }

    public Iterator<IonValue> systemIterator()
    {
        // Disable remove... what if a system value is removed?
        // What if a system value is removed?
        return new IonContainerIterator(_contents.iterator(), false);
    }

    @Override
    public void clear()
    {
        // TODO implement datagram clear
        throw new UnsupportedOperationException();

    }

    @Override
    public boolean isEmpty() throws NullValueException
    {
        return _userContents.isEmpty();
    }


    @Override
    public boolean remove(IonValue element) throws NullValueException
    {
    	checkForLock();

        // TODO may leave dead symbol tables (and/or symbols) in the datagram
        for (Iterator<IonValue> i = _userContents.iterator(); i.hasNext();)
        {
            IonValue child = i.next();
            if (child == element) // Yes, instance identity.
            {
                i.remove();
                super.remove(element);
                return true;
            }
        }
        return false;
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
    public LocalSymbolTable getSymbolTable()
    {
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
    protected LocalSymbolTable materializeSymbolTable()
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

        SymbolTable previous = null;
        IonValue    previous_value = null;

        while (_rawStream.hasNext())
        {
            IonValueImpl     child = (IonValueImpl) _rawStream.next();
            LocalSymbolTable child_symtab = child.getSymbolTable();
            assert child_symtab != null;

            // as we insert the first value we need to check and make sure there
            // is an IonVersionMarker in place (which is required for a datagram
            // but only if we didn't get one out of the input stream
            if (previous_value == null && !this.isIonVersionMarkerSymbol(child)) {
                IonSymbolImpl ivm = this.makeIonVersionMarker();

                this.addToContents(ivm);
                setDirty();

                previous_value = ivm;
            }


            if (_rawStream.currentIsHidden()) {
                // for system values these generally ARE a symbol table
                // or imply a shift to the system symbol table so we only
                // have to add it to the system contents
                addToContents(child);

                if (this.isIonVersionMarkerSymbol(child)) {
                    ((IonSymbolImpl)child).setIsIonVersionMarker(true);
                    // if this was loaded by the parser it will have the wrong encoding
                    child.setDirty();
                }
                child._isSystemValue = true;
            }
            else {
                // for user values we have to check for symbol table fixups
                if (child_symtab != previous
                 && isNeededLocalSymbolTable(child_symtab)
                ) {
                    IonStruct sym = child_symtab.getIonRepresentation();

                    // this value symbol table might already be present
                    // in which case it will be the value just before us
                    // let's check ...
                    if (sym != previous_value) {
                        // it's not, then it better be an unattached value
                        assert sym.getContainer() == null;

                        // if this doesn't have an Ion Version Marker we have to add
                        // it before the symbol table
                        if (!this.isIonVersionMarkerSymbol(previous_value)) {
                            IonSymbolImpl ivm = this.makeIonVersionMarker();
                            addToContents(ivm);
                        }

                        addToContents((IonValueImpl)sym);
                        setDirty();
                    }
                }

                addToContents(child);

                _userContents.add(child);
            }

            // TO DO doc why this would happen. Isn't child fresh from binary?
            //
            if (child.isDirty()) {
                setDirty();
            }

            previous_value = child;

            // this symbol table has been added to the datagram (if it needed to be)
            previous = child_symtab;
        }
    }
    private void addToContents(IonValueImpl v) {
        v._container = this;
        v._elementid = this._contents.size();
        this._contents.add(v);
    }


    private int updateBuffer() throws IonException
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
                if (_contents.size() > 0) {
                    IonValue first = _contents.get(0);
                    if (!isIonVersionMarkerSymbol(first)) {
                        IonSymbolImpl ivm = makeIonVersionMarker();
                        this.add(ivm, 0, -1);
                    }
                }
                for (int ii=0; ii<_userContents.size(); ii++)
                {
                    IonValueImpl ichild = (IonValueImpl)_userContents.get(ii);
                    LocalSymbolTable symtab = ichild.getSymbolTable();

                    // FIXME: remove or restore - why would we assert this? assert symtab != null;
                    if (symtab == null) {
                    	symtab = ichild.materializeSymbolTable();
                    }
                    if (symtab != null) {
                        ichild.updateSymbolTable(symtab);

                        // now that this symbol table is up to date let's make sure
                        // that (if it's got any useful symbols in it) it's serialized
                        // in the datagram *before* the value that needs it.
                        if (this.isNeededLocalSymbolTable(symtab)) {
                            IonValue ionsymtab = symtab.getIonRepresentation();
                            if (ionsymtab.getContainer() == null) {
                                int pos = ichild._elementid;
                                assert ichild._container._contents.get(pos) == ichild;

                                if (pos < 1
                                 || !isIonVersionMarkerSymbol(this._contents.get(pos - 1))
                                 ) {
                                    IonSymbolImpl ivm = makeIonVersionMarker();
                                    this.add(ivm, pos, -1);
                                    pos++;
                                }
                                this.add(ionsymtab, pos, -1);
                            }
                        }
                    }
                }
            }

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
    boolean isIonVersionMarkerSymbol(IonValue v)
    {
        if (!(v instanceof IonSymbol)) return false;
        String s = ((IonSymbol)v).stringValue();
        return SystemSymbolTable.ION_1_0.equals(s);
    }
    boolean isNeededLocalSymbolTable(SymbolTable symtab) {
        if (symtab instanceof LocalSymbolTable) {
            LocalSymbolTable local = (LocalSymbolTable)symtab;
            if (local.hasImports()) return true;
            if (local.getMaxId() > local.getSystemSymbolTable().getMaxId()) return true;
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
            IonValueImpl child = (IonValueImpl) _contents.get(ii);

            if (this.isIonVersionMarkerSymbol(child)) {

            }
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

    public byte[] toBytes() throws IonException
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

    @Override
    public BufferManager getBuffer() {
        return this._buffer;
    }
}
