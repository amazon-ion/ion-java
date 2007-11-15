/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonConstants.BINARY_VERSION_MARKER_SIZE;
import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.system.StandardIonSystem;
import com.amazon.ion.util.Printer;
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
                                        IonConstants.lnIsDatagram);

    private final static String[] EMPTY_STRING_ARRAY = new String[0];


    /**
     * The system that created this datagram.
     */
    private IonSystem             _system;

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

    public IonDatagramImpl(StandardIonSystem system) {
        this(system, make_empty_buffer());
    }


    public IonDatagramImpl(StandardIonSystem system, Reader ionText)
    {
        this(system, system.newLocalSymbolTable(), ionText);
    }


    public IonDatagramImpl(StandardIonSystem system,
                        LocalSymbolTable initialSymbolTable,
                        Reader ionText)
    {
        this(new SystemReader(system,
                              system.getCatalog(),
                              initialSymbolTable, ionText));

        // If the synthetic local table has anything in it, inject it.
        if (initialSymbolTable.size() != 0 || initialSymbolTable.hasImports())
        {
            IonStruct ionRep = initialSymbolTable.getIonRepresentation();
            assert ionRep.getSymbolTable() != null;
            assert _contents.size() != 0;

            _contents.add(0, ionRep);
            ((IonValueImpl)ionRep)._container = this;

            setDirty();
        }
    }


    /**
     *
     * @param buffer is filled with Ion data.
     */
    public IonDatagramImpl(StandardIonSystem system, BufferManager buffer)
    {
        this(new SystemReader(system, buffer));
    }



    /**
     * workhorse constructor this does the actual work
     */
    public IonDatagramImpl(SystemReader rawStream)
    {
        super(DATAGRAM_TYPEDESC, true);

        _userContents = new ArrayList<IonValue>();

        // This is only used during construction.
        _rawStream = rawStream;

        _system = rawStream.getSystem();
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
            materialize();
            // This ends up in doMaterializeValue below.
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

    //=========================================================================


    public IonType getType()
    {
        throw new UnsupportedOperationException();
    }



    // FIXME need to make add more solid, maintain symbol tables etc.

    @Override
    public void add(IonValue element)
        throws ContainedValueException, NullPointerException
    {
        // FIXME here we assume that we're inserting a user value!

        LocalSymbolTable symtab;
        int userSize = _userContents.size();
        if (userSize == 0)
        {
            symtab = _system.newLocalSymbolTable();
            IonStruct ionRep = symtab.getIonRepresentation();

            // TODO why insert at zero?  Should we just append?
            // Should grab the ST from the last elt of either kind?
            _contents.add(0, ionRep);

            ((IonValueImpl)ionRep)._container = this;
        }
        else
        {
            symtab = _userContents.get(userSize - 1).getSymbolTable();
            assert symtab != null;
        }

        add(_contents.size(), element, true);
        assert element.getSymbolTable() == null;

        // FIXME this doesn't reset any extant encoded data
        ((IonValueImpl)element).setSymbolTable(symtab);
        _userContents.add(element);
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
     * Overrides IonValueImpl.container copy to add code to
     * read the field name symbol ids (fieldSid)
     * @throws IOException
     */
    @Override
    protected void doMaterializeValue(IonBinary.Reader reader) throws IOException
    {
        while (_rawStream.hasNext())
        {
            IonValueImpl child = (IonValueImpl) _rawStream.next();
            assert child.getSymbolTable() != null;

            _contents.add(child);
            child._container = this;

            if (! _rawStream.currentIsHidden())
            {
                _userContents.add(child);
            }

            // TODO doc why this would happen. Isn't child fresh from binary?
            if (child.isDirty()) setDirty();
        }
    }


    private int updateBuffer() throws IonException
    {
        _buffer.reader().sync();   // FIXME is this correct?

        int oldSize = _buffer.buffer().size();

        if (this.isDirty()) {
            for (IonValue child : _userContents) {
                LocalSymbolTable symtab = child.getSymbolTable();
                assert symtab != null;
                ((IonValueImpl) child).updateSymbolTable(symtab);
            }
        }

        try
        {
            updateBuffer2(_buffer.writer(BINARY_VERSION_MARKER_SIZE),
                          BINARY_VERSION_MARKER_SIZE,
                          0);

            if (systemSize() == 0) {
                // Nothing should've been written.
                assert _buffer.writer().position() == BINARY_VERSION_MARKER_SIZE;
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

    @Override
    public String toString()
    {
        Printer p = new Printer();
        StringBuilder builder = new StringBuilder();
        try {
            p.print(this, builder);
            for (IonValue element : _contents)
            {
                p.print(element, builder);
                builder.append('\n');
            }
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return builder.toString();
    }
}
