/*
 * Copyright (c) 2007 Amazon.com, Inc. All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.NullValueException;
import com.amazon.ion.impl.IonBinary.Reader;
import com.amazon.ion.impl.IonBinary.Writer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;


/**
 *
 */
abstract public class IonContainerImpl
    extends IonValueImpl
    implements IonContainer
{
    /**
     * Only meaningful if {@link #_hasNativeValue}.
     */
    protected ArrayList<IonValue> _contents;

    protected IonContainerImpl(int typeDesc)
    {
        super(typeDesc);
    }

    public int size()
        throws NullValueException
    {
        validateThisNotNull();
        makeReady();
        return _contents.size();
    }

    @Override
    protected int getNakedValueLength()
        throws IOException
    {
        int length = 0;

        if (this.isDirty())
        {
            assert _hasNativeValue == true || _isPositionLoaded == false;
            if (_contents != null)
            {
                for (IonValue child : _contents)
                {
                    IonValueImpl aChild = (IonValueImpl) child;
                    length += aChild.getFullEncodedSize();
                }
            }
        }
        else
        {
            int start = this.pos_getOffsetAtActualValue();
            int end = this.pos_getOffsetofNextValue();
            length = end - start;
        }

        return length;
    }

    public boolean isEmpty()
    {
        validateThisNotNull();

        return (size() == 0);
    }

    public void clear()
    {
        if (isNullValue())
        {
            _contents = new ArrayList<IonValue>();
            _hasNativeValue = true;
            setDirty();
        }
        /*
         * TODO: if this is a big container that's not materialized, isEmpty()
         * will do a lot of work materializing it just to throw it out.
         * Optimization needed.
         */
        else if (!isEmpty())
        {
            detachAllChildren();
            _contents.clear();
            setDirty();
        }
    }

    public void makeNull()
    {
        if (!isNullValue())
        {
            if (_contents != null)
            {
                detachAllChildren();
                _contents = null;
            }
            _contents = null;
            _hasNativeValue = true;
            setDirty();
        }
    }

    private void detachAllChildren()
    {
        try {
            for (IonValue child : _contents) {
                ((IonValueImpl)child).detachFromContainer();
            }
        } catch (IOException ioe) {
            throw new IonException(ioe);
        }
    }

    void move_start_helper(int offset)
    {
        for (IonValue v : _contents)
        {
            ((IonValueImpl) v).pos_moveAll(offset);
        }
        this.pos_moveAll(offset);
    }

    /**
     * Load all children from binary into our native list.
     * <p/>
     * Postcondition:
     * <code>this._hasNativeValue == true </code>
     *
     * @throws IOException
     */
    @Override
    protected void materialize()
        throws IOException
    {
        // TODO throw IonException not IOException

        if (!_hasNativeValue)
        {
            // First materialization must be from clean state.
            assert !isDirty() || _buffer == null;
            assert _contents == null;

            if (_buffer != null)
            {
                assert _isPositionLoaded == true;

                IonBinary.Reader reader = this._buffer.reader();
                reader.sync();
                materializeAnnotations(reader);

                if (!isNullValue())
                {
                    _contents = new ArrayList<IonValue>();
                    // this skips past then td and value len
                    reader.setPosition(this.pos_getOffsetAtActualValue());
                    doMaterializeValue(reader);
                }
            }
            else
            {
                assert _isPositionLoaded == false;
            }

            _hasNativeValue = true;
        }
    }

    /**
     * Overridden by DatagramImpl to handle symtabs. And Overridden by Struct
     * to handle field sids.
     *
     * @throws IOException
     */
    @Override
    protected void doMaterializeValue(Reader reader)
        throws IOException
    {
        assert reader.position() == this.pos_getOffsetAtActualValue();
        assert this.pos_getType() != IonConstants.tidStruct;

        IonBinary.BufferManager buffer = this._buffer;
        LocalSymbolTable symtab = this.getSymbolTable();
        int end = this.pos_getOffsetofNextValue();
        int pos = reader.position();

        while (pos < end)
        {
            IonValueImpl child;
            reader.setPosition(pos);
            child = IonValueImpl.makeValueFromReader(0, reader, buffer, symtab, this);
            _contents.add(child);
            pos = child.pos_getOffsetofNextValue();
        }
    }

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

        if (_contents != null)
        {
            for (IonValue contained : _contents)
            {
                contained.deepMaterialize();
            }
        }
    }

    @Override
    protected void detachFromBuffer()
        throws IOException
    {
        materialize();
        if (_contents != null)
        {
            for (IonValue contained : _contents)
            {
                ((IonValueImpl) contained).detachFromBuffer();
            }
        }
        _buffer = null;
    }

    @Override
    protected void shiftTokenAndChildren(int delta)
    {
        assert (!this.isDirty());

        this.pos_moveAll(delta);

        if (_contents != null)
        {
            // Move our children's tokens.
            for (IonValue child : _contents)
            {
                IonValueImpl aChild = (IonValueImpl) child;
                aChild.shiftTokenAndChildren(delta);
            }
        }
    }


    @Override
    public void updateSymbolTable(LocalSymbolTable symtab)
    {
        super.updateSymbolTable(symtab);
        if (this._contents != null) {
            for (IonValue v : this._contents) {
                ((IonValueImpl)v).updateSymbolTable(symtab);
            }
        }
    }

    @Override
    protected int updateNewValue(IonBinary.Writer writer, int newPosition,
                                 int cumulativePositionDelta)
        throws IOException
    {
        assert writer.position() == newPosition;

        updateToken();

        assert pos_getOffsetAtFieldId() < 0;

        // int newValueStart = newPosition + getFieldNameOverheadLength();
        this.pos_setEntryStart(newPosition);

        assert newPosition == pos_getOffsetAtFieldId();

        // Create space for our header; children will make room for
        // themselves. header includes annotations, td, and length
        int headerSize = this.pos_getOffsetAtActualValue() - newPosition;

        writer.insert(headerSize);
        cumulativePositionDelta += headerSize;

        cumulativePositionDelta =
            writeElement(writer, cumulativePositionDelta);

        return cumulativePositionDelta;
    }

    @Override
    protected int updateOldValue(IonBinary.Writer writer, int newPosition,
                                 int cumulativePositionDelta)
        throws IOException
    {
        assert writer.position() == newPosition;

        this.pos_moveAll(cumulativePositionDelta);

        int currentPositionOfContent = pos_getOffsetAtActualValue();
        int currentPositionOfFieldId = pos_getOffsetAtFieldId();

        // The old data is at or to the right of the current position.
        assert newPosition <= currentPositionOfFieldId;

        // Recompute our final offsets and lengths.
        updateToken();

        int newPositionOfContent = pos_getOffsetAtActualValue();
        int headerOverlap = newPositionOfContent - currentPositionOfContent;

        if (headerOverlap > 0)
        {
            // We need to make more space for the field name & annotations
            // so we don't overwrite the core value.
            writer.insert(headerOverlap);
            cumulativePositionDelta += headerOverlap;
        }

        cumulativePositionDelta =
            writeElement(writer, cumulativePositionDelta);

        return cumulativePositionDelta;
    }

    protected int doWriteContainerContents(IonBinary.Writer writer,
                                           int cumulativePositionDelta)
        throws IOException
    {
        if (_contents != null)
        {
            for (int ii = 0; ii < _contents.size(); ii++)
            {
                IonValueImpl child = (IonValueImpl) _contents.get(ii);

                cumulativePositionDelta =
                    child.updateBuffer2(writer, writer.position(),
                                        cumulativePositionDelta);
            }
        }
        return cumulativePositionDelta;
    }

    @Override
    protected void doWriteNakedValue(Writer writer, int valueLen)
        throws IOException
    {
        throw new IonException("unsupported operation");
    }

    @Override
    protected int getNativeValueLength()
    {
        int len = this.pos_getOffsetofNextValue()
                  - this.pos_getOffsetAtActualValue();
        return len;
    }

    /**
     * TODO clarify behavior on null.
     */
    protected IonValueImpl getFirstChild(LocalSymbolTable symtab)
    {
        assert !isDirty();

        makeReady();

        assert this._isMaterialized == true || this._hasNativeValue == true;

        if (this.isNullValue() || this._contents.size() < 1) {
            return null;
        }

        IonValueImpl first = (IonValueImpl) this._contents.get(0);
        return first;
    }

    public IonValue get(int index)
        throws NullValueException
    {
        if (isNullValue()) {
            throw new NullValueException();
        }

        makeReady();

        return _contents.get(index);
    }

    /**
     * Materialize this container, append a child, and mark this as dirty.
     * <p>
     * This is protected because it's not a valid call for structs.
     *
     * @param child the value to append.
     *
     * @throws NullPointerException
     *   if {@code child} is {@code null}.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws IllegalArgumentException
     *   if {@code child} is an {@link IonDatagram}.
     */
    protected void add(IonValue child)
        throws NullPointerException, IllegalArgumentException
    {
        // We do this here to avoid materialization if element is bad.
        validateNewChild(child);

        makeReady();
        int size = (_contents == null ? 0 : _contents.size());

        add(size, child, true);
    }

    /**
     * @throws NullPointerException
     *   if {@code child} is {@code null}.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws IllegalArgumentException
     *   if {@code child} is an {@link IonDatagram}.
     */

    protected void add(int index, IonValue child)
        throws ContainedValueException, NullPointerException
    {
        validateNewChild(child);
        add(index, child, true);
    }

    /**
     * Ensures that a potential new child is non-null, has no container,
     * and is not a datagram.
     *
     * @throws NullPointerException
     *   if {@code child} is {@code null}.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws IllegalArgumentException
     *   if {@code child} is an {@link IonDatagram}.
     */
    protected static void validateNewChild(IonValue child)
        throws ContainedValueException, NullPointerException,
               IllegalArgumentException
    {
        // FIXME should this recognize system container?
        if (child.getContainer() != null)            // Also checks for null.
        {
            throw new ContainedValueException();
        }

        if (child instanceof IonDatagram)
        {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Materialize this container, append a child, and (perhaps) mark this as
     * dirty.
     * <p>
     * <p>
     * NOTE: this assumes that {@link #validateNewChild(IonValue)}
     * has been called.
     * TO DO: do we really need setDirty? yes.
     *
     * @param element
     *        must not be null.
     * @throws NullPointerException
     *         if the element is <code>null</code>.
     */
    protected void add(int index, IonValue element, boolean setDirty)
        throws ContainedValueException, NullPointerException
    {
        final IonValueImpl concrete = ((IonValueImpl) element);

        // TODO: true to reuse the byte array if it is present
        // and the symbol tables are compatible or
        // the value is big enough to justify embedding
        // a copy of its symbol table in the stream
        // otherwise clear the buffer and re-init the positions
        byte[] bytes = null;
        if (concrete._buffer != null
            && !concrete.isDirty()
            && (concrete.getSymbolTable().isCompatible(this.getSymbolTable())
                || concrete.deservesEmbeddingWithLocalSymbolTable()))
        {
            // TODO: resuse the bytes that are ready to go
            if (bytes == null)
            {
                // just a trick to convince Eclipse to ignore two warning
                // errors that will persist until this code is filled in
                throw new IonException("feature not implemented - this code should not be reachable.");
            }
        }
        else
        {
            // TODO: should we copy the symbols to the parent, if there are
            // any?
            concrete.clear_position_and_buffer();
        }

        makeReady();

        if (_contents == null)
        {
            _contents = new ArrayList<IonValue>();
            _hasNativeValue = true;
        }
        _contents.add(index, element);

        // We shouldn't force the child to be dirty, since we haven't
        // unsynched its materialized and binary copies.

        concrete._container = this;

        if (setDirty)
        {
            this.setDirty();
        }
    }

    void clear_position_and_buffer()
    {
        makeReady();

        if (this._contents != null)
        {
            for (IonValue v : this._contents)
            {
                ((IonValueImpl) v).clear_position_and_buffer();
            }
        }
        super.clear_position_and_buffer();
    }

    public boolean remove(IonValue element)
    {
        validateThisNotNull();
        if (element.getContainer() != this)
            return false;

        // We must already be materialized, else we wouldn't have a child.
        assert _hasNativeValue;

        // Get all the data into the DOM, since the element will be losing
        // its backing store.
        IonValueImpl concrete = (IonValueImpl) element;

        try
        {
            for (Iterator i = _contents.iterator(); i.hasNext();)
            {
                IonValue child = (IonValue) i.next();
                if (child == concrete) // Yes, instance identity.
                {
                    i.remove();

                    concrete.detachFromContainer();

                    this.setDirty();

                    return true;
                }
            }
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
        String message =
            "element is not in materialized contents of its container";
        throw new IllegalStateException(message);
    }

    public Iterator<IonValue> iterator()
    {
        validateThisNotNull();
        makeReady();
        return new IonContainerIterator(_contents.iterator(), true);
    }

    /** Encapsulates an iterator and implements a custom remove method */
    protected final class IonContainerIterator
        implements Iterator<IonValue>
    {
        private final Iterator<IonValue> it;

        private final boolean allowRemove;

        private IonValue current;

        public IonContainerIterator(Iterator<IonValue> it, boolean allowRemove)
        {
            this.it = it;
            this.allowRemove = allowRemove;
        }

        public boolean hasNext()
        {
            return it.hasNext();
        }

        public IonValue next()
        {
            current = it.next();
            return current;
        }

        /**
         * Sets the container to dirty after calling {@link Iterator#remove()}
         * on the encapsulated iterator
         */
        public void remove()
        {
            if (!allowRemove) {
                throw new UnsupportedOperationException();
            }

            it.remove();
            try
            {
                ((IonValueImpl) current).detachFromContainer();
            }
            catch (IOException e)
            {
                throw new IonException(e);
            }
            finally
            {
                setDirty();
            }
        }
    }
}
