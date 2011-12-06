// Copyright (c) 2007-2011 Amazon.com, Inc. All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.IonBinary.Reader;
import com.amazon.ion.impl.IonBinary.Writer;
import java.io.IOException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;


/**
 *
 */
abstract public class IonContainerImpl
    extends IonValueImpl
    implements IonContainerPrivate
{
    /**
     * sizes for the various types of containers
     * expected to be tuned.
     */
    static final int[] INITIAL_SIZE = make_initial_size_array();
    static int[] make_initial_size_array() {
        int[] sizes = new int[IonConstants.tidDATAGRAM + 1];
        sizes[IonConstants.tidList]     = 1;
        sizes[IonConstants.tidSexp]     = 4;
        sizes[IonConstants.tidStruct]   = 5;
        sizes[IonConstants.tidDATAGRAM] = 3;
        return sizes;
    }
    static final int[] NEXT_SIZE = make_next_size_array();
    static int[] make_next_size_array() {
        int[] sizes = new int[IonConstants.tidDATAGRAM + 1];
        sizes[IonConstants.tidList]     = 4;
        sizes[IonConstants.tidSexp]     = 8;
        sizes[IonConstants.tidStruct]   = 8;
        sizes[IonConstants.tidDATAGRAM] = 10;
        return sizes;
    }
    static final protected int initialSize(int typeDesc)
    {
        int tid = IonConstants.getTypeCode(typeDesc & 0xff);
        if (tid < 0 || tid >= INITIAL_SIZE.length) {
            assert("this should never happen: type id's should be in range".length() < 0);
            return 4;
        }
        int new_size = INITIAL_SIZE[tid];
        return new_size;
    }
    final protected int nextSize(int current_size)
    {
        int typeDesc = pos_getTypeDescriptorByte();
        int tid = IonConstants.getTypeCode(typeDesc & 0xff);

        if (current_size == 0) {
            int new_size = initialSize(typeDesc);
            return new_size;
        }

        if (tid < 0 || tid >= NEXT_SIZE.length) {
            assert("this should never happen: type id's should be in range".length() < 0);
            return current_size * 2;
        }

        int next_size = NEXT_SIZE[tid];
        if (next_size == current_size) {
            // note that unrecognized sizes, either due to unrecognized type id
            // or some sort of custom size in the initial allocation, meh.
            transitionToLargeSize(next_size);
            next_size = current_size * 2;
        }
        else {
            // includes if (next_size == 0) { ...
            assert(current_size > 0);  // we should have bailed earlier if this was 0
            next_size = current_size * 2;
        }

        return next_size;
    }
    // this is overridden in IonStructImpl to add the hashmap
    // of field names when the struct becomes modestly large
    protected void transitionToLargeSize(int size)
    {
        return;
    }
    /**
     * Only meaningful if {@link #_hasNativeValue}.
     */
    //protected ArrayList<IonValue> _contents;
    protected int        _child_count;
    protected IonValue[] _children;
    public int get_child_count() {
        return _child_count;
    }
    public IonValue get_child(int idx)
    {
        if (idx < 0 || idx >= _child_count) {
            throw new IndexOutOfBoundsException(""+idx);
        }
        return _children[idx];
    }
    public IonValue set_child(int idx, IonValue child)
    {
        if (idx < 0 || idx >= _child_count) {
            throw new IndexOutOfBoundsException(""+idx);
        }
        IonValue prev = _children[idx];
        _children[idx] = child;
        return prev;
    }
    public int add_child(int idx, IonValue child)
    {
        _isNullValue(false); // if we add children we're not null anymore
        if (_children == null || _child_count >= _children.length) {
            int old_len = (_children == null) ? 0 : _children.length;
            int new_len = this.nextSize(old_len);
            assert(new_len > idx);
            IonValue[] temp = new IonValue[new_len];
            if (old_len > 0) {
                System.arraycopy(_children, 0, temp, 0, old_len);
            }
            _children = temp;
        }
        if (idx < _child_count) {
            // TODO: verify copying backwards works!
            System.arraycopy(_children, idx, _children, idx+1, _child_count-idx);
        }
        _child_count++;
        _children[idx] = child;
        return idx;
    }
    public void remove_child(int idx) {
        assert(idx >=0 && idx < _child_count); // this also asserts child count > 0
        if (idx + 1 <= _child_count) {
            System.arraycopy(_children, idx+1, _children, idx, _child_count - idx - 1);
        }
        _child_count--;
        _children[_child_count] = null;
    }
    public int find_Child(IonValue child) {
        for (int ii=0; ii<_child_count; ii++) {
            if (_children[ii] == child) {
                return ii;
            }
        }
        return -1;
    }

    protected IonContainerImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
      //  _children = new IonValue[initialSize(typeDesc)];
    }


    @Override
    public abstract IonContainer clone();

    @Override
    public abstract int hashCode();


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
     * @throws ContainedValueException
     */
    protected void copyFrom(IonContainerImpl source)
        throws ContainedValueException, NullPointerException,
            IllegalArgumentException, IOException
    {
        // first copy the annotations and such, which
        // will materialize the value as needed.
        // This will materialize the field name and
        // annotations if present.  And it will instanciate
        // the immediate children (but it is not
        // a deep materialization, so we'll live with
        // it for now).
        copyAnnotationsFrom(source);

        // now we can copy the contents

        // first see if this value is null (and we're really
        // done here)
        if (source.isNullValue()) {
            makeNull();
        }
        else {
            // it's not null so there better be something there
            // at least 0 children :)
            assert source.get_child_count() == 0 || source._children != null;
            _isNullValue(source._isNullValue()); // this will be false

            // and we'll need a contents array to hold at least 0
            // children
            if (this._children == null) {
                int current_size = (source._children == null) ? 0 : source._children.length;
                int next_size = current_size;
                if (next_size < 1) {
                    next_size = initialSize(pos_getTypeDescriptorByte());
                }
                this._children = new IonValue[next_size];
                if (next_size >= this.nextSize(next_size)) {
                    this.transitionToLargeSize(next_size);
                }
            }
            // we should have an empty content list at this point
            assert this.get_child_count() == 0;

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
                final boolean cloningFields = (this instanceof IonStruct);

                IonValue[] sourceContents = source._children;
                int size = source.get_child_count();

                for (int i = 0; i < size; i++)
                {
                    IonValue child = sourceContents[i];
                    IonValue copy = child.clone();
                    if (cloningFields) {
                        String name = child.getFieldName();
                        ((IonValueImpl)copy).setFieldName(name);
                    }
                    this.add(i, copy, true);
                }
            }
        }
    }


    public int size()
        throws NullValueException
    {
        makeReady();
        return get_child_count();
    }

    @Override
    protected int getNakedValueLength()
        throws IOException
    {
        int length = 0;

        if (this.isDirty())
        {
            assert _hasNativeValue() == true || _isPositionLoaded() == false;
            for (int ii=0; ii<get_child_count(); ii++) {
                IonValueImpl aChild = (IonValueImpl) get_child(ii);
                length += aChild.getFullEncodedSize();
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
        checkForLock();
        if (isNullValue())
        {
            _children = null;
            _child_count = 0;
            _isNullValue(false);    // TODO: really?  It seems to be true, based on previous behavior, but why does "clear" make something "nonNull"?
            _hasNativeValue(true);
            setDirty();
        }
        /*
         * TODO: if this is a big container that's not materialized, isEmpty()
         * will do a lot of work materializing it just to throw it out.
         * Optimization needed. Especially since we'll then come in and detach
         * all the children we just created.
         */
        else if (!isEmpty())
        {
            detachAllChildren();
            _child_count = 0;
            setDirty();
        }
    }

    @Override
    // this is split so that the public version is called only
    // by the user, not internally.  This allows us to know
    // when we're at the originating value for checks and symbol
    // table processing.
    public void makeReadOnly() {
        // FIXME: it sure seems to me this should be true - but it seems not. hmmm.
        // IonValuePrivate parent = this.get_symbol_table_root();
        // if (parent != this) {
        //     throw new IonException("child members can't be make read only by themselves");
        // }
        make_read_only_helper(true);
    }
    @Override
    void make_read_only_helper(boolean is_root) {
        if (_isLocked()) return;
        synchronized (this) { // TODO why is this needed?
            deepMaterialize();
            if (is_root) {
                this.populateSymbolValues(this._symboltable);
            }
            if (_children != null) {
                for (int ii=0; ii<_child_count; ii++) {
                    IonValue child = _children[ii];
                    ((IonValueImpl)child).make_read_only_helper(false);
                }
            }
            _isLocked(true);
        }
    }

    public void makeNull()
    {
        checkForLock();

        if (_children != null)
        {
            detachAllChildren();
            _children = null;
            _child_count = 0;
        }

        _hasNativeValue(true);
        _isNullValue(true);
        setDirty();
    }

    private void detachAllChildren()
    {
        try {
            for (int ii=0; ii<_child_count; ii++) {
                IonValue child = _children[ii];
                ((IonValueImpl)child).detachFromContainer();
                _children[ii] = null;
            }
        } catch (IOException ioe) {
            throw new IonException(ioe);
        }
    }

    void move_start_helper(int offset)
    {
        for (int ii=0; ii<_child_count; ii++) {
            IonValue v = _children[ii];
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
    protected synchronized void materialize_helper()
        throws IOException
    {
        // TODO throw IonException not IOException

        if (!_hasNativeValue())
        {
            // First materialization must be from clean state.
            assert !isDirty() || _buffer == null;
            assert get_child_count() == 0; // _children == null;

            if (_buffer != null)
            {
                assert _isPositionLoaded() == true;

                IonBinary.Reader reader = this._buffer.reader();
                reader.sync();
                materializeAnnotations(reader);

                if (!isNullValue())
                {
                    _children = null;
                    _child_count = 0;
                    // this skips past then td and value len
                    reader.setPosition(this.pos_getOffsetAtActualValue());
                    doMaterializeValue(reader);
                }
            }
            else
            {
                assert _isPositionLoaded() == false;
            }

            _hasNativeValue(true);
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
        SymbolTable symtab = this.getSymbolTable();
        int end = this.pos_getOffsetofNextValue();
        int pos = reader.position();

        while (pos < end)
        {
            IonValueImpl child;
            reader.setPosition(pos);
            child = makeValueFromReader(UNKNOWN_SYMBOL_ID, reader, buffer,
                                        symtab, this, _system);
            child._elementid = get_child_count(); //_children.length;
            add_child(child._elementid, child);
            pos = child.pos_getOffsetofNextValue();
        }
    }

    @SuppressWarnings("deprecation")
    @Override
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

        for (int ii=0; ii<_child_count; ii++) {
            IonValue contained = get_child(ii);
            contained.deepMaterialize();
        }
    }

    @Override
    protected void detachFromBuffer()
        throws IOException
    {
        materialize();
        for (int ii=0; ii<_child_count; ii++) {
            IonValue contained = get_child(ii);
            ((IonValueImpl) contained).detachFromBuffer();
        }
        _buffer = null;
    }

    @Override
    protected void shiftTokenAndChildren(int delta)
    {
        assert (!this.isDirty());

        this.pos_moveAll(delta);

        for (int ii=0; ii<_child_count; ii++) {
            // Move our children's tokens.
            IonValueImpl aChild = (IonValueImpl)get_child(ii);
            aChild.shiftTokenAndChildren(delta);
        }
    }


    @Override
    public SymbolTable populateSymbolValues(SymbolTable symtab)
    {
        // the "super" copy of this method will check the lock
        symtab = super.populateSymbolValues(symtab);
        for (int ii=0; ii<_child_count; ii++) {
            IonValue v = get_child(ii);
            symtab = ((IonValueImpl)v).populateSymbolValues(symtab);
        }
        return symtab;
    }

    @Override
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
        // overriden in sexp and datagram to handle Ion Version Marker (magic cookie)
        for (int ii=0; ii<_child_count; ii++) {
            IonValueImpl child = (IonValueImpl)get_child(ii);

            cumulativePositionDelta =
                child.updateBuffer2(writer, writer.position(),
                                    cumulativePositionDelta);
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


    public IonValue get(int index)
        throws NullValueException
    {
        if (isNullValue()) {
            throw new NullValueException();
        }

        makeReady();

        return get_child(index);
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
     * @throws ContainedValueException
     */
    protected boolean add(IonValue child)
        throws NullPointerException, IllegalArgumentException,
        ContainedValueException
    {
        checkForLock();

        // We do this here to avoid materialization if element is bad.
        validateNewChild(child);

        makeReady();
        int size = get_child_count();

        add(size, child, true);
        return true;
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
        checkForLock();
        validateNewChild(child);
        add(index, child, true);
    }

    /**
     * Ensures that a potential new child is non-null, has no container,
     * is not read-only, and is not a datagram.
     *
     * @throws NullPointerException
     *   if {@code child} is {@code null}.
     * @throws ContainedValueException
     *   if {@code child} is already part of a container.
     * @throws ReadOnlyValueException
     *   if {@code child} is read only.
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

        if (child.isReadOnly()) throw new ReadOnlyValueException();

        if (child instanceof IonDatagram)
        {
            String message =
                "IonDatagram can not be inserted into another IonContainer.";
            throw new IllegalArgumentException(message);
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

        // TODO: try to reuse the byte array if it is present
        // and the symbol tables are compatible or
        // the value is big enough to justify embedding
        // a copy of its symbol table in the stream
        // otherwise clear the buffer and re-init the positions
        //byte[] bytes = null;
        //if (
        //    && concrete._buffer != null
        //    && !concrete.isDirty()
        //    && concrete.getSymbolTable().isCompatible(this.getSymbolTable()))
        //{
        //    // TODO: resuse the bytes that are ready to go
        //    if (bytes == null)
        //    {
        //        // just a trick to convince Eclipse to ignore two warning
        //        // errors that will persist until this code is filled in
        //        throw new IonException("feature not implemented - this code should not be reachable.");
        //    }
        //}
        //else
        // we do the "copy dom" case instead of the "copy bytes" variation
        {
            concrete.deepMaterialize();
            if (!(this instanceof IonDatagramImpl)) {
                concrete.makeReady();
                concrete.setSymbolTable(null);
            }
            concrete.clear_position_and_buffer();
        }

        makeReady();

        if (_children == null || _child_count == 0)
        {
            _children = new IonValue[initialSize(pos_getTypeDescriptorByte())];
            _hasNativeValue(true);
        }

        add_child(index, concrete);
        //_contents.add(index, element);
        concrete._elementid = index;
        updateElementIds(index + 1); // start at the next element, this one is fine

        // We shouldn't force the child to be dirty, since we haven't
        // unsynched its materialized and binary copies.

        concrete._container = this;

        if (setDirty)
        {
            this.setDirty();
        }
    }
    void updateElementIds(int startingIndex)
    {
        while (startingIndex<this.get_child_count()) {
            IonValueImpl v = (IonValueImpl)this.get_child(startingIndex);
            v._elementid = startingIndex;
            startingIndex++;
        }
    }

    @Override
    void clear_position_and_buffer()
    {
        makeReady();

        for (int ii=0; ii<get_child_count(); ii++) {
            IonValueImpl v = (IonValueImpl)get_child(ii);
            v.clear_position_and_buffer();
        }
        super.clear_position_and_buffer();
    }

    @Override
    void detachFromSymbolTable()
    {
        assert _hasNativeValue(); // else we don't know if _contents is valid
        for (int ii=0; ii<get_child_count(); ii++) {
            IonValueImpl v = (IonValueImpl)get_child(ii);
            v.detachFromSymbolTable();
        }
        super.detachFromSymbolTable();
    }

    public boolean remove(IonValue element)
    {
        checkForLock();
        if (element.getContainer() != this)
            return false;

        // We must already be materialized, else we wouldn't have a child.
        assert _hasNativeValue();

        // Get all the data into the DOM, since the element will be losing
        // its backing store.
        IonValueImpl concrete = (IonValueImpl) element;

        int pos = concrete._elementid;
        IonValue child = get_child(pos);
        if (child == concrete) // Yes, instance identity.
        {
            try {
                // TODO improve final state if this method throws.
                // Should be able to have the container be ok at least.
                concrete.detachFromContainer();
                remove_child(pos);
                //_contents.remove(pos);
                updateElementIds(pos);
                this.setDirty();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            return true;
        }

        throw new AssertionError("element's index is not correct");
    }

    public Iterator<IonValue> iterator()
    {
        return listIterator(0);
    }

    public final ListIterator<IonValue> listIterator()
    {
        return listIterator(0);
    }

    public ListIterator<IonValue> listIterator(int index)
    {
        if (isNullValue())
        {
            if (index != 0) throw new IndexOutOfBoundsException();
            return IonImplUtils.<IonValue>emptyIterator();
        }

        makeReady();
        return new SequenceContentIterator(index, isReadOnly());
    }

    /** Encapsulates an iterator and implements a custom remove method */
    /*  this is tied to the _child array of the IonSequenceImpl
     *  through the _children and _child_count members which this
     *  iterator directly uses.
     *
     *  TODO with the updated next and previous logic, particularly
     *  the force_position_sync logic and lastMoveWasPrevious flag
     *  we could implment add and set correctly.
     */
    protected final class SequenceContentIterator
        implements ListIterator<IonValue>
    {
        private final boolean  __readOnly;
        private       boolean  __lastMoveWasPrevious;
        private       int      __pos;
        private       IonValue __current;

        public SequenceContentIterator(int index, boolean readOnly)
        {
            if (index < 0 || index > _child_count) {
                throw new IndexOutOfBoundsException(""+index);
            }
            __pos = index;
            __readOnly = readOnly;
        }

        // split to encourage the in-lining of the common
        // case where we don't actually do anything
        private final void force_position_sync()
        {
            if (__pos <= 0 || __pos > _child_count) {
                return;
            }
            if (__current == null || __current == _children[__pos - 1]) {
                return;
            }
            force_position_sync_helper();
        }
        private final void force_position_sync_helper()
        {
            if (__readOnly) {
                throw new IonException("read only sequence was changed");
            }
            int idx = __pos - 1;
            if (__lastMoveWasPrevious) {
                idx++;
            }
            // look forward, which happens on insert
            // notably insert of a local symbol table
            // or a IVM if this is in a datagram
            for (int ii=__pos; ii<_child_count; ii++) {
                if (_children[ii] == __current) {
                    __pos = ii;
                    if (!__lastMoveWasPrevious) {
                        __pos++;
                    }
                    return;
                }
            }
            // look backward, which happens on delete
            // of a member preceding us, but should not
            // happen if the delete is through this
            // operator
            for (int ii=__pos-1; ii>=0; ii--) {
                if (_children[ii] == __current) {
                    __pos = ii;
                    if (!__lastMoveWasPrevious) {
                        __pos++;
                    }
                    return;
                }
            }
            throw new IonException("current member of iterator has been removed from the containing sequence");
        }

        public void add(IonValue element)
        {
            throw new UnsupportedOperationException();
        }

        public final boolean hasNext()
        {
            // called in nextIndex(): force_position_sync();
            return (nextIndex() < _child_count);
        }

        public final boolean hasPrevious()
        {
            // called in previousIndex(): force_position_sync();
            return (previousIndex() >= 0);
        }

        public IonValue next()
        {
            int next_idx = nextIndex();
            if (next_idx >= _child_count) {
                throw new NoSuchElementException();
            }
            __current = _children[next_idx];
            __pos = next_idx + 1; // after a next the pos will be past the current
            __lastMoveWasPrevious = false;
            return __current;
        }

        public final int nextIndex()
        {
            force_position_sync();
            if (__pos >= _child_count) {
                return _child_count;
            }
            int next_idx = __pos;
            // whether we previous-ed to get here or
            // next-ed to get here the next index is
            // whatever the current position is
            return next_idx;
        }

        public IonValue previous()
        {
            force_position_sync();
            int prev_idx = previousIndex();
            if (prev_idx < 0) {
                throw new NoSuchElementException();
            }
            __current = _children[prev_idx];
            __pos = prev_idx;
            __lastMoveWasPrevious = true;
            return __current;
        }

        public final int previousIndex()
        {
            force_position_sync();
            int prev_idx = __pos - 1;
            if (prev_idx < 0) {
                return -1;
            }
            return prev_idx;
        }

        /**
         * Sets the container to dirty after calling {@link Iterator#remove()}
         * on the encapsulated iterator
         */
        public void remove()
        {
            if (__readOnly) {
                throw new UnsupportedOperationException();
            }
            force_position_sync();

            int idx = __pos;
            if (!__lastMoveWasPrevious) {
                // position is 1 ahead of the array index
                idx--;
            }
            if (idx < 0) {
                throw new ArrayIndexOutOfBoundsException();
            }

            IonValueImpl concrete = (IonValueImpl) __current;
            int concrete_idx = concrete.getElementId();
            assert(concrete_idx == idx);

            // here we remove the member from the containers list of elements
            remove_child(idx);

            // and here we patch up the member
            // and then the remaining members index values
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
                updateElementIds(concrete_idx);
                setDirty();
            }
            if (!__lastMoveWasPrevious) {
                // if we next-ed onto this member we have to back up
                // because the next member is now current (otherwise
                // the position is fine where it is)
                __pos--;
            }
            __current = null;
        }

        public void set(IonValue element)
        {
            throw new UnsupportedOperationException();
        }
    }
}
