// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonConstants;
import com.amazon.ion.impl.IonContainerPrivate;
import com.amazon.ion.impl.IonImplUtils;
import java.io.IOException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**listIterator
 *
 */
public abstract class IonContainerLite
    extends IonValueLite
    implements IonContainerPrivate, IonContext
{

    protected int            _child_count;
    protected IonValueLite[] _children;

    protected IonContainerLite(IonContext context, boolean isNull)
    {
        // really we'll let IonValueLite handle this work
        // and we always need to know our context and if
        // we should start out null or not
        super(context, isNull);
    }

    @Override
    public abstract void accept(ValueVisitor visitor) throws Exception;

    @Override
    public abstract IonContainer clone();

    /**
     * IonContainer methods
     */
    public void clear()
    {
        checkForLock();

        if (_isNullValue())
        {
            _children = null;
            _child_count = 0;
            // TO DO: really?  Yes, for back compat.
            //        makeNull() is the alternative
            _isNullValue(false);
        }
        else if (!isEmpty())
        {
            detachAllChildren();
            _child_count = 0;
        }
    }

    private void detachAllChildren()
    {
        for (int ii=0; ii<_child_count; ii++) {
            IonValueLite child = _children[ii];
            child.detachFromContainer();
            _children[ii] = null;
        }
    }

    public boolean isEmpty() throws NullValueException
    {
        validateThisNotNull();

        return (size() == 0);
    }

    public IonValue get(int index)
        throws NullValueException
    {
        validateThisNotNull();
        IonValueLite value = get_child_lite(index);
        assert(value._isAutoCreated() == false);
        return value;
    }


    public final Iterator<IonValue> iterator()
    {
        return listIterator(0);
    }

    public final ListIterator<IonValue> listIterator()
    {
        return listIterator(0);
    }

    @SuppressWarnings("unchecked")
    public ListIterator<IonValue> listIterator(int index)
    {
        if (isNullValue())
        {
            if (index != 0) throw new IndexOutOfBoundsException();
            return (ListIterator<IonValue>) IonImplUtils.EMPTY_ITERATOR;
        }

        return new SequenceContentIterator(index, isReadOnly());
    }

    /** Encapsulates an iterator and implements a custom remove method */
    /*  this is tied to the _child array of the IonSequenceImpl
     *  through the _children and _child_count members which this
     *  iterator directly uses.
     *
     *  TODO with the updated next and previous logic, particularly
     *  the force_position_sync logic and lastMoveWasPrevious flag
     *  we could implement add and set correctly.
     *
     *  NOTE this closely resembles the user and system iterators
     *  defined in datagram, so changes here are likely to be needed
     *  in datagram as well.
     */
    protected class SequenceContentIterator
        implements ListIterator<IonValue>
    {
        protected final boolean  __readOnly;
        protected       boolean  __lastMoveWasPrevious;
        protected       int      __pos;
        protected       IonValue __current;

        public SequenceContentIterator(int index, boolean readOnly)
        {
            if (_isLocked() && !readOnly) {
                throw new IllegalStateException("you can't open an updatable iterator on a read only value");
            }
            if (index < 0 || index > _child_count) {
                throw new IndexOutOfBoundsException(""+index);
            }
            __pos = index;
            __readOnly = readOnly;
        }

        // split to encourage the in-lining of the common
        // case where we don't actually do anything
        protected final void force_position_sync()
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

            IonValueLite concrete = (IonValueLite) __current;
            int concrete_idx = concrete._elementid();
            assert(concrete_idx == idx);

            // here we remove the member from the containers list of elements
            remove_child(idx);

            // and here we patch up the member
            // and then the remaining members index values
            concrete.detachFromContainer();
            patch_elements_helper(concrete_idx);

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

    public void makeNull()
    {
        clear();            // this checks for the lock
        _isNullValue(true); // but clear() leaves the value non-null
    }

    public boolean remove(IonValue element)
    {
        if (element == null) {
            throw new NullPointerException();
        }
        assert (element instanceof IonValueLite);

        checkForLock();

        if (element.getContainer() != this) {
            return false;
        }

        // Get all the data into the DOM, since the element will be losing
        // its backing store.
        IonValueLite concrete = (IonValueLite) element;

        int pos = concrete._elementid();
        IonValueLite child = get_child_lite(pos);
        if (child == concrete) // Yes, instance identity.
        {
            // no, this is done in remove_child and will
            // if called first it will corrupt the elementid
            // no: concrete.detachFromContainer();
            remove_child(pos);
            patch_elements_helper(pos);

            return true;
        }

        throw new AssertionError("element's index is not correct");
    }

    public int size()
    {
        if (isNullValue()) {
            return 0;
        }
        return get_child_count();
    }

    @Override
    public void makeReadOnly()
    {
        if (_isLocked()) return;

        synchronized (this) { // TODO why is this needed?
            if (_children != null) {
                for (int ii=0; ii<_child_count; ii++) {
                    IonValueLite child = _children[ii];
                    child.makeReadOnly();
                }
            }
            _isLocked(true);
        }
    }


    @Override
    protected void make_readable()
    {
        if (_isLocked() == false) return;

        synchronized (this) { // TODO why is this needed?
            if (_children != null) {
                for (int ii=0; ii<_child_count; ii++) {
                    IonValueLite child = _children[ii];
                    child.make_readable();
                }
            }
            _isLocked(false);
        }
    }


    /**
     * methods from IonValue
     *
     *   public void deepMaterialize()
     *   public IonContainer getContainer()
     *   public int getFieldId()
     *   public String getFieldName()
     *   public int getFieldNameId()
     *   public String[] getTypeAnnotationStrings()
     *   public String[] getTypeAnnotations()
     *   public boolean hasTypeAnnotation(String annotation)
     *   public boolean isNullValue()
     *   public boolean isReadOnly()
     *   public void removeTypeAnnotation(String annotation)
     */


    /*
     * IonContext methods
     *
     * note that the various get* methods delegate
     * to our context.
     *
     * However getParentThroughContext() returns
     * our this pointer since we are the container.
     *
     * We always have a context.  Either a concrete
     * context if we are a loose value or a container
     * we are contained in.
     *
     */
    public IonContainerLite getParentThroughContext()
    {
        return this;
    }

    public SymbolTable getLocalSymbolTable(IonValueLite child)
    {
        return _context.getLocalSymbolTable(this);
    }

    public SymbolTable getContextSymbolTable()
    {
        return null;
    }

    public void setParentThroughContext(IonValueLite child, IonContext context)
    {
        assert(context == this);
        checkForLock();
        child.setContext(this);
    }

    public void setSymbolTableOfChild(SymbolTable symbols, IonValueLite child)
    {
        checkForLock();
        // we inherit our symbol table from out container
        // ultimately the datagram will create a context
        // or a concrete context will store the symbol table
        _context.setSymbolTableOfChild(symbols, this);
    }

    @Override
    public SymbolTable populateSymbolValues(SymbolTable symbols)
    {
        symbols = super.populateSymbolValues(symbols);
        for (int ii=0; ii<get_child_count(); ii++) {
            IonValueLite child = get_child_lite(ii);
            symbols = child.populateSymbolValues(symbols);
        }
        return symbols;
    }


    /**
     * @param child
     */
    public boolean add(IonValue child)
        throws NullPointerException, IllegalArgumentException,
        ContainedValueException
    {
        checkForLock();

        validateNewChild(child);

        int size = get_child_count();

        add(size, child);

        // updateElementIds - not needed since we're adding at the end
        // this.patch_elements_helper(size);

        return true;
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
        if (child == null) {
            throw new NullPointerException();
        }

        assert(child instanceof IonValueLite);

        // FIX ME should this recognize system container?
        //        no need, system returns null
        if (child.getContainer() != null && !(child.getContainer() instanceof IonSystem))            // Also checks for null.
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
     * @param element
     *        must not be null.
     * @throws NullPointerException
     *         if the element is <code>null</code>.
     */
    protected void add(int index, IonValue element)
        throws ContainedValueException, NullPointerException
    {
        final IonValueLite concrete = (IonValueLite)element;

        this.checkForLock();
        concrete.checkForLock();

        add_child(index, concrete);
        patch_elements_helper(index + 1);

        assert((index >= 0) && (index < get_child_count()) && (concrete == get_child(index)) && (concrete._elementid() == index));
    }


    /**
     * this copies the annotations and the field name if
     * either of these exists from the passed in instance.
     * It overwrites these values on the current instance.
     * Since these will be the string representations it
     * is unnecessary to update the symbol table ... yet.
     * Then it copies the children from the source container
     * to this container.
     * @param source instance to copy from
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws NullPointerException
     * @throws ContainedValueException
     */
    protected void copyFrom(IonContainerLite source)
        throws ContainedValueException, NullPointerException,
            IllegalArgumentException, IOException
    {
        checkForLock();

        // first copy the annotations, flags and field name
        this.copyValueContentFrom(source);
        this.make_readable(); // we can clone FROM a read only value but we create a writable value

        // now we can copy the contents

        // first see if this value is null (in which
        // case we're done here)
        if (source.isNullValue()) {
            makeNull();
        }
        else if (source.get_child_count() == 0){
            // non-null, but empty source, we're clear
            clear();
        }
        else {
            // it's not null, and the source says there are children
            // so we're non-null and need to copy the children over
            assert(source._isNullValue() == false);
            _isNullValue(false);

            // and we'll need a contents array to hold at least 0
            // children
            int current_size = (source._children == null) ? 0 : source._children.length;
            if (current_size < source._children.length) {
                int next_size = this.nextSize(current_size, false);
                this._children = new IonValueLite[next_size];
            }

            // we should have an empty content list at this point
            assert this.get_child_count() == 0;

            // if this is not buffer backed, we just have to
            // do a deep copy
            final boolean cloningFields = (this instanceof IonStruct);

            IonValueLite[] sourceContents = source._children;
            int size = source.get_child_count();

            for (int i = 0; i < size; i++)
            {
                IonValueLite child = sourceContents[i];
                IonValueLite copy = (IonValueLite)child.clone();  // TODO: remove when we upgrade the Java compiler
                if (cloningFields) {
                    String name = child.getFieldName();
                    copy.setFieldName(name);
                }
                this.add(i, copy);
                // no need to patch the element id's since
                // this is adding to the end
            }
        }
    }

    //////////////////////////////////////////////////////
    //////////////////////////////////////////////////////

    // helper routines for managing the member children

    //////////////////////////////////////////////////////
    //////////////////////////////////////////////////////


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
    final protected int initialSize()
    {
        switch (this.getType()) {
        case LIST:     return 1;
        case SEXP:     return 4;
        case STRUCT:   return 5;
        case DATAGRAM: return 3;
        default:       return 4;
        }
    }
    final protected int nextSize(int current_size, boolean call_transition)
    {
        if (current_size == 0) {
            int new_size = initialSize();
            return new_size;
        }

        int next_size;
        switch (this.getType()) {
            case LIST:     next_size =  4;      break;
            case SEXP:     next_size =  8;      break;
            case STRUCT:   next_size =  8;      break;
            case DATAGRAM: next_size = 10;      break;
            default:       return current_size * 2;
        }

        if (next_size > current_size) {
            // note that unrecognized sizes, either due to unrecognized type id
            // or some sort of custom size in the initial allocation, meh.
            if (call_transition) {
                transitionToLargeSize(next_size);
            }
        }
        else {
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

    public int get_child_count() {
        return _child_count;
    }
    public IonValue get_child(int idx) {
        return get_child_lite(idx);
    }
    public IonValueLite get_child_lite(int idx)
    {
        if (idx < 0 || idx >= _child_count) {
            throw new IndexOutOfBoundsException(""+idx);
        }
        return _children[idx];
    }
    public IonValue set_child(int idx, IonValue child)
    {
        if (child == null) {
            throw new NullPointerException();
        }
        assert(child instanceof IonValueLite);
        return set_child_lite(idx, (IonValueLite)child);
    }
    public IonValueLite set_child_lite(int idx, IonValueLite child)
    {
        if (idx < 0 || idx >= _child_count) {
            throw new IndexOutOfBoundsException(""+idx);
        }
        if (child == null) {
            throw new NullPointerException();
        }
        IonValueLite prev = _children[idx];
        _children[idx] = child;
        return prev;
    }
    public int add_child(int idx, IonValue child)
    {
        return add_child(idx, (IonValueLite)child);
    }
    public int add_child(int idx, IonValueLite child)
    {
        if (child == null) {
            throw new NullValueException();
        }
        if (child instanceof IonDatagram) {
            throw new IllegalArgumentException();
        }
        if (child._context.getParentThroughContext() != null)
        {
            throw new ContainedValueException();
        }
        assert(this.getSystem() == child.getSystem())
            || this.getSystem().getClass().equals(child.getSystem().getClass()
        );

        _isNullValue(false); // if we add children we're not null anymore
        if (_children == null || _child_count >= _children.length) {
            int old_len = (_children == null) ? 0 : _children.length;
            int new_len = this.nextSize(old_len, true);
            assert(new_len > idx);
            IonValueLite[] temp = new IonValueLite[new_len];
            if (old_len > 0) {
                System.arraycopy(_children, 0, temp, 0, old_len);
            }
            _children = temp;
        }
        if (idx < _child_count) {
            System.arraycopy(_children, idx, _children, idx+1, _child_count-idx);
        }
        _child_count++;
        _children[idx] = child;

        child._context.setParentThroughContext(child, this);
        child._elementid(idx);
        return idx;
    }
    public void remove_child(int idx)
    {
        assert(idx >=0);
        assert(idx < get_child_count()); // this also asserts child count > 0

if (get_child(idx) == null) {
            assert(get_child(idx) != null);
}

        _children[idx].detachFromContainer();
        int children_to_move = _child_count - idx - 1;
        if (children_to_move > 0) {
            System.arraycopy(_children, idx+1, _children, idx, children_to_move);
        }
        _child_count--;
        _children[_child_count] = null;
    }
    public int find_Child(IonValue child)
    {
        return find_Child((IonValueLite)child);
    }
    public int find_Child(IonValueLite child) {

        for (int ii=0; ii<_child_count; ii++) {
            if (_children[ii] == child) {
                return ii;
            }
        }
        return -1;
    }
    public final void patch_elements_helper(int lowest_bad_idx)
    {
        // patch the element Id's for all the children from
        // the child who was earliest in the array (lowest index)
        for (int ii=lowest_bad_idx; ii<get_child_count(); ii++) {
            IonValueLite child = get_child_lite(ii);
            child._elementid(ii);
        }
        return;
    }

}
