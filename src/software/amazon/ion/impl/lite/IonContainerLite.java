/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.lite;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import software.amazon.ion.ContainedValueException;
import software.amazon.ion.IonContainer;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonException;
import software.amazon.ion.IonValue;
import software.amazon.ion.NullValueException;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.ValueVisitor;
import software.amazon.ion.impl.PrivateIonConstants;
import software.amazon.ion.impl.PrivateIonContainer;
import software.amazon.ion.impl.PrivateUtils;

abstract class IonContainerLite
    extends IonValueLite
    implements PrivateIonContainer, IonContext
{

    protected int            _child_count;
    protected IonValueLite[] _children;

    protected IonContainerLite(ContainerlessContext context, boolean isNull)
    {
        // we'll let IonValueLite handle this work as we always need to know
        // our context and if we should start out as a null value or not
        super(context, isNull);
    }

    IonContainerLite(IonContainerLite existing, IonContext context, boolean isStruct) {
        super(existing, context);
        int childCount = existing._child_count;
        this._child_count = childCount;
        // when cloning the children we establish 'this' the cloned outer container as the context
        if (existing._children != null) {
            boolean isDatagram = this instanceof IonDatagramLite;
            this._children = new IonValueLite[childCount];
            for (int i = 0; i < childCount; i++) {
                IonValueLite child = existing._children[i];
                IonContext childContext = isDatagram
                     ? TopLevelContext.wrap(child.getAssignedSymbolTable(), (IonDatagramLite)this)
                     : this;

                IonValueLite copy = child.clone(childContext);
                if (isStruct) copy.setFieldName(child.getFieldName());
                this._children[i] = copy;
            }
        }
    }

    @Override
    public abstract void accept(ValueVisitor visitor) throws Exception;

    @Override
    public abstract IonContainer clone();


    public void clear()
    {
        checkForLock();

        if (_isNullValue())
        {
            assert _children == null;
            assert _child_count == 0;
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
        IonValueLite value = get_child(index);
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

    public ListIterator<IonValue> listIterator(int index)
    {
        if (isNullValue())
        {
            if (index != 0) throw new IndexOutOfBoundsException();
            return PrivateUtils.<IonValue> emptyIterator();
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
        protected       IonValueLite __current;

        public SequenceContentIterator(int index, boolean readOnly)
        {
            if (_isLocked() && !readOnly) {
                throw new IllegalStateException("you can't open an updatable iterator on a read only value");
            }
            if (index < 0 || index > _child_count) {
                throw new IndexOutOfBoundsException(Integer.toString(index));
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

            IonValueLite concrete = __current;
            int concrete_idx = concrete._elementid();
            assert(concrete_idx == idx);

            // here we remove the member from the container's list of elements
            remove_child(idx);
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
        checkForLock();

        if (element.getContainer() != this) {
            return false;
        }

        // Get all the data into the DOM, since the element will be losing
        // its backing store.
        IonValueLite concrete = (IonValueLite) element;

        int pos = concrete._elementid();
        IonValueLite child = get_child(pos);
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
    void makeReadOnlyInternal()
    {
        if (_isLocked()) return;

        if (_children != null) {
            for (int ii=0; ii<_child_count; ii++) {
                IonValueLite child = _children[ii];
                child.makeReadOnlyInternal();
            }
        }
        // we don't need to call our copy of clear symbol ID's
        // which recurses since the calls to child.makeReadOnly
        // will have clear out the child symbol ID's already
        // as the children were marked read only.  But we do need
        // to call the base clear which will clear out the symbol
        // table reference if one exists.
        super.clearSymbolIDValues();
        _isLocked(true);
    }

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

    public final IonContainerLite getContextContainer()
    {
        return this;
    }

    /**
     * @return {@code null}, since symbol tables are only directly assigned
     *          to top-level values.
     */
    public final SymbolTable getContextSymbolTable()
    {
        return null;
    }

    @Override
    void clearSymbolIDValues()
    {
        super.clearSymbolIDValues();
        for (int ii=0; ii<get_child_count(); ii++) {
            IonValueLite child = get_child(ii);
            child.clearSymbolIDValues();
        }
    }

    /**
     * @param child
     */
    public boolean add(IonValue child)
        throws NullPointerException, IllegalArgumentException,
        ContainedValueException
    {
        int size = get_child_count();

        add(size, (IonValueLite) child);

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
    void validateNewChild(IonValue child)
        throws ContainedValueException, NullPointerException,
               IllegalArgumentException
    {
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

        assert child instanceof IonValueLite
            : "Child was not created by the same ValueFactory";

        assert getSystem() == child.getSystem()
            || getSystem().getClass().equals(child.getSystem().getClass());
    }

    /**
     * Validates the child and checks locks.
     *
     * @param child
     *        must not be null.
     * @throws NullPointerException
     *         if the element is <code>null</code>.
     * @throws IndexOutOfBoundsException
     *   if the index is out of range (index < 0 || index > size()).
     */
    void add(int index, IonValueLite child)
        throws ContainedValueException, NullPointerException
    {
        if ((index < 0) || (index > get_child_count()))
        {
            throw new IndexOutOfBoundsException();
        }
        checkForLock();
        validateNewChild(child);

        add_child(index, child);
        patch_elements_helper(index + 1);

        assert((index >= 0)
               && (index < get_child_count())
               && (child == get_child(index))
               && (child._elementid() == index));
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
        int[] sizes = new int[PrivateIonConstants.tidDATAGRAM + 1];
        sizes[PrivateIonConstants.tidList]     = 1;
        sizes[PrivateIonConstants.tidSexp]     = 4;
        sizes[PrivateIonConstants.tidStruct]   = 5;
        sizes[PrivateIonConstants.tidDATAGRAM] = 3;
        return sizes;
    }
    static final int[] NEXT_SIZE = make_next_size_array();
    static int[] make_next_size_array() {
        int[] sizes = new int[PrivateIonConstants.tidDATAGRAM + 1];
        sizes[PrivateIonConstants.tidList]     = 4;
        sizes[PrivateIonConstants.tidSexp]     = 8;
        sizes[PrivateIonConstants.tidStruct]   = 8;
        sizes[PrivateIonConstants.tidDATAGRAM] = 10;
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

    /**
     * This is overriden in {@link IonStructLite} to add the {@link HashMap} of
     * field names when the struct becomes moderately large.
     *
     * @param size
     */
    void transitionToLargeSize(int size)
    {
        return;
    }

    public final int get_child_count() {
        return _child_count;
    }

    public final IonValueLite get_child(int idx) {
        if (idx < 0 || idx >= _child_count) {
            throw new IndexOutOfBoundsException(Integer.toString(idx));
        }
        return _children[idx];
    }


    final IonValueLite set_child(int idx, IonValueLite child)
    {
        if (idx < 0 || idx >= _child_count) {
            throw new IndexOutOfBoundsException(Integer.toString(idx));
        }
        if (child == null) {
            throw new NullPointerException();
        }
        IonValueLite prev = _children[idx];
        _children[idx] = child;

        // FIXME this doesn't update the child's context or index
        // which is done by add_child() above
        return prev;
    }

    /**
     * Does not validate the child or check locks.
     */
    protected int add_child(int idx, IonValueLite child)
    {
        _isNullValue(false); // if we add children we're not null anymore
        child.setContext(this.getContextForIndex(child, idx));
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

        child._elementid(idx);
        return idx;
    }


    IonContext getContextForIndex(IonValue element, int index){
        return this;
    }

    /**
     * Does not check locks.
     */
    void remove_child(int idx)
    {
        assert(idx >=0);
        assert(idx < get_child_count()); // this also asserts child count > 0
        assert get_child(idx) != null : "No child at index " + idx;

        _children[idx].detachFromContainer();
        int children_to_move = _child_count - idx - 1;
        if (children_to_move > 0) {
            System.arraycopy(_children, idx+1, _children, idx, children_to_move);
        }
        _child_count--;
        _children[_child_count] = null;
    }

    public final void patch_elements_helper(int lowest_bad_idx)
    {
        // patch the element Id's for all the children from
        // the child who was earliest in the array (lowest index)
        for (int ii=lowest_bad_idx; ii<get_child_count(); ii++) {
            IonValueLite child = get_child(ii);
            child._elementid(ii);
        }
        return;
    }
}
