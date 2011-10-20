// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.impl.UnifiedSymbolTable.isNonSystemSharedTable;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonBinaryWriter;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonReaderFactoryX;
import com.amazon.ion.impl.IonWriterBinaryCompatibility;
import com.amazon.ion.impl.UnifiedSymbolTable;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 *  The datagram generally behaves as an IonSexp. A list with space
 *  separated values.
 *
 *  As a user datagram it only shows user values as current members
 *  of the list.
 *
 *  As a system datagram it also includes system values (adding Ion
 *  version marker symbols and local symbol tables to the list).
 *
 *  Most API's operate on the user view.  The exceptions are getBytes
 *  (and related), and the system* API's.  These synthesize a system
 *  view over the user values.
 *
 *  When system values are added they translate into setsymboltableat
 *  operations.  Which sets a pending symbol table at the current
 *  user position (as specified by the add pos).  When the next add
 *  occurs if it is at the specified position the pending symbol
 *  table is applied to the value if the value doesn't already have
 *  a local symbol table defined.
 *
 *  in any event any membership update invalidates the pending symbol
 *  table.
 *
 *  In general on add if there is no pending symbol table the preceding
 *  value local symbol table is applied as the local symbol table to
 *  the new value, if it needs one.
 *
 *  The system iterator inserts system values to create the minimum
 *  additional values to represent a correct Ion sequence by injecting
 *  IVM's when the symbol table transitions to system, and local
 *  symbol tables when they first occur.
 *
 */

public class IonDatagramLite
    extends IonSequenceLite
    implements IonDatagram, IonContext
{
    private final IonSystemLite      _system;
    private       IonCatalog         _catalog;
    private       SymbolTable        _pending_symbol_table;
    private       int                _pending_symbol_table_idx;
    private       IonSymbolLite      _ivm;

    IonDatagramLite(IonSystemLite system, IonCatalog catalog) {
        super(null, false);
        _system = system;
        _catalog = catalog;
        // these should be no-op's but just to be sure:
        setFieldName(null);
        clearTypeAnnotations();
        _pending_symbol_table_idx = -1;
    }


    //////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////

    //  these are the context methods

    //////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////

    @Override
    public SymbolTable getLocalSymbolTable(IonValueLite child)
    {
        // otherwise the concrete context would have returned
        // it's already present local symbol table, there's
        // no other reason to have a concrete context with
        // a child of datagram.
        assert(child._context == this);

        // see if there's a local table in front of us
        SymbolTable symbols = getChildsSymbolTable(child);
        if (symbols.isLocalTable()) {
            return symbols;
        }

        // if not we make one here on this value
        symbols = _system.newLocalSymbolTable();
        IonConcreteContext context = _system.allocateConcreteContext();
        context.setParentThroughContext(child, this);
        context.setSymbolTableOfChild(symbols, child);
        return symbols;
    }

    // from IonContainerLite(returns this) IonContainerLite getParentThroughContext();

    @Override
    public SymbolTable getSymbolTable()
    {
        if (_pending_symbol_table != null) {
            return _pending_symbol_table;
        }
        return _system.getSystemSymbolTable();
    }

    @Override
    public SymbolTable getAssignedSymbolTable()
    {
        return null;
    }

    @Override
    public IonSystemLite getSystemLite()
    {
        return _system;
    }

    @Override
    public void setParentThroughContext(IonValueLite child, IonContext context)
    {
        assert(context == this);
        child.setContext(this);
    }

    @Override
    public void setSymbolTableOfChild(SymbolTable symbols, IonValueLite child)
    {
        if (isNonSystemSharedTable(symbols)) {
            throw new IllegalArgumentException("you can only set a symbol table to a system or local table");
        }
        //if (symbols.isSystemTable()) {
        //    assert(symbols.getSystem() == getSystemLite());
        //    return;
        //}
        assert(symbols.isLocalTable() || symbols.isSystemTable());
        if (!(child._context instanceof IonConcreteContext)) {
            IonContext context = new IonConcreteContext(_system);
            context.setParentThroughContext(child, this);
        }
        child._context.setSymbolTableOfChild(symbols, child);
    }

    @Override
    public void setSymbolTable(SymbolTable symbols)
    {
        if (isNonSystemSharedTable(symbols)) {
            throw new IllegalArgumentException("you can only set a symbol table to a system or local table");
        }
        _pending_symbol_table = symbols;
        _pending_symbol_table_idx = get_child_count();
    }

    //////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////

    //  these are the sequence methods

    //////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////

    @Override
    public boolean add(IonValue child)
        throws ContainedValueException, NullPointerException
    {
        int index = _child_count;
        add(index, child);
        return true;
    }

    @Override
    public ValueFactory add()
    {
        return new CurriedValueFactoryLite(this.getSystem())
        {
            @Override
            void handle(IonValue newValue)
            {
                add(newValue);
            }
        };
    }

    @Override
    // Increasing visibility - note this is the base, workhorse, add method
    // for datagram (it does use the add_child through super.add()) for the
    // basic child array update, but it takes care of the datagram special
    // behaviors around local symbol tables for values
    public void add(int index, IonValue element)
        throws ContainedValueException, NullPointerException
    {
        if (element == null) {
            throw new NullPointerException();
        }
        if (!(element instanceof IonValueLite)) {
            throw new IllegalArgumentException("IonValue implementation can't be mixed");
        }

        IonValueLite concrete = (IonValueLite)element;

        // super.add will check for the lock
        super.add(index, concrete);
        // handled in super.add(): patch_elements_helper(index + 1);

        SymbolTable symbols = concrete.getAssignedSymbolTable();
        if (symbols == null && this._pending_symbol_table != null && this._pending_symbol_table_idx == concrete._elementid())
        {
            concrete.setSymbolTable(_pending_symbol_table);
        }

        // the pending symbol table is only good for 1 use
        // after that the previous element logic will fill
        // in the symbol table if that is appropriate.
        _pending_symbol_table = null;
        _pending_symbol_table_idx = -1;
    }

    @Override
    public ValueFactory add(final int index)
    {
        return new CurriedValueFactoryLite(getSystem())
        {
            @Override
            void handle(IonValue newValue)
            {
                add(index, newValue);
            }
        };
    }

    @Override
    public boolean addAll(Collection<? extends IonValue> c)
    {
        boolean changed = false;
        for (IonValue v : c)
        {
            changed = add(v) || changed;
        }
        return changed;
    }

    @Override
    public boolean addAll(int index, Collection<? extends IonValue> c)
    {
        if (index < 0 || index > size())
        {
            throw new IndexOutOfBoundsException();
        }

        // TODO optimize to avoid n^2 shifting and renumbering of elements.
        boolean changed = false;
        for (IonValue v : c)
        {
            add(index++, v);
            changed = true;
        }

        if (changed) {
            patch_elements_helper(index);
        }

        return changed;
    }

    /**
     * creates a copy of this IonUserDatagramLite.  Most of the work
     * is actually done by IonContainerImpl.copyFrom() and
     * IonValueImpl.copyFrom().
     */
    @Override
    public IonDatagramLite clone()
    {
        IonDatagramLite clone = new IonDatagramLite(_system, _catalog);

        try {
            clone.copyFrom(this);
        } catch (IOException e) {
            throw new IonException(e);
        }

        return clone;
    }

    // IonSequenceLite version of this is the right one
    // public boolean contains(Object o)

    // IonSequenceLite version of this is the right one
    // public boolean containsAll(Collection<?> c)

    @Override
    public void deepMaterialize()
    {
        populateSymbolValues(null);
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

    // from IonContainerLite
    // public IonValue get(int index)

    // from IonSequenceList
    // public int indexOf(Object o)

    // from IonSequenceLite
    // public int lastIndexOf(Object o)

    // uses IonContainerLite copy
    // public ListIterator<IonValue> listIterator()

    @Override
    public ListIterator<IonValue> listIterator(int index)
    {
        ListIterator<IonValue> iterator = new SequenceContentIterator(index, this.isReadOnly());
        return iterator;
    }

    @Override
    public SymbolTable populateSymbolValues(SymbolTable symbols)
    {
        assert(symbols == null || symbols.isSystemTable());

        // we start with the system symbol table
        symbols = _system.getSystemSymbolTable();

        // thereafter each child may have it's own symbol table
        for (int ii=0; ii<get_child_count(); ii++)
        {
            IonValueLite child = get_child_lite(ii);
            SymbolTable child_symbols = child.getAssignedSymbolTable();
            if (child_symbols != null) {
                symbols = child_symbols;
            }
            // the datagram is not marked readonly, although it's
            // children may be read only
            if (child.isReadOnly() == false) {
                symbols = child.populateSymbolValues(symbols);
            }
        }
        return symbols;
    }

    protected SymbolTable getChildsSymbolTable(IonValueLite child)
    {
        SymbolTable symbols = null;
        int idx = child._elementid();

        // the caller is supposed to check this and not waste our time
        assert(child != null && child.getAssignedSymbolTable() == null);

        while (idx > 0 && symbols == null) {
            idx--;
            IonValueLite prev_child = get_child_lite(idx);
            symbols = prev_child.getAssignedSymbolTable();
        }

        if (symbols == null) {
            symbols = _system.getSystemSymbolTable();
        }

        return symbols;
    }

    // from IonSequenceLite
    // public IonValue remove(int index)

    // from IonSequenceLite
    // public boolean remove(Object o)

    // from IonSequenceLite
    // public boolean removeAll(Collection<?> c)

    // from IonSequenceLite
    // public boolean retainAll(Collection<?> c)

    @Override
    public IonValue set(int index, IonValue element)
    {
        IonValue previous = super.set(index, element);
        IonValueLite concrete = (IonValueLite)element;

        // if the new element didn't come with it's own
        // local symbol table
        if (element.getSymbolTable().isLocalTable() == false) {
            // a pending symbol table is our first choice
            if (index == this._pending_symbol_table_idx) {
                this.setSymbolTableOfChild(_pending_symbol_table, concrete);
            }
            else {
                // the preceding elements symbol table is our next
                IonValueLite preceding = (index > 0) ? get_child_lite(index - 1) : null;
                if (preceding != null && preceding._context != this) {
                    concrete.setContext(preceding._context);
                }
                else {
                    // otherwise element will just default to the system
                    // symbol table when it's context is set to this datagram
                    // should have been set by super.set()
                    assert(concrete._context == this);
                }
            }
        }
        return previous;
    }

    // from IonSequenceLite
    // public List<IonValue> subList(int fromIndex, int toIndex)

    // from IonSequenceLite
    // public synchronized IonValue[] toArray()

    // from IonSequenceLite
    // public <T> T[] toArray(T[] a)

    // from IonContainerLite
    // public void clear()

    // from IonContainerLite
    // public boolean isEmpty() throws NullValueException

    // just a cover for listIterator(0), defined in IonContainerLite
    // public Iterator<IonValue> iterator()

    // from IonContainerLite
    // public void makeNull()

    // from IonContainerLite
    // public boolean remove(IonValue element)

    // from IonContainerLite
    // public int size()

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

    @Override
    public void addTypeAnnotation(String annotation)
    {
        String message = "Datagrams do not have annotations";
        throw new UnsupportedOperationException(message);
    }

    // defined in IonValueLite
    // public void clearTypeAnnotations()

    // defined in IonValueLite
    // public void deepMaterialize()

    @Override
    public IonContainer getContainer()
    {
        return null;
    }

    // defined in IonValueLite
    // public int getFieldId()

    // defined in IonValueLite
    // public String getFieldName()

    // defined in IonValueLite
    // public int getFieldNameId()

    // Moved into the context methods list to
    // live next to context symbol table logic.
    // public SymbolTable getSymbolTable()

    @Override
    public IonSystem getSystem()
    {
        return this._system;
    }

    @Override
    public IonType getType()
    {
        return IonType.DATAGRAM;
    }

    // defined in IonValueLite - always the empty array
    //public String[] getTypeAnnotationStrings()

    // defined in IonValueLite - always the empty array
    // public String[] getTypeAnnotations()

    // defined in IonValueLite
    // public boolean hasTypeAnnotation(String annotation)

    // defined in IonValueLite - always false
    // public boolean isNullValue()

    // defined in IonValueLite
    // public boolean isReadOnly()

    // from IonContainerLite
    // public void makeReadOnly()

    // the base version of this is fine since datagrams have no
    // container to be removed from
    // public boolean removeFromContainer()

    // the base version of this is fine since datagrams have no
    // annotations to be removed from
    // public void removeTypeAnnotation(String annotation)

    //////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////

    //  these are the datagram methods

    //////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////

    /**
     * This doesn't wrap IOException because some callers need to propagate it.
     */
    private IonBinaryWriter make_filled_binary_writer()
    throws IOException
    {
        boolean streamCopyOptimized = false;
        IonWriterBinaryCompatibility.User writer =
            new IonWriterBinaryCompatibility.User(_system, _catalog,
                                                  streamCopyOptimized);
        IonReader reader = IonReaderFactoryX.makeSystemReader(this);
        writer.writeValues(reader);
        writer.finish();
        return writer;
    }

    @SuppressWarnings("deprecation")
    public int byteSize() throws IonException
    {
        try {
            IonBinaryWriter writer = make_filled_binary_writer();
            int size = writer.byteSize();
            return size;
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    @SuppressWarnings("deprecation")
    public byte[] getBytes() throws IonException
    {
        try {
            IonBinaryWriter writer = make_filled_binary_writer();
            byte[] bytes = writer.getBytes();
            return bytes;
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    public int getBytes(byte[] dst) throws IonException
    {
        try {
            IonBinaryWriter writer = make_filled_binary_writer();
            int size = writer.getBytes(dst, 0, dst.length);
            return size;
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    public int getBytes(byte[] dst, int offset) throws IonException
    {
        try {
            IonBinaryWriter writer = make_filled_binary_writer();
            int size = writer.getBytes(dst, offset, dst.length - offset);
            return size;
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    public int getBytes(OutputStream out) throws IOException, IonException
    {
        IonBinaryWriter writer = make_filled_binary_writer();
        int size = writer.writeBytes(out);
        return size;
    }

    // TODO: optimize this, if there's a real use case
    //       deprecate this is there isn't (which I suspect is actually the case)
    public IonValue systemGet(int index) throws IndexOutOfBoundsException
    {
        ListIterator<IonValue> iterator = systemIterator();
        IonValue value = null;

        if (index < 0) {
            throw new IndexOutOfBoundsException(""+index);
        }

        int ii;
        for (ii=0; ii<=index; ii++) {
            if (!iterator.hasNext()) {
                throw new IndexOutOfBoundsException(""+index);
            }
            value = iterator.next();
        }
        return value;
    }

    // TODO: optimize this, if there's a real use case
    //       deprecate this is there isn't (which I suspect is actually the case)
    public IonValue systemRemove(int index) throws IndexOutOfBoundsException
    {
        IonValue value = systemGet(index);
        assert(value instanceof IonValueLite);
        if (((IonValueLite)value)._isSystemValue() == false) {
            remove(value);
        }
        else {
            assert(is_synthetic_value((IonValueLite)value));
        }
        return value;
    }
    // just used for a complicated assertion
    private final boolean is_synthetic_value(IonValueLite value)
    {
        int idx = value._elementid();

        if (idx < 0 || idx >= get_child_count()) return true;
        if (get_child(idx) != value) return true;

        // if this value is in the child collection where
        // it says it is then it's a real value, and not synthetic
        return false;
    }

    public ListIterator<IonValue> systemIterator()
    {

/* test code  FIXME: remove this
if (this.isReadOnly() && hasUnresolvedSymbols(this)) {
    System.err.println("datagram lite has unresolved symbols at get systemIterator time !!");
}
*/
        // read only values have already had their
        // symbols updated, and should not be modified
        if (isReadOnly() == false) {
            populateSymbolValues(null);
        }

        ListIterator<IonValue> iterator = new SystemContentIterator(this.isReadOnly());
        return iterator;
    }

/* more test code: FIXME: remove this
private static boolean hasUnresolvedSymbols(IonValueLite value) {
    if (value instanceof IonContainerLite ) {
        IonContainerLite container = (IonContainerLite)value;
        for (int ii=0; ii<container.get_child_count(); ii++) {
            IonValueLite child = container.get_child_lite(ii);
            if (hasUnresolvedSymbols(child)) {
                return true;
            }
        }
    }
    else {
        SymbolTable st = value.getAssignedSymbolTable();
        String fieldname = value.getFieldName();
        if (fieldname != null) {
            int sid = st.findSymbol(fieldname);
            if (sid < 1) {
                return true;
            }
        }
        if (value instanceof IonSymbolLite) {

        }
    }
    String[] annotations = value.getTypeAnnotationStrings();
    if (annotations != null && annotations.length > 0) {
        SymbolTable st = value.getAssignedSymbolTable();
        for (int ii=0; ii<annotations.length; ii++) {
            if (st.findSymbol(annotations[ii]) < 1) {
                return true;
            }
        }
    }
    return false;
}
*/

    // TODO: optimize this, if there's a real use case
    //       deprecate this is there isn't (which I suspect is actually the case)
    public int systemSize()
    {
        int count = 0;
        ListIterator<IonValue> iterator = systemIterator();
        while (iterator.hasNext()) {
            IonValue value = iterator.next();
            count++;
        }
        return count;
    }

    public byte[] toBytes() throws IonException
    {
        byte[] bytes;
        try {
            IonBinaryWriter writer = make_filled_binary_writer();
            bytes = writer.getBytes();
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return bytes;
    }

    //////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////

    //  these are the local helper methods

    //////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////

    protected synchronized IonSymbolLite get_ivm()
    {
        if (_ivm == null) {
            _ivm = (IonSymbolLite) getSystem().newSymbol(ION_1_0);
        }
        return _ivm;
    }

    /**
     * Encapsulates an iterator and implements a custom remove method
     *
     *  this is tied to the _child array of the IonSequenceImpl
     *  through the _children and _child_count members which this
     *  iterator directly uses.
     *
     *  This is a specialization for returning a system view of the
     *  current datagram.  It does this by synthesizing system values
     *  (Ion Version Markers & local symbol tables) as symbol tables
     *  change between user values.
     *
     *  It accumulates a count of the system values it encounters
     *  the need to inject them in the iteration stream.
     *
     *  It may need more than 1 system value between user values
     *  if the system is reset or the local symbol table itself
     *  has local symbols (this is rare but in principal could
     *  be an arbitrarily long sequence).
     *
     *  The current position is between the value to be returned
     *  by next and the value that would be returned by previous.
     *  This is calculated in next_index_helper() (or previous_).
     *  It is represented by a "struct" with a position in the
     *  child array, an optional position in the local stack
     *  of system values, and a flag indicating which list should
     *  be used to fetch the actual value.
     *
     *  The local synthetic system values are held in the array
     *  __local_system_value.  These are the values that should
     *  preceed the value at _child[__user_content_pos].
     *
     *  TODO with the updated next and previous logic, particularly
     *  the force_position_sync logic and lastMoveWasPrevious flag
     *  we could implement add and set correctly.
     *
     *  NOTE this closely resembles the iterator defined in IonSequenceLite,
     *  so changes here are likely to be needed in IonSequenceLite as well.
     */
    protected final class SystemContentIterator
        implements ListIterator<IonValue>
    {
        private final boolean                __readOnly;
        private       IonValueLite           __current;
        private       SystemIteratorPosition __pos;

        private       SystemIteratorPosition __temp_pos;

        public SystemContentIterator(boolean readOnly)
        {
            if (_isLocked() && !readOnly) {
                throw new IllegalStateException("you can't open an updatable iterator on a read only value");
            }
            __readOnly = readOnly;

            __temp_pos = new SystemIteratorPosition(this); // we flip back and forth between two positions to avoid allocating these on every value (or more)
            __pos = new SystemIteratorPosition(this);
            __pos.load_initial_position();
      }

        protected IonValueLite set_position(SystemIteratorPosition newPos)
        {
            // swap our active position with our temp position
            __temp_pos = __pos;
            __pos = newPos;

            // load out current value from the position, and return it
            __current = __pos.load_position();
            return __current;
        }

        protected void force_position_sync()
        {
            int user_index = __pos.__user_index;
            if (user_index < 0 || user_index >= _child_count) {
                return;
            }
            IonValueLite user_value = __pos.__current_user_value;
            if (user_value == null || user_value == _children[user_index]) {
                return;
            }
            if (__readOnly) {
                throw new IonException("read only sequence was changed");
            }
            __pos.force_position_sync_helper();
        }

        public void add(IonValue element)
        {
            throw new UnsupportedOperationException();
        }

        public final boolean hasNext()
        {
            return __pos.has_next();
        }

        public IonValue next()
        {
            SystemIteratorPosition pos = next_index_helper();
            if (pos == null) {
                throw new NoSuchElementException();
            }
            IonValueLite current_value = set_position(pos);
            assert(current_value == this.__current);
            return current_value;
        }

        public final int nextIndex()
        {
            SystemIteratorPosition pos = next_index_helper();
            if (pos == null) {
                // we can do this because we hold the __pos
                // even when we run off the end, it will be
                // positioned at the value that wasn't there
                return __pos.get_external_pos() + 1;
            }
            int idx = pos.get_external_pos();
            return idx;
        }

        private final SystemIteratorPosition next_index_helper()
        {
            SystemIteratorPosition next = null;
            force_position_sync();

            if (__pos.has_next() == false) {
                return null;
            }

            // at this point we will have a next so prep out position now
            next = __temp_pos;
            assert(next != null && next != __pos);
            next.copyFrom(__pos);

            // see if there's a system value waiting for use
            next.__local_index++;
            if (next.__local_index < next.__local_value_count) {
                return next;
            }

            // if there's not system value there must be another user value
            // so we shouldn't get here since has_next() should have failed
            assert(next.__user_index <= get_child_count());

            // if we were on a system value then we're just stepping onto the
            // since has_next() already declared we do have a waiting value
            next.__user_index++;
            next.load_updated_position();
            // we step onto the first local value
            next.__local_index = 0;
            return next;
        }

        public final boolean hasPrevious()
        {
            return __pos.has_prev();
        }

        public IonValue previous()
        {
            SystemIteratorPosition pos = previous_index_helper();
            if (pos == null) {
                throw new NoSuchElementException();
            }
            IonValueLite current_value = set_position(pos);
            assert(current_value == this.__current);
            return current_value;
        }

        public final int previousIndex()
        {
            SystemIteratorPosition pos = previous_index_helper();
            if (pos == null) {
                return -1;
            }
            int idx = pos.get_external_pos();
            return idx;
        }

        private final SystemIteratorPosition previous_index_helper()
        {
            SystemIteratorPosition prev = null;

            force_position_sync();
            if (__pos.has_prev() == false) {
                return null;
            }

            // at this point we will have a prev so prep out position now
            prev = __temp_pos;
            assert(prev != null && prev != __pos);
            prev.copyFrom(__pos);

            prev.__local_index--;
            if (prev.__local_index >= 0) {
                return prev;
            }

            // if there's not system value there must be another user value
            // we would have bailed with has_prev returned false above otherwise
            assert(prev.__user_index > 0);

            // if this is the 2nd prev then we really have to back up
            prev.__index_adjustment -= prev.__local_value_count;
            prev.__user_index--;
            prev.load_updated_position();

            // going backwards we "start" at the end of our local list
            prev.__local_index = prev.__local_value_count - 1;

            return prev;
        }

        /**
         * removes the current member from the containing
         * datagram if it is a user value.
         *
         * If there is no current value it throws NoSuchElementException
         * If the current value is not a user value in the datagram
         * this throws UnsupportedOperationException.
         * And if the iterator is a read only iterator this also
         * throws UnsupportedOperationException.
         */
        public void remove()
        {
            if (__readOnly) {
                throw new UnsupportedOperationException();
            }
            force_position_sync();

            if (__current == null || __pos == null) {
                throw new NoSuchElementException();
            }
            if (__pos.on_system_value()) {
                throw new UnsupportedOperationException();
            }

            int idx = __pos.__user_index;
            assert(idx >= 0);

            IonValueLite concrete = __current;
            int concrete_idx = concrete._elementid();
            assert(concrete_idx == idx);

            // here we remove the member from the containers list of elements
            remove_child(idx);

            // and here we patch up the member
            // and then the remaining members index values
            concrete.detachFromContainer();
            patch_elements_helper(concrete_idx);

            // when we remove the current value we remove
            // its associated system values and this
            // may change the index adjustment
            __pos.__index_adjustment -= __pos.__local_value_count;
            if (__pos.__user_index < get_child_count() - 1)
            {
                __pos.load_updated_position();
                __pos.__local_index = -1;
            }
            __current = null;
        }

        public void set(IonValue element)
        {
            throw new UnsupportedOperationException();
        }
        protected int get_datagram_child_count()
        {
            return get_child_count();
        }
        protected IonValueLite get_datagram_child(int idx)
        {
            return get_child_lite(idx);
        }
        protected IonSystem get_datagram_system()
        {
            return _system;
        }
        protected boolean datagram_contains(IonValueLite value)
        {
            return contains(value);
        }
        protected IonSymbolLite get_datagram_ivm()
        {
            return get_ivm();
        }
    }
    static class SystemIteratorPosition
    {
        /**
         * this position points to the user value that
         * we might have just passed calling next
         *
         * that is this position is between the last
         * value returned by next and the next value
         * that will be returned.
         *
         * As we change the user index we load the
         * current user value and synthesize any needed
         * local system values
         *
         * we also push the user value onto the same
         * local stack.  that way out local index can
         * run off either end of the local "list"
         * and only when we go yet another past either
         * end do we need to reload the value to
         * one side of the other, and which side of
         * our local list we ran off of will tell
         * us that.
         */
        protected final SystemContentIterator __iterator;
        protected       int                   __index_adjustment;      // delta between the user_content_pos and the external_pos

        protected       int                   __local_index;           // index of the next value in the system array (__local_system_value), if __on_system_value
        protected       IonValueLite[]        __local_values = new IonValueLite[3]; // more than the value, a symbol table, and a version marker would be MOST unusual
        protected       int                   __local_value_count;

        protected       int                   __user_index;            // index of next value in the user content array (_children)
        protected       IonValueLite          __current_user_value;    // value from the child array at the time the user_index was moved forward == get_child(next_user_index -1)
        protected       SymbolTable           __current_symbols;
        protected       int                   __current_symbols_index;

        SystemIteratorPosition(SystemContentIterator iterator)
        {
            __iterator = iterator;
        }

        void load_initial_position()
        {
            __user_index = 0;
            __local_index = -1; // we're before the first value
            __current_symbols_index = -1;
            load_updated_position();
        }

        protected int get_external_pos()
        {
            int user_index;
            user_index  = __user_index;
            user_index += __index_adjustment;
            user_index -= __local_value_count;
            user_index += __local_index;
            return user_index;
        }

        protected boolean on_system_value()
        {
            return (__current_user_value != __local_values[0]);
        }
        protected boolean has_next()
        {
            if (__local_index + 1 < __local_value_count) {
                return true;
            }
            if (__user_index + 1 < __iterator.get_datagram_child_count()) {
                return true;
            }
            return false;
        }
        protected boolean has_prev()
        {
            // if we're not at the beginning of the datagram list
            // we always have another user value
            if (__user_index > 0) {
                return true;
            }
            if (__local_index > 0) {
                return true;
            }
            // we're out of both user and system values
            return false;
        }

        protected void copyFrom(SystemIteratorPosition source)
        {
            this.__index_adjustment         = source.__index_adjustment;
            this.__user_index               = source.__user_index;
            this.__local_index              = source.__local_index;
            this.__current_user_value       = source.__current_user_value;
            this.__current_symbols          = source.__current_symbols;
            this.__current_symbols_index    = source.__current_symbols_index;

            // for the local system values each position needs its own
            // array, but the can share the value references
            if (source.__local_value_count > 0) {
                if (this.__local_values == null || source.__local_value_count >= this.__local_values.length) {
                    this.__local_values = new IonValueLite[source.__local_values.length];
                }
                System.arraycopy(source.__local_values, 0, this.__local_values, 0, source.__local_value_count);
            }
            this.__local_value_count = source.__local_value_count;
        }

        private void load_updated_position()
        {
            IonValueLite prev_value = __current_user_value;
            // we load our referenced user value (@ __user_index if
            // it exists).
            if (__user_index < 0 || (__user_index > 0 && __user_index >=  __iterator.get_datagram_child_count())) {
                throw new IonException("attempt to position iterator past end of values");
            }
            if (__user_index < __iterator.get_datagram_child_count()) {
                __current_user_value = __iterator.get_datagram_child(__user_index);
                assert(__current_user_value != null);
            }
            else {
                // when there are no user values and we're at index == 0
                assert(__user_index == 0 && __iterator.get_datagram_child_count() == 0);
                __current_user_value = null;
            }

            int old_count = __local_value_count;
            __local_value_count = 0;
            if (__current_user_value != null) {
                push_system_value(__current_user_value);
            }
            load_current_symbol_table(prev_value);

            for (int ii=__local_value_count; ii<old_count; ii++) {
                __local_values[ii] = null;
            }
            __index_adjustment += __local_value_count - 1;

            return;
        }

        void load_current_symbol_table(IonValueLite prev_user_value)
        {
            IonValueLite curr_value  = __current_user_value;
            int          curr_index  = __user_index;

            IonValueLite prev_value  = prev_user_value;
            SymbolTable  prev_symtab = __current_symbols;
            int          prev_index  = __current_symbols_index;

            // set our new position symbol table
            __current_symbols  = null;
            __current_symbols_index = curr_index;
            SymbolTable  curr_symtab = null;
           if (curr_value != null) {
                curr_symtab = curr_value.getAssignedSymbolTable();
                __current_symbols = curr_symtab;
            }

            // if we need to we reset the previous values here
            // this happens when the caller is scanning backwards
            if (curr_index - 1 != prev_index) {
                prev_index = curr_index - 1;
                prev_symtab = null;
                if (prev_index >= 0 && prev_index < __iterator.get_datagram_child_count()) {
                    prev_value  = __iterator.get_datagram_child(prev_index);
                    prev_symtab = prev_value.getAssignedSymbolTable();
                }
            }

            // Now if there was a change push the local symbol
            // tables onto our system value stack

            // note that our chain of preceding symbol tables
            // might match our list of previous structs in the
            // user list.  Until there's a difference we don't
            // push the symbol tables (because they've already
            // been processed as real values).
            if (curr_symtab != prev_symtab) {
                SymbolTable new_symbol_table = curr_symtab;
                while (new_symbol_table != null)
                {
                    if ((new_symbol_table instanceof UnifiedSymbolTable) == false) {
                        throw new IonException("only UnifiedSymbolTable instances are currently supported by IonDatagramLite");
                    }

                    final boolean new_symbol_table_is_system = new_symbol_table.isSystemTable();
                    IonValue rep;
                    if (new_symbol_table_is_system) {
                        rep = __iterator.get_datagram_ivm();
                    }
                    else {
                        rep = ((UnifiedSymbolTable)new_symbol_table).getIonRepresentation(this.__iterator.get_datagram_system());
                    }
                    assert(rep != null && __iterator.get_datagram_system() == rep.getSystem());

                    if (rep == prev_value || (is_ivm(curr_value) && new_symbol_table_is_system)) {
                        int prev_idx = (prev_value == null) ? -1 : (prev_value._elementid() - 1);
                        if (prev_idx >= 0) {
                            prev_value = __iterator.get_datagram_child(prev_idx);
                        }
                        else {
                            prev_value = null;
                        }
                    }
                    else {
                        push_system_value((IonValueLite)rep);
                        prev_value = null; // end of the matches
                    }
                    new_symbol_table = rep.getSymbolTable();
                    if (new_symbol_table == null || new_symbol_table.isSystemTable()) {
                        break;
                    }
                }
            }
            // and at the front we need to put in the ion version marker
            if (curr_index == 0 && !is_ivm(curr_value)) {
                IonValueLite ivm = __iterator.get_datagram_ivm();
                push_system_value(ivm);
            }
        }

        private static final boolean is_ivm(IonValue value)
        {
            if (value instanceof IonSymbol) {
                IonSymbol sym = (IonSymbol)value;
                if (!sym.isNullValue()) {
                    if (sym.stringValue().equals(ION_1_0)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private void push_system_value(IonValueLite value)
        {
            if (__local_value_count >= __local_values.length) {
                int new_size = (__local_values == null) ? 2 : (__local_values.length * 2);
                assert( new_size > __local_value_count); // we should only need to add 1 value at a time
                IonValueLite[] temp = new IonValueLite[new_size];
                if (__local_value_count > 0) {
                    System.arraycopy(__local_values, 0, temp, 0, __local_value_count);
                }
                __local_values = temp;
            }
            __local_values[__local_value_count++] = value;
        }

        protected IonValueLite load_position()
        {
            IonValueLite current = null;

            assert(__local_index < __local_value_count);

            current = __local_values[__local_value_count - __local_index - 1];

            return current;
        }

        private final void force_position_sync_helper()
        {
            if (!__iterator.datagram_contains(__current_user_value)) {
                throw new IonException("current user value removed outside this iterator - position lost");
            }
            int old_index = __user_index;
            int new_index = __current_user_value._elementid();

            if (old_index != new_index) {
                // if our current value moved we have to recompute
                // the adjustment from scratch since we don't really
                // have any idea why this moved in either direction.
                int adjustment = 0;
                SymbolTable curr, prev = null;
                for (int ii=0; ii<new_index; ii--) {
                    curr = __iterator.get_datagram_child(ii).getSymbolTable();
                    if (curr != prev) {
                        adjustment += count_system_values(prev, curr);
                    }
                    prev = curr;
                }
                __index_adjustment = adjustment + __local_value_count;
                __user_index = new_index;
            }
        }

        static int count_system_values(SymbolTable prev, SymbolTable curr)
        {
            int count = 0;
            while (curr.isLocalTable()) {
                count++;
                curr = ((UnifiedSymbolTable)curr).getIonRepresentation().getSymbolTable();
            }
            // we should terminate when the symbol tables symbol table is the system symbol table
            assert(curr != null);
            if (prev == null || prev.getIonVersionId().equals(curr.getIonVersionId())) {
                count++;
            }
            return count;
        }
    }
}
