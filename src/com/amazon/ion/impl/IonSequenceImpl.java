// Copyright (c) 2007-2009 Amazon.com, Inc. All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueFactory;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;


/**
 * Base class for list and sexp implementations.
 */
public abstract class IonSequenceImpl
    extends IonContainerImpl
    implements IonSequence
{
    /**
     * A zero-length array.
     */
    protected static final IonValue[] EMPTY_VALUE_ARRAY = IonValue.EMPTY_ARRAY;
    // TODO inline and remove this

    /**
     * Constructs a sequence backed by a binary buffer.
     *
     * @param typeDesc
     */
    protected IonSequenceImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert !_hasNativeValue();
    }

    /**
     * Constructs a sequence value <em>not</em> backed by binary.
     *
     * @param typeDesc
     * @param makeNull
     */
    protected IonSequenceImpl(IonSystemImpl system, int typeDesc, boolean makeNull)
    {
        this(system, typeDesc);
        assert _children == null;
        assert isDirty();

        if (makeNull)
        {
            _isNullValue(true);
        }
        else
        {
            _isNullValue(false);
            _children = new IonValue[initialSize(typeDesc)];
            _child_count = 0;
        }
        _hasNativeValue(true);
    }

    /**
     * Constructs a sequence value <em>not</em> backed by binary.
     *
     * @param typeDesc
     *   the type descriptor byte.
     * @param elements
     *   the initial set of child elements.  If <code>null</code>, then the new
     *   instance will have <code>{@link #isNullValue()} == true</code>.
     *
     * @throws ContainedValueException if any value in <code>elements</code>
     * has <code>{@link IonValue#getContainer()} != null</code>.
     * @throws IllegalArgumentException
     * @throws NullPointerException
     */
    protected IonSequenceImpl(IonSystemImpl system,
                              int typeDesc,
                              Collection<? extends IonValue> elements)
        throws ContainedValueException, NullPointerException,
            IllegalArgumentException
    {
        this(system, typeDesc);
        assert _children == null;
        assert isDirty();

        _hasNativeValue(true);

        boolean isnull = (elements == null);
        _isNullValue(isnull);

        if (elements != null)
        {
            _children = new IonValue[elements.size()];
            for (Iterator i = elements.iterator(); i.hasNext();)
            {
                IonValue element = (IonValue) i.next();
                super.add(element);
            }

            // FIXME if add of a child fails, prior children have bad container
        }
    }

    //=========================================================================

    @Override
    public abstract IonSequenceImpl clone();

    /**
     * Calculate Ion Sequence hash code as seed value XORed with hash
     * codes of contents, rotating 3 at each step to make the code
     * order-dependent.
     * @param seed Seed value
     * @return hash code
     */
    protected int sequenceHashCode(int seed)
    {
        int hash_code = seed;
        if (!isNullValue())  {
            for (IonValue v : this)  {
                hash_code ^= v.hashCode();
                hash_code = hash_code << 29 | hash_code >>> 3;
            }
        }
        return hash_code;
    }

    //public boolean oldisNullValue()
    //{
    //    if (_hasNativeValue() || !_isPositionLoaded()) {
    //        return _isNullValue();
    //    }
    //
    //    int ln = this.pos_getLowNibble();
    //    return (ln == IonConstants.lnIsNullSequence);
    //}

    @Override
    // Increasing visibility
    public boolean add(IonValue element)
        throws ContainedValueException, NullPointerException
    {
        // super.add will check for the lock
        return super.add(element);
    }

    public boolean addAll(Collection<? extends IonValue> c)
    {
        boolean changed = false;
        for (IonValue v : c)
        {
            changed = add(v) || changed;
        }
        return changed;
    }

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
        return changed;
    }


    public ValueFactory add()
    {
        return new CurriedValueFactory(_system)
        {
            @Override
            void handle(IonValue newValue)
            {
                add(newValue);
            }
        };
    }


    @Override
    // Increasing visibility
    public void add(int index, IonValue element)
        throws ContainedValueException, NullPointerException
    {
        // super.add will check for the lock
        super.add(index, element);
    }

    public ValueFactory add(final int index)
    {
        return new CurriedValueFactory(_system)
        {
            @Override
            void handle(IonValue newValue)
            {
                add(index, newValue);
            }
        };
    }


    public IonValue set(int index, IonValue element)
    {
        checkForLock();
        final IonValueImpl concrete = ((IonValueImpl) element);

        // NOTE: size calls makeReady() so we don't have to
        if (index < 0 || index >= size())
        {
            throw new IndexOutOfBoundsException("" + index);
        }

        validateNewChild(element);

        assert _children != null; // else index would be out of bounds above.

        IonValueImpl removed = (IonValueImpl)set_child(index, concrete);
        concrete._elementid = index;
        concrete._container = this;

        try
        {
            removed.detachFromContainer();
            // calls setDirty(), UNLESS it hits an IOException
        }
        catch (IOException e)
        {
            setDirty();
            throw new IonException(e);
        }
        return removed;
    }


    public IonValue remove(int index)
    {
        // TODO optimize
        if (index < 0 || index >= get_child_count()) {
            throw new IndexOutOfBoundsException("" + index);
        }
        IonValue v = get(index);
        remove(v);
        return v;
    }

    public boolean remove(Object o)
    {
        return remove((IonValue) o);
    }

    public boolean removeAll(Collection<?> c)
    {
        boolean changed = false;
        for (Object o : c)
        {
            changed = remove(o) || changed;
        }
        return changed;
    }

    public boolean retainAll(Collection<?> c)
    {
        if (get_child_count() < 1) return false;

        // TODO this method (and probably several others) needs optimization.
        IdentityHashMap<IonValue, IonValue> keepers =
            new IdentityHashMap<IonValue, IonValue>();

        for (Object o : c)
        {
            IonValue v = (IonValue) o;
            if (this == v.getContainer()) keepers.put(v, v);
        }

        boolean changed = false;
        for (int i = get_child_count() - 1; i >= 0; i--)
        {
            IonValue v = get_child(i);
            if (! keepers.containsKey(v))
            {
                remove(v);
                changed = true;
            }
        }
        return changed;
    }


    public boolean contains(Object o)
    {
        return ((IonValue)o).getContainer() == this;
    }

    public boolean containsAll(Collection<?> c)
    {
        for (Object o : c)
        {
            if (! contains(o)) return false;
        }
        return true;
    }


    public int indexOf(Object o)
    {
        IonValueImpl v = (IonValueImpl) o;
        if (this != v.getContainer()) return -1;
        return v.getElementId();
    }

    public final int lastIndexOf(Object o)
    {
        return indexOf(o);
    }

    public List<IonValue> subList(int fromIndex, int toIndex)
    {
        // TODO JIRA ION-92
        throw new UnsupportedOperationException("JIRA issue ION-92");
    }

    public IonValue[] toArray()
    {
        if (get_child_count() < 1) return EMPTY_VALUE_ARRAY;

        IonValue[] array = new IonValue[get_child_count()];
        System.arraycopy(_children, 0, array, 0, get_child_count());
        return array;
    }


    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a)
    {
        int size = get_child_count();
        if (a.length < size)
        {
            // TODO JDK 1.6 this could use Arrays.copyOf
            Class<?> type = a.getClass().getComponentType();
            // generates unchecked warning
            a = (T[]) Array.newInstance(type, size);
        }
        if (size > 0) {
            System.arraycopy(_children, 0, a, 0, size);
        }
        if (size < a.length) {
            // A surprising bit of spec.
            // this is required even with a 0 entries
            a[size] = null;
        }
        return a;
    }

    @SuppressWarnings("unchecked")
    public <T extends IonValue> T[] extract(Class<T> type)
    {
        if (isNullValue()) return null;
        T[] array = (T[]) Array.newInstance(type, size());
        toArray(array);
        clear();
        return array;
    }

    //=========================================================================

    @Override
    protected int computeLowNibble(int valuelen)
        throws IOException
    {
        assert _hasNativeValue();

        if (_isNullValue())    { return IonConstants.lnIsNullSequence; }
        if (_children == null || _child_count == 0) { return IonConstants.lnIsEmptyContainer; }

        int contentLength = getNakedValueLength();
        if (contentLength > IonConstants.lnIsVarLen)
        {
            return IonConstants.lnIsVarLen;
        }

        return contentLength;
    }

    @Override
    protected int writeValue(IonBinary.Writer writer,
                             int cumulativePositionDelta)
        throws IOException
    {
        assert _hasNativeValue() == true || _isPositionLoaded() == false;
        assert !(this instanceof IonDatagram);

        writer.write(this.pos_getTypeDescriptorByte());

        // now we write any data bytes - unless it's null
        int vlen = this.getNativeValueLength();
        if (vlen > 0)
        {
            if (vlen >= IonConstants.lnIsVarLen)
            {
                writer.writeVarUInt7Value(vlen, true);
                // Fall through...
            }

            // TODO cleanup; this is the only line different from super
            cumulativePositionDelta =
                doWriteContainerContents(writer,
                                         cumulativePositionDelta);
        }
        return cumulativePositionDelta;
    }
}
