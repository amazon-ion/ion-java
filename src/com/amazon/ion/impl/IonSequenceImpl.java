// Copyright (c) 2007-20089 Amazon.com, Inc. All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;


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
    protected static final IonValue[] EMPTY_VALUE_ARRAY = new IonValue[0];

    /**
     * Constructs a sequence backed by a binary buffer.
     *
     * @param typeDesc
     */
    protected IonSequenceImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert !_hasNativeValue;
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
        assert _contents == null;
        assert isDirty();

        if (!makeNull)
        {
            _contents = new ArrayList<IonValue>();
        }
        _hasNativeValue = true;
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
        assert _contents == null;
        assert isDirty();

        _hasNativeValue = true;

        if (elements != null)
        {
            _contents = new ArrayList<IonValue>(elements.size());
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


    @Override
    public boolean isNullValue()
    {
        if (_hasNativeValue || !_isPositionLoaded) {
            return (_contents == null);
        }

        int ln = this.pos_getLowNibble();
        return (ln == IonConstants.lnIsNullSequence);
    }

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
        makeReady();
        if (_contents == null || _contents.isEmpty()) return false;

        // TODO this method (and probably several others) needs optimization.
        IdentityHashMap<IonValue, IonValue> keepers =
            new IdentityHashMap<IonValue, IonValue>();
        for (Object o : c)
        {
            IonValue v = (IonValue) o;
            if (this == v.getContainer()) keepers.put(v, v);
        }

        boolean changed = false;
        for (int i = _contents.size() - 1; i >= 0; i--)
        {
            IonValue v = _contents.get(i);
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


    public IonValue[] toArray()
    {
        makeReady();
        if (_contents == null || _contents.isEmpty()) return EMPTY_VALUE_ARRAY;

        IonValue[] array = new IonValue[_contents.size()];
        _contents.toArray(array);
        return array;
    }

    public <T> T[] toArray(T[] a)
    {
        makeReady();
        if (_contents == null) return a;
        return _contents.toArray(a);
    }


    //=========================================================================

    @Override
    protected int computeLowNibble(int valuelen)
        throws IOException
    {
        assert _hasNativeValue;

        if (_contents == null) { return IonConstants.lnIsNullSequence; }

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
        assert _hasNativeValue == true || _isPositionLoaded == false;
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
