// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonBinary.Reader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * Implements the Ion <code>struct</code> type.
 */
public final class IonStructImpl
    extends IonContainerImpl
    implements IonStruct
{

    private static final int NULL_STRUCT_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidStruct,
                                        IonConstants.lnIsNullStruct);

    static final int ORDERED_STRUCT_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidStruct,
                                        IonConstants.lnIsOrderedStruct);

    private static final int HASH_SIGNATURE =
        IonType.STRUCT.toString().hashCode();

    private boolean _isOrdered = false;


    /**
     * Constructs a <code>null.struct</code> value.
     */
    public IonStructImpl(IonSystemImpl system)
    {
        this(system, NULL_STRUCT_TYPEDESC);
        _hasNativeValue(true);
    }

    /**
     * Constructs a binary-backed struct value.
     */
    public IonStructImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert pos_getType() == IonConstants.tidStruct;
    }

    /**
     * creates a copy of this IonStructImpl.  Most of the work
     * is actually done by IonContainerImpl.copyFrom() and
     * IonValueImpl.copyFrom().
     */
    @Override
    public IonStructImpl clone()
    {
        IonStructImpl clone = new IonStructImpl(_system);

        try {
            clone.copyFrom(this);
        } catch (IOException e) {
            throw new IonException(e);
        }

        return clone;
    }

    /**
     * Implements {@link Object#hashCode()} consistent with equals.
     * This implementation uses a fixed constant XORs with the hash
     * codes of contents and field names.  This is insensitive to order, as it
     * should be.
     *
     * @return  An int, consistent with the contracts for
     *          {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    @Override
    public int hashCode() {
        int hash_code = HASH_SIGNATURE;
        if (!isNullValue())  {
            for (IonValue v : this)  {
                hash_code ^= v.hashCode();
                hash_code ^= v.getFieldName().hashCode();
            }
        }
        return hash_code;
    }

    public IonStruct cloneAndRemove(String... fieldNames)
    {
        return doClone(false, fieldNames);
    }

    public IonStruct cloneAndRetain(String... fieldNames)
    {
        return doClone(true, fieldNames);
    }

    private IonStruct doClone(boolean keep, String... fieldNames)
    {
        IonStruct clone;
        if (isNullValue())
        {
            clone = _system.newNullStruct();
        }
        else
        {
            clone = _system.newEmptyStruct();
            Set<String> fields =
                new HashSet<String>(Arrays.asList(fieldNames));
            for (IonValue value : this)
            {
                String fieldName = value.getFieldName();
                if (fields.contains(fieldName) == keep)
                {
                    clone.add(fieldName, value.clone());
                }
            }
        }

        // TODO add IonValue.setTypeAnnotations
        for (String annotation : getTypeAnnotations()) {
            clone.addTypeAnnotation(annotation);
        }

        return clone;
    }


    public IonType getType()
    {
        return IonType.STRUCT;
    }


    @Override
    public boolean isNullValue()
    {
        if (_hasNativeValue() || !_isPositionLoaded()) {
            return (_contents == null);
        }

        int ln = this.pos_getLowNibble();
        return (ln == IonConstants.lnIsNullStruct);
    }


    public IonValue get(IonSymbol fieldName)
    {
        makeReady();
        return get(fieldName.stringValue());
    }

    public boolean containsKey(Object fieldName)
    {
        String name = (String) fieldName;
        return (null != get(name));
    }

    public boolean containsValue(Object value)
    {
        IonValue v = (IonValue) value;
        return (v.getContainer() == this);
    }

    public IonValue get(String fieldName)
    {
        validateFieldName(fieldName);

        if (isNullValue()) return null;

        makeReady();

        for (IonValue field : this) {
            if (fieldName.equals(field.getFieldName())) {
                return field;
            }
        }
        return null;
    }

//    public IonValueImpl findField(String name, IonValueImpl prev)
//    {
//        makeReady();
//
//        int sid = this.getSymbolTable().findSymbol(name);
//        for (IonValue v : this) {
//            IonValueImpl vi = (IonValueImpl)v;
//            if (vi._fieldSid == sid) return vi;
//        }
//
//        return null;
//    }

    public void put(String fieldName, IonValue value)
    {
        // TODO maintain _isOrdered

        checkForLock();

        validateFieldName(fieldName);
        if (value != null) validateNewChild(value);

        makeReady();

        int size;
        if (_contents != null) {
            try {
                // Walk backwards to minimize array movement
                int lowestRemovedIndex = -1;
                for (int i = _contents.size() - 1; i >= 0; i--)
                {
                    IonValue child = _contents.get(i);
                    if (fieldName.equals(child.getFieldName()))
                    {
                        ((IonValueImpl)child).detachFromContainer();
                        _contents.remove(i);
                        lowestRemovedIndex = i;
                    }
                }
                if (lowestRemovedIndex >= 0)
                {
                    updateElementIds(lowestRemovedIndex);
                }
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            size = _contents.size();
        }
        else {
            size = 0;
        }

        if (value != null)
        {
            IonValueImpl concrete = (IonValueImpl) value;
            add(size, concrete, true);

            // This is true because we've validated that its not contained.
            assert value.getFieldName() == null;
            concrete.setFieldName(fieldName);
        }
    }

    public ValueFactory put(final String fieldName)
    {
        return new CurriedValueFactory(_system)
        {
            @Override
            void handle(IonValue newValue)
            {
                put(fieldName, newValue);
            }
        };
    }

    public void putAll(Map<? extends String, ? extends IonValue> m)
    {
        // TODO this is very inefficient
        for (Entry<? extends String, ? extends IonValue> entry : m.entrySet())
        {
            put(entry.getKey(), entry.getValue());
        }
    }


    public void add(String fieldName, IonValue value)
    {
        // TODO maintain _isOrdered

        checkForLock();

        validateFieldName(fieldName);
//      validateNewChild(value);          // This is done by add() below.

        IonValueImpl concrete = (IonValueImpl) value;
        add(concrete);

        // This should be true because we've validated that its not contained.
        assert value.getFieldName() == null;
        concrete.setFieldName(fieldName);
    }

    public ValueFactory add(final String fieldName)
    {
        return new CurriedValueFactory(_system)
        {
            @Override
            void handle(IonValue newValue)
            {
                add(fieldName, newValue);
            }
        };
    }

    public IonValue remove(String fieldName)
    {
        checkForLock();

        // TODO optimize
        for (Iterator<IonValue> i = iterator(); i.hasNext();)
        {
            IonValue field = i.next();
            if (fieldName.equals(field.getFieldName()))
            {
                i.remove();
                return field;
            }
        }
        return null;
    }

    public boolean removeAll(String... fieldNames)
    {
        boolean removedAny = false;

        checkForLock();

        for (Iterator<IonValue> i = iterator(); i.hasNext();)
        {
            IonValue field = i.next();
            if (isListedField(field, fieldNames))
            {
                i.remove();
                removedAny = true;
            }
        }
        return removedAny;
    }

    public boolean retainAll(String... fieldNames)
    {
        checkForLock();

        boolean removedAny = false;
        for (Iterator<IonValue> i = iterator(); i.hasNext();)
        {
            IonValue field = i.next();
            if (! isListedField(field, fieldNames))
            {
                i.remove();
                removedAny = true;
            }
        }
        return removedAny;
    }

    /**
     *
     * @param field must not be null.  It is not required to have a field name.
     * @param fields must not be null, and must not contain and nulls.
     * @return true if {@code field.getFieldName()} is in {@code fields}.
     */
    private static boolean isListedField(IonValue field, String[] fields)
    {
        String fieldName = field.getFieldName();
        for (String key : fields)
        {
            if (key.equals(fieldName)) return true;
        }
        return false;
    }


    /**
     * Ensures that a given field name is valid. Used as a helper for
     * methods that have that precondition.
     *
     * @throws IllegalArgumentException if <code>fieldName</code> is empty.
     * @throws NullPointerException if the <code>fieldName</code>
     * is <code>null</code>.
     */
    private static void validateFieldName(String fieldName)
    {
        if (fieldName.length() == 0)
        {
            throw new IllegalArgumentException("fieldName must not be empty");
        }
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }


    //=========================================================================
    // Helpers

    @Override
    public int getTypeDescriptorAndLengthOverhead(int contentLength)
    {
        int len = IonConstants.BB_TOKEN_LEN;

        if (isNullValue() || isEmpty())
        {
            assert contentLength == 0;
            return len;
        }

        if (_isOrdered || contentLength >= IonConstants.lnIsVarLen)
        {
            len += IonBinary.lenVarUInt7(contentLength);
        }

        return len;
    }

    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue();

        if (_contents == null) return IonConstants.lnIsNullStruct;

        if (_contents.isEmpty()) return IonConstants.lnIsEmptyContainer;

        if (_isOrdered) return IonConstants.lnIsOrderedStruct;

        int contentLength = this.getNativeValueLength();  // FIXME check nakedLength?
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

        writer.write(this.pos_getTypeDescriptorByte());

        if (_contents != null && _contents.size() > 0)
        {
            // TODO Rewrite this to avoid computing length again
            int vlen = this.getNativeValueLength();

            if (_isOrdered || vlen >= IonConstants.lnIsVarLen)
            {
                writer.writeVarUInt7Value(vlen, true);
            }

            cumulativePositionDelta =
                doWriteContainerContents(writer, cumulativePositionDelta);
        }

        return cumulativePositionDelta;
    }


    /**
     * Overrides IonValueImpl.container copy to add code to
     * read the field name symbol ids (fieldSid)
     * @throws IOException
     */
    @Override
    protected void doMaterializeValue(Reader reader) throws IOException
    {
        IonValueImpl            child;
        SymbolTable             symtab = this.getSymbolTable();
        IonBinary.BufferManager buffer = this._buffer;

        int pos = this.pos_getOffsetAtActualValue();
        int end = this.pos_getOffsetofNextValue();

        // TODO compute _isOrdered flag

        while (pos < end) {
            reader.setPosition(pos);
            int sid = reader.readVarUInt7IntValue();
            child = makeValueFromReader(sid, reader, buffer, symtab, this, _system);
            child._elementid = _contents.size();
            _contents.add(child);
            pos = child.pos_getOffsetofNextValue();
        }
        if (pos != end) {
            // TODO should we throw an IonException here?
            // this would be caused by improperly formed binary
            assert pos == end;
        }
    }
}
