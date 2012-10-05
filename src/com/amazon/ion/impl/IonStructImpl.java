// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_IonConstants.lnIsNullStruct;
import static com.amazon.ion.impl._Private_IonConstants.lnIsOrderedStruct;
import static com.amazon.ion.impl._Private_IonConstants.makeTypeDescriptor;
import static com.amazon.ion.impl._Private_IonConstants.tidStruct;

import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonBinary.Reader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Implements the Ion <code>struct</code> type.
 */
final class IonStructImpl
    extends IonContainerImpl
    implements IonStruct
{

    private static final int NULL_STRUCT_TYPEDESC =
        makeTypeDescriptor(tidStruct, lnIsNullStruct);

    static final int ORDERED_STRUCT_TYPEDESC =
        makeTypeDescriptor(tidStruct, lnIsOrderedStruct);

    private static final int HASH_SIGNATURE =
        IonType.STRUCT.toString().hashCode();

    // TODO: add support for _isOrdered: private boolean _isOrdered = false;
    private static final boolean _isOrdered = false;


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
        assert pos_getType() == _Private_IonConstants.tidStruct;
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

    private HashMap<String, Integer> _field_map;
    private int                      _field_map_duplicate_count;
    @Override
    protected void transitionToLargeSize(int size)
    {
        if (_field_map != null) return;
        _field_map = new HashMap<String, Integer>(size);
        int count = get_child_count();
        for (int ii=0; ii<count; ii++) {
            IonValueImpl v = get_child(ii);
            String name = v.getFieldName();
            _field_map.put(name, ii);
        }
        return;
    }
    private int find_duplicate(String fieldName)
    {
        int size = get_child_count();
        for (int ii=0; ii<size; ii++) {
            IonValueImpl field = get_child(ii);
            if (fieldName.equals(field.getFieldName())) {
                return ii;
            }
        }
        return -1;
    }
    private void add_field(String fieldName, int newFieldIdx)
    {
        Integer idx = _field_map.get(fieldName);
        if (idx == null) {
            // TODO: should we put the latest field over the top of the
            //       previous duplicate?
            _field_map.put(fieldName, newFieldIdx);
        }
        else {
            _field_map_duplicate_count++;
        }

    }
    private void remove_field(String fieldName, int idx)
    {
        _field_map.remove(fieldName);
        if (_field_map_duplicate_count > 0) {
            int ii = find_duplicate(fieldName);
            if (ii >= 0) {
                _field_map.put(fieldName, ii);
                _field_map_duplicate_count--;
            }
        }
    }
    protected void updateFieldName(String oldname, String name, IonValue field)
    {
        assert(name != null && name.equals(field.getFieldName()));
        if (oldname == null) return;
        if (_field_map == null) return;
        Integer idx = _field_map.get(oldname);
        if (idx == null) return;
        IonValue oldfield = get_child(idx);
        // yes, we want object identity in this test
        if (oldfield == field) {
            remove_field(oldname, idx);
            add_field(name, idx);
        }
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
                SymbolToken fieldNameSymbol = value.getFieldNameSymbol();
                String fieldName = fieldNameSymbol.getText();
                if (fields.contains(fieldName) == keep)
                {
                    clone.add(fieldNameSymbol, value.clone());
                }
            }
        }

        clone.setTypeAnnotationSymbols(getTypeAnnotationSymbols());

        return clone;
    }


    public IonType getType()
    {
        return IonType.STRUCT;
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

        IonValue field = null;
        if (_field_map != null) {
            Integer idx = _field_map.get(fieldName);
            if (idx != null) {
                field = get_child(idx);
            }
        }
        else {
            int ii, size = get_child_count();
            for (ii=0; ii<size; ii++) {
                field = get_child(ii);
                if (fieldName.equals(field.getFieldName())) {
                    break;
                }
            }
            if (ii>=size) {
                field = null;
            }
        }
        return field;
    }

    public void put(String fieldName, IonValue value)
    {
        // TODO maintain _isOrdered

        checkForLock();

        validateFieldName(fieldName);
        if (value != null) validateNewChild(value);

        makeReady();

        int size;
        if (_children != null) {
            try {
                // Walk backwards to minimize array movement
                int lowestRemovedIndex = -1;
                for (int i = get_child_count() - 1; i >= 0; i--)
                {
                    IonValue child = get_child(i);
                    if (fieldName.equals(child.getFieldNameSymbol().getText()))
                    {
                        ((IonValueImpl)child).detachFromContainer();
                        this.remove_child(i);
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
            size = this.get_child_count();
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
        return new _Private_CurriedValueFactory(_system)
        {
            @Override
            protected void handle(IonValue newValue)
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

        if (_field_map != null) {
            add_field(fieldName, concrete._elementid);
        }
    }

    public void add(SymbolToken fieldName, IonValue child)
    {
        String text = fieldName.getText();
        if (text != null)
        {
            // Ignoring the sid is safe, but perhaps not the most efficient.
            add(text, child);
            return;
        }

        if (fieldName.getSid() < 0)
        {
            throw new IllegalArgumentException("fieldName has no text or ID");
        }

//      validateNewChild(value);          // This is done by add() below.

        IonValueImpl concrete = (IonValueImpl) child;
        add(concrete);

        concrete.setFieldNameSymbol(fieldName);
    }

    public ValueFactory add(final String fieldName)
    {
        return new _Private_CurriedValueFactory(_system)
        {
            @Override
            protected void handle(IonValue newValue)
            {
                add(fieldName, newValue);
            }
        };
    }

    public IonValue remove(String fieldName)
    {
        checkForLock();

        IonValue field = get(fieldName);
        if (field != null) {
            super.remove(field);
            if (_field_map != null) {
                assert(field instanceof IonValueImpl);
                remove_field(fieldName, ((IonValueImpl)field)._elementid);
            }
        }
        return field;
    }

    public boolean removeAll(String... fieldNames)
    {
        boolean removedAny = false;

        checkForLock();

        int size = get_child_count();
        for (int ii=size; ii>0; ) {
            ii--;
            IonValue field = get_child(ii);
            if (isListedField(field, fieldNames)) {
                field.removeFromContainer();
                removedAny = true;
            }
        }

        return removedAny;
    }

    public boolean retainAll(String... fieldNames)
    {
        checkForLock();

        boolean removedAny = false;
        int size = get_child_count();
        for (int ii=size; ii>0; ) {
            ii--;
            IonValue field = get_child(ii);
            if (! isListedField(field, fieldNames))
            {
                field.removeFromContainer();
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
        if (fieldName == null)
        {
            throw new NullPointerException("fieldName is null");
        }
        if (fieldName.length() == 0)
        {
            throw new IllegalArgumentException("fieldName is empty");
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
        int len = _Private_IonConstants.BB_TOKEN_LEN;

        if (isNullValue() || isEmpty())
        {
            assert contentLength == 0;
            return len;
        }

        if (_isOrdered || contentLength >= _Private_IonConstants.lnIsVarLen)
        {
            len += IonBinary.lenVarUInt(contentLength);
        }

        return len;
    }

    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue();

        if (_isNullValue())    { return _Private_IonConstants.lnIsNullSequence; }

        if (get_child_count() == 0) return _Private_IonConstants.lnIsEmptyContainer;

        if (_isOrdered) return _Private_IonConstants.lnIsOrderedStruct;

        int contentLength = this.getNativeValueLength();  // FIXME check nakedLength?
        if (contentLength > _Private_IonConstants.lnIsVarLen)
        {
            return _Private_IonConstants.lnIsVarLen;
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

        if (get_child_count() > 0)
        {
            // TODO Rewrite this to avoid computing length again
            int vlen = this.getNativeValueLength();

            if (_isOrdered || vlen >= _Private_IonConstants.lnIsVarLen)
            {
                writer.writeVarUIntValue(vlen, true);
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
            int sid = reader.readVarUIntAsInt();
            child = makeValueFromReader(sid, reader, buffer, symtab, this, _system);
            child._elementid = get_child_count();
            add_child(child._elementid, child);
            pos = child.pos_getOffsetofNextValue();
        }
        if (pos != end) {
            // TODO should we throw an IonException here?
            // this would be caused by improperly formed binary
            assert pos == end;
        }
    }
}
