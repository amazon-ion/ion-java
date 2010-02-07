// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonValueImpl;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 *
 */
public class IonStructLite
    extends IonContainerLite
    implements IonStruct
{
    private static final int HASH_SIGNATURE =
        IonType.STRUCT.toString().hashCode();

    // TODO: add support for _isOrdered: private boolean _isOrdered = false;
    private static final boolean _isOrdered = false;


    /**
     * Constructs a binary-backed struct value.
     */
    public IonStructLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }

    /**
     * creates a copy of this IonStructImpl.  Most of the work
     * is actually done by IonContainerImpl.copyFrom() and
     * IonValueImpl.copyFrom().
     */
    @Override
    public IonStructLite clone()
    {
        IonStructLite clone = new IonStructLite(_context.getSystemLite(), false);

        try {
            clone.copyFrom(this);
            // copy over the field map proper
            // FIXME - I think this should be cleaned up w.r.t. copyFrom as this is slightly redundant
            //         due to transitioningToLarge size in copyFrom.
            if (clone._field_map != null) {
                clone._field_map = new HashMap<String, Integer>(_field_map);
            }
            clone._field_map_duplicate_count = _field_map_duplicate_count;
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
            IonValueLite v = get_child_lite(ii);
            String name = v.getFieldName();
            _field_map.put(name, ii);
        }
        return;
    }
    private int find_duplicate(String fieldName)
    {
        int size = get_child_count();
        for (int ii=0; ii<size; ii++) {
            IonValueLite field = get_child_lite(ii);
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
            clone = getSystem().newNullStruct();
        }
        else
        {
            clone = getSystem().newEmptyStruct();
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


    @Override
    public IonType getType()
    {
        return IonType.STRUCT;
    }

    public IonValue get(IonSymbol fieldName)
    {
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

        int size;
        if (_children != null) {

            // Walk backwards to minimize array movement
            int lowestRemovedIndex = -1;
            for (int i = get_child_count() - 1; i >= 0; i--)
            {
                IonValueLite child = get_child_lite(i);
                if (fieldName.equals(child.getFieldName()))
                {
                    child.detachFromContainer();
                    this.remove_child(i);
                    lowestRemovedIndex = i;
                }
            }
            if (lowestRemovedIndex >= 0)
            {
                patch_elements_helper(lowestRemovedIndex);
            }
            size = this.get_child_count();
        }
        else {
            size = 0;
        }

        if (value != null)
        {
            IonValueLite concrete = (IonValueLite) value;
            add(size, concrete);
            // updateElementIds - not needed since we're adding at the end
            // patch_elements_helper(lowest_bad_idx)

            // This is true because we've validated that its not contained.
            // assert value.getFieldName() == null;
            concrete.setFieldName(fieldName);
            if (_field_map != null) {
                add_field(fieldName, concrete._elementid());
            }
        }
    }

    public ValueFactory put(final String fieldName)
    {
        return new CurriedValueFactoryLite(_context.getSystemLite())
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
    
    @Override
    public boolean add(IonValue child)
        throws NullPointerException, IllegalArgumentException,
        ContainedValueException
    {
        String field_name = child.getFieldName();
        add(field_name, child);
        return true;
    }

    public void add(String fieldName, IonValue value)
    {
        // TODO maintain _isOrdered

        validateNewChild(value);
        validateFieldName(fieldName);

        IonValueLite concrete = (IonValueLite) value;
        int size = get_child_count();

        add(size, concrete);
        concrete.setFieldName(fieldName);

        if (_field_map != null) {
            add_field(fieldName, concrete._elementid());
        }
    }


    public ValueFactory add(final String fieldName)
    {
        return new CurriedValueFactoryLite(_context.getSystemLite())
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

        IonValue field = get(fieldName);
        if (field != null) {
            super.remove(field);
            if (_field_map != null) {
                assert(field instanceof IonValueImpl);
                remove_field(fieldName, ((IonValueLite)field)._elementid());
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
        if (fieldName.length() == 0)
        {
            throw new IllegalArgumentException("fieldName must not be empty");
        }
    }


    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }


}
