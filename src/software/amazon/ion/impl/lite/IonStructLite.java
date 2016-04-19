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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import software.amazon.ion.ContainedValueException;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.ValueFactory;
import software.amazon.ion.ValueVisitor;
import software.amazon.ion.impl.PrivateCurriedValueFactory;
import software.amazon.ion.util.Equivalence;
import java.util.Set;

final class IonStructLite
    extends IonContainerLite
    implements IonStruct
{
    private static final int HASH_SIGNATURE =
        IonType.STRUCT.toString().hashCode();

    // TODO amznlabs/ion-java#41: add support for _isOrdered

    IonStructLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    private IonStructLite(IonStructLite existing, IonContext context)
    {
        super(existing, context, true);
        // field map can be shallow cloned due to it dealing with String and Integer
        // values - both of which are immutable constructs and so safe to retain as references
        this._field_map = null == _field_map
                                ? null
                                : new HashMap<String, Integer>(existing._field_map);
        this._field_map_duplicate_count = existing._field_map_duplicate_count;
    }

    private Map<String, Integer> _field_map;


    public int                      _field_map_duplicate_count;

    @Override
    IonStructLite clone(IonContext parentContext)
    {
        return new IonStructLite(this, parentContext);
    }

    @Override
    public IonStructLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    protected void transitionToLargeSize(int size)
    {
        if (_field_map != null) return;

        build_field_map();
        return;
    }
    protected void build_field_map()
    {
        int size = (_children == null) ? 0 : _children.length;

        _field_map = new HashMap<String, Integer>(size);
        _field_map_duplicate_count = 0;

        int count = get_child_count();
        for (int ii=0; ii<count; ii++) {
            IonValueLite v = get_child(ii);
            SymbolToken fieldNameSymbol = v.getFieldNameSymbol();
            String name = fieldNameSymbol.getText();
            if (_field_map.get(name) != null) {
                _field_map_duplicate_count++;
            }
            _field_map.put(name, ii); // this causes the map to have the largest index value stored
        }
        return;
    }
    private void add_field(String fieldName, int newFieldIdx)
    {
        Integer idx = _field_map.get(fieldName);
        if (idx != null) {
            _field_map_duplicate_count++;
            if (idx.intValue() > newFieldIdx) {
                newFieldIdx = idx.intValue();
            }
        }
        _field_map.put(fieldName, newFieldIdx);
    }
    private void remove_field(String fieldName, int lowest_idx, int copies)
    {
        if (_field_map == null) {
            return;
        }

        Integer field_idx = _field_map.get(fieldName);
        assert(field_idx != null);
        _field_map.remove(fieldName);
        _field_map_duplicate_count -= (copies - 1);
    }

    private void remove_field_from_field_map(String fieldName, int idx)
    {
        Integer field_idx = _field_map.get(fieldName);
        assert(field_idx != null);

        if (field_idx.intValue() != idx) {
            // if the map has a different index, this must
            // be a duplicate, and this copy isn't in the map
            assert(_field_map_duplicate_count > 0);
            _field_map_duplicate_count--;
        }
        else if (_field_map_duplicate_count > 0) {
            // if we have any duplicates we have to check
            // every time since we don't track which field
            // is duplicated - so any dup can be expensive
            int ii = find_last_duplicate(fieldName, idx);

            if (ii == -1) {
                // this is the last copy of this key
                _field_map.remove(fieldName);
            }
            else {
                // replaces this fields (the one being
                // removed) array idx in the map with
                // the preceding duplicates index
                _field_map.put(fieldName, ii);
                _field_map_duplicate_count--;
            }
        }
        else {
            // since there are not dup's we can just update
            // the map by removing this fieldname
            _field_map.remove(fieldName);
        }
    }

    private void patch_map_elements_helper(int removed_idx)
    {
        if (_field_map == null) {
            return;
        }

        if (removed_idx >= get_child_count()) {
            // if this was the at the end of the list
            // there's nothing to change
            return;
        }

        for (int ii=removed_idx; ii<get_child_count(); ii++) {
            IonValueLite value = get_child(ii);
            String  field_name = value.getFieldName();
            Integer map_idx = _field_map.get(field_name);
            if (map_idx.intValue() != ii) {
                // if this is a field that to the right of
                // the removed (in process of removing) value
                // we need to patch the index value
                _field_map.put(field_name, ii);
            }
        }
    }

    @Override
    public void dump(PrintWriter out)
    {
        super.dump(out);

        if (_field_map == null) {
            return;
        }

        out.println("   dups: "+_field_map_duplicate_count);
        Iterator<Entry<String, Integer>> it = _field_map.entrySet().iterator();
        out.print("   map: [");
        boolean first = true;
        while (it.hasNext()) {
            Entry<String, Integer> e = it.next();
            if (!first) {
                out.print(",");
            }
            out.print(e.getKey()+":"+e.getValue());
            first = false;
        }
        out.println("]");
    }

    @Override
    public String validate()
    {
        if (_field_map == null) {
            return null;
        }
        String error = "";
        Iterator<Entry<String, Integer>> it = _field_map.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Integer> e = it.next();
            int idx = e.getValue().intValue();
            IonValueLite v = (idx >= 0 && idx < get_child_count()) ? get_child(idx) : null;
            if (v == null || idx != v._elementid() || (e.getKey().equals(v.getFieldName()) == false)) {
                error += "map entry ["+e+"] doesn't match list value ["+v+"]\n";
            }
        }

        return (error == "") ? null : error;
    }

    private int find_last_duplicate(String fieldName, int existing_idx)
    {
        for (int ii=existing_idx; ii>0; ) {
            ii--;
            IonValueLite field = get_child(ii);
            if (fieldName.equals(field.getFieldName())) {
                return ii;
            }
        }
        assert(there_is_only_one(fieldName, existing_idx));
        return -1;
    }
    private boolean there_is_only_one(String fieldName, int existing_idx)
    {
        int count = 0;
        for (int ii=0; ii<get_child_count(); ii++) {
            IonValueLite v = get_child(ii);
            if (v.getFieldName().equals(fieldName)) {
                count++;
            }
        }
        if (count == 1 || count == 0) {
            return true;
        }
        return false;
    }
//
//    updateFieldName is unnecessary since field names are immutable
//    (except when the value is unattached to any struct)
//
//    protected void updateFieldName(String oldname, String name, IonValue field)
//    {
//        assert(name != null && name.equals(field.getFieldName()));
//
//        if (oldname == null) return;
//        if (_field_map == null) return;
//
//        Integer idx = _field_map.get(oldname);
//        if (idx == null) return;
//
//        IonValue oldfield = get_child(idx);
//
//        // yes, we want object identity in this test
//        if (oldfield == field) {
//            remove_field(oldname, idx);
//            add_field(name, idx);
//        }
//    }

    /**
     * Implements {@link Object#hashCode()} consistent with equals.
     * This is insensitive to order of fields.
     * <p>
     * This method must follow the contract of {@link Object#equals(Object)},
     * which is located at {@link Equivalence#ionEquals(IonValue, IonValue)}.
     *
     * @return  An int, consistent with the contracts for
     *          {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    @Override
    int hashCode(SymbolTableProvider symbolTableProvider)
    {
        final int nameHashSalt  = 16777619; // prime to salt name of each Field
        final int valueHashSalt = 8191;     // prime to salt value of each Field
        final int sidHashSalt   = 127;      // prime to salt sid of fieldname
        final int textHashSalt  = 31;       // prime to salt text of fieldname

        int result = HASH_SIGNATURE;

        if (!isNullValue())  {
            for (IonValue v : this)  {
                IonValueLite vlite = (IonValueLite) v;
                // If fieldname's text is unknown, use its sid instead
                SymbolToken token = vlite.getFieldNameSymbol(symbolTableProvider);
                String text = token.getText();

                int nameHashCode = text == null
                    ? token.getSid()  * sidHashSalt
                    : text.hashCode() * textHashSalt;

                // mixing to account for small text and sid deltas
                nameHashCode ^= (nameHashCode << 17) ^ (nameHashCode >> 15);

                int fieldHashCode = HASH_SIGNATURE;
                fieldHashCode = valueHashSalt * fieldHashCode + vlite.hashCode(symbolTableProvider);
                fieldHashCode = nameHashSalt  * fieldHashCode + nameHashCode;

                // another mix step for each Field of the struct
                fieldHashCode ^= (fieldHashCode << 19) ^ (fieldHashCode >> 13);

                // Additive hash is used to ensure insensitivity to order of
                // fields, and will not lose data on value hash codes
                result += fieldHashCode;
            }
        }

        return hashTypeAnnotations(result, symbolTableProvider);
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
            Set<String> fields =
                new HashSet<String>(Arrays.asList(fieldNames));
            if (keep && fields.contains(null))
            {
                throw new NullPointerException("Can't retain an unknown field name");
            }

            clone = getSystem().newEmptyStruct();
            for (IonValue value : this)
            {
                SymbolToken fieldNameSymbol = value.getFieldNameSymbol();
                String fieldName = fieldNameSymbol.getText();
                if (fields.contains(fieldName) == keep)
                {
                    // This ensures that we don't copy an unknown field name.
                    fieldName = value.getFieldName();
                    clone.add(fieldName, value.clone());
                }
            }
        }

        clone.setTypeAnnotationSymbols(getTypeAnnotationSymbols());

        return clone;
    }


    @Override
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
        int field_idx = find_field_helper(fieldName);
        IonValue field;

        if (field_idx < 0) {
            field = null;
        }
        else {
            field = get_child(field_idx);
        }

        return field;
    }
    private int find_field_helper(String fieldName)
    {
        validateFieldName(fieldName);

        if (isNullValue()) {
            // nothing to see here, move along
        }
        else if (_field_map != null) {
            Integer idx = _field_map.get(fieldName);
            if (idx != null) {
                return idx.intValue();
            }
        }
        else {
            int ii, size = get_child_count();
            for (ii=0; ii<size; ii++) {
                IonValue field = get_child(ii);
                if (fieldName.equals(field.getFieldName())) {
                    return ii;
                }
            }
        }
        return -1;
    }

    @Override
    public void clear()
    {
        super.clear();
        _field_map = null;
        _field_map_duplicate_count = 0;
    }

    @Override
    public boolean add(IonValue child)
        throws NullPointerException, IllegalArgumentException,
        ContainedValueException
    {
        // TODO validate in struct.setFieldName
        String text = child.getFieldNameSymbol().getText();
        if (text != null)
        {
            validateFieldName(text);
        }

        IonValueLite concrete = (IonValueLite) child;
        _add(text, concrete);

        return true;
    }


    public ValueFactory add(final String fieldName)
    {
        return new PrivateCurriedValueFactory(_context.getSystem())
        {
            @Override
            protected void handle(IonValue newValue)
            {
                add(fieldName, newValue);
            }
        };
    }


    /**
     * Validates the child and checks locks.
     *
     * @param fieldName may be null
     * @param child must be validated and have field name or id set
     */
    private void _add(String fieldName, IonValueLite child)
    {
        int size = get_child_count();

        // add this to the Container child collection
        add(size, child);

        // if we have a hash map we need to update it now
        if (_field_map != null) {
            add_field(fieldName, child._elementid());
        }
    }

    public void add(String fieldName, IonValue value)
    {
        // Validate everything before altering the child
        checkForLock();
        validateNewChild(value);
        validateFieldName(fieldName);

        IonValueLite concrete = (IonValueLite) value;

        _add(fieldName, concrete);
        concrete.setFieldName(fieldName);
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

        if (fieldName.getSid() < 1)
        {
            throw new IllegalArgumentException("fieldName has no text or ID");
        }

        // Validate everything before altering the child
        checkForLock();
        validateNewChild(child);

        IonValueLite concrete = (IonValueLite) child;
        concrete.setFieldNameSymbol(fieldName);
        _add(text, concrete);
    }


    public ValueFactory put(final String fieldName)
    {
        return new PrivateCurriedValueFactory(_context.getSystem())
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

    /**
     * put is "make this value the one and only value
     * associated with this fieldName".  The side effect
     * is that if there were multiple fields with this
     * name when put is complete there will only be the
     * one value in the collection.
     */
    public void put(String fieldName, IonValue value)
    {
        checkForLock();

        validateFieldName(fieldName);
        if (value != null) validateNewChild(value);

        int lowestRemovedIndex = get_child_count();
        boolean any_removed = false;

        // first we remove the any existing fields
        // associated with fieldName (which may be none)
        if (_field_map != null && _field_map_duplicate_count == 0)
        {
            // we have a map and no duplicates so the index
            // (aka map) is all we need to find the only
            // value associated with fieldName, if there is one
            Integer idx = _field_map.get(fieldName);
            if (idx != null) {
                lowestRemovedIndex = idx.intValue();
                remove_field_from_field_map(fieldName, lowestRemovedIndex);
                remove_child(lowestRemovedIndex);
                any_removed = true;
            }
        }
        else {
            // either we don't have a map (index) or there
            // are duplicates in both cases we have to
            // scan the child list directly.
            // Walk backwards to minimize array movement
            // as we remove fields as we encounter them.
            int copies_removed = 0;
            for (int ii = get_child_count(); ii > 0; )
            {
                ii--;
                IonValueLite child = get_child(ii);
                if (fieldName.equals(child.getFieldNameSymbol().getText()))
                {
                    // done by remove_child: child.detachFromContainer();
                    remove_child(ii);
                    lowestRemovedIndex = ii;
                    copies_removed++;
                    any_removed = true;
                }
            }
            if (any_removed) {
                remove_field(fieldName, lowestRemovedIndex, copies_removed);
            }
        }
        if (any_removed) {
            patch_map_elements_helper(lowestRemovedIndex);
            patch_elements_helper(lowestRemovedIndex);
        }

        // once we've removed any existing copy we now add,
        // this (delete + add == put) turns out be be the
        // right choice since:
        //   1 - because of possible duplicates we can't
        //       guarantee the idx is stable
        //   2 - we have to maintain the hash and that
        //       really means we end up with the delete
        //       anyway
        // strictly speaking this approach, while simpler,
        // is more expensive when we don't have a has and
        // the value already exists, and it's not at the
        // end of the field list anyway.
        if (value != null) {
            add(fieldName, value);
        }
    }

    @Override
    public ListIterator<IonValue> listIterator(int index) {
        return new SequenceContentIterator(index, isReadOnly()) {
            @Override
            public void remove() {
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

                if (_field_map != null) {
                    remove_field_from_field_map(concrete.getFieldName(), idx);
                }
                super.remove();

                if (_field_map != null) {
                    patch_map_elements_helper(idx);
                }
            }
        };
    }

    public IonValue remove(String fieldName)
    {
        checkForLock();

        IonValue field = get(fieldName);
        if (field == null) {
            return null;
        }

        int idx = ((IonValueLite)field)._elementid();

        // update the hash map first we don't want
        // the child list changed until we've done
        // this since the map update expects the
        // index value of the remove field to be
        // correct and unchanged.
        if (_field_map != null) {
            remove_field_from_field_map(fieldName, idx);
        }

        super.remove(field);

        if (_field_map != null) {
            patch_map_elements_helper(idx);
        }

        return field;
    }

    @Override
    public boolean remove(IonValue element)
    {
        if (element == null) {
            throw new NullPointerException();
        }

        checkForLock();

        if (element.getContainer() != this) {
            return false;
        }

        IonValueLite concrete = (IonValueLite) element;
        int idx = concrete._elementid();

        // update the hash map first we don't want
        // the child list changed until we've done
        // this since the map update expects the
        // index value of the remove field to be
        // correct and unchanged.
        if (_field_map != null) {
            remove_field_from_field_map(concrete.getFieldName(), idx);
        }

        super.remove(concrete);

        if (_field_map != null) {
            patch_map_elements_helper(idx);
        }

        return true;
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

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        if (isNullValue())
        {
            writer.writeNull(IonType.STRUCT);
        }
        else
        {
            writer.stepIn(IonType.STRUCT);
            writeChildren(writer, this, symbolTableProvider);
            writer.stepOut();
        }
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }
}
