/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.impl._Private_Utils.EMPTY_STRING_ARRAY;
import static com.amazon.ion.impl._Private_Utils.newSymbolToken;
import static com.amazon.ion.util.Equivalence.ionEquals;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl._Private_IonValue;
import com.amazon.ion.impl._Private_IonWriter;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.system.IonTextWriterBuilder;
import java.io.IOException;
import java.io.PrintWriter;

/**
 *  Base class of the light weight implementation of
 *  Ion values.
 */
abstract class IonValueLite
    implements _Private_IonValue
{
    private static final int TYPE_ANNOTATION_HASH_SIGNATURE =
        "TYPE ANNOTATION".hashCode();

    /**
     * this hold all the various boolean flags we have
     * in a single int.  Use set_flag(), clear_flag(), is_true()
     * and the associated int flag to check the various flags.
     * This is to avoid the overhead java seems to impose
     * for a boolean value - it should be a bit, but it seems
     * to be an int (4 bytes for 1 bit seems excessive).
     */
    protected static final int IS_LOCKED          = 0x01;
    protected static final int IS_SYSTEM_VALUE    = 0x02;
    protected static final int IS_NULL_VALUE      = 0x04;
    protected static final int IS_BOOL_TRUE       = 0x08;
    protected static final int IS_IVM             = 0x10;
    protected static final int IS_AUTO_CREATED    = 0x20;
    protected static final int IS_SYMBOL_PRESENT  = 0x40;
    /**
     * Symbol ID present refers to there being the <i>possibility</i> that the IonValueLite, or it's contained sub graph
     * <i>(e.g. if it is a {@link IonContainerLite} based sub type)</i> contains one or more defined
     * Symbol ID's (SID)'s. This flag is used to track lifecycle, such that operations that require SID's are purged
     * from the IonValueLite and it's contained sub-DOM can conduct a fast evaluation rather than having to do a full
     * contained graph traversal on each and every invocation.
     */
    protected static final int IS_SYMBOL_ID_PRESENT = 0x80;

    private   static final int ELEMENT_MASK       = 0xff;
    protected static final int ELEMENT_SHIFT      = 8; // low 8 bits is flag, upper 24 (or 48 is element id)

    // This value was chosen somewhat arbitrarily; it can/should be changed if it is found to be insufficient.
    private static final int CONTAINER_STACK_INITIAL_CAPACITY = 16;

    // 'withCharsetAscii' is only specified for consistency with the behavior of the previous implementation of
    // toString. Technically users are not allowed to rely on a canonical encoding, but in practice they often do.
    private static final IonTextWriterBuilder DEFAULT_TO_STRING_WRITER_BUILDER = IonTextWriterBuilder.standard().withCharsetAscii().immutable();

    /**
     * Used by subclasses to retrieve metadata set by
     * {@link #_setMetadata(int, int, int)}.
     * @param mask the location of the metadata to retrieve.
     * @param shift the number of bits to right-shift the metadata so that
     *              it starts at bit index 0.
     * @return the metadata from _flags at the given mask.
     */
    protected final int _getMetadata(int mask, int shift) {
        return (_flags & mask) >>> shift;
    }

    /**
     * May be used by subclasses to reuse _flag bits for purposes specific
     * to that subclass. It is important that only flag bits not currently
     * used by that subclass are chosen; otherwise important data may be
     * overwritten. NOTE: only the lower 8 bits may be used, because the
     * upper 24 are reserved for the element ID.
     * @param metadata the metadata to set.
     * @param mask the location at which to set the metadata. Must be within
     *             the lower 8 bits.
     * @param shift the number of bits to left-shift the metadata so that
     *              it starts at the index of the mask's LSB.
     */
    protected final void _setMetadata(int metadata, int mask, int shift) {
        assert(mask <= ELEMENT_MASK); // don't overwrite the element ID
        _flags &= ~mask;
        _flags |= ((metadata << shift) & mask);
    }

    protected final void _elementid(int elementid) {
        _flags &= ELEMENT_MASK;
        _flags |= (elementid << ELEMENT_SHIFT);
        assert(_elementid() == elementid);
    }
    protected final int _elementid() {
        int elementid =  _flags  >>> ELEMENT_SHIFT;
        return elementid;
    }

    private final boolean is_true(int flag_bit) {
        return ((_flags & flag_bit) != 0);
    }
    private final void set_flag(int flag_bit) {
        assert(flag_bit != 0);
        _flags |= flag_bit;
    }
    private final void clear_flag(int flag_bit) {
        assert(flag_bit != 0);
        _flags &= ~flag_bit;
    }

    protected final boolean _isLocked() { return is_true(IS_LOCKED); }
    protected final boolean _isLocked(boolean flag) {
        if (flag) {
            set_flag(IS_LOCKED);
        }
        else {
            clear_flag(IS_LOCKED);
        }
        return flag;
    }
    protected final boolean _isSystemValue() { return is_true(IS_SYSTEM_VALUE); }
    protected final boolean _isSystemValue(boolean flag) {
        if (flag) {
            set_flag(IS_SYSTEM_VALUE);
        }
        else {
            clear_flag(IS_SYSTEM_VALUE);
        }
        return flag;
    }
    protected final boolean _isNullValue() { return is_true(IS_NULL_VALUE); }
    protected final boolean _isNullValue(boolean flag) {
        if (flag) {
            set_flag(IS_NULL_VALUE);
        }
        else {
            clear_flag(IS_NULL_VALUE);
        }
        return flag;
    }
    protected final boolean _isBoolTrue() { return is_true(IS_BOOL_TRUE); }
    protected final boolean _isBoolTrue(boolean flag) {
        if (flag) {
            set_flag(IS_BOOL_TRUE);
        }
        else {
            clear_flag(IS_BOOL_TRUE);
        }
        return flag;
    }

    protected final boolean _isIVM() { return is_true(IS_IVM); }
    protected final boolean _isIVM(boolean flag) {
        if (flag) {
            set_flag(IS_IVM);
        }
        else {
            clear_flag(IS_IVM);
        }
        return flag;
    }

    protected final boolean _isAutoCreated() { return is_true(IS_AUTO_CREATED); }
    protected final boolean _isAutoCreated(boolean flag) {
        if (flag) {
            set_flag(IS_AUTO_CREATED);
        }
        else {
            clear_flag(IS_AUTO_CREATED);
        }
        return flag;
    }

    protected final boolean _isSymbolPresent() { return is_true(IS_SYMBOL_PRESENT); }
    protected final boolean _isSymbolPresent(boolean flag) {
        if (flag) {
            set_flag(IS_SYMBOL_PRESENT);
        }
        else {
            clear_flag(IS_SYMBOL_PRESENT);
        }
        return flag;
    }

    protected final boolean _isSymbolIdPresent() { return is_true(IS_SYMBOL_ID_PRESENT); }
    protected final boolean _isSymbolIdPresent(boolean flag) {
        if (flag) {
            set_flag(IS_SYMBOL_ID_PRESENT);
        }
        else {
            clear_flag(IS_SYMBOL_ID_PRESENT);
        }
        return flag;
    }

    /**
     * Lazy memoized symtab provider. Should be used when a call path
     * conditionally needs access to a value's symbol table. This provider
     * can be "passed down" through the path, cutting down on
     * potentially expensive IonValue#getSymbolTable calls.
     */
    static class LazySymbolTableProvider
        implements SymbolTableProvider
    {
        SymbolTable symtab = null;
        final IonValueLite value;

        LazySymbolTableProvider(IonValueLite value)
        {
            this.value = value;
        }

        public SymbolTable getSymbolTable()
        {
            if (symtab == null)
            {
                symtab = value.getSymbolTable();
            }
            return symtab;
        }

    }

    /*
     * KEEP ALL MEMBER FIELDS HERE!
     *
     * This impl is intended to have a very light memory footprint. So tracking
     * member fields is especially important.
     *
     * SO PLEASE KEEP THE MEMBER DECLARATIONS HERE AND TOGETHER!
     *
     * Thank you.
     *
     * If this instance is not a struct field, then
     *   _fieldId = UNKNOWN_SYMBOL_ID  and  _fieldName = null
     * Otherwise, at least one must be defined.
     */
    private   int              _flags;
    private   int              _fieldId = UNKNOWN_SYMBOL_ID;

    /** Not null. */
    protected IonContext       _context;
    private   String           _fieldName;

    /**
     * The annotation sequence. This array is overallocated and may have
     * nulls at the end denoting unused slots.
     */
    private   SymbolToken[] _annotations;

    // current size 32 bit: 3*4 + 4 +  8 = 24 (32 bytes allocated)
    //              64 bit: 3*8 + 4 + 16 = 52 (56 bytes allocated)

    /**
     * The constructor, which is called from the concrete subclasses'
     * constructors.
     *
     * @param context the context that this value is associated with
     * @param isNull if true, sets the null bit in the flags member field
     */
    IonValueLite(ContainerlessContext context, boolean isNull)
    {
        assert context != null;
        _context = context;
        if (isNull) {
            set_flag(IS_NULL_VALUE);
        }
    }

    /**
     * Copy Constructor *purely* for cloning - NOTE; this means that the clone is not <i>perfect</i>
     * as if the original entity was <b>read-only</b> the cloned value will now be <b>mutable</b>.
     *
     * @param existing the non-null existing IonValueLite entity to clone
     * @param context the non-null parent context to use for the cloned entity.
     */
    IonValueLite(IonValueLite existing, IonContext context) {
        // Symbols are *immutable* therefore a shallow copy is sufficient
        boolean hasSIDsRetained = false;
        if (null == existing._annotations) {
            this._annotations = null;
        } else {
            int size = existing._annotations.length;
            this._annotations = new SymbolToken[size];
            for (int i = 0; i < size; i++) {
                SymbolToken existingToken = existing._annotations[i];
                if (existingToken != null) {
                    String text = existingToken.getText();
                    if (text != null) {
                        this._annotations[i] =
                            _Private_Utils.newSymbolToken(text, UNKNOWN_SYMBOL_ID);
                    } else {
                        // TODO - amazon-ion/ion-java/issues/223 needs consistent handling, should attempt to resolve and if it cant; fail
                        this._annotations[i] = existing._annotations[i];
                        hasSIDsRetained |= this._annotations[i].getSid() > UNKNOWN_SYMBOL_ID;
                    }
                }
            }
        }
        // We don't copy the field name, that happens in IonStruct's clone
        this._flags       = existing._flags;
        this._context     = context;

        // as IonValue.clone() mandates that the returned value is mutable, regardless of the
        // existing 'read only' flag - we force the deep-copy back to being mutable
        clear_flag(IS_LOCKED);
        // whilst the clone *should* guarantee symbol context is purged, the annotation behavior existing above
        // under the TO DO for amazon-ion/ion-java/issues/223 does mean that SID context can be propogated through a clone, therefore
        // the encoding flag has to reflect this reality
        _isSymbolIdPresent(hasSIDsRetained);
    }

    public abstract void accept(ValueVisitor visitor) throws Exception;

    public /* synchronized */ void addTypeAnnotation(String annotation)
    {
        checkForLock();

        if (annotation == null || annotation.length() < 1) {
            throw new IllegalArgumentException("a user type annotation must be a non-empty string");
        }

        // we don't add duplicate annotations
        if (hasTypeAnnotation(annotation)) return;

        SymbolToken sym = newSymbolToken(annotation, UNKNOWN_SYMBOL_ID);

        int old_len = (_annotations == null) ? 0 : _annotations.length;
        if (old_len > 0) {
            for (int ii=0; ii<old_len; ii++) {
                if (_annotations[ii] == null) {
                    _annotations[ii] = sym;
                    return;
                }
            }
        }

        int new_len = (old_len == 0) ? 1 : old_len * 2;
        SymbolToken temp[] = new SymbolToken[new_len];
        if (old_len > 0) {
            System.arraycopy(_annotations, 0, temp, 0, old_len);
        }
        _annotations = temp;
        _annotations[old_len] = sym;
    }

    public final /* synchronized */ void clearTypeAnnotations()
    {
        checkForLock();

        int old_len = (_annotations == null) ? 0 : _annotations.length;
        if (old_len > 0) {
            for (int ii=0; ii<old_len; ii++) {
                if (_annotations[ii] == null) {
                    break;
                }
                _annotations[ii] = null;
            }
        }

        // although annotations have been removed from the node, which *may* mean the sub-graph from this Node is
        // now without encodings... the check is expensive for container types (need to check all immediate children)
        // and so we will opt to clear down encoding present in a lazy fashion (e.g. when something actually needs it)
    }

    /**
     * {@inheritDoc}
     * <p>
     * The user can only call this method on the concrete (not abstract)
     * subclasses of IonValueLite (e.g. IonIntLite). The explicit clone logic
     * is contained in {@link #clone(IonContext)} which should in turn be implemented by
     * using a copy-constructor.
     */
    @Override
    public abstract IonValue clone();

    abstract IonValueLite clone(IonContext parentContext);

    /**
     * @return the constant hash signature unique to the value's type.
     */
    abstract int hashSignature();

    private static final int sidHashSalt   = 127; // prime to salt symbol IDs
    private static final int textHashSalt  = 31; // prime to salt text
    private static final int nameHashSalt  = 16777619; // prime to salt field names
    private static final int valueHashSalt = 8191; // prime to salt values

    private static class HashHolder {
        int valueHash = 0;
        IonContainerLite parent = null;
        IonContainerLite.SequenceContentIterator iterator = null;

        private int hashStructField(int partial, IonValueLite value) {
            // If the field name's text is unknown, use its sid instead
            String text = value._fieldName;

            int nameHashCode = text == null
                ? value._fieldId * sidHashSalt
                : text.hashCode() * textHashSalt;

            // mixing to account for small text and sid deltas
            nameHashCode ^= (nameHashCode << 17) ^ (nameHashCode >> 15);

            int fieldHashCode = parent.hashSignature();
            fieldHashCode = valueHashSalt * fieldHashCode + partial;
            fieldHashCode = nameHashSalt  * fieldHashCode + nameHashCode;

            // another mix step for each Field of the struct
            fieldHashCode ^= (fieldHashCode << 19) ^ (fieldHashCode >> 13);
            return fieldHashCode;
        }

        void update(int partial, IonValueLite value) {
            if (parent == null) {
                valueHash = partial;
            } else {
                if (parent instanceof IonStructLite) {
                    // Additive hash is used to ensure insensitivity to order of
                    // fields, and will not lose data on value hash codes
                    valueHash += hashStructField(partial, value);
                } else {
                    valueHash = valueHashSalt * valueHash + partial;
                    // mixing at each step to make the hash code order-dependent
                    valueHash ^= (valueHash << 29) ^ (valueHash >> 3);
                }
            }
        }
    }

    /**
     * Since {@link #equals(Object)} is overridden, each concrete class representing a scalar value must provide
     * an implementation of {@link #scalarHashCode()}.
     * @return hash code for instance consistent with equals().
     */
    @Override
    public int hashCode() {
        // Branching early for scalars is a significant performance optimization because it avoids allocating an
        // unnecessary hash stack for these values. Scalars *should* be the most commonly hashed kind of value.
        if ((_flags & IS_NULL_VALUE) != 0) {
            return hashTypeAnnotations(hashSignature());
        } else if (this instanceof IonContainerLite) {
            return containerHashCode();
        }
        return scalarHashCode();
    }

    private int containerHashCode() {
        HashHolder[] hashStack = new HashHolder[CONTAINER_STACK_INITIAL_CAPACITY];
        int hashStackIndex = 0;
        hashStack[hashStackIndex] = new HashHolder();
        IonValueLite value = this;
        do {
            HashHolder hashHolder = hashStack[hashStackIndex];
            if ((value._flags & IS_NULL_VALUE) != 0) {
                // Null values are hashed using a constant signature unique to the type.
                hashHolder.update(value.hashTypeAnnotations(value.hashSignature()), value);
            } else if (!(value instanceof IonContainerLite)) {
                hashHolder.update(value.scalarHashCode(), value);
            } else {
                // Step into the container by pushing a HashHolder for the container onto the stack.
                if (++hashStackIndex >= hashStack.length) {
                    hashStack = growHashStack(hashStack);
                }
                hashHolder = hashStack[hashStackIndex];
                if (hashHolder == null) {
                    hashHolder = new HashHolder();
                    hashStack[hashStackIndex] = hashHolder;
                }
                hashHolder.parent = (IonContainerLite) value;
                hashHolder.iterator = hashHolder.parent.new SequenceContentIterator(0, true);
                hashHolder.valueHash = value.hashSignature();
            }
            do {
                if (hashHolder.parent == null) {
                    // Iteration has returned to the top level; return the top-level value's hash code.
                    return hashHolder.valueHash;
                }
                value = hashHolder.iterator.nextOrNull();
                if (value == null) {
                    // The end of the container has been reached. Pop from the stack and update the parent's hash.
                    hashHolder = hashStack[hashStackIndex--];
                    IonValueLite container = hashHolder.parent;
                    int containerHash = container.hashTypeAnnotations(hashHolder.valueHash);
                    hashHolder.parent = null;
                    hashHolder.iterator = null;
                    hashHolder = hashStack[hashStackIndex];
                    hashHolder.update(containerHash, container);
                }
            } while (value == null);
        } while (true);
    }

    private static HashHolder[] growHashStack(HashHolder[] hashStack) {
        HashHolder[] newHashStack = new HashHolder[hashStack.length * 2];
        System.arraycopy(hashStack, 0, newHashStack, 0, hashStack.length);
        return newHashStack;
    }

    /**
     * Internal HashCode implementation to be overridden by scalar subclasses. Must only be called on non-null values.
     */
    abstract int scalarHashCode();

    public IonContainerLite getContainer()
    {
        return _context.getContextContainer();
    }

    public IonValueLite topLevelValue()
    {
        assert ! (this instanceof IonDatagram);

        IonValueLite value = this;
        for (;;) {
            IonContainerLite c = value._context.getContextContainer();
            if (c == null || c instanceof IonDatagram) {
                break;
            }
            value = c;
        }
        return value;
    }


    public final int getElementId()
    {
        return this._elementid();
    }


    public final int getFieldId()
    {
        if (_fieldId != UNKNOWN_SYMBOL_ID || _fieldName == null)
        {
            return _fieldId;
        }

        SymbolToken tok = getSymbolTable().find(_fieldName);

        return (tok != null ? tok.getSid() : UNKNOWN_SYMBOL_ID);
    }


    public SymbolToken getFieldNameSymbol()
    {
        // TODO amazon-ion/ion-java/issues/27 We should memoize the results of symtab lookups.
        // BUT: that could cause thread-safety problems for read-only values.
        // I think makeReadOnly should populate the tokens fully
        // so that we only need to lookup from mutable instances.
        // However, the current invariants on these fields are nonexistant so
        // I do not trust that its safe to alter them here.

        return getFieldNameSymbol(new LazySymbolTableProvider(this));
    }

    public final SymbolToken getFieldNameSymbol(SymbolTableProvider symbolTableProvider)
    {
        int sid = _fieldId;
        String text = _fieldName;
        if (text != null)
        {
            if (sid == UNKNOWN_SYMBOL_ID)
            {
                SymbolToken tok = symbolTableProvider.getSymbolTable().find(text);
                if (tok != null)
                {
                    return tok;
                }
            }
        }
        else if (sid > 0) {
            text = symbolTableProvider.getSymbolTable().findKnownSymbol(sid);
        }
        else if(sid != 0){
            // not a struct field
            return null;
        }
        return _Private_Utils.newSymbolToken(text, sid);
    }

    public final SymbolToken getKnownFieldNameSymbol()
    {
        SymbolToken token = this.getFieldNameSymbol();
        if (token.getText() == null && token.getSid() != 0) {
            throw new UnknownSymbolException(_fieldId);
        }
        return token;
    }

    private static class ClearSymbolIDsHolder
    {
        boolean allSIDsClear = true;
        IonContainerLite parent = null;
        IonContainerLite.SequenceContentIterator iterator = null;
    }

    private static ClearSymbolIDsHolder[] growClearSymbolIDsHolderStack(
            ClearSymbolIDsHolder[] clearSymbolIDsHolderStack)
    {
        ClearSymbolIDsHolder[] newClearSymbolIDsHolderStack = new ClearSymbolIDsHolder[clearSymbolIDsHolderStack.length * 2];
        System.arraycopy(clearSymbolIDsHolderStack, 0, newClearSymbolIDsHolderStack, 0,
                clearSymbolIDsHolderStack.length);
        return newClearSymbolIDsHolderStack;
    }

    private boolean clearSymbolIDsIterative(boolean readOnlyMode) {
        ClearSymbolIDsHolder[] stack = new ClearSymbolIDsHolder[CONTAINER_STACK_INITIAL_CAPACITY];
        int stackIndex = 0;
        stack[stackIndex] = new ClearSymbolIDsHolder();
        IonValueLite value = this;
        do {
            ClearSymbolIDsHolder holder = stack[stackIndex];
            if (!(value instanceof IonContainerLite)) {
                holder.allSIDsClear &= value.scalarClearSymbolIDValues();
                if (readOnlyMode) {
                    value._isLocked(true);
                }
            } else if (value._isSymbolIdPresent() || readOnlyMode) {
                // The value is a container, and it is necessary to walk its children.
                // Step into the container by pushing a ClearSymbolIDsHolder for the container onto the stack.
                if (++stackIndex >= stack.length) {
                    stack = growClearSymbolIDsHolderStack(stack);
                }
                holder = stack[stackIndex];
                if (holder == null) {
                    holder = new ClearSymbolIDsHolder();
                    stack[stackIndex] = holder;
                }
                holder.parent = (IonContainerLite) value;
                holder.iterator = holder.parent.new SequenceContentIterator(0, true);
                holder.allSIDsClear = value.attemptClearSymbolIDValues();
            }
            do {
                if (holder.parent == null) {
                    // Iteration has returned to the top level. Return the top-level flag.
                    return holder.allSIDsClear;
                }
                value = holder.iterator.nextOrNull();
                if (value == null) {
                    boolean allChildSidsClear = holder.allSIDsClear;
                    if (allChildSidsClear) {
                        // clear the symbolID status flag
                        holder.parent._isSymbolIdPresent(false);
                    }
                    if (readOnlyMode) {
                        holder.parent._isLocked(true);
                    }
                    // The end of the container has been reached. Pop from the stack and update the parent's flag.
                    holder.parent = null;
                    holder.iterator = null;
                    holder = stack[--stackIndex];
                    holder.allSIDsClear &= allChildSidsClear;
                }
            } while (value == null);
        } while (true);
    }

    private boolean scalarClearSymbolIDValues()
    {
        // short circuit exit - no SID's present to remove - so can exit immediately
        if (!_isSymbolIdPresent())
        {
            return true;
        }

        boolean allSIDsRemoved = attemptClearSymbolIDValues();
        // if all the SID's have been successfully removed - the SID Present flag can be set to false
        if (allSIDsRemoved)
        {
            // clear the symbolID status flag
            _isSymbolIdPresent(false);
        }
        return allSIDsRemoved;
    }

    final boolean clearSymbolIDValues()
    {
        // short circuit exit - no SID's present to remove - so can exit immediately
        if (!_isSymbolIdPresent())
        {
            return true;
        }
        if (this instanceof IonContainerLite)
        {
            return clearSymbolIDsIterative(false);
        }
        return scalarClearSymbolIDValues();
    }

    /**
     * Sets this value's symbol table to null, and erases any SIDs here and
     * recursively.
     *
     * @return true if all SID's have been successfully removed, otherwise false
     */
    boolean attemptClearSymbolIDValues()
    {
        boolean sidsRemain = false;
        if (_fieldName != null)
        {
            _fieldId = UNKNOWN_SYMBOL_ID;
        } else if (_fieldId > UNKNOWN_SYMBOL_ID)
        {
            // retaining the field SID, as it couldn't be cleared due to loss of context
            // TODO - for SID handling consistency; this should attempt resolution first
            sidsRemain = true;
        }

        if (_annotations != null)
        {
            for (int i = 0; i < _annotations.length; i++)
            {
                SymbolToken annotation = _annotations[i];

                // _annotations may have nulls at the end.
                if (annotation == null) break;

                String text = annotation.getText();
                // TODO - for SID handling consistency; this should attempt resolution first, not just drop entry
                if (text != null && annotation.getSid() != UNKNOWN_SYMBOL_ID)
                {
                    _annotations[i] =
                        newSymbolToken(text, UNKNOWN_SYMBOL_ID);
                }
            }
        }

        return !sidsRemain;
    }

    protected void cascadeSIDPresentToContextRoot() {
        // start with self
        IonValueLite node = this;
        // iterate from leaf to context root setting encoding present until either context root is hit OR a node is
        // encountered that already has encodings present (and so no further propogation is required).
        while (null != node && !node._isSymbolIdPresent()) {
            node._isSymbolIdPresent(true);
            node = node.getContainer();
        }
    }

    final void setFieldName(String name)
    {
        assert getContainer() instanceof IonStructLite;
        // We can never change a field name once it's set.
        assert _fieldId == UNKNOWN_SYMBOL_ID && _fieldName == null;
        _fieldName = name;
    }

    /**
     * Sets the field name and ID based on a SymbolToken.
     * Both parts of the SymbolToken are trusted!
     *
     * @param name is not retained by this value, but both fields are copied.
     */
    final void setFieldNameSymbol(SymbolToken name)
    {
        assert _fieldId == UNKNOWN_SYMBOL_ID && _fieldName == null;
        _fieldName = name.getText();
        _fieldId   = name.getSid();

        // if a SID has been added by this operation to a previously SID-less node we have to mark upwards
        // towards the context root that a SID is present
        if (UNKNOWN_SYMBOL_ID != _fieldId && !_isSymbolIdPresent()) {
            cascadeSIDPresentToContextRoot();
        }
    }

    public final String getFieldName()
    {
        if (_fieldName != null) return _fieldName;
        if (_fieldId <= 0) return null;

        // TODO amazon-ion/ion-java/issues/27 why no symtab lookup, like getFieldNameSymbol()?
        throw new UnknownSymbolException(_fieldId);
    }

    public final int getFieldNameId()
    {
        return getFieldId();
    }

    /**
     * @return not null, <b>in conflict with the public documentation</b>.
     */
    public SymbolTable getSymbolTable()
    {
        assert ! (this instanceof IonDatagram);

        SymbolTable symbols = topLevelValue()._context.getContextSymbolTable();
        if (symbols != null) {
            return symbols;
        }
        return getSystem().getSystemSymbolTable();
    }

    public SymbolTable getAssignedSymbolTable()
    {
        assert ! (this instanceof IonDatagram);

        SymbolTable symbols = _context.getContextSymbolTable();
        return symbols;
    }

    public IonSystemLite getSystem()
    {
        return _context.getSystem();
    }

    public IonType getType()
    {
        throw new UnsupportedOperationException("this type "+this.getClass().getSimpleName()+" should not be instantiated, there is not IonType associated with it");
    }

    public SymbolToken[] getTypeAnnotationSymbols()
    {
        return getTypeAnnotationSymbols(new LazySymbolTableProvider(this));
    }

    public final SymbolToken[] getTypeAnnotationSymbols(SymbolTableProvider symbolTableProvider)
    {
        // first we have to count the number of non-null
        // elements there are in the annotations array
        int count = 0;
        if (_annotations != null) {
            for (int i = 0; i < _annotations.length; i++) {
                if (_annotations[i] != null) {
                    count++;
                }
            }
        }
        // if there aren't any, we're done
        if (count == 0) {
            return SymbolToken.EMPTY_ARRAY;
        }

        SymbolToken[] users_copy = new SymbolToken[count];
        for (int i = 0; i < count; i++)
        {
            SymbolToken token = _annotations[i];
            String text = token.getText();
            if (text != null && token.getSid() == UNKNOWN_SYMBOL_ID)
            {
                // TODO amazon-ion/ion-java/issues/27 We should memoize the result of symtab lookups
                // into _annotations.
                // See getFieldNameSymbol() for challenges doing so.

                SymbolToken interned = symbolTableProvider.getSymbolTable().find(text);
                if (interned != null)
                {
                    token = interned;
                }
            }

            users_copy[i] = token;
        }
        return users_copy;
    }

    public void setTypeAnnotationSymbols(SymbolToken... annotations)
    {
        checkForLock();

        if (annotations == null || annotations.length == 0)
        {
            // Normalize all empty lists to the same instance.
            _annotations = SymbolToken.EMPTY_ARRAY;
        }
        else
        {
            _annotations = annotations.clone();

            // new annotations could have SID's - so if this node is not currently marked as SID
            // present then the new annotations have to be interrogated to see if they contain SID's and if they
            // do - the SID Present flag must be cascaded.
            if (!_isSymbolIdPresent()) {
                for (SymbolToken token : _annotations) {
                    // upon finding first match of a symbol token containing a SID - cascade upwards and exit
                    if (null != token && UNKNOWN_SYMBOL_ID != token.getSid()) {
                        cascadeSIDPresentToContextRoot();
                        break;
                    }
                }
            }
        }
    }

    public final String[] getTypeAnnotations()
    {
        // first we have to count the number of non-null
        // elements there are in the annotations array
        int count = 0;
        if (_annotations != null) {
            for (int ii=0; ii<_annotations.length; ) {
                if (_annotations[ii] == null) {
                    break;
                }
                ii++;
                count = ii;
            }
        }
        // if there aren't any, we're done
        if (count == 0) {
            return EMPTY_STRING_ARRAY;
        }

        return _Private_Utils.toStrings(_annotations, count);
    }

    public void setTypeAnnotations(String... annotations)
    {
        checkForLock();

        _annotations = _Private_Utils.newSymbolTokens(getSymbolTable(),
                                                       annotations);
    }

    public final boolean hasTypeAnnotation(String annotation)
    {
        if (annotation != null && annotation.length() > 0) {
            int pos = findTypeAnnotation(annotation);
            if (pos >= 0) {
                return true;
            }
        }
        return false;
    }

    public final int findTypeAnnotation(String annotation)
    {
        assert(annotation != null && annotation.length() > 0);

        if (_annotations != null) {
            for (int ii=0; ii<_annotations.length; ii++) {
                SymbolToken a = _annotations[ii];
                if (a == null) {
                    break;
                }
                if (annotation.equals(a.getText())) {
                    return ii;
                }
            }
        }
        return -1;
    }

    protected int hashTypeAnnotations(final int original)
    {
        final SymbolToken[] tokens = _annotations == null ? SymbolToken.EMPTY_ARRAY : _annotations;
        if (tokens.length == 0)
        {
            return original;
        }

        int result = valueHashSalt * original + tokens.length;

        for (final SymbolToken token : tokens)
        {
            String text = token.getText();

            int tokenHashCode = text == null
                ? token.getSid()  * sidHashSalt
                : text.hashCode() * textHashSalt;

            // mixing to account for small text and sid deltas
            tokenHashCode ^= (tokenHashCode << 19) ^ (tokenHashCode >> 13);

            result = valueHashSalt * result + tokenHashCode;

            // mixing at each step to make the hash code order-dependent
            result ^= (result << 25) ^ (result >> 7);
        }

        return result;
    }

    /**
     * Implements equality over values.
     * This is currently defined using the Equivalence class.
     *
     * @see com.amazon.ion.util.Equivalence
     *
     * @param   other   The value to compare with.
     *
     * @return  A boolean, true if the other value is an Ion Value that is the same
     *          content and annotations.
     */
    @Override
    public final boolean equals(final Object other)
    {
        if (other == this) {
            // we shouldn't make 3 deep method calls for this common case
            return true;
        }
        if (other instanceof IonValue)
        {
            return ionEquals(this, (IonValue) other);
        }
        return false;
    }


    public final boolean isNullValue()
    {
        return _isNullValue();
    }

    public final boolean isReadOnly()
    {
        return _isLocked();
    }

    public void makeReadOnly()
    {
        if (!_isLocked()) {
            makeReadOnlyInternal();
        }
    }

    void makeReadOnlyInternal() {
        if (this instanceof IonContainerLite)
        {
            clearSymbolIDsIterative(true);
        } else {
            scalarClearSymbolIDValues();
            this._isLocked(true);
        }
    }

    /**
     * Verifies that this value is not read-only.
     *
     * @throws ReadOnlyValueException
     *   if this value {@link #isReadOnly()}.
     */
    final void checkForLock()
        throws ReadOnlyValueException
    {
        if (_isLocked()) {
            throw new ReadOnlyValueException();
        }
    }


    public boolean removeFromContainer()
    {
        checkForLock();

        boolean removed = false;
        IonContainerLite parent = _context.getContextContainer();
        if (parent != null) {
            removed = parent.remove(this);
        }
        return removed;
    }

    public void removeTypeAnnotation(String annotation)
    {
        checkForLock();

        if (annotation != null && annotation.length() > 0) {
            int pos = findTypeAnnotation(annotation);
            if (pos < 0) {
                return;
            }
            int ii;
            for (ii=pos; ii<_annotations.length - 1; ii++) {
                SymbolToken a = _annotations[ii+1];
                if (a == null) {
                    break;
                }
                _annotations[ii] = a;
            }
            if (ii<_annotations.length) {
                _annotations[ii] = null;
            }
        }
    }

    @Override
    public String toString()
    {
        return toString(DEFAULT_TO_STRING_WRITER_BUILDER);
    }

    public String toString(IonTextWriterBuilder writerBuilder)
    {
        StringBuilder buf = new StringBuilder(1024);
        try
        {
            IonWriter writer = writerBuilder.build(buf);
            writeTo(writer);
            writer.finish();
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
        return buf.toString();
    }

    public String toPrettyString()
    {
        return toString(IonTextWriterBuilder.pretty());
    }

    public void writeTo(IonWriter writer)
    {
        // we use a Lazy 1-time resolution of the SymbolTable in case there is no need to
        // pull the symbol table, including situations where no symbol table would logically
        // be attached
        writeTo(writer, new LazySymbolTableProvider(this));
    }

    private static void writeFieldNameAndAnnotations(IonWriter writer, IonValueLite value, SymbolTableProvider symbolTableProvider) {
        if (writer.isInStruct()
            && ! ((_Private_IonWriter) writer).isFieldNameSet())
        {
            if (value._fieldName != null) {
                writer.setFieldName(value._fieldName);
            } else {
                SymbolToken tok = value.getFieldNameSymbol(symbolTableProvider);
                if (tok == null)
                {
                    throw new IllegalStateException("Field name not set");
                }

                writer.setFieldNameSymbol(tok);
            }
        }

        if (value._annotations != null) {
            writer.setTypeAnnotationSymbols(value._annotations);
        }
    }

    /*  The following template may be used to iterate over the tree at the current value. Copying this structure
        and filling in the blanks or customizing as necessary performed better (and consumed about the same number of
        lines of code) than factoring it into a method and injecting logic into the blanks via lambdas as of 8/2023,
        when writeTo and hashCode were made iterative instead of recursive. See those methods for examples of this
        template being used.
        --------------------------------------------------
        IonContainerLite.SequenceContentIterator[] iteratorStack = new IonContainerLite.SequenceContentIterator[CONTAINER_STACK_INITIAL_CAPACITY];
        int iteratorStackIndex = -1;
        IonContainerLite.SequenceContentIterator currentIterator = null;
        IonValueLite value = this;
        do {
            // BLANK: insert logic that needs to be performed before each value, such as handling field names.
            if ((value._flags & IS_NULL_VALUE) != 0) {
                // BLANK: insert logic for handling nulls of any type.
            } else if (!(value instanceof IonContainerLite)) {
                // BLANK: insert logic for handling non-null scalar values.
            } else {
                if (++iteratorStackIndex >= iteratorStack.length) {
                    iteratorStack = growIteratorStack(iteratorStack);
                }
                currentIterator = ((IonContainerLite) value).new SequenceContentIterator(0, true);
                iteratorStack[iteratorStackIndex] = currentIterator;
                // BLANK: insert logic for handling the start of a container value.
            }
            do {
                if (currentIterator == null) {
                    // BLANK: insert logic for handling the end of iteration.
                    return;
                }
                value = currentIterator.nextOrNull();
                if (value == null) {
                    // BLANK: insert logic for handling the end of a container value.
                    iteratorStack[iteratorStackIndex] = null; // Allow this to be garbage collected
                    currentIterator = (iteratorStackIndex == 0) ? null : iteratorStack[--iteratorStackIndex];
                }
            } while (value == null);
        } while (true);
     */

    private void writeToIterative(IonWriter writer, SymbolTableProvider symbolTableProvider) throws IOException {
        IonContainerLite.SequenceContentIterator[] iteratorStack = new IonContainerLite.SequenceContentIterator[CONTAINER_STACK_INITIAL_CAPACITY];
        int iteratorStackIndex = -1;
        IonContainerLite.SequenceContentIterator currentIterator = null;
        IonValueLite value = this;
        do {
            writeFieldNameAndAnnotations(writer, value, symbolTableProvider);
            if ((value._flags & IS_NULL_VALUE) != 0) {
                writer.writeNull(value.getType());
            } else if (!(value instanceof IonContainerLite)) {
                value.writeBodyTo(writer, symbolTableProvider);
            } else {
                if (++iteratorStackIndex >= iteratorStack.length) {
                    iteratorStack = growIteratorStack(iteratorStack);
                }
                currentIterator = ((IonContainerLite) value).new SequenceContentIterator(0, true);
                iteratorStack[iteratorStackIndex] = currentIterator;
                writer.stepIn(value.getType());
            }
            do {
                if (currentIterator == null) {
                    return;
                }
                value = currentIterator.nextOrNull();
                if (value == null) {
                    writer.stepOut();
                    iteratorStack[iteratorStackIndex] = null; // Allow this to be garbage collected
                    currentIterator = (iteratorStackIndex == 0) ? null : iteratorStack[--iteratorStackIndex];
                }
            } while (value == null);
        } while (true);
    }
    private static IonContainerLite.SequenceContentIterator[] growIteratorStack(IonContainerLite.SequenceContentIterator[] iteratorStack) {
        IonContainerLite.SequenceContentIterator[] newIteratorStack = new IonContainerLite.SequenceContentIterator[iteratorStack.length * 2];
        System.arraycopy(iteratorStack, 0, newIteratorStack, 0, iteratorStack.length);
        return newIteratorStack;
    }

    final void writeTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
    {
        try {
            if ((_flags & IS_NULL_VALUE) != 0) {
                writeFieldNameAndAnnotations(writer, this, symbolTableProvider);
                writer.writeNull(getType());
            } else if (this instanceof IonContainerLite) {
                writeToIterative(writer, symbolTableProvider);
            } else {
                writeFieldNameAndAnnotations(writer, this, symbolTableProvider);
                writeBodyTo(writer, symbolTableProvider);
            }
        } catch (IOException e) {
            throw new IonException(e);
        }
    }

    /**
     * Writes a *non-null scalar* value to the given writer. It is incorrect to call this method on a container value.
     * @param writer an IonWriter.
     * @param symbolTableProvider a SymbolTableProvider.
     * @throws IOException if thrown when writing the value.
     */
    abstract void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException;


    public void setSymbolTable(SymbolTable symbols)
    {
        if (getContext() instanceof TopLevelContext){
            IonDatagramLite datagram = (IonDatagramLite) getContainer();
            datagram.setSymbolTableAtIndex(_elementid(), symbols);
        } else if (this.topLevelValue() == this) {
            setContext(ContainerlessContext.wrap(getContext().getSystem(), symbols));
        } else {
            throw new UnsupportedOperationException("can't set the symboltable of a child value");
        }
    }

    /**
     * This method is used to re-set the context of an
     * IonValue.  This may occur when the value is added into
     * or removed from a container.  It may occur when the
     * symbol table state of a container changes.
     *
     * @param context must not be null.
     */
    final void setContext(IonContext context)
    {
        assert context != null;
        checkForLock();

        //Clear all known sIDs.
        this.clearSymbolIDValues();
        _context = context;
    }

    /**
     * used to query the current context.
     *
     * @return this value's context. Not null.
     */
    IonContext getContext()
    {
        return _context;
    }

    /**
     * Ensures that this value is not an Ion null.  Used as a helper for
     * methods that have that precondition.
     * @throws NullValueException if <code>this.isNullValue()</code>
     */
    final void validateThisNotNull()
        throws NullValueException
    {
        if (_isNullValue())
        {
            throw new NullValueException();
        }
    }

    /**
     * Removes this value from its container, ensuring that all data stays
     * available.  Dirties this value and it's original container.
     */
    final void detachFromContainer() // throws IOException
    {
        checkForLock();

        clearSymbolIDValues();
        _context = ContainerlessContext.wrap(getSystem());

        _fieldName = null;
        _fieldId = UNKNOWN_SYMBOL_ID;
        _elementid(0);
    }

    public void dump(PrintWriter out)
    {
        out.println(this);
    }

    public String validate()
    {
        return null;
    }
}

// current size 32 bit: 5*4 + 2 + 1 +  8 = 31 bytes (32 allocated)
//              64 bit: 5*8 + 2 + 1 + 16 = 59 bytes (64 allocated)

/*

private byte             _flags;

private IonValueContextLite _context {
    private IonContainerLite _parent;
    private IonSystem        _system;
    private SymbolTable      _symbolTable;
}

/ * nope: the cost of cloning a value when
* a field name is added seems like over
* kill.
*
* The cost of virtual calls for all value
* access also seems like too high a price
* to pay for the 1 reference saving.
*
* So we'll leave fieldName as a local member
*
//    private IonValueContentLite {
//        union IonValueValueLite {
//            IonContainer
//            IonScalar
//            IonField {
//                String _fieldName;
//            }
//        }
//    }


/ * nope:  by the time you have the reference to the
 *        array list like object (IonValueAnnotations
 *        below) you have half the overhead.  The vast
 *        majority of values have no annotations.  Of
 *        those that have annotations the vast majority
 *        have only 1.  2 and more drop off very quickly.
 *
 *        As such just an array that realloc-ed and the
 *        annotations come and go should be fine.
 *        Certainly the realloc cost is high but the
 *        normal case is alloc for 1 and you're done.
 *
 *        adding support for ignoring null references
 *        means we can mostly calculate the count as
 *        needed, drop out of search loops soon, and
 *        just pay a scan on add annotation.
 *
 * private IonValueAnnotations {
 *   private String[]         _annotations;
 *   private short            _annotation_count;
 * }
 * /
private String[]         _annotations; // just realloc as needed

32 bit: 3*4 + 1

*/

/**
 * ContainingContext is an interface supported
 * by all containers + a ConcreteContext object.
 *
 * For IonDatagramLite and any "uncontained" value
 * the _context member points to a ConcreteContext
 * where:
 *      getContainer return null
 *      getSymbolTable returns the current symbol table
 *      getSystem returns the instance of IonSystemLite
 *        that created this datagram (and it's contained
 *        values)
 *
 * For IonContainerLite each container implements the
 * interface directly.  In addition their _context
 * member has either their parent container or a concrete
 * context.  The IonContainerLite implementations return:
 *
 *      getContainer returns this
 *      getSymbolTable delegates to the _context object
 *      getSystem delegates to the _context object
 *
 *
 *    interface IonContainingContextLite {
 *        IonContainer getParent();
 *        IonSystem    getSystem();
 *        SymbolTable  getSymbolTable();
 *    }
 *
 *   private IonContainerLite _parent;
 *   private IonSystem        _system;
 *   private SymbolTable      _symbolTable;
 *
 */
