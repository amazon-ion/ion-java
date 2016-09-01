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

import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static software.amazon.ion.impl.PrivateUtils.EMPTY_STRING_ARRAY;
import static software.amazon.ion.impl.PrivateUtils.newSymbolToken;
import static software.amazon.ion.util.Equivalence.ionEquals;

import java.io.IOException;
import java.io.PrintWriter;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonException;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.IonWriter;
import software.amazon.ion.NullValueException;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.UnknownSymbolException;
import software.amazon.ion.ValueVisitor;
import software.amazon.ion.impl.PrivateIonValue;
import software.amazon.ion.impl.PrivateIonWriter;
import software.amazon.ion.impl.PrivateUtils;
import software.amazon.ion.system.IonTextWriterBuilder;

/**
 *  Base class of the light weight implementation of
 *  Ion values.
 *
 *  This implementation is not backed by a buffer
 *  and is therefore fully materialized.  If you need
 *  only a few values from a large datagram this
 *  implementation may be more expensive than the
 *  original implementation.
 */
abstract class IonValueLite
    implements PrivateIonValue
{
    private static final int TYPE_ANNOTATION_HASH_SIGNATURE =
        "TYPE ANNOTATION".hashCode();

    private static final IonTextWriterBuilder TO_STRING_TEXT_WRITER_BUILDER =
        IonTextWriterBuilder.standard().withCharsetAscii().immutable();

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
    private   static final int ELEMENT_MASK       = 0xff;
    protected static final int ELEMENT_SHIFT      = 8; // low 8 bits is flag, upper 24 (or 48 is element id)

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
                            PrivateUtils.newSymbolToken(text, UNKNOWN_SYMBOL_ID);
                    } else {
                        // TODO - this is clearly wrong; however was the existing behavior as
                        // existing under #getAnnotationTypeSymbols();
                        this._annotations[i] = existing._annotations[i];
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
     * Since {@link #equals(Object)} is overridden, each concrete class must provide
     * an implementation of {@link Object#hashCode()}.
     * @return hash code for instance consistent with equals().
     */
    /*
     * internally ALL implementations will be delegated too with the SymbolTable
     * which is to prevent the SymbolTable being continually re-located in complex structures.
     *
     * This is near universally true - however does not apply for IonDatagramLite - hence it
     * explicitly overrides hashCode()
     */
    @Override
    public int hashCode() {
        // Supply a lazy symbol table provider, which will call getSymbolTable()
        // only once it's actually necessary.
        // This works for all child types with the exception of
        // IonDatagramLite which has a different, explicit behavior for hashCode()
        // (hence this method cannot be final).
        return hashCode(new LazySymbolTableProvider(this));
    }

    /*
     * Internal HashCode implementation which utilizes a SymbolTableProvider
     * to resolve the SymbolTable to use in encoding the sub-graph.
     */
    abstract int hashCode(SymbolTableProvider symbolTableProvider);

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

    public SymbolToken getFieldNameSymbol()
    {
        // TODO amznlabs/ion-java#27 We should memoize the results of symtab lookups.
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
        else {
            // not a struct field
            return null;
        }

        return PrivateUtils.newSymbolToken(text, sid);
    }

    /**
     * Sets this value's symbol table to null, and erases any SIDs here and
     * recursively.
     */
    void clearSymbolIDValues()
    {
        if (_fieldName != null)
        {
            _fieldId = UNKNOWN_SYMBOL_ID;
        }

        if (_annotations != null)
        {
            for (int i = 0; i < _annotations.length; i++)
            {
                SymbolToken annotation = _annotations[i];

                // _annotations may have nulls at the end.
                if (annotation == null) break;

                String text = annotation.getText();
                if (text != null && annotation.getSid() != UNKNOWN_SYMBOL_ID)
                {
                    _annotations[i] =
                        newSymbolToken(text, UNKNOWN_SYMBOL_ID);
                }
            }
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
        assert(this.getContainer() == null);
        assert _fieldId == UNKNOWN_SYMBOL_ID && _fieldName == null;
        _fieldName = name.getText();
        _fieldId   = name.getSid();
    }

    public final String getFieldName()
    {
        if (_fieldName != null) return _fieldName;
        if (_fieldId < 0) return null;

        // TODO amznlabs/ion-java#27 why no symtab lookup, like getFieldNameSymbol()?
        throw new UnknownSymbolException(_fieldId);
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
        return _context.getSystem().getSystemSymbolTable();
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
        throw new UnsupportedOperationException("this type "+this.getClass().getSimpleName()+" should not be instanciated, there is not IonType associated with it");
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
                // TODO amznlabs/ion-java#27 We should memoize the result of symtab lookups
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
            PrivateUtils.ensureNonEmptySymbols(annotations);
            _annotations = annotations.clone();
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

        return PrivateUtils.toStrings(_annotations, count);
    }

    public void setTypeAnnotations(String... annotations)
    {
        checkForLock();

        _annotations = PrivateUtils.newSymbolTokens(getSymbolTable(),
                                                       annotations);
    }

    public final boolean hasTypeAnnotation(String annotation)
    {
        if (annotation != null && annotation.length() > 0) {
            int pos = find_type_annotation(annotation);
            if (pos >= 0) {
                return true;
            }
        }
        return false;
    }
    private final int find_type_annotation(String annotation)
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

    protected int hashTypeAnnotations(final int original, SymbolTableProvider symbolTableProvider)
    {
        final SymbolToken[] tokens = getTypeAnnotationSymbols(symbolTableProvider);
        if (tokens.length == 0)
        {
            return original;
        }

        final int sidHashSalt   = 127;      // prime to salt sid of annotation
        final int textHashSalt  = 31;       // prime to salt text of annotation
        final int prime = 8191;
        int result = original ^ TYPE_ANNOTATION_HASH_SIGNATURE;

        result = prime * original + tokens.length;

        for (final SymbolToken token : tokens)
        {
            String text = token.getText();

            int tokenHashCode = text == null
                ? token.getSid()  * sidHashSalt
                : text.hashCode() * textHashSalt;

            // mixing to account for small text and sid deltas
            tokenHashCode ^= (tokenHashCode << 19) ^ (tokenHashCode >> 13);

            result = prime * result + tokenHashCode;

            // mixing at each step to make the hash code order-dependent
            result ^= (result << 25) ^ (result >> 7);
        }

        return result;
    }

    /**
     * Implements equality over values.
     * This is currently defined using the Equivalence class.
     *
     * @see software.amazon.ion.util.Equivalence
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

    void makeReadOnlyInternal()
    {
        clearSymbolIDValues();
        _isLocked(true);
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
            int pos = find_type_annotation(annotation);
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
        return toString(TO_STRING_TEXT_WRITER_BUILDER);
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

    final void writeChildren(IonWriter writer, Iterable<IonValue> container,
                             SymbolTableProvider symbolTableProvider)
    {
        boolean isDatagram = this instanceof IonDatagram;
        // unfortunately JDK5 does not allow for generic co-variant returns; i.e. specifying
        // IonContainerLite#iterator() return type as Iterator<IonValueLite> causes a compile-time
        // error under JDK5 as it doesn't understand this is an acceptable co-variant for
        // Iterator<IonValue> IonContainer#iterator(). This said we know the underlying data
        // structure is IonValueLite[] - so we can conduct the cast within the loop. When ion-java is
        // moved to allow JDK6+ compile time dependency we can remove these crufty casts.
        for (IonValue iv : container) {
            IonValueLite vlite = (IonValueLite) iv;
            if(isDatagram)
            {
                vlite.writeTo(writer);
            }
            else
            {
                vlite.writeTo(writer, symbolTableProvider);
            }
        }
    }

    final void writeTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
    {
        if (writer.isInStruct()
            && ! ((PrivateIonWriter) writer).isFieldNameSet())
        {
            SymbolToken tok = getFieldNameSymbol(symbolTableProvider);
            if (tok == null)
            {
                throw new IllegalStateException("Field name not set");
            }

            writer.setFieldNameSymbol(tok);
        }

        SymbolToken[] annotations = getTypeAnnotationSymbols(symbolTableProvider);
        writer.setTypeAnnotationSymbols(annotations);

        try
        {
            writeBodyTo(writer, symbolTableProvider);
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }

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

