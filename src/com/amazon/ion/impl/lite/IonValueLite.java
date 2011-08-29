// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.impl.IonImplUtils.EMPTY_STRING_ARRAY;
import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;
import static com.amazon.ion.util.Equivalence.ionEquals;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.NullValueException;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.impl.IonValuePrivate;
import com.amazon.ion.impl.UnifiedSymbolTable;
import com.amazon.ion.util.Printer;
import java.io.IOException;

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
public abstract class IonValueLite
    implements IonValuePrivate
{
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
     * KEEP ALL MEMBER VALUES HERE
     *
     * This impl is intended to have a very light
     * memory footprint.  So tracking member values
     * is especially important.
     *
     * So KEEP THE MEMBER DECLARATIONS HERE
     *    AND TOGETHER
     *
     * Thank you.
     *
     */
    private   int              _flags;
    protected IonContext       _context;
    private   String           _fieldName;

    /**
     * The annotation sequence. This array is overallocated and may have
     * nulls at the end.
     */
    private   String[]         _annotations;

    // current size 32 bit: 3*4 + 4 +  8 = 24 (32 bytes allocated)
    //              64 bit: 3*8 + 4 + 16 = 52 (56 bytes allocated)

    /**
     * the constructor, which is called from the child constructors
     * simply sets the context and may or may not set the null
     * bit in the flags member.
     */
    protected IonValueLite(IonContext context, boolean isNull)
    {
        _context = context;
        if (isNull) {
            set_flag(IS_NULL_VALUE);
        }
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

        int old_len = (_annotations == null) ? 0 : _annotations.length;
        if (old_len > 0) {
            for (int ii=0; ii<old_len; ii++) {
                if (_annotations[ii] == null) {
                    _annotations[ii] = annotation;
                    return;
                }
            }
        }

        int new_len = (old_len == 0) ? 1 : old_len * 2;
        String temp[] = new String[new_len];
        if (old_len > 0) {
            System.arraycopy(_annotations, 0, temp, 0, old_len);
        }
        _annotations = temp;
        _annotations[old_len] = annotation;
        return;

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
     * The base classes in IonValue(Impl) should not be called for
     * cloning directly. The user should be calling clone on the
     * Actually (leaf) instances class (such as IonIntImpl).  The
     * shared clone work is handled in copyFrom(src) in any base
     * classes that need to support this - including IonValueImpl,
     * IonContainerImpl, IonTextImpl and IonLobImpl.
     */
    @Override
    public abstract IonValue clone();

    /**
     * copyValueContentFrom is used to make a duplicate
     * of the value properties during a value clone.
     * This includes the flags, annotations, and context,
     * but NOT the field name.
     * <p>
     * This instance will always be unlocked after calling this method.
     *
     * @param original value
     */
    protected final void copyValueContentFrom(IonValueLite original)
    {
        // values we copy
        this._flags        = original._flags;
        this._annotations  = original.getTypeAnnotationStrings();
        if (original._context instanceof IonConcreteContext) {
            // if the original value had context, we need to
            // copy that too, so we attach our copy to its
            // system owner through a fresh concrete context
            IonConcreteContext.attachWithConcreteContext(
                                   this.getSystemLite()
                                 , this
                                 , original.getAssignedSymbolTable()
            );
        }
        else {
            // otherwise we attach the copy to its system directly
            this._context = this.getSystemLite();
        }

        // and now values we don't copy
        this._fieldName = null;

        // IonValue.clone() is specified to return a modifiable copy, but the
        // flag assignment above copies all the flags, including locked.
        this._isLocked(false);
    }


    /**
     * Since {@link #equals(Object)} is overridden, each concrete class must provide
     * an implementation of {@link Object#hashCode()}
     * @return hash code for instance consistent with equals().
     */
    @Override
    public /* abstract */ int hashCode()
    {
        throw new UnsupportedOperationException("this type "+this.getClass().getSimpleName()+" should not be instanciated, there is not IonType associated with it");
    }

    public void deepMaterialize()
    {
        return;
    }

    public IonContainer getContainer()
    {
        return _context.getParentThroughContext();
    }

    public IonValuePrivate getRoot()
    {
        IonValuePrivate parent = _context.getParentThroughContext();
        if (parent == null || parent instanceof IonDatagram) {
            return this;
        }
        return parent.getRoot();
    }


    public final int getElementId()
    {
        return this._elementid();
    }

    public final int getFieldId()
    {
        SymbolTable symbolTable = getSymbolTable();
        if (_fieldName == null || symbolTable == null || _isLocked()) {
            return UNKNOWN_SYMBOL_ID;
        }
        int sid = symbolTable.findSymbol(_fieldName);
        if (sid < 1) {
            checkForLock();
            symbolTable = _context.getLocalSymbolTable(this);
            sid = symbolTable.addSymbol(_fieldName);
        }
        return sid;
    }

    public SymbolTable populateSymbolValues(SymbolTable symbols)
    {
        if (_isLocked()) {
            // we can't, and don't need to, update symbol id's
            // for a locked value - there are none - so do nothing here
        }
        else {
            // this is redundant now:  checkForLock();
            if (symbols == null) {
                symbols = getSymbolTable();
                assert(symbols != null); // we should get a system symbol table if nothing else
            }

            if (_fieldName != null) {
                symbols = resolve_symbol(symbols, _fieldName);
            }

            if (_annotations != null) {
                for (int ii=0; ii<_annotations.length; ii++) {
                    String name = _annotations[ii];
                    if (name == null) {
                        break;
                    }
                    symbols = resolve_symbol(symbols, name);
                }
            }
        }
        return symbols;
    }

    void clearSymbolIDValues()
    {
        IonContext context = getContext();
        if (context != null) {
            context.clearLocalSymbolTable();
        }
        return;
    }

    protected final SymbolTable resolve_symbol(SymbolTable symbols, String name)
    {
        assert(name != null && symbols != null);

        int sid = symbols.findSymbol(name);

        if (sid == UnifiedSymbolTable.UNKNOWN_SYMBOL_ID) {
            if (!symbols.isLocalTable()) {
                symbols = this._context.getLocalSymbolTable(this);
            }
            sid = symbols.addSymbol(name);
        }
        return symbols;
    }

    protected final void setFieldName(String name)
    {
        assert(this.getContainer() == null);
        _fieldName = name;
    }

    public final String getFieldName()
    {
        return _fieldName;
    }

    public final int getFieldNameId()
    {
        return getFieldId();
    }

    public SymbolTable getSymbolTable()
    {
        SymbolTable symbols;
        IonValueLite root = (IonValueLite)getRoot(); // this saves us two trips up the chain

        if (root != null) {
            symbols = root.getAssignedSymbolTable();
            if (symbols != null) {
                return symbols;
            }
        }

        IonContainer container = root.getContainer();
        if (container instanceof IonDatagramLite) {
            symbols = ((IonDatagramLite)container).getChildsSymbolTable(root);
        }
        else {
            symbols = _context.getSystemLite().getSystemSymbolTable();
        }
        return symbols;
    }

    public SymbolTable getAssignedSymbolTable()
    {
        IonValuePrivate root = this.getRoot();

        if (root == null) {
            return null;
        }
        assert(root instanceof IonValueLite);

        SymbolTable symbols = ((IonValueLite)root)._context.getContextSymbolTable();
        return symbols;
    }

    public SymbolTable getUpdatableSymbolTable()
    {
        SymbolTable symbols = getSymbolTable();
        if (UnifiedSymbolTable.isLocalTable(symbols)) {
            return symbols;
        }
        if (symbols == null) {
            IonSystem system = getSystem();
            symbols = system.getSystemSymbolTable();
            symbols = makeNewLocalSymbolTable(system, symbols);
        }
        return symbols;
    }

    public int resolveSymbol(String name)
    {
        SymbolTable symbols = getSymbolTable();
        if (symbols == null) {
            return UnifiedSymbolTable.UNKNOWN_SID;
        }
        int sid = symbols.findSymbol(name);
        return sid;
    }

    public String resolveSymbol(int sid)
    {
        SymbolTable symbols = getSymbolTable();
        if (symbols == null) {
            return null;
        }
        String name = symbols.findKnownSymbol(sid);
        return name;
    }

    public int addSymbol(String name)
    {
        int sid = resolveSymbol(name);
        if (sid != UnifiedSymbolTable.UNKNOWN_SID) {
            return sid;
        }
        checkForLock();

        SymbolTable symbols = getUpdatableSymbolTable();
        sid = symbols.addSymbol(name);
        return sid;
    }

    public IonSystem getSystem()
    {
        return _context.getSystemLite();
    }
    public IonSystemLite getSystemLite()
    {
        return _context.getSystemLite();
    }

    public IonType getType()
    {
        throw new UnsupportedOperationException("this type "+this.getClass().getSimpleName()+" should not be instanciated, there is not IonType associated with it");
    }

    public final /* synchronized ?? */ String[] getTypeAnnotationStrings()
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

        // it there are we allocate a user array and
        // copy the references into it. Note that our
        // count above lets us use arraycopy
        String [] users_copy = new String[count];
        System.arraycopy(_annotations, 0, users_copy, 0, count);

        return users_copy;
    }

    public final String[] getTypeAnnotations()
    {
        return getTypeAnnotationStrings();
    }

    public void setTypeAnnotations(String... annotations)
    {
        checkForLock();

        if (annotations == null || annotations.length == 0)
        {
            // Normalize all empty lists to the same instance.
            _annotations = EMPTY_STRING_ARRAY;
        }
        else
        {
            IonImplUtils.ensureNonEmptySymbols(annotations);
            _annotations = annotations.clone();
        }
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
                String a = _annotations[ii];
                if (a == null) {
                    break;
                }
                if (annotation.equals(a)) {
                    return ii;
                }
            }
        }
        return -1;
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
    public boolean equals(final Object other) {
        // TODO we can make this more efficient since we have impl details

        boolean same = false;
        if (other instanceof IonValue)
        {
            same = ionEquals(this, (IonValue) other);
        }
        return same;
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
        // nope, we just want to clear them ... populateSymbolValues(null);
        clearSymbolIDValues();
        _isLocked(true);
    }

    /**
     * Verifies that this value is not read-only.
     *
     * @throws ReadOnlyValueException
     *   if this value {@link #isReadOnly()}.
     */
    protected final void checkForLock()
        throws ReadOnlyValueException
    {
        if (_isLocked()) {
            throwReadOnlyException();
        }
    }
    private final void throwReadOnlyException()
        throws ReadOnlyValueException
    {
        throw new ReadOnlyValueException();
    }


    public boolean removeFromContainer()
    {
        checkForLock();

        boolean removed = false;
        IonContainer parent = _context.getParentThroughContext();
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
                String a = _annotations[ii+1];
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
    public String toString() {
        Printer p = new Printer();
        StringBuilder builder = new StringBuilder();
        try {
            p.print(this, builder);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return builder.toString();
    }

    public void setSymbolTable(SymbolTable symbols)
    {
        if (_context == null) {
            throw new IllegalStateException("a symbol table cannot be set on a value unless it has a context");
        }
        _context.setSymbolTableOfChild(symbols, this);
    }

    /**
     * This method is used to re-set the context of an
     * IonValue.  This may occur when the value is into
     * or out of a container.  It may occur when the
     * symbol table state of a container changes.
     * @param context
     */
    protected void setContext(IonContext context)
    {
        checkForLock();
        _context = context;
    }
    /**
     * used to query the current context.
     *
     * @return IonContext currently associated with this value
     */
    protected IonContext getContext()
    {
        return _context;
    }

    /**
     * Ensures that this value is not an Ion null.  Used as a helper for
     * methods that have that precondition.
     * @throws NullValueException if <code>this.isNullValue()</code>
     */
    protected final void validateThisNotNull()
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
    protected final void detachFromContainer() // throws IOException
    {
        checkForLock();

        // we make the system the parent of this member
        // when it's reattached to a container this will
        // be reset

        // this has the effect of erasing the symbol table
        // or reverting it back to the system symbol table
        // in the values all the symbol value should be
        // represented by their string values so this should
        // not be an issue.
        _context = this.getSystemLite();

        _fieldName = null;
        _elementid(0);
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

