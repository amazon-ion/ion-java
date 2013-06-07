// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.impl._Private_Utils.EMPTY_STRING_ARRAY;
import static com.amazon.ion.impl._Private_Utils.newSymbolToken;
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
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl._Private_IonValue;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.util.Printer;
import java.io.IOException;
import java.io.PrintWriter;

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
    implements _Private_IonValue
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

    /*
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
     * When this is not a field,
     *   _fieldId = UNKNOWN_SYMBOL_ID  and  _fieldName = null
     * Otherwise at least one must be defined.
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
     * the constructor, which is called from the child constructors
     * simply sets the context and may or may not set the null
     * bit in the flags member.
     */
    IonValueLite(IonContext context, boolean isNull)
    {
        assert context != null;
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
    final void copyValueContentFrom(IonValueLite original)
    {
         assert _context instanceof IonSystemLite; // Baby I was born this way

        // values we copy
        this._flags        = original._flags;
        this._annotations  = original.getTypeAnnotationSymbols();
        if (original._context instanceof TopLevelContext) {
            // if the original value had context, we need to
            // copy that too, so we attach our copy to its
            // system owner through a fresh concrete context
            TopLevelContext.wrap(getSystem(),
                                 original.getAssignedSymbolTable(),
                                 this);
        }

        // and now values we don't copy
        assert _fieldName == null && _fieldId == UNKNOWN_SYMBOL_ID;

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

    private int findSymbol(String text)
    {
        SymbolTable symbolTable = getSymbolTable();
        if (symbolTable != null)
        {
            SymbolToken tok = symbolTable.find(_fieldName);
            if (tok != null) return tok.getSid();
        }
        return UNKNOWN_SYMBOL_ID;
    }

    public final int getFieldId()
    {
        if (_fieldId != UNKNOWN_SYMBOL_ID || _fieldName == null)
        {
            return _fieldId;
        }

        int sid = findSymbol(_fieldName);
        return sid;
    }


    public final SymbolToken getFieldNameSymbol()
    {
        int sid = _fieldId;
        String text;
        if (_fieldName != null) {
            text = _fieldName;
            if (sid == UNKNOWN_SYMBOL_ID) {
                sid = findSymbol(_fieldName);
            }
        }
        else if (sid > 0) {
            text = getSymbolTable().findKnownSymbol(sid);
            // TODO memoize interned text?
        }
        else {
            // not a field
            return null;
        }

        return _Private_Utils.newSymbolToken(text, sid);
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
                // TODO assertion contradicts the spec for getSymbolTable()
                assert(symbols != null); // we should get a system symbol table if nothing else
            }

            if (_fieldName != null) {
                symbols = resolve_symbol(symbols, _fieldName);
            }

            if (_annotations != null) {
                for (int ii=0; ii<_annotations.length; ii++) {
                    SymbolToken sym = _annotations[ii];
                    if (sym == null) {
                        break;
                    }
                    String text = sym.getText();
                    if (text != null) {
                        symbols = resolve_symbol(symbols, text);

                        // TODO we've interned the symbol but forgotten the sid
                        // We also haven't made sure we're using the same
                        // String instance as the symtab.
                    }
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

    /** Attempts to intern the given symbol */
    final SymbolTable resolve_symbol(SymbolTable symbols, String name)
    {
        assert(name != null && symbols != null);

        int sid = symbols.findSymbol(name);

        if (sid == UNKNOWN_SYMBOL_ID) {
            if (!symbols.isLocalTable()) {
                symbols = this._context.ensureLocalSymbolTable(this);
            }
            sid = symbols.addSymbol(name);
        }
        return symbols;
    }

    final void setFieldName(String name)
    {
        assert(this.getContainer() == null);
        assert _fieldId == UNKNOWN_SYMBOL_ID && _fieldName == null;
        _fieldName = name;
    }

    /** Both parts of the symbol are trusted! */
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

        throw new UnknownSymbolException(_fieldId);
    }

    public final int getFieldNameId()
    {
        return getFieldId();
    }

    public SymbolTable getSymbolTable()
    {
        assert ! (this instanceof IonDatagram);

        IonValueLite top = topLevelValue();
        SymbolTable symbols = top._context.getContextSymbolTable();
        if (symbols != null) {
            return symbols;
        }

        IonContainer container = top.getContainer();
        if (container != null) {
            // A symtab wasn't found above since the datagram doesn't force
            // all children to have symtabs directly assigned.
            symbols = ((IonDatagramLite)container).getChildsSymbolTable(top);
        }
        else {
            // TODO ION-258 bad assumption about system symtab context
            symbols = _context.getSystem().getSystemSymbolTable();
        }
        return symbols;
    }

    public SymbolTable getAssignedSymbolTable()
    {
        assert ! (this instanceof IonDatagram);

        SymbolTable symbols = _context.getContextSymbolTable();
        return symbols;
    }

    public SymbolTable getUpdatableSymbolTable()
    {
        SymbolTable symbols = getSymbolTable();
        if (symbols == null || ! symbols.isLocalTable()) {
            IonSystem system = getSystem();
            symbols = system.getSystemSymbolTable();
            symbols = _Private_Utils.newLocalSymtab(system, symbols);
        }
        return symbols;
    }

    public int resolveSymbol(String name)
    {
        SymbolTable symbols = getSymbolTable();
        if (symbols == null) {
            return UNKNOWN_SYMBOL_ID;
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
        if (sid != UNKNOWN_SYMBOL_ID) {
            return sid;
        }
        checkForLock();

        SymbolTable symbols = getUpdatableSymbolTable();
        sid = symbols.addSymbol(name);
        return sid;
    }

    public IonSystemLite getSystem()
    {
        return _context.getSystem();
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

        return _Private_Utils.toStrings(_annotations, count);
    }

    public final /* synchronized ?? */ SymbolToken[] getTypeAnnotationSymbols()
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
            return SymbolToken.EMPTY_ARRAY;
        }

        // if there are we allocate a user array and
        // copy the references into it. Note that our
        // count above lets us use arraycopy
        SymbolToken[] users_copy = new SymbolToken[count];
        // TODO should try to fill in sids
        System.arraycopy(_annotations, 0, users_copy, 0, count);
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
            _Private_Utils.ensureNonEmptySymbols(annotations);
            _annotations = annotations.clone();
        }
    }

    public final String[] getTypeAnnotations()
    {
        return getTypeAnnotationStrings();
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
        IonContainer parent = _context.getContextContainer();
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
        _context.setSymbolTableOfChild(symbols, this);
    }

    /**
     * This method is used to re-set the context of an
     * IonValue.  This may occur when the value is into
     * or out of a container.  It may occur when the
     * symbol table state of a container changes.
     *
     * @param context must not be null.
     */
    final void setContext(IonContext context)
    {
        assert context != null;
        checkForLock();
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

        // we make the system the parent of this member
        // when it's reattached to a container this will
        // be reset

        // this has the effect of erasing the symbol table
        // or reverting it back to the system symbol table
        // in the values all the symbol value should be
        // represented by their string values so this should
        // not be an issue.
        _context = this.getSystem();

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

