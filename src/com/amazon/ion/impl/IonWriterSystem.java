// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.impl._Private_Utils.newLocalSymtab;
import static com.amazon.ion.impl._Private_Utils.newSymbolToken;
import static com.amazon.ion.impl._Private_Utils.newSymbolTokens;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.UnknownSymbolException;
import java.io.IOException;


/**
 *
 */
abstract class IonWriterSystem
    extends IonWriterBaseImpl
{
    /**
     * The system symtab used when resetting the stream.
     * Must not be null.
     */
    final SymbolTable _default_system_symbol_table;

    /**
     * Must be either local or system table, and never null.
     * May only be changed between top-level values.
     */
    private SymbolTable _symbol_table;

    /** really ion type is only used for int, string or null (unknown) */
    private IonType     _field_name_type;
    private String      _field_name;
    private int         _field_name_sid = UNKNOWN_SYMBOL_ID;

    private static final int DEFAULT_ANNOTATION_COUNT = 4;

    private int         _annotation_count;
    private SymbolToken[] _annotations =
        new SymbolToken[DEFAULT_ANNOTATION_COUNT];


    //========================================================================

    /**
     * @param defaultSystemSymbolTable must not be null.
     */
    IonWriterSystem(SymbolTable defaultSystemSymbolTable)
    {
        defaultSystemSymbolTable.getClass(); // Efficient null check
        _default_system_symbol_table = defaultSystemSymbolTable;
        _symbol_table = defaultSystemSymbolTable;
    }


    //========================================================================
    // Context management

    final SymbolTable getDefaultSystemSymtab()
    {
        return _default_system_symbol_table;
    }

    public final SymbolTable getSymbolTable()
    {
        return _symbol_table;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation simply validates that the argument is not a
     * shared symbol table, and assigns it to {@link #_symbol_table}.
     */
    @Override
    public final void setSymbolTable(SymbolTable symbols)
        throws IOException
    {
        if (symbols == null || _Private_Utils.symtabIsSharedNotSystem(symbols)) {
            throw new IllegalArgumentException("symbol table must be local or system to be set, or reset");
        }
        if (getDepth() > 0) {
            throw new IllegalStateException("the symbol table cannot be set, or reset, while a container is open");
        }
        _symbol_table = symbols;
    }


    /**
     * Sets {@link #_symbol_table}.
     * Subclasses should override to generate output.
     */
    void writeIonVersionMarker(SymbolTable systemSymtab)
        throws IOException
    {
        assert systemSymtab.isSystemTable();
        _symbol_table = systemSymtab;
    }

    @Override // TODO ION-271 make final after IMS is migrated
    public void writeIonVersionMarker()
        throws IOException
    {
        writeIonVersionMarker(_default_system_symbol_table);
    }


    void writeLocalSymtab(SymbolTable symtab)
        throws IOException
    {
        assert symtab.isLocalTable();
        _symbol_table = symtab;
    }


    /**
     * Builds a new local symbol table from the current contextual symtab
     * (a system symtab).
     * @return not null.
     */
    SymbolTable inject_local_symbol_table() throws IOException
    {
        assert _symbol_table.isSystemTable();
        // no catalog since it doesn't matter as this is a
        // pure local table, with no imports
        return newLocalSymtab(null /*system*/, _symbol_table);
    }

    @Override
    final String assumeKnownSymbol(int sid)
    {
        String text = _symbol_table.findKnownSymbol(sid);
        if (text == null)
        {
            throw new UnknownSymbolException(sid);
        }
        return text;
    }

    final int add_symbol(String name) throws IOException
    {
        int sid = _symbol_table.findSymbol(name);
        if (sid != UNKNOWN_SYMBOL_ID) return sid;

        if (_symbol_table.isSystemTable()) {
            _symbol_table = inject_local_symbol_table();
        }
        assert _symbol_table.isLocalTable();

        sid = _symbol_table.addSymbol(name);
        return sid;
    }


    /** Writes a symbol without checking for system ID. */
    abstract void writeSymbolAsIs(int symbolId) throws IOException;

    /** Writes a symbol without checking for system ID. */
    abstract void writeSymbolAsIs(String value) throws IOException;


    @Deprecated
    public final void writeSymbol(int symbolId) throws IOException
    {
        if (symbolId < 1) {
            throw new IllegalArgumentException("symbol IDs are greater than 0");
        }

        if (symbolId == SystemSymbols.ION_1_0_SID && getDepth() == 0)
        {
            // TODO make sure to get the right symtab, default may differ.
            writeIonVersionMarker();
        }
        else
        {
            writeSymbolAsIs(symbolId);
        }
    }


    public final void writeSymbol(String value) throws IOException
    {
        if (SystemSymbols.ION_1_0.equals(value) && getDepth() == 0)
        {
            // TODO make sure to get the right symtab, default may differ.
            writeIonVersionMarker();
        }
        else {
            writeSymbolAsIs(value);
        }
    }


    public void finish() throws IOException
    {
        if (getDepth() != 0) {
            throw new IllegalStateException(ERROR_FINISH_NOT_AT_TOP_LEVEL);
        }

        flush();

        // TODO this should always be $ion_1_0
        _symbol_table = _default_system_symbol_table;
    }


    //========================================================================
    // Field names

    // This handles converting string to int (or the reverse) using the current
    // symbol table, if that is needed.  These routines are not generally
    // overridden except to throw UnsupportedOperationException when they are
    // not supported.


    @Override
    final boolean isFieldNameSet()
    {
        if (_field_name_type != null) {
            switch (_field_name_type) {
            case STRING:
                return _field_name != null && _field_name.length() > 0;
            case INT:
                return _field_name_sid > 0;
            default:
                break;
            }
        }
        return false;
    }

    /**
     * This returns the field name of the value about to be written
     * if the field name has been set.  If the field name has not been
     * defined this will return null.
     *
     * @return String name of the field about to be written or null if it is
     * not yet set.
     */
    @Deprecated // TODO ION-271 remove after IMS is migrated
    String getFieldName()
    {
        String name;

        if (_field_name_type == null) {
            throw new IllegalStateException("the field has not be set");
        }
        switch (_field_name_type) {
        case STRING:
            name = _field_name;
            break;
        case INT:
            name = _symbol_table.findSymbol(_field_name_sid);
            break;
        default:
            throw new IllegalStateException("the field has not be set");
        }

        return name;
    }

    final void clearFieldName()
    {
        _field_name_type = null;
        _field_name = null;
        _field_name_sid = UNKNOWN_SYMBOL_ID;
    }


    public final void setFieldName(String name)
    {
        if (!this.isInStruct()) {
            throw new IllegalStateException();
        }
        if (name.length() == 0) {
            throw new EmptySymbolException();
        }
        _field_name_type = IonType.STRING;
        _field_name = name;
        _field_name_sid = UNKNOWN_SYMBOL_ID;
    }

    public final void setFieldNameSymbol(SymbolToken name)
    {
        if (!this.isInStruct()) {
            throw new IllegalStateException();
        }

        String text = name.getText();
        if (text != null)
        {
            if (text.length() == 0) {
                throw new EmptySymbolException();
            }
            _field_name_type = IonType.STRING;
            _field_name = text;
            _field_name_sid = UNKNOWN_SYMBOL_ID;
        }
        else
        {
            int sid = name.getSid();
            if (sid <= 0) {
                throw new IllegalArgumentException();
            }

            _field_name_type = IonType.INT;
            _field_name_sid = sid;
            _field_name = null;
        }
    }

    /**
     * Returns the symbol id of the current field name, if the field name
     * has been set.  If the name has not been set, either as either a String
     * or a symbol id value, this returns -1 (undefined symbol).
     * @return symbol id of the name of the field about to be written or -1 if
     * it is not set
     */
    final int getFieldId()
    {
        int id;

        if (_field_name_type == null) {
            throw new IllegalStateException("the field has not be set");
        }
        switch (_field_name_type) {
        case STRING:
                try {
                    id = add_symbol(_field_name);
                }
                catch (IOException e) {
                    throw new IonException(e);
                }
                // TODO cache the sid?
            break;
        case INT:
            id = _field_name_sid;
            break;
        default:
            throw new IllegalStateException("the field has not be set");
        }

        return id;
    }

    @Deprecated
    public final void setFieldId(int id)
    {
        if (!this.isInStruct()) {
            throw new IllegalStateException();
        }
        _field_name_type = IonType.INT;
        _field_name_sid = id;
        _field_name = null;
    }

    final SymbolToken assumeFieldNameSymbol()
    {
        if (_field_name_type == null)  {
            throw new IllegalStateException(ERROR_MISSING_FIELD_NAME);
        }
        // Exactly one of our fields is set.
        assert _field_name != null ^ _field_name_sid >= 0;
        return new SymbolTokenImpl(_field_name, _field_name_sid);
    }

    //========================================================================
    // Annotations

    /**
     * Ensures that our {@link #_annotations} and {@link #_annotation_sids}
     * arrays have enough capacity to hold the given number of annotations.
     * Does not increase {@link #_annotation_count}.
     */
    final void ensureAnnotationCapacity(int length) {
        int oldlen = (_annotations == null) ? 0 : _annotations.length;
        if (length < oldlen) return;

        int newlen = (_annotations == null) ? 10 : (_annotations.length * 2);
        if (length > newlen) {
            newlen = length;
        }

        SymbolToken[] temp1 = new SymbolToken[newlen];

        if (oldlen > 0) {
            System.arraycopy(_annotations, 0, temp1, 0, oldlen);
        }
        _annotations = temp1;
    }


    final int[] internAnnotationsAndGetSids() throws IOException
    {
        int count = _annotation_count;
        if (count == 0) return _Private_Utils.EMPTY_INT_ARRAY;

        int[] sids = new int[count];
        for (int i = 0; i < count; i++)
        {
            SymbolToken sym = _annotations[i];
            int sid = sym.getSid();
            if (sid == UNKNOWN_SYMBOL_ID)
            {
                String text = sym.getText();
                sid = add_symbol(text);
                _annotations[i] = new SymbolTokenImpl(text, sid);
            }
            sids[i] = sid;
        }
        return sids;
    }


    final boolean hasAnnotations()
    {
        return _annotation_count != 0;
    }

    final int annotationCount()
    {
        return _annotation_count;
    }

    final void clearAnnotations()
    {
        _annotation_count = 0;
    }


    @Override
    final boolean has_annotation(String name, int id)
    {
        assert(this._symbol_table.findKnownSymbol(id).equals(name));
        if (_annotation_count < 1) {
            return false;
        }

        for (int ii=0; ii<_annotation_count; ii++) {
            if (name.equals(_annotations[ii].getText())) {
                return true;
            }
        }
        return false;
    }

    final SymbolToken[] getTypeAnnotationSymbols()
    {
        int count = _annotation_count;
        if (count == 0) return SymbolToken.EMPTY_ARRAY;

        SymbolToken[] syms = new SymbolToken[count];
        System.arraycopy(_annotations, 0, syms, 0, count);
        return syms;
    }

    public final void setTypeAnnotationSymbols(SymbolToken... annotations)
    {
        if (annotations == null || annotations.length == 0)
        {
            _annotation_count = 0;
        }
        else
        {
            int count = annotations.length;
            // TODO the following makes two copy passes
            // TODO validate the input
            ensureAnnotationCapacity(count);

            SymbolTable symtab = getSymbolTable();
            for (int i = 0; i < count; i++)
            {
                SymbolToken sym = annotations[i];
                sym = _Private_Utils.localize(symtab, sym);
                _annotations[i] = sym;
            }
            _annotation_count = count;
        }
    }

    @Override
    final String[] getTypeAnnotations()
    {
        return _Private_Utils.toStrings(_annotations, _annotation_count);
    }

    public final void setTypeAnnotations(String... annotations)
    {
        if (annotations == null || annotations.length == 0)
        {
            _annotation_count = 0;
        }
        else
        {
            SymbolToken[] syms =
                newSymbolTokens(getSymbolTable(), annotations);
            int count = syms.length;
            // TODO the following makes two copy passes
            ensureAnnotationCapacity(count);
            System.arraycopy(syms, 0, _annotations, 0, count);
            _annotation_count = count;
        }
    }

    public final void addTypeAnnotation(String annotation)
    {
        SymbolToken is = newSymbolToken(getSymbolTable(), annotation);
        ensureAnnotationCapacity(_annotation_count + 1);
        _annotations[_annotation_count++] = is;
    }


    @Override
    final int[] getTypeAnnotationIds()
    {
        return _Private_Utils.toSids(_annotations, _annotation_count);
    }

    public final void setTypeAnnotationIds(int... annotationIds)
    {
        _annotations = newSymbolTokens(getSymbolTable(), annotationIds);
        _annotation_count = _annotations.length;
    }

    public final void addTypeAnnotationId(int annotationId)
    {
        SymbolToken is = newSymbolToken(getSymbolTable(), annotationId);
        ensureAnnotationCapacity(_annotation_count + 1);
        _annotations[_annotation_count++] = is;
    }
}
