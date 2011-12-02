// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.impl.IonImplUtils.EMPTY_INT_ARRAY;
import static com.amazon.ion.impl.IonImplUtils.EMPTY_STRING_ARRAY;
import static com.amazon.ion.impl.UnifiedSymbolTable.isNonSystemSharedTable;
import static com.amazon.ion.impl.UnifiedSymbolTable.makeNewLocalSymbolTable;

import com.amazon.ion.EmptySymbolException;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
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
    private int         _field_name_sid;

    private static final int DEFAULT_ANNOTATION_COUNT = 4;

    /** really ion type is only used for int, string or null (unknown) */
    private IonType     _annotations_type;
    private int         _annotation_count;
    private String[]    _annotations = new String[DEFAULT_ANNOTATION_COUNT];
    private int[]       _annotation_sids = new int[DEFAULT_ANNOTATION_COUNT];


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
        if (symbols == null || isNonSystemSharedTable(symbols)) {
            throw new IllegalArgumentException("symbol table must be local or system to be set, or reset");
        }
        if (getDepth() > 0) {
            throw new IllegalStateException("the symbol table cannot be set, or reset, while a container is open");
        }
        _symbol_table = symbols;
    }

    /**
     * Builds a new local symbol table from the current contextual symtab
     * (a system symtab).
     * @return not null.
     */
    UnifiedSymbolTable inject_local_symbol_table() throws IOException
    {
        assert _symbol_table.isSystemTable();
        // no catalog since it doesn't matter as this is a
        // pure local table, with no imports
        return makeNewLocalSymbolTable(null /*system*/, _symbol_table);
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


    final void clearFieldName()
    {
        _field_name_type = null;
        _field_name = null;
        _field_name_sid = UnifiedSymbolTable.UNKNOWN_SYMBOL_ID;
    }


    /**
     * This returns the field name of the value about to be written
     * if the field name has been set.  If the field name has not been
     * defined this will return null.
     *
     * @return String name of the field about to be written or null if it is
     * not yet set.
     *
     * @throws UnknownSymbolException if the text of the field name is unknown.
     */
    final String getFieldName()
    {
        // TODO streamline
        String name;

        if (_field_name_type == null) {
            // TODO contradicts documented behavior
            throw new IllegalStateException("the field has not be set");
        }
        switch (_field_name_type) {
        case STRING:
            name = _field_name;
            break;
        case INT:
            name = null;
            break;
        default:
            throw new IllegalStateException("the field has not be set");
        }

        return name;
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

    public final void setFieldId(int id)
    {
        if (!this.isInStruct()) {
            throw new IllegalStateException();
        }
        _field_name_type = IonType.INT;
        _field_name_sid = id;
    }


    //========================================================================
    // Annotations


    private final boolean no_illegal_annotations()
    {
        for (int ii=0; ii<_annotation_count; ii++) {
            String a = _annotations[ii];
            if (a == null || a.length() < 1) {
                return false;
            }
        }
        return true;
    }

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

        String[] temp1 = new String[newlen];
        int[]    temp2 = new int[newlen];

        if (oldlen > 0) {
            if (_annotations_type == IonType.STRING) {
                System.arraycopy(_annotations, 0, temp1, 0, oldlen);
            }
            if (_annotations_type == IonType.INT) {
                System.arraycopy(_annotation_sids, 0, temp2, 0, oldlen);
            }
        }
        _annotations = temp1;
        _annotation_sids = temp2;
    }


    /**
     * @throws UnknownSymbolException if the text of any annotation is
     *  unknown.
     */
    private final String[] get_type_annotations_as_strings()
    {
        if (_annotation_count < 1) {
            return EMPTY_STRING_ARRAY;
        }
        else if (_annotations_type == IonType.INT) {
            for (int ii=0; ii<_annotation_count; ii++) {
                int id = _annotation_sids[ii];
                String name = assumeKnownSymbol(id);
                _annotations[ii] = name;
            }
        }
        return _annotations;
    }

    final int[] get_type_annotations_as_ints()
    {
        if (_annotation_count < 1) {
            return EMPTY_INT_ARRAY;
        }
        else if (_annotations_type == IonType.STRING) {
            for (int ii=0; ii<_annotation_count; ii++) {
                String name = _annotations[ii];
                try {
                    _annotation_sids[ii] = add_symbol(name);
                }
                catch (IOException e) {
                    throw new IonException(e);
                }
            }
        }
        return _annotation_sids;
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
        _annotations_type = IonType.NULL;
    }


    @Override
    final boolean has_annotation(String name, int id)
    {
        assert(this._symbol_table.findKnownSymbol(id).equals(name));
        if (_annotation_count < 1) {
            return false;
        }
        else if (_annotations_type == IonType.INT) {
            for (int ii=0; ii<_annotation_count; ii++) {
                if (_annotation_sids[ii] == id) {
                    return true;
                }
            }
        }
        else if (_annotations_type == IonType.STRING) {
            for (int ii=0; ii<_annotation_count; ii++) {
                // TODO: currently this method is only called internally for
                //       system symbols.  If this is to be expanded to user
                //       symbols (or our system symbols get more complex)
                //       these names will have to be "washed" to handle
                //       escape characters and $15 style names
                if (name.equals(_annotations[ii])) {
                    return true;
                }
            }
        }
        else {
            assert("if there are annotation they have to be either string or int".length() < 0);
        }
        return false;
    }


    @Override
    final String[] getTypeAnnotations()
    {
        if (_annotation_count < 1) {
            // no annotations, just give them the empty array
            return EMPTY_STRING_ARRAY;
        }
        if (IonType.INT.equals(_annotations_type) && getSymbolTable() == null) {
            // the native form of the annotations are ints
            // but there's no symbol table to convert them
            // we're done - no data for the caller
            return null;
        }

        // go get the string (original or converted from ints)
        String[] user_copy = new String[_annotation_count];
        String[] annotations = get_type_annotations_as_strings();
        System.arraycopy(annotations, 0, user_copy, 0, _annotation_count);

        return user_copy;
    }

    public final void setTypeAnnotations(String... annotations)
    {
        _annotations_type = IonType.STRING;
        if (annotations == null) {
            annotations = IonImplUtils.EMPTY_STRING_ARRAY;
        }
        else if (annotations.length > _annotation_count) {
            ensureAnnotationCapacity(annotations.length);
        }
        System.arraycopy(annotations, 0, _annotations, 0, annotations.length);
        _annotation_count = annotations.length;
        assert(no_illegal_annotations() == true);
    }

    public final void addTypeAnnotation(String annotation)
    {
        ensureAnnotationCapacity(_annotation_count + 1);
        if (_annotations_type == IonType.INT) {
            int sid;
            try {
                sid = add_symbol(annotation);
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            addTypeAnnotationId(sid);
        }
        else {
            _annotations_type = IonType.STRING;
            // FIXME: annotations need to be "washed" through a symbol
            //        table to address issues like $0234 -> $234 or 'xyzzx'
            _annotations[_annotation_count++] = annotation;
        }
    }


    @Override
    final int[] getTypeAnnotationIds()
    {
        if (_annotation_count < 1) {
            // no annotations, just give them the empty array
            return EMPTY_INT_ARRAY;
        }
        if (IonType.STRING.equals(_annotations_type) && getSymbolTable() == null) {
            // the native form of the annotations are strings
            // but there's no symbol table to convert them
            // we're done - no data for the caller
            return null;
        }

        // get the user the ids, either native or converted
        // throught the current symbol table
        int[] user_copy = new int[_annotation_count];
        int[] annotations = get_type_annotations_as_ints();
        System.arraycopy(annotations, 0, user_copy, 0, _annotation_count);

        return user_copy;
    }

    public final void setTypeAnnotationIds(int... annotationIds)
    {
        _annotations_type = IonType.INT;
        if (annotationIds == null) {
            annotationIds = IonImplUtils.EMPTY_INT_ARRAY;
        }
        else if (annotationIds.length > _annotation_count) {
            ensureAnnotationCapacity(annotationIds.length);
        }
        System.arraycopy(annotationIds, 0, _annotation_sids, 0, annotationIds.length);
        _annotation_count = annotationIds.length;
    }

    public final void addTypeAnnotationId(int annotationId)
    {
        ensureAnnotationCapacity(_annotation_count + 1);
        if (_annotations_type == IonType.STRING) {
            String annotation = assumeKnownSymbol(annotationId);
            addTypeAnnotation(annotation);
        }
        else {
            _annotations_type = IonType.INT;
            _annotation_sids[_annotation_count++] = annotationId;
        }
    }
}
