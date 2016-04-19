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

package software.amazon.ion.impl;

import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static software.amazon.ion.impl.PrivateUtils.newLocalSymtab;
import static software.amazon.ion.impl.PrivateUtils.newSymbolToken;
import static software.amazon.ion.impl.PrivateUtils.newSymbolTokens;

import java.io.IOException;
import software.amazon.ion.EmptySymbolException;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.SystemSymbols;
import software.amazon.ion.UnknownSymbolException;
import software.amazon.ion.system.IonWriterBuilder.InitialIvmHandling;
import software.amazon.ion.system.IonWriterBuilder.IvmMinimizing;


abstract class IonWriterSystem
    extends PrivateIonWriterBase
{
    /**
     * The system symtab used when resetting the stream.
     * Must not be null.
     */
    final SymbolTable _default_system_symbol_table;

    /**
     * What to do about IVMs at the start of the stream.
     * Becomes null as soon as we write anything.
     * After a {@link #finish()} this becomes {@link InitialIVMHandling#ENSURE}
     * because we must force another IVM to be emitted.
     */
    private InitialIvmHandling _initial_ivm_handling;

    /**
     * What to do about non-initial IVMs.
     */
    private final IvmMinimizing _ivm_minimizing;

    /**
     * Indicates whether the (immediately previous emitted value was an IVM.
     * This is cleared by {@link #endValue()}.
     */
    private boolean _previous_value_was_ivm;

    /**
     * Indicates whether we've written anything at all.
     */
    private boolean _anything_written;

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
    IonWriterSystem(SymbolTable defaultSystemSymbolTable,
                    InitialIvmHandling initialIvmHandling,
                    IvmMinimizing ivmMinimizing)
    {
        defaultSystemSymbolTable.getClass(); // Efficient null check
        _default_system_symbol_table = defaultSystemSymbolTable;
        _symbol_table = defaultSystemSymbolTable;
        _initial_ivm_handling = initialIvmHandling;
        _ivm_minimizing = ivmMinimizing;
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
        if (symbols == null || PrivateUtils.symtabIsSharedNotSystem(symbols)) {
            throw new IllegalArgumentException("symbol table must be local or system to be set, or reset");
        }
        if (getDepth() > 0) {
            throw new IllegalStateException("the symbol table cannot be set, or reset, while a container is open");
        }
        _symbol_table = symbols;
    }

    boolean shouldWriteIvm()
    {
        if (_initial_ivm_handling == InitialIvmHandling.ENSURE)
        {
            return true;
        }
        if (_initial_ivm_handling == InitialIvmHandling.SUPPRESS)
        {
            // TODO amznlabs/ion-java#24 Must write IVM if given system != 1.0
            return false;
        }
        // TODO amznlabs/ion-java#24 Add SUPPRESS_ALL to suppress non 1.0 IVMs

        if (_ivm_minimizing == IvmMinimizing.ADJACENT)
        {
            // TODO amznlabs/ion-java#24 Write IVM if current system version != given system
            // For now we assume that it's the same since we only support 1.0
            return ! _previous_value_was_ivm;
        }
        if (_ivm_minimizing == IvmMinimizing.DISTANT)
        {
            // TODO amznlabs/ion-java#24 Write IVM if current system version != given system
            // For now we assume that it's the same since we only support 1.0
            return ! _anything_written;
        }

        return true;
    }

    /**
     * Sets {@link #_symbol_table} and clears {@link #_initial_ivm_handling}.
     * Subclasses should override to generate output.
     */
    final void writeIonVersionMarker(SymbolTable systemSymtab)
        throws IOException
    {
        if (getDepth() != 0)
        {
            String message =
                "Ion Version Markers are only valid at the top level of a " +
                "data stream";
            throw new IllegalStateException(message);
        }
        assert systemSymtab.isSystemTable();

        if (! SystemSymbols.ION_1_0.equals(systemSymtab.getIonVersionId()))
        {
            String message = "This library only supports Ion 1.0";
            throw new UnsupportedOperationException(message);
        }

        if (shouldWriteIvm())
        {
            _initial_ivm_handling = null;

            writeIonVersionMarkerAsIs(systemSymtab);

            _previous_value_was_ivm = true;
        }

        _symbol_table = systemSymtab;
    }

    /**
     * Writes an IVM without checking preconditions or
     * {@link InitialIVMHandling}.
     */
    abstract void writeIonVersionMarkerAsIs(SymbolTable systemSymtab)
        throws IOException;


    @Override
    public final void writeIonVersionMarker()
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
        int sid;
        if (_symbol_table.isSystemTable()) {
            sid = _symbol_table.findSymbol(name);
            if (sid != UNKNOWN_SYMBOL_ID) {
                return sid;
            }
            // @name is not a system symbol, so we inject a local symtab
            _symbol_table = inject_local_symbol_table();
        }
        assert _symbol_table.isLocalTable();
        sid = _symbol_table.intern(name).getSid();
        return sid;
    }

    void startValue() throws IOException
    {
        if (_initial_ivm_handling == InitialIvmHandling.ENSURE)
        {
            writeIonVersionMarker(_default_system_symbol_table);
        }
    }

    void endValue()
    {
        _initial_ivm_handling = null;
        _previous_value_was_ivm = false;
        _anything_written = true;
    }


    /** Writes a symbol without checking for system ID. */
    abstract void writeSymbolAsIs(int symbolId) throws IOException;

    /** Writes a symbol without checking for system ID. */
    abstract void writeSymbolAsIs(String value) throws IOException;

    @Override
    final void writeSymbol(int symbolId) throws IOException
    {
        if (symbolId < 1) {
            throw new IllegalArgumentException("symbol IDs are greater than 0");
        }

        if (symbolId == SystemSymbols.ION_1_0_SID
            && getDepth() == 0
            && _annotation_count == 0) {
            // $ion_1_0 is written as an IVM only if it is not annotated
            // TODO amznlabs/ion-java#24 Make sure to get the right symtab, default may differ.
            writeIonVersionMarker();
        }
        else
        {
            writeSymbolAsIs(symbolId);
        }
    }

    public final void writeSymbol(String value) throws IOException
    {
        if (SystemSymbols.ION_1_0.equals(value)
            && getDepth() == 0
            && _annotation_count == 0) {
            // $ion_1_0 is written as an IVM only if it is not annotated
            // TODO amznlabs/ion-java#24 Make sure to get the right symtab, default may differ.
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

        _previous_value_was_ivm = false;
        _initial_ivm_handling = InitialIvmHandling.ENSURE;
        _symbol_table = _default_system_symbol_table;
    }


    //========================================================================
    // Field names

    // This handles converting string to int (or the reverse) using the current
    // symbol table, if that is needed.  These routines are not generally
    // overridden except to throw UnsupportedOperationException when they are
    // not supported.


    @Override
    public final boolean isFieldNameSet()
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
        if (count == 0) return PrivateUtils.EMPTY_INT_ARRAY;

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
                sym = PrivateUtils.localize(symtab, sym);
                _annotations[i] = sym;
            }
            _annotation_count = count;
        }
    }

    @Override
    final String[] getTypeAnnotations()
    {
        return PrivateUtils.toStrings(_annotations, _annotation_count);
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
        return PrivateUtils.toSids(_annotations, _annotation_count);
    }
}
