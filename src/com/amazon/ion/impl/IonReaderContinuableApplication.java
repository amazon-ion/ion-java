// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;

import java.util.Iterator;

/**
 * IonCursor with the application-level IonReader interface methods. Useful for adapting an IonCursor implementation
 * into a application-level IonReader.
 */
interface IonReaderContinuableApplication extends IonReaderContinuableCore {

    /**
     * Returns the symbol table that is applicable to the current value.
     * This may be either a system or local symbol table.
     */
    SymbolTable getSymbolTable();

    /**
     * Return the annotations of the current value as an array of strings.
     *
     * @return the (ordered) annotations on the current value, or an empty
     * array (not {@code null}) if there are none.
     *
     * @throws UnknownSymbolException if any annotation has unknown text.
     */
    String[] getTypeAnnotations();

    /**
     * Return the annotations on the curent value as an iterator.  The
     * iterator is empty (hasNext() returns false on the first call) if
     * there are no annotations on the current value.
     *
     * Implementations *may* throw {@link UnknownSymbolException} from
     * this method if any annotation contains unknown text. Alternatively,
     * implementations may provide an Iterator that throws
     * {@link UnknownSymbolException} only when the user navigates the
     * iterator to an annotation with unknown text.
     *
     * @return not null.
     */
    Iterator<String> iterateTypeAnnotations();

    /**
     * Return the field name of the current value. Or null if there is no valid
     * current value or if the current value is not a field of a struct.
     *
     * @throws UnknownSymbolException if the field name has unknown text.
     */
    String getFieldName();

    /**
     * Gets the current value's annotations as symbol tokens (text + ID).
     *
     * @return the (ordered) annotations on the current value, or an empty
     * array (not {@code null}) if there are none.
     *
     */
    SymbolToken[] getTypeAnnotationSymbols();

    /**
     * Gets the current value's field name as a symbol token (text + ID).
     * If the text of the token isn't known, the result's
     * {@link SymbolToken#getText()} will be null.
     * If the symbol ID of the token isn't known, the result's
     * {@link SymbolToken#getSid()} will be
     * {@link SymbolTable#UNKNOWN_SYMBOL_ID}.
     * At least one of the two fields will be defined.
     *
     * @return null if there is no current value or if the current value is
     *  not a field of a struct.
     *
     */
    SymbolToken getFieldNameSymbol();

    /**
     * Returns the current value as a symbol token (text + ID).
     * This is only valid when {@link #getType()} returns
     * {@link IonType#SYMBOL}.
     *
     * @return null if {@link #isNullValue()}
     *
     */
    SymbolToken symbolValue();
}
