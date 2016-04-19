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

import java.io.PrintWriter;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public interface PrivateIonValue
    extends IonValue
{

    /**
     * Provides an IonValue's SymbolTable.
     */
    public interface SymbolTableProvider {
        public SymbolTable getSymbolTable();
    }

    /**
     *
     * @return int the offset of this value in its containers member list
     */
    public int         getElementId();

    /**
     * Overrides {@link IonValue#getFieldNameSymbol()} for use when there exists
     * a SymbolTableProvider implementation for this IonValue.
     * @param symbolTableProvider - provides this IonValue's symbol table
     * @return the field name SymbolToken
     * @see IonValue#getFieldNameSymbol()
     */
    public SymbolToken getFieldNameSymbol(SymbolTableProvider symbolTableProvider);

    /**
     * Overrides {@link IonValue#getTypeAnnotationSymbols()} for use when there exists
     * a SymbolTableProvider implementation for this IonValue.
     * @param symbolTableProvider - provides this IonValue's symbol table
     * @return the type annotation SymbolTokens
     * @see IonValue#getTypeAnnotationSymbols()
     */
    public SymbolToken[] getTypeAnnotationSymbols(SymbolTableProvider symbolTableProvider);

    /**
     * Makes this symbol table current for this value.
     * This may directly apply to this IonValue if this
     * value is either loose or a top level datagram
     * member.  Or it may be delegated to the IonContainer
     * this value is a contained in.
     * <p>
     * Assigning null forces any symbol values to be
     * resolved to strings and any associated symbol
     * table will be removed.
     * <p>
     * @param symbols must be local or system table. May be null.
     *
     * @throws UnsupportedOperationException if this is a datagram.
     */
    public void setSymbolTable(SymbolTable symbols);

    /**
     * Returns the symbol table that is directly associated with this value,
     * without doing any recursive lookup.
     * Values that are not top-level will return null as they don't actually
     * own their own symbol table.
     *
     * @throws UnsupportedOperationException if this is an {@link IonDatagram}.
     */
    public SymbolTable getAssignedSymbolTable();

    public void dump(PrintWriter out);

    public String validate();
}
