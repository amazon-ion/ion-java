// Copyright (c) 2016 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonSymbol;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl.PrivateIonValue.SymbolTableProvider;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public interface PrivateIonSymbol
    extends IonSymbol
{
    /**
     * Overrides {@link IonSymbol#symbolValue()} for use when there exists
     * a SymbolTableProvider implementation for this IonSymbol.
     * @param symbolTableProvider - provides this IonSymbol's symbol table
     * @return a SymbolToken representing this IonSymbol
     * @see IonSymbol#symbolValue()
     */
    public SymbolToken symbolValue(SymbolTableProvider symbolTableProvider);

}
