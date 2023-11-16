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

package com.amazon.ion.impl;

import com.amazon.ion.IonSymbol;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl._Private_IonValue.SymbolTableProvider;

/**
 * NOT FOR APPLICATION USE!
 */
public interface _Private_IonSymbol
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
