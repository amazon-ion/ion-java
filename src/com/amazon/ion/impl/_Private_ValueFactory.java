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

import com.amazon.ion.IonStruct;
import com.amazon.ion.ValueFactory;

public interface _Private_ValueFactory extends ValueFactory
{
    /**
     * Gets the {@link LocalSymbolTableAsStruct.Factory} associated with this
     * {@link ValueFactory}. This is used to construct local symbol tables
     * backed by {@link IonStruct}s. Note that this should not be used in new
     * code; use {@link LocalSymbolTable} instead.
     *
     * @return a LocalSymbolTableAsStruct.Factory; never null.
     */
    @SuppressWarnings("javadoc")
    public _Private_LocalSymbolTableFactory getLstFactory();
}
