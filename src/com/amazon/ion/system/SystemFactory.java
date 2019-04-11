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

package com.amazon.ion.system;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;

/**
 * The factory for creating {@link IonSystem}s.
 * Most applications will only have one or two system instances;
 * see {@link IonSystem} for important constraints.
 * <p>
 * Most long-lived applications will want to provide a custom
 * {@link IonCatalog} implementation rather than using the default
 * {@link SimpleCatalog}.
 *
 * @deprecated Use the more configurable {@link IonSystemBuilder} instead.
 */
@Deprecated
public final class SystemFactory
{
    /**
     * Constructs a new system instance with a default configuration.
     * <p>
     * The catalog used by the new instance will be a {@link SimpleCatalog}
     * with no initial entries, so please be aware of the limitations of that
     * class.
     *
     * @return a new {@link IonSystem} instance; not null.
     *
     * @deprecated Use {@link IonSystemBuilder IonSystemBuilder.standard().build()} instead.
     */
    @Deprecated
    public static IonSystem newSystem()
    {
        return IonSystemBuilder.standard().build();
    }

    /**
     * Constructs a new system instance with the given catalog.
     *
     * @param catalog the catalog to use in the new system.
     *   If null, a new {@link SimpleCatalog} will be used.
     * @return a new {@link IonSystem} instance; not null.
     *
     * @deprecated Use {@link IonSystemBuilder IonSystemBuilder.standard().withCatalog(catalog).build()} instead.
     */
    @Deprecated
    public static IonSystem newSystem(IonCatalog catalog)
    {
        return IonSystemBuilder.standard().withCatalog(catalog).build();
    }
}
