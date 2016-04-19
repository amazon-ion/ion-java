/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import software.amazon.ion.facet.Faceted;

/**
 * Provides a simple implementation of {@link Faceted}
 * that delegates facet interpolation as a cast.
 */
abstract class DowncastingFaceted
    implements Faceted
{
    public final <T> T asFacet(final Class<T> type)
    {
        if (!type.isInstance(this))
        {
            return null;
        }
        return type.cast(this);
    }
}
