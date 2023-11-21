/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
 *  permissions and limitations under the License.
 */

package com.amazon.ion.impl.macro.ionelement.api

/**
 * Base exception which includes the [location] in the message if it was included at construction time.
 */
public open class IonElementException internal constructor(
    public val location: IonLocation?,
    public val description: String,
    cause: Throwable? = null
) : Error(locationToString(location) + ": $description", cause)

/** Exception thrown by [IonElementLoader]. */
public class IonElementLoaderException internal constructor(
    location: IonLocation?,
    description: String,
    cause: Throwable? = null
) : IonElementException(location, description, cause)

/**
 * Exception thrown by [IonElement] accessor functions to indicate a type or nullness constraint has been violated.
 *
 * [blame] is the [IonElement] instance that violates the constraint.
 */
public class IonElementConstraintException internal constructor(
    private val elementToBlame: IonElement,
    description: String,
    cause: Throwable? = null
) : IonElementException(elementToBlame.metas.location, description, cause) {
    public val blame: AnyElement get() = elementToBlame.asAnyElement()
}
