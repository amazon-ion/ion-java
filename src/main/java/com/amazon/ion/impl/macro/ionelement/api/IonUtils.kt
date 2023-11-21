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

@file:JvmName("IonUtils")
package com.amazon.ion.impl.macro.ionelement.api

import com.amazon.ion.IonValue
import com.amazon.ion.ValueFactory

/**
 * Bridge function that converts from an immutable [IonElement] to a mutable [IonValue].
 *
 * New code that doesn't need to integrate with existing uses of the mutable DOM should not use this.
 *
 * @param factory A [ValueFactory] to use to create the new [IonValue] instances.  Note that any
 * [com.amazon.ion.IonSystem] instance maybe be used here, in addition to other implementations of [ValueFactory].
 */
public fun IonElement.toIonValue(factory: ValueFactory): IonValue {
    // We still have to use IonSystem under the covers for this to get an IonWriter that writes to a dummy list.
    val dummyList = factory.newList()
    dummyList.system.newWriter(dummyList).use { writer ->
        this.writeTo(writer)
    }
    // .removeAt(0) below detaches the `IonValue` that was written above so that it may be added to other
    // IonContainer instances without needing to be `IonValue.cloned()`'d.
    return dummyList.removeAt(0)
}

/**
 * Bridge function that converts from the mutable [IonValue] to an [AnyElement].
 *
 * New code that does not need to integrate with uses of the mutable DOM should not use this.
 */
public fun IonValue.toIonElement(): AnyElement =
    this.system.newReader(this).use { reader ->
        createIonElementLoader().loadSingleElement(reader)
    }

/** Throws an [IonElementException], including the [IonLocation] (if available). */
internal fun constraintError(blame: IonElement, description: String): Nothing {
    throw IonElementConstraintException(blame.asAnyElement(), description)
}
