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

internal const val ION_LOCATION_META_TAG: String = "\$ion_location"

public sealed class IonLocation

/**
 * Represents a location in an Ion text stream using 1-based indexing of the line number and character offset.
 */
public data class IonTextLocation(val line: Long, val charOffset: Long) : IonLocation() {
    override fun toString(): String = "$line:$charOffset"
}

/**
 * Represents a location in an Ion binary stream as a 0-based offset from the start of the stream.
 */
public data class IonBinaryLocation(val byteOffset: Long) : IonLocation() {
    override fun toString(): String = byteOffset.toString()
}

public fun locationToString(loc: IonLocation?): String = loc?.toString() ?: "<unknown location>"

public val MetaContainer.location: IonLocation?
    get() = this[ION_LOCATION_META_TAG] as? IonLocation
