// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.util

import com.amazon.ion.IonException
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract

/**
 * Similar to the `!!` operator, this function assumes that the value is not null. Unlike the
 * `!!` operator, this function does it without actually checking if the value is null.
 *
 * Why? This has no branches. If we actually checked if it was null, then there would be branching.
 */
@OptIn(ExperimentalContracts::class)
internal inline fun <T> T?.assumeNotNull(): T {
    contract { returns() implies (this@assumeNotNull != null) }
    privateAssumeNotNull(this)
    return this
}

/**
 * Supporting function for `assumeNotNull`.
 * This function exists just to hold the contract to trick the Kotlin compiler into deducing that a value is not null.
 */
@OptIn(ExperimentalContracts::class)
private inline fun <T> privateAssumeNotNull(value: T?) {
    contract { returns() implies (value != null) }
}

/**
 * Tell the compiler that some condition is true. Must have a comment indicating why it is safe to trick the compiler.
 */
@OptIn(ExperimentalContracts::class)
internal inline fun assumeUnchecked(assumption: Boolean) {
    contract { returns() implies assumption }
}

/**
 * Checks an assumption, throwing an [IonException] with a lazily created message if the assumption is false.
 *
 * This is named `confirm` because `check` and `require` are already similar functions in the Kotlin Std Lib, and
 * `expect`, `verify`, and `assert` are used for test frameworks.
 */
internal inline fun confirm(assumption: Boolean, lazyMessage: () -> String) {
    if (!assumption) {
        throw IonException(lazyMessage())
    }
}

/**
 * Marks a branch as unreachable (for human readability).
 */
internal fun unreachable(reason: String? = null): Nothing = throw IllegalStateException(reason)
