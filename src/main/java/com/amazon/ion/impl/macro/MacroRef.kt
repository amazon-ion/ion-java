// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

/**
 * A reference to a particular macro, either by name or by template id.
 */
sealed interface MacroRef {
    // TODO: See if these could be inline value classes
    @JvmInline value class ByName(val name: String) : MacroRef
    @JvmInline value class ById(val id: Long) : MacroRef
    // TODO: Since system macros have an independent address space, do we need to have a `SystemById` variant?
}
