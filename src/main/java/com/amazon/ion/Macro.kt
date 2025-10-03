// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion

/**
 * An Ion macro.
 *
 * This interface is intentionally opaque to avoid unnecessarily coupling Ion 1.1 macro implementation details
 * with the macro implementation details for any future versions of Ion.
 *
 * To obtain a `Macro` instance for Ion 1.1, see [MacroBuilder][com.amazon.ion.ion_1_1.MacroBuilder].
 */
interface Macro
