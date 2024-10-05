// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.impl.macro.Macro.*

/**
 * Convenience functions for concisely creating [Macro.Parameter]s.
 */
object ParameterFactory {
    @JvmStatic
    fun zeroToManyTagged(name: String) = Parameter(name, ParameterEncoding.Tagged, ParameterCardinality.ZeroOrMore)
    @JvmStatic
    fun exactlyOneTagged(name: String) = Parameter(name, ParameterEncoding.Tagged, ParameterCardinality.ExactlyOne)
    @JvmStatic
    fun exactlyOneFlexInt(name: String) = Parameter(name, ParameterEncoding.CompactInt, ParameterCardinality.ExactlyOne)
}
