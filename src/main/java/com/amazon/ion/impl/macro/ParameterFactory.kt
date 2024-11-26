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
    fun zeroOrOneTagged(name: String) = Parameter(name, ParameterEncoding.Tagged, ParameterCardinality.ZeroOrOne)
    @JvmStatic
    fun oneToManyTagged(name: String) = Parameter(name, ParameterEncoding.Tagged, ParameterCardinality.OneOrMore)
    @JvmStatic
    fun exactlyOneTagged(name: String) = Parameter(name, ParameterEncoding.Tagged, ParameterCardinality.ExactlyOne)
}
