// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

import com.amazon.ion.impl.macro.*

/**
 * Builder interface for creating Encoding Expressions (EExpressions).
 *
 * This interface provides a fluent API for constructing EExpressions by specifying
 * their name and associated macro definitions.
 */
interface EExpressionBuilder {
    /**
     * Sets the name for the EExpression being built.
     *
     * @param name The name to assign to the EExpression, or null for an anonymous expression
     * @return This builder for method chaining
     */
    fun withName(name: String?): EExpressionBuilder

    /**
     * Associates a macro with this EExpression and transitions to argument building.
     *
     * @param macro The macro definition to use for this EExpression
     * @return An argument builder for specifying the macro's arguments
     */
    fun withMacro(macro: Macro): EExpressionArgumentBuilder<out EExpression>
}
