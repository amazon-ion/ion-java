// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro

import com.amazon.ion.impl.*

/**
 * Represents a template macro. A template macro is defined by a signature, and a list of template expressions.
 * A template macro only gains a name and/or ID when it is added to a macro table.
 */
class TemplateMacro @JvmOverloads constructor(
    override val signature: List<Macro.Parameter>,
    override val body: List<Expression.TemplateBodyExpression>,
    override val bodyTape: ExpressionTape.Core = ExpressionTape.inlineNestedInvocations(ExpressionTape.from(body))
) : Macro {

    override val isSimple by lazy {
        bodyTape.numberOfVariables == signature.size && bodyTape.areVariablesOrdered()
    }

    // TODO: Consider rewriting the body of the macro if we discover that there are any macros invoked using only
    //       constants as arguments—either at compile time or lazily.
    //       For example, the body of: (macro foo (x)  (values (make_string "foo" "bar") x))
    //       could be rewritten as: (values "foobar" x)

    private val cachedHashCode by lazy { signature.hashCode() * 31 + body.hashCode() }
    override fun hashCode(): Int = cachedHashCode

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TemplateMacro) return false
        // Check the hashCode as a quick check before we dive into the actual data.
        if (cachedHashCode != other.cachedHashCode) return false
        if (signature != other.signature) return false
        if (body != other.body) return false
        return true
    }

    override val dependencies: List<Macro> by lazy {
        body.filterIsInstance<Expression.MacroInvocation>()
            .map { it.macro }
            .distinct()
    }
}
