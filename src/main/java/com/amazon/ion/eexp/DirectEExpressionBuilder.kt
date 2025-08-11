// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

import com.amazon.ion.impl.bin.*
import com.amazon.ion.impl.macro.*

/**
 * A builder for creating direct encoding expressions.
 *
 * This implementation writes encoding expressions directly to an Ion writer as they
 * are built, rather than storing them for later use.
 */
class DirectEExpressionBuilder internal constructor(private val writer: IonManagedWriter_1_1) : EExpressionBuilder {
    private var name: String? = null

    override fun withName(name: String?): DirectEExpressionBuilder {
        this.name = name
        return this
    }

    override fun withMacro(macro: Macro): EExpressionArgumentBuilder<DirectEExpression> {
        val name = name
        if (name != null) {
            writer.startMacro(name, macro)
        } else {
            writer.startMacro(macro)
        }
        return DirectEExpressionArgumentBuilder(macro, writer)
    }
}
