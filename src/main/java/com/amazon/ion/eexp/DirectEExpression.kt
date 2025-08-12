// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.eexp

/**
 * This object serves as a sentinel/nonce type to help users of the library correctly
 * use the Argument Builder APIs. It enforces type safety and proper usage patterns
 * in the fluent builder API for direct encoding expressions.
 *
 * See [DirectEExpressionArgumentBuilder] and [WriteAsIon][com.amazon.ion.WriteAsIon]
 */
object DirectEExpression : EExpression
