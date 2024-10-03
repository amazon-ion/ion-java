// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import com.amazon.ion.SymbolToken

// Exposes "package-private" java functions in _Private_Utils as "internal" kotlin functions

internal fun newSymbolToken(text: String): SymbolToken = _Private_Utils.newSymbolToken(text)
internal fun newSymbolToken(text: String, sid: Int): SymbolToken = _Private_Utils.newSymbolToken(text, sid)
internal fun newSymbolToken(sid: Int): SymbolToken = _Private_Utils.newSymbolToken(sid)
