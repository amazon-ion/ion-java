// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
@file:JvmName("Formatting")
package com.amazon.ion.util

@OptIn(ExperimentalStdlibApi::class)
fun ByteArray.toPrettyHexString(bytesPerWord: Int = 4, wordsPerLine: Int = 8): String {
    return map { it.toHexString(HexFormat.UpperCase) }
        .windowed(bytesPerWord, bytesPerWord, partialWindows = true)
        .windowed(wordsPerLine, wordsPerLine, partialWindows = true)
        .joinToString("\n") { it.joinToString("   ") { it.joinToString(" ") } }
}
