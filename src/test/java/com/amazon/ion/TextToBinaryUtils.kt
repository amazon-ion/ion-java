// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion

import java.util.Locale
import java.util.stream.Collectors
import java.util.stream.Stream

object TextToBinaryUtils {
    /**
     * Converts a string of octets in the given radix to a byte array. Octets must be separated by a space.
     * @param octetString the string of space-separated octets.
     * @param radix the radix of the octets in the string.
     * @return a new byte array.
     */
    @JvmStatic
    private fun octetStringToByteArray(octetString: String, radix: Int): ByteArray {
        if (octetString.isEmpty()) return ByteArray(0)
        val bytesAsStrings = octetString.split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val bytesAsBytes = ByteArray(bytesAsStrings.size)
        for (i in bytesAsBytes.indices) {
            bytesAsBytes[i] = (bytesAsStrings[i].toInt(radix) and 0xFF).toByte()
        }
        return bytesAsBytes
    }

    /**
     * Converts a string of binary octets, such as "10010111 00010011", to a byte array.
     */
    @JvmStatic
    fun String.binaryStringToByteArray(): ByteArray {
        return octetStringToByteArray(this, 2)
    }

    /**
     * Converts a string of hex octets, such as "BE EF", to a byte array.
     */
    @JvmStatic
    fun String.hexStringToByteArray(): ByteArray {
        return octetStringToByteArray(this, 16)
    }

    /**
     * @param hexBytes a string containing white-space delimited pairs of hex digits representing the expected output.
     * The string may contain multiple lines. Anything after a `|` character on a line is ignored, so
     * you can use `|` to add comments.
     */
    @JvmStatic
    fun String.cleanCommentedHexBytes(): String {
        return Stream.of(*this.split("\n".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
            .map { it.replace("\\|.*$".toRegex(), "").trim() }
            .filter { it.trim().isNotEmpty() }
            .collect(Collectors.joining(" "))
            .replace("\\s+".toRegex(), " ")
            .uppercase(Locale.getDefault())
            .trim()
    }
}
