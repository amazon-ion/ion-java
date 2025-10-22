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
     * Converts a string of octets in the given radix to an int array. Octets must be separated by a space.
     * @param octetString the string of space-separated octets.
     * @param radix the radix of the octets in the string.
     * @return a new int array.
     */
    @JvmStatic
    private fun octetStringToIntArray(octetString: String, radix: Int): IntArray {
        if (octetString.isEmpty()) return IntArray(0)
        val intsAsStrings = octetString.split(" +".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val intsAsInts = IntArray(intsAsStrings.size)
        for (i in intsAsInts.indices) {
            intsAsInts[i] = intsAsStrings[i].toInt(radix)
        }
        return intsAsInts
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

    /**
     * Converts a byte array to a string of bits, such as "00110110 10001001".
     * The purpose of this method is to make it easier to read and write test assertions.
     */
    @JvmStatic
    fun ByteArray.byteArrayToBitString(): String {
        return this.joinToString(" ") { it.toUByte().toString(2).padStart(8, '0') }
    }

    /**
     * Converts a byte array to a string of hex bytes, such as "A5 0F EC 52".
     * The purpose of this method is to make it easier to read and write test assertions.
     */
    @JvmStatic
    fun ByteArray.byteArrayToHexString(): String {
        return this.joinToString(" ") { it.toUByte().toString(16).padStart(2, '0') }
    }

    /**
     * Converts a string of decimal integers, such as "105 -9349549 0 -12 99999", to an int array.
     */
    @JvmStatic
    fun String.decimalStringToIntArray(): IntArray {
        return octetStringToIntArray(this, 10)
    }

    /**
     * Helper function for generating FlexUInt hex strings from an unsigned integer. Useful for test
     * cases that programmatically generate length-prefixed payloads.
     */
    @JvmStatic
    fun Int.toSingleHexByte(): String {
        return this.toUByte().toString(16).padStart(2, '0')
    }
}
