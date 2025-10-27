// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11.bytearray

import com.amazon.ion.TextToBinaryUtils.hexStringToByteArray
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.util.unsignedToInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class TimestampDecoderTest {

    @ParameterizedTest
    @CsvSource(
        "80 35,                          2023T",
        "81 35 05,                       2023-10T",
        "82 35 7D,                       2023-10-15T",
        "83 35 7D CB 0A,                 2023-10-15T11:22Z",
        "84 35 7D CB 1A 02,              2023-10-15T11:22:33Z",
        "84 35 7D CB 12 02,              2023-10-15T11:22:33-00:00",
        "85 35 7D CB 12 F2 06,           2023-10-15T11:22:33.444-00:00",
        "86 35 7D CB 12 2E 22 1B,        2023-10-15T11:22:33.444555-00:00",
        "87 35 7D CB 12 4A 86 FD 69,     2023-10-15T11:22:33.444555666-00:00",
        "88 35 7D CB EA 01,              2023-10-15T11:22+01:15",
        "89 35 7D CB EA 85,              2023-10-15T11:22:33+01:15",
        "8A 35 7D CB EA 85 BC 01,        2023-10-15T11:22:33.444+01:15",
        "8B 35 7D CB EA 85 8B C8 06,     2023-10-15T11:22:33.444555+01:15",
        "8C 35 7D CB EA 85 92 61 7F 1A,  2023-10-15T11:22:33.444555666+01:15",
    )
    fun `short timestamps are decoded correctly`(input: String, expectedValue: String) {
        val data = input.hexStringToByteArray()
        val opcode = data[0].unsignedToInt()
        val timestamp = TimestampDecoder.readShortTimestamp(data, 1, opcode)
        val expectedTimestamp = Timestamp.valueOf(expectedValue.trim())
        assertEquals(expectedTimestamp, timestamp)
    }
}
