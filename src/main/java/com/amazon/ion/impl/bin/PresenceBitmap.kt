// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.impl.macro.*
import com.amazon.ion.impl.macro.Macro.*
import java.nio.ByteBuffer

/**
 * Utility class for setting, storing, reading, and writing presence bits.
 *
 * This class provides an API that maps 1:1 with parameters, with a maximum of 128 parameters.
 *
 * ### Usage – Binary Writer
 * When stepping into an E-Expression, obtain a [PresenceBitmap] instance, [initialize] using the macro signature, and
 * then reserve the correct number of bytes (see [byteSize]) to later encode the presence bits.
 * While in the E-Expression, track the number of expressions or expression groups that have been written with that
 * E-Expression as the immediate parent—this is the _parameter_ index. For each expression or expression group that is
 * written directly in that container, call [PresenceBitmap.set] with the _parameter_ index and one of [VOID],
 * [EXPRESSION], or [GROUP]. To omit an argument, callers to the binary writer will need to write an empty expression
 * group (which should be elided and the corresponding presence bits set to `00`) or the binary writer must expose a
 * `writeNoExpression()` method or similar.
 * When stepping out of the E-Expression, use [PresenceBitmap.writeTo] to encode them into the appropriate location.
 *
 * ### Usage – Binary Reader
 * When stepping into an E-Expression, obtain a [PresenceBitmap] instance, [initialize] using the macro signature, ensure
 * that [byteSize] number of bytes is available in the reader's buffer, and call [readFrom] to populate the
 * [PresenceBitmap] instance. Then, the presence bits for each parameter can be accessed by its _parameter_ index.
 *
 * ### Implementation Notes
 *
 *  - We pretend that all parameters (including `!` (required) parameter) will get presence bits, and when reading we
 *    set the bits for the positions of the `!` parameters to `01` (single expression).
 *    - Since all the parameter cardinalities (other than `!`) use the same presence bit semantics, the writer doesn't
 *      need to inspect the signature to figure out what bits to put in our presence bits buffer.
 *    - Because we have dummy bits for `!` parameters, [PresenceBits] can present an API that corresponds 1:1 with
 *      parameters, so we don't need to separately keep track of a presence bit index and the parameter count.
 *  - Why longs instead of an array?
 *    - An array would add another level of indirection
 *    - An array would require a loop in order to reset all the bytes to zero.
 *  - Why only 128 parameters?
 *    - Until proven otherwise, we should not assume that an arbitrarily large number of parameters MUST be supported.
 *    - The number of parameters could be increased (within limits). It seems reasonable to try to keep this class small
 *      enough to fit in a single cache line for a modern system—typically 64 bytes.
 *
 * TODO: Consider whether we can "compile" a specific function that can read the presence bits when we compile a macro.
 *       That _might_ be more efficient than this approach.
 */
internal class PresenceBitmap {

    companion object {
        const val VOID = 0b00L
        const val EXPRESSION = 0b01L
        const val GROUP = 0b10L
        const val RESERVED = 0b11L

        private const val TWO_BIT_MASK = 0b11L
        private const val PRESENCE_BITS_SIZE_THRESHOLD = 2
        private const val PB_SLOTS_PER_BYTE = 4
        private const val PB_SLOTS_PER_LONG = 32
        private const val PB_BITS_PER_SLOT = 2

        const val MAX_SUPPORTED_PARAMETERS = PB_SLOTS_PER_LONG * 4
    }

    private var signature: List<Parameter> = emptyList()

    /** The number of parameters for which presence bits must be written. */
    private var size: Int = 0

    /** The total number of parameters in the macro signature */
    val totalParameterCount: Int
        get() = signature.size

    /** The first 32 presence bits slots */
    private var a: Long = 0
    /** The second 32 presence bits slots */
    private var b: Long = 0
    /** The third 32 presence bits slots */
    private var c: Long = 0
    /** The fourth 32 presence bits slots */
    private var d: Long = 0

    /** The number of bytes required to encode this [PresenceBitmap] */
    val byteSize: Int
        get() = size divideByRoundingUp PB_SLOTS_PER_BYTE

    /** Resets this [PresenceBitmap] for the given [macro]. */
    fun initialize(signature: List<Parameter>) {
        a = 0
        b = 0
        c = 0
        d = 0
        size = calculateNumPresenceBits(signature)
        this.signature = signature
    }

    /**
     * Checks that all presence bits are valid for their corresponding parameters.
     * Throws [IonException] if any are not.
     */
    fun validate() {
        signature.forEachIndexed { i, it ->
            val presenceValue = get(i)
            val isValid = when (it.cardinality) {
                ParameterCardinality.AtMostOne -> presenceValue == VOID || presenceValue == EXPRESSION
                ParameterCardinality.One -> presenceValue == EXPRESSION
                ParameterCardinality.AtLeastOne -> presenceValue == EXPRESSION || presenceValue == GROUP
                ParameterCardinality.Any -> presenceValue != RESERVED
            }
            if (!isValid) throw IonException("Invalid argument for parameter: $it")
        }
    }

    /**
     * Populates this [PresenceBitmap] from the given [ByteBuffer] that is positioned on the first
     * byte that (potentially) contains presence bits.
     *
     * When complete, the buffer is positioned on the first byte that does not contain presence bits.
     */
    fun readFrom(bytes: ByteArray, startInclusive: Int) {
        var currentByte: Byte = -1
        var currentPosition: Int = startInclusive
        var bitmapIndex = 0

        signature.forEachIndexed { i, it ->
            if (it.cardinality == ParameterCardinality.One) {
                set(i, EXPRESSION)
            } else {
                if (bitmapIndex % PB_SLOTS_PER_BYTE == 0) {
                    currentByte = bytes[currentPosition++]
                }
                val pbValue = ((currentByte.toLong()) shr ((bitmapIndex % PB_SLOTS_PER_BYTE) * PB_BITS_PER_SLOT)) and TWO_BIT_MASK
                set(i, pbValue)
                bitmapIndex++
            }
        }
    }

    /** Calculates the actual number of presence bits that will be encoded for the given signature. */
    private fun calculateNumPresenceBits(signature: List<Parameter>): Int {
        if (signature.size > MAX_SUPPORTED_PARAMETERS) throw IonException("Macros with more than 128 parameters are not supported by this implementation.")
        val nonRequiredParametersCount = signature.count { it.cardinality != ParameterCardinality.One }
        val usePresenceBits = nonRequiredParametersCount > PRESENCE_BITS_SIZE_THRESHOLD || signature.any { it.type.isTagless }
        return if (usePresenceBits) nonRequiredParametersCount else 0
    }

    /**
     * Gets by _parameter_ index, which includes _required_ parameters that have no presence bits.
     * The slots corresponding to a required parameter with always return [RESERVED].
     */
    operator fun get(index: Int): Long {
        if (index >= totalParameterCount || index < 0) throw IndexOutOfBoundsException("$index")
        val bits = when (index / PB_SLOTS_PER_LONG) {
            0 -> a
            1 -> b
            2 -> c
            3 -> d
            else -> TODO("Unreachable")
        }
        val shift = (index % PB_SLOTS_PER_LONG) * PB_BITS_PER_SLOT
        return (bits shr shift) and TWO_BIT_MASK
    }

    /**
     * Sets a presence bits "slot" using bitwise OR with the existing contents.
     *
     * It is not possible to reset individual presence bits, nor
     * is it possible to change the presence bits for a required parameter.
     */
    operator fun set(index: Int, value: Long) {
        if (index >= totalParameterCount || index < 0) throw IndexOutOfBoundsException("$index")
        val shiftedBits = (value shl ((index % PB_SLOTS_PER_LONG) * PB_BITS_PER_SLOT))
        when (index / PB_SLOTS_PER_LONG) {
            0 -> a = a or shiftedBits
            1 -> b = b or shiftedBits
            2 -> c = c or shiftedBits
            3 -> d = d or shiftedBits
            else -> TODO("Unreachable")
        }
    }

    /**
     * Writes this [PresenceBitmap] to [buffer] at the given [position].
     */
    fun writeTo(buffer: WriteBuffer, position: Long) {
        if (size == 0) return
        var resultBuffer = 0L
        var resultPosition = 0
        var writePosition = position
        signature.forEachIndexed { i, it ->
            if (it.cardinality == ParameterCardinality.One) return@forEachIndexed
            val bits = get(i)
            val destShift = resultPosition * PB_BITS_PER_SLOT
            resultBuffer = resultBuffer or (bits shl destShift)
            resultPosition++
            if (resultPosition == PB_SLOTS_PER_LONG) {
                buffer.writeFixedIntOrUIntAt(writePosition, resultBuffer, Long.SIZE_BYTES)
                writePosition += Long.SIZE_BYTES
                resultPosition = 0
                resultBuffer = 0
            }
        }

        val numBytes = resultPosition divideByRoundingUp PB_SLOTS_PER_BYTE
        if (numBytes > 0) buffer.writeFixedIntOrUIntAt(writePosition, resultBuffer, numBytes)
    }

    /**
     * Integer division that rounds up instead of down.
     * E.g.:
     *   - 0/4 = 0
     *   - 1/4 = 1
     *   - ...
     *   - 4/4 = 1
     *   - 5/4 = 2
     */
    private infix fun Int.divideByRoundingUp(other: Int): Int = (this + (other - 1)) / other
}
