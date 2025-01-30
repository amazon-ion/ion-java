// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.impl.bin.PresenceBitmap.Companion.EXPRESSION
import com.amazon.ion.impl.bin.PresenceBitmap.Companion.GROUP
import com.amazon.ion.impl.bin.PresenceBitmap.Companion.VOID
import com.amazon.ion.impl.macro.*
import com.amazon.ion.impl.macro.Macro.*
import com.amazon.ion.impl.macro.MacroEvaluator.*
import java.util.*

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
internal class PresenceBitmap(
    var signature: List<Parameter>,
    /** The number of parameters for which presence bits must be written. */
    private var size: Int,
    /** The total number of parameters in the macro signature */
    var totalParameterCount: Int
) {

    constructor() : this(emptyList(), 0, 0)

    companion object {
        const val VOID = 0b00L
        const val EXPRESSION = 0b01L
        const val GROUP = 0b10L
        const val RESERVED = 0b11L

        private const val TWO_BIT_MASK = 0b11L
        private const val PRESENCE_BITS_SIZE_THRESHOLD = 0
        private const val PB_SLOTS_PER_BYTE = 4
        private const val PB_SLOTS_PER_LONG = 32
        private const val PB_BITS_PER_SLOT = 2

        const val MAX_SUPPORTED_PARAMETERS = PB_SLOTS_PER_LONG * 4

        private val ZERO_PARAMETERS: PresenceBitmap = allRequired(0) { PresenceBitmap() }
        private val ONE_REQUIRED_PARAMETER: PresenceBitmap = allRequired(1) { PresenceBitmap() }
        private val TWO_REQUIRED_PARAMETERS: PresenceBitmap = allRequired(2) { PresenceBitmap() }
        private val THREE_REQUIRED_PARAMETERS: PresenceBitmap = allRequired(3) { PresenceBitmap() }
        private val FOUR_REQUIRED_PARAMETERS: PresenceBitmap = allRequired(4) { PresenceBitmap() }

        /** Pool for PresenceBitmap instances. */
        class PooledFactory {

            private var index = 0

            private var pool: Array<PresenceBitmap?> = Array(32) { null }

            /** Gets an instance from the pool, allocating a new one only if necessary. The returned instance must be reset. */
            fun get(): PresenceBitmap {
                if (index >= pool.size) {
                    pool = pool.copyOf(pool.size * 2)
                }
                if (pool[index] == null) {
                    pool[index] = PresenceBitmap()
                }
                return pool[index++]!!
            }

            /** Clears the pool. Calling this method invalidates all instances previously returned by [get]. */
            fun clear() {
                index = 0
            }
        }

        /** Creates a PresenceBitmap for the given number of required parameters */
        private inline fun allRequired(numberOfParameters: Int, supplier: () -> PresenceBitmap): PresenceBitmap {
            if (numberOfParameters > MAX_SUPPORTED_PARAMETERS) throw IonException("Macros with more than 128 parameters are not supported by this implementation.")
            val bitmap = supplier().reset(emptyList(), 0, numberOfParameters)
            for (i in 0 until numberOfParameters) {
                bitmap.setUnchecked(i, EXPRESSION)
            }
            return bitmap
        }

        /** Creates or reuses a [PresenceBitmap] for the given signature. */
        @JvmStatic
        fun create(signature: List<Parameter>, pool: PooledFactory): PresenceBitmap {
            if (signature.size > MAX_SUPPORTED_PARAMETERS) throw IonException("Macros with more than 128 parameters are not supported by this implementation.")
            // Calculate the actual number of presence bits that will be encoded for the given signature.
            var nonRequiredParametersCount = 0
            var usePresenceBits = false
            for (i in signature.indices) {
                val parameter = signature[i]
                if (parameter.cardinality != ParameterCardinality.ExactlyOne) {
                    nonRequiredParametersCount++
                }
                usePresenceBits = usePresenceBits or (parameter.type.taglessEncodingKind != null)
            }
            usePresenceBits = usePresenceBits or (nonRequiredParametersCount > PRESENCE_BITS_SIZE_THRESHOLD)
            val size = if (usePresenceBits) nonRequiredParametersCount else 0
            return if (size > 0) {
                pool.get().reset(signature, nonRequiredParametersCount, signature.size)
            } else {
                when (signature.size) {
                    0 -> ZERO_PARAMETERS
                    1 -> ONE_REQUIRED_PARAMETER
                    2 -> TWO_REQUIRED_PARAMETERS
                    3 -> THREE_REQUIRED_PARAMETERS
                    4 -> FOUR_REQUIRED_PARAMETERS
                    else -> allRequired(signature.size, pool::get)
                }
            }
        }
    }

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

    /**
     * Resets this [PresenceBitmap] for the given signature, size, and parameter count. After this method is called,
     * callers must set the bitmap as necessary, such as by calling [readFrom].
     */
    fun reset(signature: List<Parameter>, size: Int, totalParameterCount: Int): PresenceBitmap {
        this.signature = signature
        this.size = size
        this.totalParameterCount = totalParameterCount
        a = 0
        b = 0
        c = 0
        d = 0
        return this
    }

    /** Resets this [PresenceBitmap] for the given signature. */
    fun initialize(signature: List<Parameter>) {
        if (signature.size > MAX_SUPPORTED_PARAMETERS) throw IonException("Macros with more than 128 parameters are not supported by this implementation.")
        this.signature = signature
        a = 0
        b = 0
        c = 0
        d = 0
        // TODO – performance: consider calculating this once for a macro when it is compiled
        // Calculate the actual number of presence bits that will be encoded for the given signature.
        val nonRequiredParametersCount = signature.count { it.cardinality != ParameterCardinality.ExactlyOne }
        val usePresenceBits = nonRequiredParametersCount > PRESENCE_BITS_SIZE_THRESHOLD || signature.any { it.type.taglessEncodingKind != null }
        size = if (usePresenceBits) nonRequiredParametersCount else 0
        totalParameterCount = signature.size
    }

    /**
     * Checks that all presence bits are valid for their corresponding parameters.
     * Throws [IonException] if any are not.
     */
    fun validate() {
        val parameters = signature.iterator()
        var i = 0
        while (parameters.hasNext()) {
            val p = parameters.next()
            val v = getUnchecked(i++)
            val isValid = when (p.cardinality) {
                ParameterCardinality.ZeroOrOne -> v == VOID || v == EXPRESSION
                ParameterCardinality.ExactlyOne -> v == EXPRESSION
                ParameterCardinality.OneOrMore -> v == EXPRESSION || v == GROUP
                ParameterCardinality.ZeroOrMore -> v != RESERVED
            }
            if (!isValid) throw IonException("Invalid argument for parameter: $p")
        }
    }

    /**
     * Populates this [PresenceBitmap] from the given [ByteArray] that is positioned on the first
     * byte that (potentially) contains presence bits.
     *
     * When complete, the buffer is positioned on the first byte that does not contain presence bits.
     */
    fun readFrom(bytes: ByteArray, startInclusive: Int) {
        // Doesn't always contain the full byte. We shift the bits over every time we read a value
        // so that the next value is always the least significant bits.
        var currentByte: Long = -1
        var currentPosition: Int = startInclusive
        var bitmapIndex = 0
        var i = 0

        val parameters = signature.iterator()
        while (parameters.hasNext()) {
            val p = parameters.next()
            if (p.cardinality == ParameterCardinality.ExactlyOne) {
                setUnchecked(i++, EXPRESSION)
            } else {
                if (bitmapIndex % PB_SLOTS_PER_BYTE == 0) {
                    currentByte = bytes[currentPosition++].toLong()
                }
                setUnchecked(i++, currentByte and TWO_BIT_MASK)
                currentByte = currentByte shr PB_BITS_PER_SLOT
                bitmapIndex++
            }
        }
    }

    /**
     * Gets by _parameter_ index, which includes _required_ parameters that have no presence bits.
     * The slots corresponding to a required parameter with always return [RESERVED].
     */
    operator fun get(index: Int): Long {
        if (index >= totalParameterCount || index < 0) throw IndexOutOfBoundsException("$index")
        return getUnchecked(index)
    }

    /** Gets a presence bits "slot" without any bounds checking. See [get]. */
    private inline fun getUnchecked(index: Int): Long {
        val shift = (index % PB_SLOTS_PER_LONG) * PB_BITS_PER_SLOT
        when (index / PB_SLOTS_PER_LONG) {
            0 -> return (a shr shift) and TWO_BIT_MASK
            1 -> return (b shr shift) and TWO_BIT_MASK
            2 -> return (c shr shift) and TWO_BIT_MASK
            3 -> return (d shr shift) and TWO_BIT_MASK
            else -> TODO("Unreachable")
        }
    }

    /**
     * Sets a presence bits "slot" using bitwise OR with the existing contents.
     *
     * It is not possible to reset individual presence bits, nor
     * is it possible to change the presence bits for a required parameter.
     */
    operator fun set(index: Int, value: Long) {
        if (index >= totalParameterCount || index < 0) throw IndexOutOfBoundsException("$index")
        setUnchecked(index, value)
    }

    /** Sets a presence bits "slot" without any bounds checking. See [set]. */
    private inline fun setUnchecked(index: Int, value: Long) {
        val shiftedBits = (value shl ((index % PB_SLOTS_PER_LONG) * PB_BITS_PER_SLOT))
        when (index / PB_SLOTS_PER_LONG) {
            0 -> a = a or shiftedBits
            1 -> b = b or shiftedBits
            2 -> c = c or shiftedBits
            3 -> d = d or shiftedBits
        }
    }

    /**
     * Writes this [PresenceBitmap] to [buffer] at the given [position].
     */
    fun writeTo(buffer: WriteBuffer, position: Long) {
        if (size == 0) return
        var resultBuffer: Long = 0
        var resultPosition = 0
        var writePosition = position
        var i = 0
        val parameters = signature.iterator()

        while (parameters.hasNext()) {
            val parameter = parameters.next()
            val bits = getUnchecked(i++)
            if (parameter.cardinality == ParameterCardinality.ExactlyOne) continue
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
