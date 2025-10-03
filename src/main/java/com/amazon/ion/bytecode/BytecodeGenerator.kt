// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode

import com.amazon.ion.Decimal
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer
import java.math.BigInteger

/**
 * Abstracts a particular input source (e.g. [ByteArray], [ByteBuffer][java.nio.ByteBuffer]) and Ion version out
 * of the [BytecodeIonReader].
 *
 * The BytecodeGenerator serves as an adapter layer between the [BytecodeIonReader] and various Ion data sources,
 * enabling efficient reading of Ion data by converting it to bytecode instructions. This abstraction allows the
 * reader to work with different input formats and Ion versions while minimizing branching in the hot paths. It
 * also allows the source data to be read using a "push" model (in chunks), decoupling it from the "pull" model
 * of the IonReader API.
 *
 * ## Usage Pattern
 *
 * The typical interaction flow is:
 * 1. [BytecodeIonReader] calls [refill] to populate its bytecode buffer with instructions
 * 2. As the reader processes bytecode, it calls the appropriate `read*Reference` methods to resolve
 *    scalar values that reference the original source data
 * 3. When an IVM is encountered, [getGeneratorForMinorVersion] may be called to switch versions
 *
 * ## Implementation Notes
 *
 * In the future, it might be possible to push all the encoding context management into this layer, which _might_
 * provide some performance benefits by reducing the need to switch context between the BytecodeGenerator and the BytecodeIonReader.
 *
 * It would be possible to simplify this interface by replacing all the `read*` functions with a single function, such as this:
 * ```
 * fun <T> readReference(instruction: Int, operand: Int): T
 * ```
 * However, this seems like it would probably have a negative impact on the throughput of the reader because the call-site
 * for the function is already in a branch for that specific kind of reference, so the single function approach would
 * require additional branching inside the implementation of `readReference`.
 *
 * If we add references with int64 positions, add overrides of the `read*` methods that support a `long` position.
 */
internal interface BytecodeGenerator {

    // TODO: Does this method need to return the symbol table and/or constant pool as well?
    //       No, we're not going to update the encoding context in the bytecode generator.
    //       That might limit the applicability because the bytecode needs to contain directives
    //       for all possible Ion Versions... but we'll deal with that later.
    /**
     * Refills [destination] with one or more top-level user values and optionally
     * one system value. When a system value (symbol table, directive, IVM) is encountered,
     * no more top-level values may be filled into the destination.
     *
     * If there is incomplete data, and no values can be returned, it should
     * throw IncompleteDataException (if streaming source) or IonException (if
     * fully-buffered source).
     */
    fun refill(
        /** The BytecodeBuffer that is to be filled with the bytecode */
        destination: BytecodeBuffer,
        /** A container for holding instances of eagerly materialized values, such as those in template definitions. */
        constantPool: AppendableConstantPoolView,
        /** Bytecode for each macro in the effective macro table */
        macroSrc: IntArray,
        /**
         * A lookup table indicating for each macro address, where to find the first
         * instruction for that macro in [macroSrc]. For example, to read the bytecode for the macro,
         * you would do something like this:
         * ```
         * var i = macroIndices[macroAddress]
         * var currentInstruction = macroSrc[i++]
         * while (currentInstruction.
         * ```
         */
        macroIndices: IntArray,
        /** The current symbol table */
        symTab: Array<String?>,
    )

    fun readBigIntegerReference(position: Int, length: Int): BigInteger
    fun readDecimalReference(position: Int, length: Int): Decimal
    fun readShortTimestampReference(position: Int, opcode: Int): Timestamp
    fun readTimestampReference(position: Int, length: Int): Timestamp
    fun readTextReference(position: Int, length: Int): String
    fun readBytesReference(position: Int, length: Int): ByteArray

    /** The Ion Minor Version supported by this [BytecodeGenerator] */
    fun ionMinorVersion(): Int

    /**
     * When the [BytecodeIonReader] encounters an IVM that requires a version change, it will call this method.
     * Implementations must return a BytecodeGenerator that supports the requested version and is positioned
     * to continue compiling bytecode exactly where this BytecodeGenerator left off.
     */
    fun getGeneratorForMinorVersion(minorVersion: Int): BytecodeGenerator = throw UnsupportedOperationException()
}
