// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11

import com.amazon.ion.Decimal
import com.amazon.ion.IonException
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.BytecodeGenerator
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTable
import com.amazon.ion.bytecode.bin11.bytearray.PrimitiveDecoder.readFixedIntAsBigInteger
import com.amazon.ion.bytecode.bin11.bytearray.TimestampDecoder
import com.amazon.ion.bytecode.ir.Instructions
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.ByteSlice
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.unsignedToInt
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoder
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoderPool
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.math.BigInteger
import java.nio.ByteBuffer

@SuppressFBWarnings("EI_EXPOSE_REP2", justification = "constructor does not make a defensive copy of source as a performance optimization")
internal class ByteArrayBytecodeGenerator11(
    private val source: ByteArray,
    private var currentPosition: Int,
) : BytecodeGenerator {
    private val utf8Decoder: Utf8StringDecoder = Utf8StringDecoderPool.getInstance().orCreate

    override fun refill(
        destination: BytecodeBuffer,
        constantPool: AppendableConstantPoolView,
        macroSrc: IntArray,
        macroIndices: IntArray,
        symTab: Array<String?>
    ) {
        var opcode = 0
        while (currentPosition < source.size && !isSystemValue(opcode)) {
            opcode = source[currentPosition++].unsignedToInt()
            val handler = OpcodeHandlerTable.handler(opcode)
            currentPosition += handler.convertOpcodeToBytecode(
                opcode,
                source,
                currentPosition,
                destination,
                constantPool,
                macroSrc,
                macroIndices,
                symTab
            )
        }

        if (currentPosition < source.size) {
            destination.add(Instructions.I_REFILL)
        } else {
            destination.add(Instructions.I_END_OF_INPUT)
        }
    }

    override fun readBigIntegerReference(position: Int, length: Int): BigInteger {
        return readFixedIntAsBigInteger(source, position, length)
    }

    override fun readDecimalReference(position: Int, length: Int): Decimal {
        TODO("Not yet implemented")
    }

    override fun readShortTimestampReference(position: Int, opcode: Int): Timestamp {
        return TimestampDecoder.readShortTimestamp(source, position, opcode)
    }

    override fun readTimestampReference(position: Int, length: Int): Timestamp {
        TODO("Not yet implemented")
    }

    override fun readTextReference(position: Int, length: Int): String {
        val buffer = ByteBuffer.wrap(source, position, length)
        return utf8Decoder.decode(buffer, length)
    }

    override fun readBytesReference(position: Int, length: Int): ByteSlice {
        return ByteSlice(source, position, position + length)
    }

    override fun ionMinorVersion(): Int {
        return 1
    }

    override fun getGeneratorForMinorVersion(minorVersion: Int): BytecodeGenerator {
        return when (minorVersion) {
            1 -> ByteArrayBytecodeGenerator11(source, currentPosition)
            // TODO: update with ByteArrayBytecodeGenerator10 once it implements BytecodeGenerator
            else -> throw IonException("Minor version $minorVersion not yet implemented for ByteArray-backed data sources.")
        }
    }

    private fun isSystemValue(opcode: Int): Boolean {
        return opcode in 0xE0..0xE8
    }
}
