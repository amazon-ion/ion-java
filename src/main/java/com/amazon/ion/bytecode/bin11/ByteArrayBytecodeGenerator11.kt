// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11

import com.amazon.ion.Decimal
import com.amazon.ion.IonException
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.BytecodeEmitter
import com.amazon.ion.bytecode.BytecodeGenerator
import com.amazon.ion.bytecode.bin11.bytearray.OpcodeHandlerTable
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
    private var i: Int,
) : BytecodeGenerator {
    private val decoder: Utf8StringDecoder = Utf8StringDecoderPool.getInstance().orCreate

    override fun refill(
        destination: BytecodeBuffer,
        constantPool: AppendableConstantPoolView,
        macroSrc: IntArray,
        macroIndices: IntArray,
        symTab: Array<String?>
    ) {
        // For now, write a single instruction to the bytecode buffer, plus the refill or EOF instruction.
        // The strategy here will need to be revisited.
        val opcode = source[i++].unsignedToInt()
        val handler = OpcodeHandlerTable.handler(opcode)
        i += handler.convertOpcodeToBytecode(
            opcode,
            source,
            i,
            destination,
            constantPool,
            macroSrc,
            macroIndices,
            symTab
        )

        // Emit the refill or end of input instruction so caller knows what to do once they run out
        // of bytecode in the buffer.
        if (i < source.size) {
            BytecodeEmitter.emitRefill(destination)
        } else {
            BytecodeEmitter.emitEndOfInput(destination)
        }
    }

    override fun readBigIntegerReference(position: Int, length: Int): BigInteger {
        TODO("Not yet implemented")
    }

    override fun readDecimalReference(position: Int, length: Int): Decimal {
        TODO("Not yet implemented")
    }

    override fun readShortTimestampReference(position: Int, opcode: Int): Timestamp {
        TODO("Not yet implemented")
    }

    override fun readTimestampReference(position: Int, length: Int): Timestamp {
        TODO("Not yet implemented")
    }

    override fun readTextReference(position: Int, length: Int): String {
        val buffer = ByteBuffer.wrap(source, position, length)
        return decoder.decode(buffer, length)
    }

    override fun readBytesReference(position: Int, length: Int): ByteSlice {
        return ByteSlice(source, position, position + length - 1)
    }

    override fun ionMinorVersion(): Int {
        return 1
    }

    override fun getGeneratorForMinorVersion(minorVersion: Int): BytecodeGenerator {
        return when (minorVersion) {
            1 -> ByteArrayBytecodeGenerator11(source, i)
            // TODO: update with ByteArrayBytecodeGenerator10 once it implements BytecodeGenerator
            else -> throw IonException("Minor version $minorVersion not yet implemented for ByteArray-backed data sources.")
        }
    }
}
