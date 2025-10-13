// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.bytecode.bin11

import com.amazon.ion.Decimal
import com.amazon.ion.Timestamp
import com.amazon.ion.bytecode.BytecodeGenerator
import com.amazon.ion.bytecode.bin11.opcodes.OpcodeHandlerTable
import com.amazon.ion.bytecode.util.AppendableConstantPoolView
import com.amazon.ion.bytecode.util.BytecodeBuffer
import com.amazon.ion.bytecode.util.byteToInt
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import java.math.BigInteger

@SuppressFBWarnings("EI_EXPOSE_REP2", justification = "constructor does not make a defensive copy of source as a performance optimization")
internal class ByteArrayBytecodeGenerator11(
    private val source: ByteArray,
    private var start: Int,
) : BytecodeGenerator {
    override fun refill(
        destination: BytecodeBuffer,
        constantPool: AppendableConstantPoolView,
        macroSrc: IntArray,
        macroIndices: IntArray,
        symTab: Array<String?>
    ) {
        val opcode = byteToInt(source[start])
        start += OpcodeHandlerTable.handler(opcode).convertOpcodeToBytecode(
            opcode,
            source,
            start,
            destination
        )
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
        TODO("Not yet implemented")
    }

    override fun readBytesReference(position: Int, length: Int): ByteArray {
        TODO("Not yet implemented")
    }

    override fun ionMinorVersion(): Int {
        return 1
    }
}

