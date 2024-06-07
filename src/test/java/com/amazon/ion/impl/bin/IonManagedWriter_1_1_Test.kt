// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.IonEncodingVersion.*
import com.amazon.ion.impl.*
import com.amazon.ion.system.*
import java.io.ByteArrayOutputStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class IonManagedWriter_1_1_Test {

    val appendable = StringBuilder()
    val writer = ION_1_1.textWriterBuilder()
        .withSymbolInliningStrategy(SymbolInliningStrategy.ALWAYS_INLINE)
        .build(appendable) as IonManagedWriter_1_1

    @Test
    fun `attempting to manually write a symbol table throws an exception`() {
        writer.addTypeAnnotation(SystemSymbols.ION_SYMBOL_TABLE)
        assertThrows<IonException> { writer.stepIn(IonType.STRUCT) }
    }

    @Test
    fun `attempting to step into a scalar type throws an exception`() {
        assertThrows<IllegalArgumentException> { writer.stepIn(IonType.NULL) }
    }

    @Test
    fun `write an IVM`() {
        writer.writeIonVersionMarker()
        writer.close()
        assertEquals("\$ion_1_1 \$ion_1_1", appendable.toString().trim())
    }

    @Test
    fun `write an IVM in a container should write a symbol`() {
        with(writer) {
            stepIn(IonType.LIST)
            writeIonVersionMarker()
            stepOut()
            close()
        }
        assertEquals("\$ion_1_1 [\$ion_1_1]", appendable.toString().trim())
    }

    private fun `transform symbol IDS`(writeValuesFn: _Private_IonWriter.(IonReader) -> Unit) {
        // Craft the input data: {a: b::c}, encoded as {$10: $11::$12}
        val input = ByteArrayOutputStream()
        ION_1_0.binaryWriterBuilder().build(input).use {
            it.stepIn(IonType.STRUCT)
            it.setFieldName("a")
            it.addTypeAnnotation("b")
            it.writeSymbol("c")
            it.stepOut()
        }
        // Do a system-level transcode of the Ion 1.0 data to Ion 1.1, adding 32 to each local symbol ID.
        val system = IonSystemBuilder.standard().build() as _Private_IonSystem
        val output = ByteArrayOutputStream()
        system.newSystemReader(input.toByteArray()).use { reader ->
            (ION_1_1.binaryWriterBuilder().build(output) as _Private_IonWriter).use {
                it.writeValuesFn(reader)
            }
        }
        // Verify the transformed symbol IDs using another system read.
        system.newSystemReader(output.toByteArray()).use {
            while (it.next() == IonType.SYMBOL) {
                assertEquals("\$ion_1_1", it.stringValue())
            }
            assertEquals(IonType.STRUCT, it.next())
            it.stepIn()
            assertEquals(IonType.SYMBOL, it.next())
            assertEquals(42, it.fieldNameSymbol.sid)
            assertEquals(43, it.typeAnnotationSymbols[0].sid)
            assertEquals(44, it.symbolValue().sid)
            assertNull(it.next())
            it.stepOut()
        }
    }

    @Test
    fun `use writeValues to transform symbol IDS`() {
        `transform symbol IDS` { reader ->
            writeValues(reader) { sid -> sid + 32 }
        }
    }

    @Test
    fun `use writeValue to transform symbol IDS`() {
        `transform symbol IDS` { reader ->
            while (reader.next() != null) {
                writeValue(reader) { sid -> sid + 32 }
            }
        }
    }
}
