// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin

import com.amazon.ion.*
import com.amazon.ion.IonEncodingVersion.*
import org.junit.jupiter.api.Assertions.assertEquals
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
}
