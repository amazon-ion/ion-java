// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import com.amazon.ion.facet.Facets;
import com.amazon.ion.system.IonReaderBuilder;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

public class TextSpanHoistingTest
{
    @Test
    public void hoistingTextSpanAlsoHoistsSymbolTable() throws IOException {
        String textWithSymbolTables =
                "$ion_1_0 $ion_symbol_table::{ symbols:[\"bar\"] } { foo1: $10 }" +
                "$ion_1_0 $ion_symbol_table::{ symbols:[\"baz\"] } { foo2: $10 }";

        IonReader reader = IonReaderBuilder.standard().build(textWithSymbolTables);
        SeekableReader seekableReader = Facets.asFacet(SeekableReader.class, reader);

        Assertions.assertEquals(IonType.STRUCT, reader.next());
        Span span = seekableReader.currentSpan();
        reader.stepIn();
        Assertions.assertEquals(IonType.SYMBOL, reader.next());
        Assertions.assertEquals("foo1", reader.getFieldName());
        Assertions.assertEquals("bar", reader.stringValue());
        reader.stepOut();

        Assertions.assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        Assertions.assertEquals(IonType.SYMBOL, reader.next());
        Assertions.assertEquals("foo2", reader.getFieldName());
        Assertions.assertEquals("baz", reader.stringValue());
        reader.stepOut();

        // now re-seek to the first value
        seekableReader.hoist(span);

        Assertions.assertEquals(IonType.STRUCT, reader.next());
        reader.stepIn();
        Assertions.assertEquals(IonType.SYMBOL, reader.next());
        Assertions.assertEquals("foo1", reader.getFieldName());

        // this assertion fails
        Assertions.assertEquals("bar", reader.stringValue());
        reader.stepOut();
    }
}
