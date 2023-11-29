/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.bin;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.amazon.ion.system.IonBinaryWriterBuilder;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class IonManagedBinaryWriterTest extends IonManagedBinaryWriterTestCase
{
    private ByteArrayOutputStream source = new ByteArrayOutputStream();
    private ByteArrayOutputStream expectedOut = new ByteArrayOutputStream();
    private ByteArrayOutputStream source_32K = new ByteArrayOutputStream();
    private ByteArrayOutputStream expectedOut_32K = new ByteArrayOutputStream();
    private ByteArrayOutputStream source_67K = new ByteArrayOutputStream();
    private ByteArrayOutputStream expectedOut_67K = new ByteArrayOutputStream();
    @Before
    public void generateTestData() throws IOException {
        // Writing test data with continuous extending symbol table. After completing writing 3300th value, the local symbol table will stop growing.
        IonWriter defaultWriter = IonBinaryWriterBuilder.standard().build(source);
        int i = 0;
        while (i < 3300) {
            defaultWriter.stepIn(IonType.STRUCT);
            defaultWriter.setFieldName("taco" + i);
            defaultWriter.writeString("burrito");
            defaultWriter.stepOut();
            i++;
        }
        while (i >= 3300 && i < 3400) {
            defaultWriter.stepIn(IonType.STRUCT);
            defaultWriter.setFieldName("taco" + 3);
            defaultWriter.writeString("burrito");
            defaultWriter.stepOut();
            i++;
        }
        defaultWriter.close();
        IonReader reader = system().newReader(source.toByteArray());
        // While writing the 2990th data structure, the cumulative size of the written data will exceed the current block size.
        // If auto-flush enabled, the flush() operation will be executed after completing the 2990th struct. The output should be the same as manually flush after writing the 2990th data.
        int flushPeriod = 2990;
        int index = 0;

        IonWriter expectedWriter = IonBinaryWriterBuilder.standard().withLocalSymbolTableAppendEnabled().build(expectedOut);
        while (reader.next() != null) {
            expectedWriter.writeValue(reader);
            index++;
            if (index == flushPeriod) {
                expectedWriter.flush();
                index = 0;
            }
        }
        expectedWriter.finish();
    }

    @Before
    public void generateTestData32K() throws IOException {
        // Writing test data with continuous extending symbol table. The total data written in user's block is 32K.
        IonWriter defaultWriter = IonBinaryWriterBuilder.standard().build(source_32K);
        int i = 0;
        while (i < 2990) {
            defaultWriter.stepIn(IonType.STRUCT);
            defaultWriter.setFieldName("taco" + i);
            defaultWriter.writeString("burrito");
            defaultWriter.stepOut();
            i++;
        }
        defaultWriter.close();
        IonReader reader = system().newReader(source_32K.toByteArray());
        // No flush should be expected since the total data size written into user's buffer can fit into one block.
        IonWriter expectedWriter = IonBinaryWriterBuilder.standard().withLocalSymbolTableAppendEnabled().build(expectedOut_32K);
        while (reader.next() != null) {
            expectedWriter.writeValue(reader);
        }
        expectedWriter.close();
    }
    @Before
    public void generateTestData67K() throws IOException {
        // Writing test data with continuous extending symbol table. After completing writing 3200th value, the local symbol table will stop growing.
        IonWriter defaultWriter = IonBinaryWriterBuilder.standard().build(source_67K);
        int i = 0;
        while (i < 3200) {
            defaultWriter.stepIn(IonType.STRUCT);
            defaultWriter.setFieldName("taco" + i);
            defaultWriter.writeString("burrito");
            defaultWriter.stepOut();
            i++;
        }
        while (i >= 3200 && i < 6400) {
            defaultWriter.stepIn(IonType.STRUCT);
            defaultWriter.setFieldName("taco" + 3);
            defaultWriter.writeString("burrito");
            defaultWriter.stepOut();
            i++;
        }
        defaultWriter.close();
        IonReader reader = system().newReader(source_67K.toByteArray());
        // After writing the 2990th data, the cumulative size of the written data in user's buffer will exceed the current block size.
        // After writing the 6246th data, the cumulative size of the written data user's buffer will exceed the current block size.
        // If auto-flush enabled, the flush() operation will be executed after completing the 2990th data and 6246th data. The output should be the same as manually flush after writing the 2990th and 6246th data.
        int first_flush = 2990;
        int second_flush = 6246;
        int index = 0;
        IonWriter expectedWriter = IonBinaryWriterBuilder.standard().withLocalSymbolTableAppendEnabled().build(expectedOut_67K);
        while (reader.next() != null) {
            expectedWriter.writeValue(reader);
            index++;
            if (index == first_flush || index == second_flush) {
                expectedWriter.flush();
            }
        }
        expectedWriter.close();
    }

    @Test
    public void testSetStringAnnotations() throws Exception
    {
        writer.setTypeAnnotations("a", "b", "c", "d", "e", "z");
        writer.writeInt(1);
        assertValue("a::b::c::d::e::z::1");
    }

    @Test
    public void testAddStringAnnotation() throws Exception
    {
        writer.addTypeAnnotation("a");
        writer.addTypeAnnotation("b");
        writer.writeInt(1);
        assertValue("a::b::1");
    }

    @Test
    public void testUserSymbol() throws Exception
    {
        writer.writeSymbol("hello");
        assertValue("hello");
    }

    @Test
    public void testSystemSymbol() throws Exception
    {
        writer.writeSymbol("name");
        assertValue("name");
    }

    @Test
    public void testLocalSymbolTableAppend() throws Exception
    {
        writer.writeSymbol("taco");
        writer.flush();
        writer.writeSymbol("burrito");
        writer.finish();

        IonReader reader = system().newReader(writer.getBytes());
        reader.next();
        assertEquals(reader.getSymbolTable().findSymbol("taco"), 15);
        assertEquals(reader.getSymbolTable().findSymbol("burrito"), lstAppendMode.isEnabled() ? -1 : 16);
        reader.next();
        assertEquals(reader.getSymbolTable().findSymbol("taco"), 15);
        assertEquals(reader.getSymbolTable().findSymbol("burrito"), 16);
        assertNull(reader.next());

        IonDatagram dg = system().getLoader().load(writer.getBytes());
        assertEquals(2, dg.size());
        assertEquals("taco", ((IonSymbol) dg.get(0)).stringValue());
        assertEquals("burrito", ((IonSymbol) dg.get(1)).stringValue());
    }

    @Test
    public void testFlushImmediatelyAfterIVM() throws Exception
    {
        writer.flush();
        writer.writeSymbol("burrito");
        writer.finish();
        IonReader reader = system().newReader(writer.getBytes());
        reader.next();
        assertEquals(reader.getSymbolTable().findSymbol("taco"), -1);
        assertEquals(reader.getSymbolTable().findSymbol("burrito"), 15);
        assertNull(reader.next());

        IonDatagram dg = system().getLoader().load(writer.getBytes());
        // Should be IVM SYMTAB burrito
        assertEquals(3, dg.systemSize());
        assertEquals("$ion_symbol_table", ((IonStruct) dg.systemGet(1)).getTypeAnnotations()[0]);
        assertEquals("burrito", ((IonSymbol) dg.systemGet(2)).stringValue());
    }

    @Test
    public void testAutoFlush() throws Exception{
        IonReader reader = system().newReader(source.toByteArray());
        ByteArrayOutputStream actual = new ByteArrayOutputStream();
        IonWriter actualWriter = IonBinaryWriterBuilder.standard().withAutoFlushEnabled(true).build(actual);
        while (reader.next() != null) {
            actualWriter.writeValue(reader);
        }
        actualWriter.close();
        if (lstAppendMode.isEnabled() && autoFlushMode.isEnabled()) {
            assertArrayEquals(actual.toByteArray(), expectedOut.toByteArray());
        }
        assertArrayEquals(actual.toByteArray(), expectedOut.toByteArray());
    }

    @Test
    public void testAutoFlush_32K() throws Exception{

        IonReader reader = system().newReader(source_32K.toByteArray());
        ByteArrayOutputStream actual = new ByteArrayOutputStream();
        IonWriter actualWriter = IonBinaryWriterBuilder.standard().withAutoFlushEnabled(true).build(actual);
        while (reader.next() != null) {
            actualWriter.writeValue(reader);
        }
        actualWriter.close();
        if (lstAppendMode.isEnabled() && autoFlushMode.isEnabled()) {
            assertArrayEquals(actual.toByteArray(), expectedOut_32K.toByteArray());
        }
        assertArrayEquals(actual.toByteArray(), expectedOut_32K.toByteArray());
    }

    @Test
    public void testAutoFlush_67K() throws Exception{
        IonReader reader = system().newReader(source_67K.toByteArray());
        ByteArrayOutputStream actual = new ByteArrayOutputStream();
        IonWriter actualWriter = IonBinaryWriterBuilder.standard().withAutoFlushEnabled(true).build(actual);
        while (reader.next() != null) {
            actualWriter.writeValue(reader);
        }
        actualWriter.close();
        if (lstAppendMode.isEnabled() && autoFlushMode.isEnabled()) {
            assertArrayEquals(actual.toByteArray(), expectedOut_67K.toByteArray());
        }
        assertArrayEquals(actual.toByteArray(), expectedOut_67K.toByteArray());
    }

    @Test
    public void testNoNewSymbolsAfterFlush() throws Exception
    {
        writer.writeSymbol("taco");
        writer.flush();
        writer.writeInt(123);
        writer.finish();

        IonDatagram dg = system().getLoader().load(writer.getBytes());
        // Should be IVM SYMTAB taco 123
        assertEquals(4, dg.systemSize());
        assertEquals("$ion_symbol_table", ((IonStruct) dg.systemGet(1)).getTypeAnnotations()[0]);
        assertEquals("taco", ((IonSymbol) dg.systemGet(2)).stringValue());
        assertEquals(123, ((IonInt) dg.systemGet(3)).intValue());
    }

    @Test
    public void testManuallyWriteLSTAppendWithImportsFirst() throws Exception
    {
        writer.writeSymbol("taco");
        writer.addTypeAnnotation("$ion_symbol_table");
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("imports");
        writer.writeSymbol("$ion_symbol_table");
        writer.setFieldName("symbols");
        writer.stepIn(IonType.LIST);
        writer.writeString("burrito");
        writer.stepOut();
        writer.stepOut();
        writer.writeSymbol("burrito");
        writer.finish();

        IonReader reader = system().newReader(writer.getBytes());
        reader.next();
        assertEquals(reader.getSymbolTable().findSymbol("taco"), 15);
        assertEquals(reader.getSymbolTable().findSymbol("burrito"), lstAppendMode.isEnabled() ? -1 : 16);
        reader.next();
        assertEquals(reader.getSymbolTable().findSymbol("taco"), 15);
        assertEquals(reader.getSymbolTable().findSymbol("burrito"), 16);
        assertNull(reader.next());

        IonDatagram dg = system().getLoader().load(writer.getBytes());
        assertEquals(2, dg.size());
        assertEquals("taco", ((IonSymbol) dg.get(0)).stringValue());
        assertEquals("burrito", ((IonSymbol) dg.get(1)).stringValue());
    }

    @Test
    public void testManuallyWriteLSTAppendWithSymbolsFirst() throws Exception
    {
        writer.writeSymbol("taco");
        writer.addTypeAnnotation("$ion_symbol_table");
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("symbols");
        writer.stepIn(IonType.LIST);
        writer.writeString("burrito");
        writer.stepOut();
        writer.setFieldName("imports");
        writer.writeSymbol("$ion_symbol_table");
        writer.stepOut();
        writer.writeSymbol("burrito");
        writer.finish();

        IonReader reader = system().newReader(writer.getBytes());
        reader.next();
        assertEquals(reader.getSymbolTable().findSymbol("taco"), 15);
        assertEquals(reader.getSymbolTable().findSymbol("burrito"), lstAppendMode.isEnabled() ? -1 : 16);
        reader.next();
        assertEquals(reader.getSymbolTable().findSymbol("taco"), 15);
        assertEquals(reader.getSymbolTable().findSymbol("burrito"), 16);
        assertNull(reader.next());

        IonDatagram dg = system().getLoader().load(writer.getBytes());
        assertEquals(2, dg.size());
        assertEquals("taco", ((IonSymbol) dg.get(0)).stringValue());
        assertEquals("burrito", ((IonSymbol) dg.get(1)).stringValue());
    }

    @Test
    public void testUserFieldNames() throws Exception
    {
        writer.stepIn(IonType.STRUCT);
        {
            writer.setFieldName("a");
            writer.writeInt(1);

            writer.setFieldName("b");
            writer.writeInt(2);

            writer.setFieldName("c");
            writer.writeInt(3);

            writer.setFieldName("d");
            writer.writeInt(4);

            writer.setFieldName("e");
            writer.writeInt(5);
        }
        writer.stepOut();
        assertValue("{a:1, b:2, c:3, d:4, e:5}");
    }

    @Test
    public void testSymbolTableExport() throws Exception {
        writer.stepIn(IonType.STRUCT);
        {
            writer.setFieldName("a");
            writer.writeInt(1);

            writer.setFieldName("d");
            writer.writeInt(4);
        }
        writer.stepOut();
        assertValue("{a:1, d:4}");

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IonWriter symbolTableWriter = system().newBinaryWriter(bos);
        try {
            writer.getSymbolTable().writeTo(symbolTableWriter);
        } finally {
            symbolTableWriter.close();
        }
        // SymbolTable expected = system().newSharedSymbolTable("version", )
        bos.toByteArray();

    }

    @Test
    public void testNestedEmptyContainer() throws Exception
    {
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("bar");
        writer.stepIn(IonType.LIST);
        writer.stepOut();
        writer.stepOut();
        assertValue("{bar: []}");
    }

    @Test
    public void testNestedEmptyAnnotatedContainer() throws Exception
    {
        writer.stepIn(IonType.STRUCT);
        writer.setFieldName("bar");
        writer.addTypeAnnotation("foo");
        writer.stepIn(IonType.LIST);
        writer.stepOut();
        writer.stepOut();
        assertValue("{bar: foo::[]}");
    }
}
