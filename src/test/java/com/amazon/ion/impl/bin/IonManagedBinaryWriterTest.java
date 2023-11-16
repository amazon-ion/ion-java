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

import org.junit.Test;

@SuppressWarnings("deprecation")
public class IonManagedBinaryWriterTest extends IonManagedBinaryWriterTestCase
{

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
