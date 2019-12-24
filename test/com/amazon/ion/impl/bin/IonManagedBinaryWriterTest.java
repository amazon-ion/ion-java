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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonMutableCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.impl.bin.IonManagedBinaryWriter.ImportedSymbolResolverMode;
import com.amazon.ion.impl.bin._Private_IonManagedBinaryWriterBuilder.AllocatorMode;
import com.amazon.ion.junit.Injected.Inject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class IonManagedBinaryWriterTest extends IonRawBinaryWriterTest
{
    @SuppressWarnings("unchecked")
    private static final List<List<String>> SHARED_SYMBOLS = unmodifiableList(asList(
        unmodifiableList(asList(
            "a",
            "b",
            "c"
        )),
        unmodifiableList(asList(
            "d",
            "e"
        ))
    ));

    private static final Map<String, Integer> SHARED_SYMBOL_LOCAL_SIDS ;
    static
    {
        final Map<String, Integer> sidMap = new HashMap<String, Integer>();

        for (final SymbolToken token : Symbols.systemSymbols())
        {
            sidMap.put(token.getText(), token.getSid());
        }
        int sid = SystemSymbols.ION_1_0_MAX_ID + 1;
        for (final List<String> symbolList : SHARED_SYMBOLS)
        {
            for (final String symbol : symbolList) {
                sidMap.put(symbol, sid);
                sid++;
            }
        }
        SHARED_SYMBOL_LOCAL_SIDS = unmodifiableMap(sidMap);
    }

    private void checkSymbolTokenAgainstImport(final SymbolToken token)
    {
        final Integer sid = SHARED_SYMBOL_LOCAL_SIDS.get(token.getText());
        if (sid != null)
        {
            assertEquals(sid.intValue(), token.getSid());
        }
    }

    @Override
    protected void additionalValueAssertions(final IonValue value)
    {
        for (final SymbolToken token : value.getTypeAnnotationSymbols()) {
            checkSymbolTokenAgainstImport(token);
        }
        final IonType type = value.getType();
        if (type == IonType.SYMBOL && !value.isNullValue())
        {
            checkSymbolTokenAgainstImport(((IonSymbol) value).symbolValue());
        }
        else if (IonType.isContainer(type))
        {
            for (final IonValue child : ((IonContainer) value))
            {
                additionalValueAssertions(child);
            }
        }
    }

    @Override
    public int ivmLength() {
        return 4;
    }


    @Inject("importedSymbolResolverMode")
    public static final ImportedSymbolResolverMode[] RESOLVER_DIMENSIONS = ImportedSymbolResolverMode.values();

    private ImportedSymbolResolverMode importedSymbolResolverMode;

    public void setImportedSymbolResolverMode(final ImportedSymbolResolverMode mode)
    {
        importedSymbolResolverMode = mode;
    }

    @Override
    protected IonWriter createWriter(final OutputStream out) throws IOException
    {
        final IonMutableCatalog catalog = ((IonMutableCatalog) system().getCatalog());

        final List<SymbolTable> symbolTables = new ArrayList<SymbolTable>();
        int i = 1;
        for (final List<String> symbols : SHARED_SYMBOLS) {
            final SymbolTable table = system().newSharedSymbolTable("test_" + (i++), 1, symbols.iterator());
            symbolTables.add(table);
            catalog.putTable(table);
        }

        final IonWriter writer = _Private_IonManagedBinaryWriterBuilder
            .create(AllocatorMode.POOLED)
            .withImports(importedSymbolResolverMode, symbolTables)
            .withPreallocationMode(preallocationMode)
            .withFloatBinary32Enabled()
            .newWriter(out);

        final SymbolTable locals = writer.getSymbolTable();
        assertEquals(14, locals.getImportedMaxId());

        return writer;
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
        assertEquals(reader.getSymbolTable().findSymbol("burrito"), -1);
        reader.next();
        assertEquals(reader.getSymbolTable().findSymbol("taco"), 15);
        assertEquals(reader.getSymbolTable().findSymbol("burrito"), 16);
        assertNull(reader.next());

        IonDatagram dg = system().getLoader().load(writer.getBytes());
        // Should be IVM SYMTAB taco SYMTAB burrito
        assertEquals(5, dg.systemSize());
        assertEquals("$ion_symbol_table", ((IonStruct) dg.systemGet(1)).getTypeAnnotations()[0]);
        assertEquals("taco", ((IonSymbol) dg.systemGet(2)).stringValue());
        assertEquals("$ion_symbol_table", ((IonStruct) dg.systemGet(3)).getTypeAnnotations()[0]);
        assertEquals("burrito", ((IonSymbol) dg.systemGet(4)).stringValue());
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
}
