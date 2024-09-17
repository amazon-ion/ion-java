// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.FakeSymbolToken;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroRef;
import com.amazon.ion.impl.macro.TemplateMacro;
import com.amazon.ion.system.IonReaderBuilder;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that Ion 1.1 encoding directives are correctly compiled from streams of Ion data.
 */
public class EncodingDirectiveCompilationTest {

    private static void assertMacroTablesEqual(IonReader reader, Map<MacroRef, Macro> expected) {
        Map<MacroRef, Macro> actual = ((IonReaderContinuableCoreBinary) reader).getEncodingContext().getMacroTable();
        assertEquals(expected, actual);
    }

    private static Map<MacroRef, Macro> newMacroTable(Macro... macros) {
        int address = 0;
        Map<MacroRef, Macro> macroTable = new HashMap<>();
        for (Macro macro : macros) {
            macroTable.put(MacroRef.byId(address++), macro);
        }
        return macroTable;
    }

    // Note: this may go away once the Ion 1.1 system symbol table is finalized and implemented, or if we were to
    // make use of inline symbols in the encoding directive.
    private static Map<String, Integer> initializeSymbolTable(IonRawWriter_1_1 writer, String... userSymbols) {
        Map<String, Integer> symbols = new HashMap<>();
        int localSymbolId = SystemSymbols.ION_1_0_MAX_ID;
        writer.writeAnnotations(SystemSymbols.ION_SYMBOL_TABLE_SID);
        writer.stepInStruct(false);
        writer.writeFieldName(SystemSymbols.SYMBOLS);
        writer.stepInList(false);
        writer.writeString(SystemSymbols.ION_ENCODING);
        symbols.put(SystemSymbols.ION_ENCODING, ++localSymbolId);
        writer.writeString(SystemSymbols.SYMBOL_TABLE);
        symbols.put(SystemSymbols.SYMBOL_TABLE, ++localSymbolId);
        writer.writeString(SystemSymbols.MACRO_TABLE);
        symbols.put(SystemSymbols.MACRO_TABLE, ++localSymbolId);
        writer.writeString("macro");
        symbols.put("macro", ++localSymbolId);
        writer.writeString("?");
        symbols.put("?", ++localSymbolId);
        for (String userSymbol : userSymbols) {
            writer.writeString(userSymbol);
            symbols.put(userSymbol, ++localSymbolId);
        }
        writer.stepOut();
        writer.stepOut();
        return symbols;
    }

    private static void startEncodingDirective(IonRawWriter_1_1 writer, Map<String, Integer> symbols) {
        writer.writeAnnotations(symbols.get(SystemSymbols.ION_ENCODING));
        writer.stepInSExp(false);
    }

    private static void endEncodingDirective(IonRawWriter_1_1 writer) {
        writer.stepOut();
    }

    private static void writeEncodingDirectiveSymbolTable(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String... userSymbols) {
        writer.stepInSExp(false);
        writer.writeSymbol(symbols.get(SystemSymbols.SYMBOL_TABLE));
        writer.stepInList(false);
        for (String userSymbol : userSymbols) {
            writer.writeString(userSymbol);
        }
        writer.stepOut();
        writer.stepOut();
    }

    private static void startMacroTable(IonRawWriter_1_1 writer, Map<String, Integer> symbols) {
        writer.stepInSExp(false);
        writer.writeSymbol(symbols.get(SystemSymbols.MACRO_TABLE));
    }

    private static void endMacroTable(IonRawWriter_1_1 writer) {
        writer.stepOut();
    }

    private static void startMacro(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String name) {
        writer.stepInSExp(false);
        writer.writeSymbol(symbols.get("macro"));
        writer.writeSymbol(symbols.get(name));
    }

    private static void endMacro(IonRawWriter_1_1 writer) {
        writer.stepOut();
    }

    private static void writeMacroSignature(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String... signature) {
        writer.stepInSExp(false);
        for (String parameter : signature) {
            writer.writeSymbol(symbols.get(parameter));
        }
        writer.stepOut();
    }

    private static void writeVariableField(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String fieldName, String variableName) {
        writer.writeFieldName(symbols.get(fieldName));
        writer.writeSymbol(symbols.get(variableName));
    }

    private static byte[] getBytes(IonRawWriter_1_1 writer, ByteArrayOutputStream out) {
        writer.close();
        return out.toByteArray();
    }

    @Test
    public void structMacroWithOneOptional() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "People", "ID", "Name", "Bald", "$ID", "$Name", "$Bald");
        startEncodingDirective(writer, symbols);
        startMacroTable(writer, symbols);
        startMacro(writer, symbols, "People");
        writeMacroSignature(writer, symbols, "$ID", "$Name", "$Bald", "?");
        // The macro body
        writer.stepInStruct(false);
        writeVariableField(writer, symbols, "ID", "$ID");
        writeVariableField(writer, symbols, "Name", "$Name");
        writeVariableField(writer, symbols, "Bald", "$Bald");
        writer.stepOut();
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);
        writer.writeInt(0);
        byte[] data = getBytes(writer, out);

        Macro expectedMacro = new TemplateMacro(
            Arrays.asList(
                new Macro.Parameter("$ID", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne),
                new Macro.Parameter("$Name", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne),
                new Macro.Parameter("$Bald", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrOne)
            ),
            Arrays.asList(
                new Expression.StructValue(Collections.emptyList(), 0, 7, new HashMap<String, List<Integer>>() {{
                    put("ID", Collections.singletonList(2));
                    put("Name", Collections.singletonList(4));
                    put("Bald", Collections.singletonList(6));
                }}),
                new Expression.FieldName(new FakeSymbolToken("ID", symbols.get("ID"))),
                new Expression.VariableRef(0),
                new Expression.FieldName(new FakeSymbolToken("Name", symbols.get("Name"))),
                new Expression.VariableRef(1),
                new Expression.FieldName(new FakeSymbolToken("Bald", symbols.get("Bald"))),
                new Expression.VariableRef(2)
            )
        );

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.INT, reader.next());
            assertMacroTablesEqual(reader, newMacroTable(expectedMacro));
        }
    }

    @Test
    public void constantMacroWithUserSymbol() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "Pi");
        startEncodingDirective(writer, symbols);
        writeEncodingDirectiveSymbolTable(writer, symbols, "foo");
        startMacroTable(writer, symbols);
        startMacro(writer, symbols, "Pi");
        writeMacroSignature(writer, symbols); // Empty signature
        writer.writeDecimal(new BigDecimal("3.14159")); // The body: a constant
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);
        // Note: this will change when the system symbol table is implemented. This is the first local symbol ID.
        writer.writeSymbol(10); // foo
        byte[] data = getBytes(writer, out);

        Macro expectedMacro = new TemplateMacro(
            Collections.emptyList(),
            Collections.singletonList(new Expression.DecimalValue(Collections.emptyList(), new BigDecimal("3.14159")))
        );

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.SYMBOL, reader.next());
            assertMacroTablesEqual(reader, newMacroTable(expectedMacro));
            assertEquals("foo", reader.stringValue());
        }
    }

    @Test
    public void structMacroWithOneOptionalInvoked() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "People", "ID", "Name", "Bald", "$ID", "$Name", "$Bald");
        startEncodingDirective(writer, symbols);
        startMacroTable(writer, symbols);
        startMacro(writer, symbols, "People");
        writeMacroSignature(writer, symbols, "$ID", "$Name", "$Bald", "?");
        // The macro body
        writer.stepInStruct(false);
        writeVariableField(writer, symbols, "ID", "$ID");
        writeVariableField(writer, symbols, "Name", "$Name");
        writeVariableField(writer, symbols, "Bald", "$Bald");
        writer.stepOut();
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro expectedMacro = new TemplateMacro(
            Arrays.asList(
                new Macro.Parameter("$ID", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne),
                new Macro.Parameter("$Name", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne),
                new Macro.Parameter("$Bald", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrOne)
            ),
            Arrays.asList(
                new Expression.StructValue(Collections.emptyList(), 0, 7, new HashMap<String, List<Integer>>() {{
                    put("ID", Collections.singletonList(2));
                    put("Name", Collections.singletonList(4));
                    put("Bald", Collections.singletonList(6));
                }}),
                new Expression.FieldName(new FakeSymbolToken("ID", symbols.get("ID"))),
                new Expression.VariableRef(0),
                new Expression.FieldName(new FakeSymbolToken("Name", symbols.get("Name"))),
                new Expression.VariableRef(1),
                new Expression.FieldName(new FakeSymbolToken("Bald", symbols.get("Bald"))),
                new Expression.VariableRef(2)
            )
        );
        writer.stepInEExp(0, false, expectedMacro);
        writer.writeInt(123);
        writer.writeString("Bob");
        writer.writeBool(false);
        writer.stepOut();
        writer.stepInEExp(0, false, expectedMacro);
        writer.writeInt(Long.MIN_VALUE);
        writer.writeString("Sue");
        // The optional "Bald" is not included.
        writer.stepOut();
        writer.writeInt(42); // Not a macro invocation
        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.STRUCT, reader.next());
            assertMacroTablesEqual(reader, newMacroTable(expectedMacro));
            reader.stepIn();
            assertEquals(1, reader.getDepth());
            assertEquals(IonType.INT, reader.next());
            assertEquals("ID", reader.getFieldName());
            assertEquals(123, reader.intValue());
            assertEquals(IonType.STRING, reader.next());
            assertEquals("Name", reader.getFieldName());
            assertEquals("Bob", reader.stringValue());
            assertEquals(IonType.BOOL, reader.next());
            assertEquals("Bald", reader.getFieldName());
            assertFalse(reader.booleanValue());
            assertNull(reader.next());
            reader.stepOut();

            assertEquals(0, reader.getDepth());
            assertEquals(IonType.STRUCT, reader.next());
            reader.stepIn();
            assertEquals(IonType.INT, reader.next());
            assertEquals("ID", reader.getFieldName());
            assertEquals(Long.MIN_VALUE, reader.longValue());
            assertEquals(IonType.STRING, reader.next());
            assertEquals("Name", reader.getFieldName());
            assertEquals("Sue", reader.stringValue());
            assertNull(reader.next());
            reader.stepOut();

            assertEquals(IonType.INT, reader.next());
            assertEquals(42, reader.intValue());

            assertNull(reader.next());
        }
    }

    @Test
    public void macroInvocationWithinStruct() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "People", "ID", "Name", "Bald", "$ID", "$Name", "$Bald");
        startEncodingDirective(writer, symbols);
        writeEncodingDirectiveSymbolTable(writer, symbols, "foo");
        startMacroTable(writer, symbols);
        startMacro(writer, symbols, "People");
        writeMacroSignature(writer, symbols, "$ID", "$Name", "?", "$Bald", "?");
        // The macro body
        writer.stepInStruct(false);
        writeVariableField(writer, symbols, "ID", "$ID");
        writeVariableField(writer, symbols, "Name", "$Name");
        writeVariableField(writer, symbols, "Bald", "$Bald");
        writer.stepOut();
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro expectedMacro = new TemplateMacro(
            Arrays.asList(
                new Macro.Parameter("$ID", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne),
                new Macro.Parameter("$Name", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrOne),
                new Macro.Parameter("$Bald", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrOne)
            ),
            Arrays.asList(
                new Expression.StructValue(Collections.emptyList(), 0, 7, new HashMap<String, List<Integer>>() {{
                    put("ID", Collections.singletonList(2));
                    put("Name", Collections.singletonList(4));
                    put("Bald", Collections.singletonList(6));
                }}),
                new Expression.FieldName(new FakeSymbolToken("ID", symbols.get("ID"))),
                new Expression.VariableRef(0),
                new Expression.FieldName(new FakeSymbolToken("Name", symbols.get("Name"))),
                new Expression.VariableRef(1),
                new Expression.FieldName(new FakeSymbolToken("Bald", symbols.get("Bald"))),
                new Expression.VariableRef(2)
            )
        );

        writer.stepInStruct(true);
        // Note: this will change when the system symbol table is implemented. This is the first local symbol ID.
        writer.writeFieldName(10);
        writer.stepInEExp(0, false, expectedMacro);
        // Note: this will change when the system symbol table is implemented. This is the first local symbol ID.
        writer.writeSymbol(10);
        // Two trailing optionals are elided.
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.STRUCT, reader.next());
            assertMacroTablesEqual(reader, newMacroTable(expectedMacro));
            reader.stepIn();
            assertEquals(IonType.STRUCT, reader.next());
            assertEquals("foo", reader.getFieldName());
            reader.stepIn();
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("ID", reader.getFieldName());
            assertEquals("foo", reader.stringValue());
            assertNull(reader.next());
            reader.stepOut();
            // TODO future fix: currently this next() is needed, otherwise the reader thinks it's still evaluating a
            //  macro on the next stepOut.
            assertNull(reader.next());
            reader.stepOut();
            assertNull(reader.next());
        }
    }

    @Test
    public void macroInvocationWithOptionalSuppressedBeforeEndWithinStruct() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "People", "ID", "Name", "Bald", "$ID", "$Name", "$Bald");
        startEncodingDirective(writer, symbols);
        writeEncodingDirectiveSymbolTable(writer, symbols, "foo");
        startMacroTable(writer, symbols);
        startMacro(writer, symbols, "People");
        writeMacroSignature(writer, symbols, "$ID", "$Name", "?", "$Bald", "?");
        // The macro body
        writer.stepInStruct(false);
        writeVariableField(writer, symbols, "ID", "$ID");
        writeVariableField(writer, symbols, "Name", "$Name");
        writeVariableField(writer, symbols, "Bald", "$Bald");
        writer.stepOut();
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro expectedMacro = new TemplateMacro(
            Arrays.asList(
                new Macro.Parameter("$ID", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne),
                new Macro.Parameter("$Name", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrOne),
                new Macro.Parameter("$Bald", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrOne)
            ),
            Arrays.asList(
                new Expression.StructValue(Collections.emptyList(), 0, 7, new HashMap<String, List<Integer>>() {{
                    put("ID", Collections.singletonList(2));
                    put("Name", Collections.singletonList(4));
                    put("Bald", Collections.singletonList(6));
                }}),
                new Expression.FieldName(new FakeSymbolToken("ID", symbols.get("ID"))),
                new Expression.VariableRef(0),
                new Expression.FieldName(new FakeSymbolToken("Name", symbols.get("Name"))),
                new Expression.VariableRef(1),
                new Expression.FieldName(new FakeSymbolToken("Bald", symbols.get("Bald"))),
                new Expression.VariableRef(2)
            )
        );

        writer.stepInStruct(true);
        // Note: this will change when the system symbol table is implemented. This is the first local symbol ID.
        writer.writeFieldName(10);
        writer.stepInEExp(0, false, expectedMacro);
        // Note: this will change when the system symbol table is implemented. This is the first local symbol ID.
        writer.writeSymbol(10);
        // Explicitly elide the optional "Name"
        writer.stepInExpressionGroup(false);
        writer.stepOut();
        writer.writeBool(true);
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.STRUCT, reader.next());
            assertMacroTablesEqual(reader, newMacroTable(expectedMacro));
            reader.stepIn();
            assertEquals(IonType.STRUCT, reader.next());
            assertEquals("foo", reader.getFieldName());
            reader.stepIn();
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("ID", reader.getFieldName());
            assertEquals("foo", reader.stringValue());
            assertEquals(IonType.BOOL, reader.next());
            assertEquals("Bald", reader.getFieldName());
            assertTrue(reader.booleanValue());
            assertNull(reader.next());
            reader.stepOut();
            assertNull(reader.next());
            reader.stepOut();
            assertNull(reader.next());
        }
    }

    @Test
    public void constantMacroInvoked() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "Pi");
        startEncodingDirective(writer, symbols);
        writeEncodingDirectiveSymbolTable(writer, symbols, "foo");
        startMacroTable(writer, symbols);
        startMacro(writer, symbols, "Pi");
        writeMacroSignature(writer, symbols); // Empty signature
        writer.writeDecimal(new BigDecimal("3.14159")); // The body: a constant
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro expectedMacro = new TemplateMacro(
            Collections.emptyList(),
            Collections.singletonList(new Expression.DecimalValue(Collections.emptyList(), new BigDecimal("3.14159")))
        );

        writer.stepInEExp(0, false, expectedMacro);
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.DECIMAL, reader.next());
            assertMacroTablesEqual(reader, newMacroTable(expectedMacro));
            assertEquals(new BigDecimal("3.14159"), reader.decimalValue());
        }
    }

    private Macro writeSimonSaysMacro(IonRawWriter_1_1 writer) {
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "SimonSays", "anything");
        startEncodingDirective(writer, symbols);
        writeEncodingDirectiveSymbolTable(writer, symbols, "foo");
        startMacroTable(writer, symbols);
        startMacro(writer, symbols, "SimonSays");
        writeMacroSignature(writer, symbols, "anything");
        writer.writeSymbol(symbols.get("anything")); // The body: a variable
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        return new TemplateMacro(
            Collections.singletonList(new Macro.Parameter("anything", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne)),
            Collections.singletonList(new Expression.VariableRef(0))
        );
    }

    @Test
    public void structAsParameter() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        Macro expectedMacro = writeSimonSaysMacro(writer);

        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInStruct(true);
        // Note: this will change when the system symbol table is implemented. This is the first local symbol ID.
        writer.writeFieldName(10);
        writer.writeInt(123);
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.STRUCT, reader.next());
            assertMacroTablesEqual(reader, newMacroTable(expectedMacro));
            reader.stepIn();
            assertEquals(IonType.INT, reader.next());
            assertEquals("foo", reader.getFieldName());
            assertEquals(123, reader.intValue());
            reader.stepOut();
            assertNull(reader.next());
        }
    }

    @Test
    public void macroInvocationAsParameter() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        Macro expectedMacro = writeSimonSaysMacro(writer);

        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInEExp(0, false, expectedMacro);
        writer.writeFloat(1.23);
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.FLOAT, reader.next());
            assertEquals(1.23, reader.doubleValue(), 1e-9);
            assertNull(reader.next());
        }
    }

    @Test
    public void macroInvocationNestedWithinParameter() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        Macro expectedMacro = writeSimonSaysMacro(writer);

        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInList(true);
        writer.stepInEExp(0, false, expectedMacro);
        writer.writeFloat(1.23);
        writer.stepOut();
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.LIST, reader.next());
            reader.stepIn();
            assertEquals(IonType.FLOAT, reader.next());
            assertEquals(1.23, reader.doubleValue(), 1e-9);
            assertNull(reader.next());
            reader.stepOut();
            assertNull(reader.next());
        }
    }

    @Test
    public void macroInvocationsNestedWithinParameter() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        Macro expectedMacro = writeSimonSaysMacro(writer);

        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInList(true);
        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInStruct(true);
        // Note: this will change when the system symbol table is implemented. This is the first local symbol ID.
        writer.writeFieldName(10);
        writer.writeFloat(1.23);
        writer.stepOut();
        writer.stepOut();
        writer.stepInEExp(0, false, expectedMacro);
        writer.writeInt(123);
        writer.stepOut();
        writer.writeString("abc");
        writer.stepOut();
        writer.stepOut();
        writer.stepInList(true);
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.LIST, reader.next());
            reader.stepIn();
            assertEquals(IonType.STRUCT, reader.next());
            reader.stepIn();
            assertEquals(IonType.FLOAT, reader.next());
            assertEquals("foo", reader.getFieldName());
            assertEquals(1.23, reader.doubleValue(), 1e-9);
            assertNull(reader.next());
            reader.stepOut();
            assertEquals(IonType.INT, reader.next());
            assertEquals(123, reader.intValue());
            assertEquals(IonType.STRING, reader.next());
            assertEquals("abc", reader.stringValue());
            assertNull(reader.next());
            reader.stepOut();
            assertEquals(IonType.LIST, reader.next());
            assertNull(reader.next());
        }
    }

    @Test
    public void annotationInParameter() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);
        Macro expectedMacro = writeSimonSaysMacro(writer);

        writer.stepInEExp(0, false, expectedMacro);
        // Note: this will change when the system symbol table is implemented. This is the first local symbol ID.
        writer.writeAnnotations(10);
        writer.writeNull(IonType.TIMESTAMP);
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.TIMESTAMP, reader.next());
            assertTrue(reader.isNullValue());
            String[] annotation = reader.getTypeAnnotations();
            assertEquals(1, annotation.length);
            assertEquals("foo", annotation[0]);
            assertNull(reader.next());
        }
    }

    @Test
    public void twoArgumentGroups() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);

        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "Groups", "these", "those", "*", "+");
        startEncodingDirective(writer, symbols);
        writeEncodingDirectiveSymbolTable(writer, symbols, "foo");
        startMacroTable(writer, symbols);
        startMacro(writer, symbols, "Groups");
        writeMacroSignature(writer, symbols, "these", "*", "those", "+");
        writer.stepInList(true);
        writer.stepInList(true);
        writer.writeSymbol(symbols.get("those")); // The body: a variable
        writer.stepOut();
        writer.stepInList(true);
        writer.writeSymbol(symbols.get("these"));
        writer.stepOut();
        writer.stepOut();
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro expectedMacro = new TemplateMacro(
            Arrays.asList(
                new Macro.Parameter("these", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrMore),
                new Macro.Parameter("those", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.OneOrMore)
            ),
            Arrays.asList(
                new Expression.ListValue(Collections.emptyList(), 0, 2),
                new Expression.VariableRef(1), // those
                new Expression.SExpValue(Collections.emptyList(), 2, 4),
                new Expression.VariableRef(0) // these
            )
        );

        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInExpressionGroup(false); // TODO add a test for length-prefixed argument groups
        // Note: this will change when the system symbol table is implemented. This is the first local symbol ID.
        writer.writeSymbol(10);
        writer.writeString("bar");
        writer.stepOut();
        writer.stepInExpressionGroup(false);
        writer.writeBool(true);
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.LIST, reader.next());
            reader.stepIn();
            assertEquals(IonType.LIST, reader.next());
            reader.stepIn();
            assertEquals(2, reader.getDepth());
            assertEquals(IonType.BOOL, reader.next());
            assertTrue(reader.booleanValue());
            reader.stepOut();
            assertEquals(1, reader.getDepth());
            assertEquals(IonType.LIST, reader.next());
            reader.stepIn();
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("foo", reader.symbolValue().assumeText());
            assertEquals(IonType.STRING, reader.next());
            assertEquals("bar", reader.stringValue());
            assertNull(reader.next());
            reader.stepOut();
            reader.stepOut();
            assertNull(reader.next());
        }
    }

    @Test
    public void macroInvocationInMacroDefinition() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = IonRawBinaryWriter_1_1.from(out, 256, 0);

        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "SimonSays", "anything", "Echo");
        startEncodingDirective(writer, symbols);
        writeEncodingDirectiveSymbolTable(writer, symbols, "foo");
        startMacroTable(writer, symbols);
        startMacro(writer, symbols, "SimonSays");
        writeMacroSignature(writer, symbols, "anything");
        writer.writeSymbol(symbols.get("anything")); // The body: a variable
        endMacro(writer);
        startMacro(writer, symbols, "Echo");
        writeMacroSignature(writer, symbols); // empty signature
        writer.stepInSExp(true); // A macro invocation in TDL
        writer.writeInt(0); // Macro ID 0 ("SimonSays")
        writer.writeInt(123); // The argument to SimonSays
        writer.stepOut();
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro simonSaysMacro = new TemplateMacro(
            Collections.singletonList(
                Macro.Parameter.exactlyOneTagged("anything")
            ),
            Collections.singletonList(
                new Expression.VariableRef(0)
            )
        );

        Macro expectedMacro = new TemplateMacro(
            Collections.emptyList(),
            Arrays.asList(
                new Expression.MacroInvocation(simonSaysMacro, 0, 2),
                new Expression.LongIntValue(Collections.emptyList(), 123)
            )
        );

        writer.stepInEExp(1, false, expectedMacro);
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = IonReaderBuilder.standard().build(data)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(IntegerSize.INT, reader.getIntegerSize());
            assertEquals(123, reader.intValue());
            assertNull(reader.next());
        }
    }

    // TODO cover every Ion type
    // TODO tagless values and tagless argument groups
    // TODO annotations in macro definition (using 'annotate' system macro)
    // TODO macro invocation that expands to a system value
    // TODO test error conditions
    // TODO support continuable and lazy evaluation
    // TODO early step-out of evaluation; skipping evaluation.
    // TODO ZeroOrOne and ExactlyOne cardinality parameter with single-element group (legal?)
}
