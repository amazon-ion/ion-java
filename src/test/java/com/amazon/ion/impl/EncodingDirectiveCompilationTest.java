// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.FakeSymbolToken;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonEncodingVersion;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1;
import com.amazon.ion.impl.macro.EncodingContext;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroRef;
import com.amazon.ion.impl.macro.ParameterFactory;
import com.amazon.ion.impl.macro.TemplateMacro;
import com.amazon.ion.system.IonReaderBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that Ion 1.1 encoding directives are correctly compiled from streams of Ion data.
 */
public class EncodingDirectiveCompilationTest {

    private static final int FIRST_LOCAL_SYMBOL_ID = 1;

    private static void assertMacroTablesEqual(IonReader reader, StreamType streamType, SortedMap<String, Macro> expected) {
        Map<MacroRef, Macro> expectedByRef = streamType.newMacroTableByMacroRef(expected);
        Map<MacroRef, Macro> actual = streamType.getEncodingContext(reader).getMacroTable();
        assertEquals(expectedByRef, actual);
    }

    private static void startEncodingDirective(IonRawWriter_1_1 writer) {
        writer.writeAnnotations(SystemSymbols_1_1.ION_ENCODING);
        writer.stepInSExp(false);
    }

    private static void endEncodingDirective(IonRawWriter_1_1 writer) {
        writer.stepOut();
    }

    private static void writeEncodingDirectiveSymbolTable(IonRawWriter_1_1 writer, boolean append, String... userSymbols) {
        writer.stepInSExp(false);
        writer.writeSymbol(SystemSymbols.SYMBOL_TABLE);
        if (append) {
            writer.writeSymbol(SystemSymbols.ION_ENCODING);
        }
        writer.stepInList(false);
        for (String userSymbol : userSymbols) {
            writer.writeString(userSymbol);
        }
        writer.stepOut();
        writer.stepOut();
    }

    private static void writeEncodingDirectiveSymbolTable(IonRawWriter_1_1 writer, String... userSymbols) {
        writeEncodingDirectiveSymbolTable(writer, false, userSymbols);
    }

    private static Map<String, Integer> initializeSymbolTable(IonRawWriter_1_1 writer, String... userSymbols) {
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, userSymbols);
        endEncodingDirective(writer);
        Map<String, Integer> symbols = new HashMap<>();
        int localSymbolId = FIRST_LOCAL_SYMBOL_ID;
        for (String userSymbol : userSymbols) {
            symbols.put(userSymbol, localSymbolId++);
        }
        return symbols;
    }

    private static void startMacroTable(IonRawWriter_1_1 writer) {
        writer.stepInSExp(false);
        writer.writeSymbol(SystemSymbols_1_1.MACRO_TABLE);
    }

    private static void endMacroTable(IonRawWriter_1_1 writer) {
        writer.stepOut();
    }

    private static void writeSymbolToken(Consumer<String> tokenTextWriter, Consumer<Integer> tokenSidWriter, Map<String, Integer> symbols, String value) {
        Integer sid = symbols.get(value);
        if (sid == null) {
            // There is no mapping; write as text
            tokenTextWriter.accept(value);
        } else {
            tokenSidWriter.accept(sid);
        }
    }

    private static void writeSymbol(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String value) {
        writeSymbolToken(writer::writeSymbol, writer::writeSymbol, symbols, value);
    }

    private static void writeFieldName(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String name) {
        writeSymbolToken(writer::writeFieldName, writer::writeFieldName, symbols, name);
    }

    private static void startMacro(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String name) {
        writer.stepInSExp(false);
        writer.writeSymbol(SystemSymbols_1_1.MACRO);
        writeSymbol(writer, symbols, name);
    }

    private static void endMacro(IonRawWriter_1_1 writer) {
        writer.stepOut();
    }

    private static void writeMacroSignature(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String... signature) {
        writer.stepInSExp(false);
        for (String parameter : signature) {
            writeSymbol(writer, symbols, parameter);
        }
        writer.stepOut();
    }

    private static void writeVariableExpansion(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String variableName) {
        writer.stepInSExp(false);
        writer.writeSymbol("%");
        writeSymbol(writer, symbols, variableName);
        writer.stepOut();
    }

    private static void stepInTdlMacroInvocation(IonRawWriter_1_1 writer, Integer macroAddress) {
        writer.stepInSExp(false);
        writer.writeSymbol(".");
        writer.writeInt(macroAddress);
    }

    private static void writeVariableField(IonRawWriter_1_1 writer, Map<String, Integer> symbols, String fieldName, String variableName) {
        writeFieldName(writer, symbols, fieldName);
        writeVariableExpansion(writer, symbols, variableName);
    }

    private static byte[] getBytes(IonRawWriter_1_1 writer, ByteArrayOutputStream out) {
        writer.close();
        return out.toByteArray();
    }

    public enum StreamType {
        BINARY {
            @Override
            IonRawWriter_1_1 newWriter(OutputStream out) {
                return IonRawBinaryWriter_1_1.from(out, 256, 0);
            }

            @Override
            EncodingContext getEncodingContext(IonReader reader) {
                return ((IonReaderContinuableCoreBinary) reader).getEncodingContext();
            }

            @Override
            Map<MacroRef, Macro> newMacroTableByMacroRef(SortedMap<String, Macro> macrosByName) {
                int address = 0;
                Map<MacroRef, Macro> macroTable = new HashMap<>();
                for (Macro macro : macrosByName.values()) {
                    macroTable.put(MacroRef.byId(address++), macro);
                }
                return macroTable;
            }

            @Override
            void startMacroInvocationByName(IonRawWriter_1_1 writer, String name, Map<String, Macro> macrosByName) {
                int id = 0;
                for (Map.Entry<String, Macro> nameAndMacro : macrosByName.entrySet()) {
                    if (nameAndMacro.getKey().equals(name)) {
                        break;
                    }
                    id++;
                }
                writer.stepInEExp(id, false, macrosByName.get(name));
            }
        },
        TEXT {
            @Override
            IonRawWriter_1_1 newWriter(OutputStream out) {
                return IonRawTextWriter_1_1.from(out, 256, IonEncodingVersion.ION_1_1.textWriterBuilder());
            }

            @Override
            EncodingContext getEncodingContext(IonReader reader) {
                return ((IonReaderTextSystemX) reader).getEncodingContext();
            }

            @Override
            Map<MacroRef, Macro> newMacroTableByMacroRef(SortedMap<String, Macro> macrosByName) {
                Map<MacroRef, Macro> macroTable = new HashMap<>();
                int id = 0;
                for (Map.Entry<String, Macro> nameAndMacro : macrosByName.entrySet()) {
                    Macro macro = nameAndMacro.getValue();
                    macroTable.put(MacroRef.byId(id++), macro);
                    String name = nameAndMacro.getKey();
                    if (name != null) {
                        macroTable.put(MacroRef.byName(name), macro);
                    }
                }
                return macroTable;
            }

            @Override
            void startMacroInvocationByName(IonRawWriter_1_1 writer, String name, Map<String, Macro> macrosByName) {
                writer.stepInEExp(name);
            }
        };

        abstract IonRawWriter_1_1 newWriter(OutputStream out);
        abstract EncodingContext getEncodingContext(IonReader reader);
        abstract Map<MacroRef, Macro> newMacroTableByMacroRef(SortedMap<String, Macro> macrosByName);
        abstract void startMacroInvocationByName(IonRawWriter_1_1 writer, String name, Map<String, Macro> macrosByName);
    }

    public enum InputType {
        INPUT_STREAM {
            @Override
            IonReader newReader(byte[] input) {
                return IonReaderBuilder.standard().build(new ByteArrayInputStream(input));
            }
        },
        BYTE_ARRAY {
            @Override
            IonReader newReader(byte[] input) {
                return IonReaderBuilder.standard().build(input);
            }
        };

        abstract IonReader newReader(byte[] input);
    }

    public static Arguments[] allCombinations() {
        InputType[] inputTypes = InputType.values();
        StreamType[] streamTypes = StreamType.values();
        Arguments[] combinations = new Arguments[inputTypes.length * streamTypes.length];
        int i = 0;
        for (InputType inputType : inputTypes) {
            for (StreamType streamType : streamTypes) {
                combinations[i++] = Arguments.of(inputType, streamType);
            }
        }
        return combinations;
    }

    private static int getSymbolId(Map<String, Integer> symbols, String value) {
        Integer sid = symbols.get(value);
        return sid == null ? -1 : sid;
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void symbolsOnly(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, "foo", "bar");
        endEncodingDirective(writer);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID + 1);
        byte[] data = getBytes(writer, out);
        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("foo", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("bar", reader.stringValue());
            assertNull(reader.next());
        }
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void symbolAppendWithoutMacros(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, "foo", "bar");
        endEncodingDirective(writer);
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, true, "baz");
        endEncodingDirective(writer);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID + 1);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID + 2);
        byte[] data = getBytes(writer, out);
        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("foo", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("bar", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("baz", reader.stringValue());
            assertNull(reader.next());
        }
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void structMacroWithOneOptional(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();
        Map<String, Integer> symbols;
        if (streamType == StreamType.BINARY) {
            symbols = initializeSymbolTable(writer, "People", "ID", "Name", "Bald", "$ID", "$Name", "$Bald", "?");
        } else {
            symbols = Collections.emptyMap();
        }
        startEncodingDirective(writer);
        startMacroTable(writer);
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

        SortedMap<String, Macro> expectedMacroTable = new TreeMap<>();
        expectedMacroTable.put("People", new TemplateMacro(
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
                new Expression.FieldName(new FakeSymbolToken("ID", getSymbolId(symbols, "ID"))),
                new Expression.VariableRef(0),
                new Expression.FieldName(new FakeSymbolToken("Name", getSymbolId(symbols, "Name"))),
                new Expression.VariableRef(1),
                new Expression.FieldName(new FakeSymbolToken("Bald", getSymbolId(symbols, "Bald"))),
                new Expression.VariableRef(2)
            )
        ));

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.INT, reader.next());
            assertMacroTablesEqual(reader, streamType, expectedMacroTable);
        }
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void constantMacroWithUserSymbol(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "Pi");
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, "foo");
        startMacroTable(writer);
        startMacro(writer, symbols, "Pi");
        writeMacroSignature(writer, symbols); // Empty signature
        writer.writeDecimal(new BigDecimal("3.14159")); // The body: a constant
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID); // foo
        byte[] data = getBytes(writer, out);

        SortedMap<String, Macro> expectedMacroTable = new TreeMap<>();
        expectedMacroTable.put("Pi", new TemplateMacro(
            Collections.emptyList(),
            Collections.singletonList(new Expression.DecimalValue(Collections.emptyList(), new BigDecimal("3.14159")))
        ));

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.SYMBOL, reader.next());
            assertMacroTablesEqual(reader, streamType, expectedMacroTable);
            assertEquals("foo", reader.stringValue());
        }
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void structMacroWithOneOptionalInvoked(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "People", "ID", "Name", "Bald", "$ID", "$Name", "$Bald", "?");
        startEncodingDirective(writer);
        startMacroTable(writer);
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

        SortedMap<String, Macro> expectedMacroTable = new TreeMap<>();
        expectedMacroTable.put("People", new TemplateMacro(
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
        ));
        streamType.startMacroInvocationByName(writer, "People", expectedMacroTable);
        writer.writeInt(123);
        writer.writeString("Bob");
        writer.writeBool(false);
        writer.stepOut();
        writer.stepInEExp(0, false, expectedMacroTable.get("People"));
        writer.writeInt(Long.MIN_VALUE);
        writer.writeString("Sue");
        // The optional "Bald" is not included.
        writer.stepOut();
        writer.writeInt(42); // Not a macro invocation
        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.STRUCT, reader.next());
            assertMacroTablesEqual(reader, streamType, expectedMacroTable);
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

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationWithinStruct(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "People", "ID", "Name", "Bald", "$ID", "$Name", "$Bald", "?");
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, "foo");
        startMacroTable(writer);
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

        SortedMap<String, Macro> expectedMacroTable = new TreeMap<>();
        expectedMacroTable.put("People", new TemplateMacro(
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
        ));

        writer.stepInStruct(true);
        writer.writeFieldName(FIRST_LOCAL_SYMBOL_ID);
        streamType.startMacroInvocationByName(writer, "People", expectedMacroTable);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID);
        // Two trailing optionals are elided.
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.STRUCT, reader.next());
            assertMacroTablesEqual(reader, streamType, expectedMacroTable);
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

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationWithOptionalSuppressedBeforeEndWithinStruct(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "People", "ID", "Name", "Bald", "$ID", "$Name", "$Bald", "?");
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, "foo");
        startMacroTable(writer);
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

        SortedMap<String, Macro> expectedMacroTable = new TreeMap<>();
        expectedMacroTable.put("People", new TemplateMacro(
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
        ));

        writer.stepInStruct(true);
        writer.writeFieldName(FIRST_LOCAL_SYMBOL_ID);
        writer.stepInEExp(0, false, expectedMacroTable.get("People"));
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID);
        // Explicitly elide the optional "Name"
        writer.stepInExpressionGroup(false);
        writer.stepOut();
        writer.writeBool(true);
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.STRUCT, reader.next());
            assertMacroTablesEqual(reader, streamType, expectedMacroTable);
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

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void constantMacroInvoked(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "Pi");
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, "foo");
        startMacroTable(writer);
        startMacro(writer, symbols, "Pi");
        writeMacroSignature(writer, symbols); // Empty signature
        writer.writeDecimal(new BigDecimal("3.14159")); // The body: a constant
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        SortedMap<String, Macro> expectedMacroTable = new TreeMap<>();
        expectedMacroTable.put("Pi", new TemplateMacro(
            Collections.emptyList(),
            Collections.singletonList(new Expression.DecimalValue(Collections.emptyList(), new BigDecimal("3.14159")))
        ));

        writer.stepInEExp(0, false, expectedMacroTable.get("Pi"));
        writer.stepOut();
        streamType.startMacroInvocationByName(writer, "Pi", expectedMacroTable);
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.DECIMAL, reader.next());
            assertMacroTablesEqual(reader, streamType, expectedMacroTable);
            assertEquals(new BigDecimal("3.14159"), reader.decimalValue());
            assertEquals(IonType.DECIMAL, reader.next());
            assertEquals(new BigDecimal("3.14159"), reader.decimalValue());
        }
    }

    private Macro writeSimonSaysMacro(IonRawWriter_1_1 writer) {
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "SimonSays", "anything");
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, "foo");
        startMacroTable(writer);
        startMacro(writer, symbols, "SimonSays");
        writeMacroSignature(writer, symbols, "anything");
        // The body
        writeVariableExpansion(writer, symbols, "anything");
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        return new TemplateMacro(
            Collections.singletonList(new Macro.Parameter("anything", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne)),
            Collections.singletonList(new Expression.VariableRef(0))
        );
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void structAsParameter(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        SortedMap<String, Macro> expectedMacroTable = new TreeMap<String, Macro>() {{
            put("SimonSays", writeSimonSaysMacro(writer));
        }};

        streamType.startMacroInvocationByName(writer, "SimonSays", expectedMacroTable);
        writer.stepInStruct(true);
        writer.writeFieldName(FIRST_LOCAL_SYMBOL_ID);
        writer.writeInt(123);
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.STRUCT, reader.next());
            assertMacroTablesEqual(reader, streamType, expectedMacroTable);
            reader.stepIn();
            assertEquals(IonType.INT, reader.next());
            assertEquals("foo", reader.getFieldName());
            assertEquals(123, reader.intValue());
            reader.stepOut();
            assertNull(reader.next());
        }
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationAsParameter(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        Macro expectedMacro = writeSimonSaysMacro(writer);

        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInEExp(0, false, expectedMacro);
        writer.writeFloat(1.23);
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.FLOAT, reader.next());
            assertEquals(1.23, reader.doubleValue(), 1e-9);
            assertNull(reader.next());
        }
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationNestedWithinParameter(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        Macro expectedMacro = writeSimonSaysMacro(writer);

        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInList(true);
        writer.stepInEExp(0, false, expectedMacro);
        writer.writeFloat(1.23);
        writer.stepOut();
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.LIST, reader.next());
            reader.stepIn();
            assertEquals(IonType.FLOAT, reader.next());
            assertEquals(1.23, reader.doubleValue(), 1e-9);
            assertNull(reader.next());
            reader.stepOut();
            assertNull(reader.next());
        }
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationsNestedWithinParameter(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        Macro expectedMacro = writeSimonSaysMacro(writer);

        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInList(true);
        writer.stepInEExp(0, false, expectedMacro);
        writer.stepInStruct(true);
        writer.writeFieldName(FIRST_LOCAL_SYMBOL_ID);
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

        try (IonReader reader = inputType.newReader(data)) {
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

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void annotationInParameter(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        Macro expectedMacro = writeSimonSaysMacro(writer);

        writer.stepInEExp(0, false, expectedMacro);
        writer.writeAnnotations(FIRST_LOCAL_SYMBOL_ID);
        writer.writeNull(IonType.TIMESTAMP);
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.TIMESTAMP, reader.next());
            assertTrue(reader.isNullValue());
            String[] annotation = reader.getTypeAnnotations();
            assertEquals(1, annotation.length);
            assertEquals("foo", annotation[0]);
            assertNull(reader.next());
        }
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void twoArgumentGroups(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);

        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "Groups", "these", "those", "*", "+");
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, "foo");
        startMacroTable(writer);
        startMacro(writer, symbols, "Groups");
        writeMacroSignature(writer, symbols, "these", "*", "those", "+");
        writer.stepInList(true);
        writer.stepInList(true);
        writeVariableExpansion(writer, symbols, "those");
        writer.stepOut();
        writer.stepInList(true);
        writeVariableExpansion(writer, symbols, "these");
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
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID);
        writer.writeString("bar");
        writer.stepOut();
        writer.stepInExpressionGroup(false);
        writer.writeBool(true);
        writer.stepOut();
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
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

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationInMacroDefinition(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);

        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "SimonSays", "anything", "Echo");
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, "foo");
        startMacroTable(writer);
        startMacro(writer, symbols, "SimonSays");
        writeMacroSignature(writer, symbols, "anything");
        writeVariableExpansion(writer, symbols, "anything"); // The body: a variable
        endMacro(writer);
        startMacro(writer, symbols, "Echo");
        writeMacroSignature(writer, symbols); // empty signature
        stepInTdlMacroInvocation(writer, 0); // Macro ID 0 ("SimonSays")
        writer.writeInt(123); // The argument to SimonSays
        writer.stepOut();
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro simonSaysMacro = new TemplateMacro(
            Collections.singletonList(
                ParameterFactory.exactlyOneTagged("anything")
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

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(IntegerSize.INT, reader.getIntegerSize());
            assertEquals(123, reader.intValue());
            assertNull(reader.next());
        }
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void blobsAndClobs(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);

        byte[] blobContents = new byte[] {1, 2};
        byte[] clobContents = new byte[] {3};
        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "lobs", "a");
        startEncodingDirective(writer);
        startMacroTable(writer);
        startMacro(writer, symbols, "lobs");
        writeMacroSignature(writer, symbols, "a");
        writer.stepInSExp(true);
        writer.writeBlob(blobContents);
        writeVariableExpansion(writer, symbols, "a");
        writer.stepOut();
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro expectedMacro = new TemplateMacro(
            Collections.singletonList(new Macro.Parameter("a", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne)),
            Arrays.asList(
                new Expression.SExpValue(Collections.emptyList(), 0, 3),
                new Expression.BlobValue(Collections.emptyList(), blobContents),
                new Expression.VariableRef(0)
            )
        );

        writer.stepInEExp(0, false, expectedMacro);
        writer.writeClob(clobContents);
        writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.SEXP, reader.next());
            reader.stepIn();
            assertEquals(IonType.BLOB, reader.next());
            assertArrayEquals(blobContents, reader.newBytes());
            assertEquals(IonType.CLOB, reader.next());
            assertArrayEquals(clobContents, reader.newBytes());
            reader.stepOut();
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
