// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.FakeSymbolToken;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonEncodingVersion;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonText;
import com.amazon.ion.IonType;
import com.amazon.ion.MacroAwareIonReader;
import com.amazon.ion.MacroAwareIonWriter;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1;
import com.amazon.ion.impl.macro.EncodingContext;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroRef;
import com.amazon.ion.impl.macro.MacroTable;
import com.amazon.ion.impl.macro.ParameterFactory;
import com.amazon.ion.impl.macro.SystemMacro;
import com.amazon.ion.impl.macro.TemplateMacro;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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

    private static void assertMacroTablesContainsExpectedMappings(IonReader reader, StreamType streamType, SortedMap<String, Macro> expected) {
        Map<MacroRef, Macro> expectedByRef = streamType.newMacroTableByMacroRef(expected);

        MacroTable actual = streamType.getEncodingContext(reader).getMacroTable();
        // TODO: This assertion is weak, we don't know that the actual macro table contains *only* the expectations
        expectedByRef.forEach((k,v) -> assertEquals(v, actual.get(k)));
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

    private static Map<String, Integer> makeSymbolsMap(int startId, String... userSymbols) {
        Map<String, Integer> symbols = new HashMap<>();
        int localSymbolId = startId;
        for (String userSymbol : userSymbols) {
            symbols.put(userSymbol, localSymbolId++);
        }
        return symbols;
    }

    private static Map<String, Integer> initializeSymbolTable(IonRawWriter_1_1 writer, String... userSymbols) {
        startEncodingDirective(writer);
        writeEncodingDirectiveSymbolTable(writer, userSymbols);
        endEncodingDirective(writer);
        return makeSymbolsMap(FIRST_LOCAL_SYMBOL_ID, userSymbols);
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

    private static void writeMacroSignatureFromDatagram(IonRawWriter_1_1 writer, Map<String, Integer> symbols, IonDatagram... signature) {
        writer.stepInSExp(false);
        for (IonDatagram parameter : signature) {
            if (parameter.size() > 2) {
                throw new IllegalStateException("Parameters can only have two components: a name and a cardinality.");
            }
            IonText name = (IonText) parameter.get(0);
            String[] encoding = name.getTypeAnnotations();
            if (encoding.length == 1) {
                // The encoding, e.g. uint8
                writer.writeAnnotations(encoding);
            } else if (encoding.length > 1) {
                throw new IllegalStateException("Only one encoding annotation is allowed.");
            }
            // The name
            writeSymbol(writer, symbols, name.stringValue());
            if (parameter.size() == 2) {
                // The cardinality, e.g. *
                writeSymbol(writer, symbols, ((IonText) parameter.get(1)).stringValue());
            }
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

            @Override
            MacroAwareIonWriter newMacroAwareWriter(OutputStream out) {
                return (MacroAwareIonWriter) IonEncodingVersion.ION_1_1.binaryWriterBuilder().build(out);
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

            @Override
            MacroAwareIonWriter newMacroAwareWriter(OutputStream out) {
                return (MacroAwareIonWriter) IonEncodingVersion.ION_1_1.textWriterBuilder().build(out);
            }
        };

        abstract IonRawWriter_1_1 newWriter(OutputStream out);
        abstract EncodingContext getEncodingContext(IonReader reader);
        abstract Map<MacroRef, Macro> newMacroTableByMacroRef(SortedMap<String, Macro> macrosByName);
        abstract void startMacroInvocationByName(IonRawWriter_1_1 writer, String name, Map<String, Macro> macrosByName);
        abstract MacroAwareIonWriter newMacroAwareWriter(OutputStream out);
    }

    public enum InputType {
        INPUT_STREAM {
            @Override
            IonReader newReader(byte[] input) {
                return IonReaderBuilder.standard().build(new ByteArrayInputStream(input));
            }

            @Override
            MacroAwareIonReader newMacroAwareReader(byte[] input) {
                throw new UnsupportedOperationException("Building MacroAwareIonReader from InputStream not yet supported.");
            }
        },
        BYTE_ARRAY {
            @Override
            IonReader newReader(byte[] input) {
                return IonReaderBuilder.standard().build(input);
            }

            @Override
            MacroAwareIonReader newMacroAwareReader(byte[] input) {
                return ((_Private_IonReaderBuilder) IonReaderBuilder.standard()).buildMacroAware(input);
            }
        };

        abstract IonReader newReader(byte[] input);
        abstract MacroAwareIonReader newMacroAwareReader(byte[] input);
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
            assertMacroTablesContainsExpectedMappings(reader, streamType, expectedMacroTable);
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
            assertMacroTablesContainsExpectedMappings(reader, streamType, expectedMacroTable);
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
            assertMacroTablesContainsExpectedMappings(reader, streamType, expectedMacroTable);
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
            assertMacroTablesContainsExpectedMappings(reader, streamType, expectedMacroTable);
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
            assertMacroTablesContainsExpectedMappings(reader, streamType, expectedMacroTable);
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
            assertMacroTablesContainsExpectedMappings(reader, streamType, expectedMacroTable);
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
            assertMacroTablesContainsExpectedMappings(reader, streamType, expectedMacroTable);
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

    private byte[] macroInvocationsNestedWithinParameter(StreamType streamType) {
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

        return getBytes(writer, out);
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationsNestedWithinParameter(InputType inputType, StreamType streamType) throws Exception {
        byte[] data = macroInvocationsNestedWithinParameter(streamType);
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

    /**
     * Counts the number of times the given substring occurs in the given string.
     * @param string the string.
     * @param substring the substring.
     * @return the number of occurrences.
     */
    private static int countOccurrencesOfSubstring(String string, String substring) {
        int lastMatchIndex = 0;
        int count = 0;
        while (lastMatchIndex >= 0) {
            lastMatchIndex = string.indexOf(substring, lastMatchIndex);
            if (lastMatchIndex >= 0) {
                lastMatchIndex += substring.length();
                count++;
            }
        }
        return count;
    }

    /**
     * Performs a macro-aware transcode of the given data, verifying that the resulting stream has the
     * characteristics described by the arguments to this method and that it is data-model equivalent
     * to the source data.
     * @param data the source data.
     * @param inputType the InputType to test.
     * @param streamType the StreamType to which the source data will be transcoded.
     * @param expectedNumberOfIonVersionMarkers the expected number of Ion version markers in the transcoded stream (text only).
     * @param expectedNumberOfAddSymbolsInvocations the expected number of add_symbols invocations in the transcoded stream (text only).
     * @param expectedNumberOfAddMacrosInvocations the expected number of add_macros invocations in the transcoded stream (text only).
     * @param expectedNumberOfSetSymbolsInvocations the expected number of set_symbols invocations in the transcoded stream (text only).
     * @param expectedNumberOfSetMacrosInvocations the expected number of set_macros invocations in the transcoded stream (text only).
     * @param expectedNumberOfExplicitEncodingDirectives the expected number of $ion_encoding occurrences in the transcoded stream (text only).
     */
    private void verifyMacroAwareTranscode(
        byte[] data,
        InputType inputType,
        StreamType streamType,
        int expectedNumberOfIonVersionMarkers,
        int expectedNumberOfAddSymbolsInvocations,
        int expectedNumberOfAddMacrosInvocations,
        int expectedNumberOfSetSymbolsInvocations,
        int expectedNumberOfSetMacrosInvocations,
        int expectedNumberOfExplicitEncodingDirectives
    ) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (
            MacroAwareIonReader reader = inputType.newMacroAwareReader(data);
            MacroAwareIonWriter rewriter = streamType.newMacroAwareWriter(out);
        ) {
            reader.transcodeTo(rewriter);
        }
        if (streamType == StreamType.TEXT) {
            String rewritten = out.toString(StandardCharsets.UTF_8.name());
            assertEquals(expectedNumberOfIonVersionMarkers, countOccurrencesOfSubstring(rewritten, "$ion_1_1"));
            assertEquals(expectedNumberOfAddSymbolsInvocations, countOccurrencesOfSubstring(rewritten, SystemSymbols_1_1.ADD_SYMBOLS.getText()));
            assertEquals(expectedNumberOfAddMacrosInvocations, countOccurrencesOfSubstring(rewritten, SystemSymbols_1_1.ADD_MACROS.getText()));
            assertEquals(expectedNumberOfSetSymbolsInvocations, countOccurrencesOfSubstring(rewritten, SystemSymbols_1_1.SET_SYMBOLS.getText()));
            assertEquals(expectedNumberOfSetMacrosInvocations, countOccurrencesOfSubstring(rewritten, SystemSymbols_1_1.SET_MACROS.getText()));
            assertEquals(expectedNumberOfExplicitEncodingDirectives, countOccurrencesOfSubstring(rewritten, SystemSymbols_1_1.ION_ENCODING.getText()));
        }
        IonSystem system = IonSystemBuilder.standard().build();
        IonDatagram actual = system.getLoader().load(out.toByteArray());
        IonDatagram expected = system.getLoader().load(data);
        assertEquals(expected, actual);
    }

    @Test // TODO parameterize for all combinations once support for macro-aware text reading is added
    public void macroInvocationsNestedWithinParameterMacroAwareTranscode() throws Exception {
        byte[] data = macroInvocationsNestedWithinParameter(StreamType.BINARY);
        verifyMacroAwareTranscode(
            data,
            InputType.BYTE_ARRAY,
            StreamType.TEXT,
            1,
            0,
            0,
            0,
            0,
            2
        );
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

    private byte[] macroInvocationInMacroDefinition(StreamType streamType) {
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

        return getBytes(writer, out);
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationInMacroDefinition(InputType inputType, StreamType streamType) throws Exception {
        byte[] data = macroInvocationInMacroDefinition(streamType);
        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(IntegerSize.INT, reader.getIntegerSize());
            assertEquals(123, reader.intValue());
            assertNull(reader.next());
        }
    }

    @Test // TODO parameterize for all combinations once support for macro-aware text reading is added
    public void macroInvocationInMacroDefinitionMacroAwareTranscode() throws Exception {
        byte[] data = macroInvocationInMacroDefinition(StreamType.BINARY);
        verifyMacroAwareTranscode(
            data,
            InputType.BYTE_ARRAY,
            StreamType.TEXT,
            1,
            0,
            0,
            0,
            0,
            2
        );
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

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationInTaggedExpressionGroup(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);

        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "foo", "value");
        startEncodingDirective(writer);
        startMacroTable(writer);
        startMacro(writer, symbols, "foo");
        writeMacroSignature(writer, symbols, "value", "*");
        writeVariableExpansion(writer, symbols, "value");
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro expectedMacro = new TemplateMacro(
            Collections.singletonList(new Macro.Parameter("value", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrMore)),
            Collections.singletonList(new Expression.VariableRef(0))
        );

        writer.stepInEExp(0, false, expectedMacro); {
            writer.stepInExpressionGroup(true); {
                writer.stepInEExp(SystemMacro.Values); {
                    writer.writeString("bar");
                } writer.stepOut();
            } writer.stepOut();
        } writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.STRING, reader.next());
            assertEquals("bar", reader.stringValue());
            assertNull(reader.next());
        }
    }

    private static final IonLoader LOADER = IonSystemBuilder.standard().build().getLoader();

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void taglessExpressionGroup(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);

        writer.writeIVM();
        Map<String, Integer> symbols = initializeSymbolTable(writer, "foo", "value");
        startEncodingDirective(writer);
        startMacroTable(writer);
        startMacro(writer, symbols, "foo");
        writeMacroSignatureFromDatagram(writer, symbols, LOADER.load("uint8::value '*'"));
        writeVariableExpansion(writer, symbols, "value");
        endMacro(writer);
        endMacroTable(writer);
        endEncodingDirective(writer);

        Macro expectedMacro = new TemplateMacro(
            Collections.singletonList(new Macro.Parameter("value", Macro.ParameterEncoding.Uint8, Macro.ParameterCardinality.ZeroOrMore)),
            Collections.singletonList(new Expression.VariableRef(0))
        );

        writer.stepInEExp(0, false, expectedMacro); {
            writer.stepInExpressionGroup(true); {
                writer.writeInt(1);
                writer.writeInt(2);
            } writer.stepOut();
        } writer.stepOut();

        byte[] data = getBytes(writer, out);

        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.INT, reader.next());
            assertEquals(1, reader.intValue());
            assertEquals(IonType.INT, reader.next());
            assertEquals(2, reader.intValue());
            assertNull(reader.next());
        }
    }

    private static void writeSymbolTableEExpression(boolean isAppend, IonRawWriter_1_1 writer, String... symbols) {
        writer.stepInEExp(isAppend ? SystemMacro.AddSymbols : SystemMacro.SetSymbols);
        writer.stepInExpressionGroup(false);
        for (String symbol : symbols) {
            writer.writeString(symbol);
        }
        writer.stepOut();
        writer.stepOut();
    }

    private static Map<String, Integer> writeSymbolTableSetEExpression(IonRawWriter_1_1 writer, String... symbols) {
        writeSymbolTableEExpression(false, writer, symbols);
        return makeSymbolsMap(FIRST_LOCAL_SYMBOL_ID, symbols);
    }

    private static void writeSymbolTableAppendEExpression(IonRawWriter_1_1 writer, Map<String, Integer> existingSymbols, String... newSymbols) {
        writeSymbolTableEExpression(true, writer, newSymbols);
        int localSymbolId = FIRST_LOCAL_SYMBOL_ID + existingSymbols.size();
        for (String newSymbol : newSymbols) {
            existingSymbols.putIfAbsent(newSymbol, localSymbolId++);
        }
    }

    private static byte[] macroInvocationsProduceEncodingDirectivesThatModifySymbolTable(StreamType streamType) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();

        Map<String, Integer> symbols = writeSymbolTableSetEExpression(writer, "foo", "bar");
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID + 1);

        writeSymbolTableAppendEExpression(writer, symbols, "baz");
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID + 2);

        writeSymbolTableSetEExpression(writer, "abc", "def");
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID + 1);

        return getBytes(writer, out);
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationsProduceEncodingDirectivesThatModifySymbolTable(InputType inputType, StreamType streamType) throws Exception {
        byte[] data = macroInvocationsProduceEncodingDirectivesThatModifySymbolTable(streamType);
        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("foo", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("bar", reader.stringValue());
            // Symbol "baz" added
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("foo", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("baz", reader.stringValue());
            // Symbol table replaced with "abc", "def"
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("abc", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("def", reader.stringValue());
        }
    }

    @Test // TODO parameterize for all combinations once support for macro-aware text reading is added
    public void macroInvocationsProduceEncodingDirectivesThatModifySymbolTableMacroAwareTranscode() throws Exception {
        byte[] data = macroInvocationsProduceEncodingDirectivesThatModifySymbolTable(StreamType.BINARY);
        verifyMacroAwareTranscode(
            data,
            InputType.BYTE_ARRAY,
            StreamType.TEXT,
            1,
            1,
            0,
            2,
            0,
            0
        );
    }

    private static Map<String, Integer> systemSymbols() {
        return makeSymbolsMap(FIRST_LOCAL_SYMBOL_ID, SystemSymbols_1_1.allSymbolTexts().toArray(new String[0]));
    }

    private static byte[] macroInvocationsProduceEncodingDirectivesThatModifyMacroTable(StreamType streamType) {
        BigDecimal pi = new BigDecimal("3.14159");
        SortedMap<String, Macro> macroTable = new TreeMap<>();
        macroTable.put("Pi", new TemplateMacro(
            Collections.emptyList(),
            Collections.singletonList(new Expression.DecimalValue(Collections.emptyList(), pi))
        ));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();

        Map<String, Integer> symbols = systemSymbols();
        writeSymbolTableAppendEExpression(writer, symbols, "Pi"); // appends Pi after the system symbols.

        writer.stepInEExp(SystemMacro.AddMacros);
        startMacro(writer, symbols, "Pi");
        writeMacroSignature(writer, symbols); // Empty signature
        writer.writeDecimal(pi);
        endMacro(writer);
        writer.stepOut();

        writer.writeSymbol(SystemSymbols_1_1.size() + FIRST_LOCAL_SYMBOL_ID); // Pi
        streamType.startMacroInvocationByName(writer, "Pi", macroTable);
        writer.stepOut();

        symbols = writeSymbolTableSetEExpression(writer, "Pi", "foo");

        macroTable.put("foo", new TemplateMacro(
            Collections.emptyList(),
            Collections.singletonList(new Expression.StringValue(Collections.emptyList(), "bar"))
        ));

        writer.stepInEExp(SystemMacro.AddMacros);
        startMacro(writer, symbols, "foo");
        writeMacroSignature(writer, symbols); // Empty signature
        writer.writeString("bar");
        endMacro(writer);
        writer.stepOut();

        writer.stepInEExp(1, false, macroTable.get("foo")); // ID 1 because Pi (ID 0) is still in the table.
        writer.stepOut();
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID); // Now "Pi" because SetSymbols was used, replacing the system symbols.

        writer.stepInEExp(SystemMacro.SetMacros);
        startMacro(writer, symbols, "foo");
        writeMacroSignature(writer, symbols); // Empty signature
        writer.writeString("baz");
        endMacro(writer);
        writer.stepOut();

        writer.stepInEExp(0, false, macroTable.get("foo")); // ID 0 now because SetMacros was used.
        writer.stepOut();
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID + 1); // Still foo because AddMacros/SetMacros does not mutate the symbol table.

        return getBytes(writer, out);
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void macroInvocationsProduceEncodingDirectivesThatModifyMacroTable(InputType inputType, StreamType streamType) throws Exception {
        byte[] data = macroInvocationsProduceEncodingDirectivesThatModifyMacroTable(streamType);
        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("Pi", reader.stringValue());
            assertEquals(IonType.DECIMAL, reader.next());
            assertEquals(new BigDecimal("3.14159"), reader.bigDecimalValue());

            assertEquals(IonType.STRING, reader.next());
            assertEquals("bar", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("Pi", reader.stringValue());

            assertEquals(IonType.STRING, reader.next());
            assertEquals("baz", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("foo", reader.stringValue());

            assertNull(reader.next());
        }
    }

    @Test // TODO parameterize for all combinations once support for macro-aware text reading is added
    public void macroInvocationsProduceEncodingDirectivesThatModifyMacroTableMacroAwareTranscode() throws Exception {
        byte[] data = macroInvocationsProduceEncodingDirectivesThatModifyMacroTable(StreamType.BINARY);
        verifyMacroAwareTranscode(
            data,
            InputType.BYTE_ARRAY,
            StreamType.TEXT,
            1,
            1,
            2,
            1,
            1,
            0
        );
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void multipleListsWithinSymbolTableDeclaration(InputType inputType, StreamType streamType) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();

        startEncodingDirective(writer);
        writer.stepInSExp(false);
        writer.writeSymbol(SystemSymbols.SYMBOL_TABLE);
        writer.stepInList(false);
        writer.writeString("foo");
        writer.stepOut();
        writer.stepInList(true);
        writer.writeString("bar");
        writer.stepOut();
        writer.stepOut();
        endEncodingDirective(writer);

        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID + 1);
        writer.writeSymbol(FIRST_LOCAL_SYMBOL_ID);

        byte[] data = getBytes(writer, out);
        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("bar", reader.stringValue());
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("foo", reader.stringValue());

            assertNull(reader.next());
        }
    }

    private byte[] emptyMacroAppendToEmptyTable(StreamType streamType) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();

        startEncodingDirective(writer);
        startMacroTable(writer);
        writer.writeSymbol(SystemSymbols_1_1.ION_ENCODING);
        endMacroTable(writer);
        endEncodingDirective(writer);

        return getBytes(writer, out);
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void emptyMacroAppendToEmptyTable(InputType inputType, StreamType streamType) throws Exception {
        byte[] data = emptyMacroAppendToEmptyTable(streamType);
        try (IonReader reader = inputType.newReader(data)) {
            assertNull(reader.next());
        }
    }

    @Test // TODO parameterize for all combinations once support for macro-aware text reading is added
    public void emptyMacroAppendToEmptyTableMacroAwareTranscode() throws Exception {
        byte[] data = emptyMacroAppendToEmptyTable(StreamType.BINARY);
        verifyMacroAwareTranscode(
            data,
            InputType.BYTE_ARRAY,
            StreamType.TEXT,
            1,
            0,
            0,
            0,
            0,
            0 // The empty append to an empty table has no effect, and it is not transcoded. This is a known limitation. TODO document the limitations
        );
    }

    private byte[] emptyMacroAppendToNonEmptyTable(StreamType streamType) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();

        SortedMap<String, Macro> macroTable = new TreeMap<>();
        macroTable.put("foo", new TemplateMacro(
            Collections.singletonList(new Macro.Parameter("foo", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ExactlyOne)),
            Collections.singletonList(new Expression.VariableRef(0))
        ));
        Map<String, Integer> symbols = Collections.emptyMap();

        startEncodingDirective(writer); {
            startMacroTable(writer); {
                startMacro(writer, symbols, "foo"); {
                    writeMacroSignature(writer, symbols, "x");
                    writeVariableExpansion(writer, symbols, "x");
                } endMacro(writer);
            } endMacroTable(writer);
        } endEncodingDirective(writer);


        startEncodingDirective(writer); {
            startMacroTable(writer); {
                writer.writeSymbol(SystemSymbols_1_1.ION_ENCODING);
            } endMacroTable(writer);
            writeEncodingDirectiveSymbolTable(writer, true, "bar");
        } endEncodingDirective(writer);

        writer.stepInEExp(0, true, macroTable.get("foo")); {
            writer.writeSymbol(1);
        } writer.stepOut();

        return getBytes(writer, out);
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void emptyMacroAppendToNonEmptyTable(InputType inputType, StreamType streamType) throws Exception {
        byte[] data = emptyMacroAppendToNonEmptyTable(streamType);
        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.SYMBOL, reader.next());
            assertEquals("bar", reader.stringValue());
        }
    }

    @Test // TODO parameterize for all combinations once support for macro-aware text reading is added
    public void emptyMacroAppendToNonEmptyTableMacroAwareTranscode() throws Exception {
        byte[] data = emptyMacroAppendToNonEmptyTable(StreamType.BINARY);
        verifyMacroAwareTranscode(
            data,
            InputType.BYTE_ARRAY,
            StreamType.TEXT,
            1,
            0,
            0,
            0,
            0,
            3 // Two encoding directives, plus one $ion_encoding symbol to denote the macro table append.
        );
    }

    private byte[] invokeUnqualifiedSystemMacroInTDL(StreamType streamType) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = streamType.newWriter(out);
        writer.writeIVM();

        SortedMap<String, Macro> macroTable = new TreeMap<>();
        macroTable.put("foo", new TemplateMacro(
            Collections.singletonList(new Macro.Parameter("x", Macro.ParameterEncoding.Tagged, Macro.ParameterCardinality.ZeroOrMore)),
            Arrays.asList(
                new Expression.MacroInvocation(SystemMacro.Default, 0, 3),
                new Expression.VariableRef(0),
                new Expression.StringValue(Collections.emptyList(), "hello world")
            )
        ));
        Map<String, Integer> symbols = Collections.emptyMap();


        startEncodingDirective(writer); {
            startMacroTable(writer); {
                // Define our macro (macro foo (x) (.default (%x) "hello world"))
                startMacro(writer, symbols, "foo"); {
                    writeMacroSignatureFromDatagram(writer, symbols, LOADER.load("x '*'"));
                    stepInTdlMacroInvocation(writer, (int) SystemMacro.Default.getId()); {
                        writeVariableExpansion(writer, symbols, "x");
                        writer.writeString("hello world");
                    } endMacro(writer); // (.default
                } endMacro(writer); // (macro foo
            } endMacroTable(writer);
        } endEncodingDirective(writer);

        // Invoke (:foo) with no parameter
        writer.stepInEExp(0, true, macroTable.get("foo")); {
        } writer.stepOut();

        return getBytes(writer, out);
    }

    @ParameterizedTest(name = "{0},{1}")
    @MethodSource("allCombinations")
    public void invokeUnqualifiedSystemMacroInTDL(InputType inputType, StreamType streamType) throws Exception {
        byte[] data = invokeUnqualifiedSystemMacroInTDL(streamType);
        try (IonReader reader = inputType.newReader(data)) {
            assertEquals(IonType.STRING, reader.next());
            assertEquals("hello world", reader.stringValue());
        }
    }

    @Test // TODO parameterize for all combinations once support for macro-aware text reading is added
    public void invokeUnqualifiedSystemMacroInTDLMacroAwareTranscode() throws Exception {
        byte[] data = invokeUnqualifiedSystemMacroInTDL(StreamType.BINARY);
        verifyMacroAwareTranscode(
            data,
            InputType.BYTE_ARRAY,
            StreamType.TEXT,
            1,
            0,
            0,
            0,
            0,
            1
        );
    }

    @Test // TODO parameterize for all combinations once support for macro-aware text reading is added
    public void multipleIonVersionMarkersMacroAwareTranscode() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonRawWriter_1_1 writer = StreamType.BINARY.newWriter(out);
        Map<String, Integer> symbols = new HashMap<>();
        writer.writeIVM();
        writeSymbolTableAppendEExpression(writer, symbols, "foo");
        writer.writeSymbol(SystemSymbols_1_1.size() + FIRST_LOCAL_SYMBOL_ID); // foo
        writer.writeIVM();
        writeSymbolTableAppendEExpression(writer, symbols, "bar"); // bar
        writer.writeSymbol(SystemSymbols_1_1.size() + FIRST_LOCAL_SYMBOL_ID);
        byte[] data = getBytes(writer, out);
        verifyMacroAwareTranscode(
            data,
            InputType.BYTE_ARRAY,
            StreamType.TEXT,
            2,
            2,
            0,
            0,
            0,
            0
        );
    }

    // TODO cover every Ion type
    // TODO annotations in macro definition (using 'annotate' system macro)
    // TODO test error conditions
    // TODO support continuable and lazy evaluation
    // TODO early step-out of evaluation; skipping evaluation.
    // TODO ZeroOrOne and ExactlyOne cardinality parameter with single-element group (legal?)
}
