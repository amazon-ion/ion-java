// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.FakeSymbolToken;
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

    // TODO additional tests
}
