// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonSystem;
import com.amazon.ion.impl._Private_IonReaderBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MacroizeTest {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    private static void testMacroize(
        String input,
        String spec,
        boolean outputBinary,
        Integer limit,
        String expectedOutput,
        Map<String, Integer> expectedOccurrences
    ) throws IOException {
        StringBuilder invocations = new StringBuilder();
        ByteArrayOutputStream headless = new ByteArrayOutputStream();
        ByteArrayOutputStream context = new ByteArrayOutputStream();
        ByteArrayOutputStream complete = new ByteArrayOutputStream();
        StringBuilder log = new StringBuilder();
        Macroize.macroize(
            () -> IonReaderBuilder.standard().build(input),
            () -> IonTextWriterBuilder.pretty().build(invocations),
            () -> IonReaderBuilder.standard().build(invocations.toString()),
            () -> headless,
            () -> context,
            () -> {
                complete.write(context.toByteArray());
                complete.write(headless.toByteArray());
            },
            () -> IonReaderBuilder.standard().build(spec),
            outputBinary,
            limit,
            log
        );
        IonDatagram expected = SYSTEM.getLoader().load(expectedOutput);
        IonDatagram from11 = SYSTEM.getLoader().load(complete.toByteArray());
        assertEquals(expected, from11);
        for (Map.Entry<String, Integer> expectedOccurrence : expectedOccurrences.entrySet()) {
            assertTrue(log.toString().contains(
                String.format("%s (total occurrences: %d)", expectedOccurrence.getKey(), expectedOccurrence.getValue()))
            );
        }
        // TODO assert that the text patterns were matched as expected
    }

    @ParameterizedTest(name = "outputBinary={0}")
    @ValueSource(booleans = {true, false})
    public void macroizeWithSpec(boolean outputBinary) throws IOException {
        String spec = "{macros: [(macro foobar (foo bar?) {foo: (%foo), bar: (%bar)})], textPatterns: [(verbatim [baz]), (prefix \"/user/files/\" [a, b])]}";
        String input = "{foo: 1, bar: 2} {foo: 3} \"baz\" {foobar: {foo: 4, bar: 5}, path: \"/user/files/a\"} \"/user/files/c\"";
        Map<String, Integer> expectedOccurrences = new HashMap<String, Integer>() {{
            put("foobar", 3);
        }};
        testMacroize(input, spec, outputBinary, null, input, expectedOccurrences);
    }

    @ParameterizedTest(name = "outputBinary={0}")
    @ValueSource(booleans = {true, false})
    public void macroizeWithSpecAndLimit(boolean outputBinary) throws IOException {
        String spec = "{macros: [(macro foobar (foo bar?) {foo: (%foo), bar: (%bar)})], textPatterns: [(verbatim [baz]), (prefix \"/user/files/\" [a, b])]}";
        String input = "{foo: 1, bar: 2} {foo: 3} \"baz\" {foobar: {foo: 4, bar: 5}, path: \"/user/files/a\"} \"/user/files/c\"";
        Map<String, Integer> expectedOccurrences = new HashMap<String, Integer>() {{
            put("foobar", 1);
        }};
        testMacroize(input, spec, outputBinary, 1, "{foo: 1, bar: 2}", expectedOccurrences);
    }

    @ParameterizedTest(name = "outputBinary={0}")
    @ValueSource(booleans = {true, false})
    public void verbatimWithLimit(boolean outputBinary) throws IOException {
        String input = "$ion_1_1 (:values foo bar) (:values baz) (:values 123 456)";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Macroize.verbatimTranscode(
            () -> ((_Private_IonReaderBuilder) IonReaderBuilder.standard()).buildMacroAware(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))),
            () -> out,
            outputBinary,
            2
        );
        // Limited to the first two items in the stream.
        IonDatagram expected = SYSTEM.getLoader().load("$ion_1_1 (:values foo bar) (:values baz)");
        IonDatagram fromVerbatim = SYSTEM.getLoader().load(out.toByteArray());
        // Note: the accuracy of the verbatim transcode is tested elsewhere.
        assertEquals(expected, fromVerbatim);
    }

    // TODO add tests that exercise using every Ion type in macro definitions
    // TODO test substring text pattern
    // TODO address known limitations, as documented in the top-level JavaDoc on MacroizeSpec
}
