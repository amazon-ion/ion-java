// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonSystem;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
            log
        );
        IonDatagram from10 = SYSTEM.getLoader().load(input);
        IonDatagram from11 = SYSTEM.getLoader().load(complete.toByteArray());
        assertEquals(from10, from11);
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
        testMacroize(input, spec, outputBinary, expectedOccurrences);
    }

    // TODO add tests that exercise using every Ion type in macro definitions
    // TODO test substring text pattern
    // TODO address known limitations, as documented in the top-level JavaDoc on MacroizeSpec
}
