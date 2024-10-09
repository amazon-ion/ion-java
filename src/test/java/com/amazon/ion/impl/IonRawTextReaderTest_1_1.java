// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.system.SimpleCatalog;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IonRawTextReaderTest_1_1 {

    public enum ExpressionType {
        E_EXPRESSION,
        EXPRESSION_GROUP,
        NONE;

        void verifyExpressionType(IonReaderTextRawX rawReader) {
            switch (this) {
                case E_EXPRESSION:
                    assertTrue(rawReader._container_is_e_expression);
                    assertFalse(rawReader._container_is_expression_group);
                    break;
                case EXPRESSION_GROUP:
                    assertFalse(rawReader._container_is_e_expression);
                    assertTrue(rawReader._container_is_expression_group);
                    break;
                case NONE:
                    assertFalse(rawReader._container_is_e_expression);
                    assertFalse(rawReader._container_is_expression_group);
                    break;
            }
        }
    }

    static Arguments[] validSyntax() {
        return new Arguments[] {
            Arguments.of(1, "(:foo)", "foo", null, ExpressionType.E_EXPRESSION),
            Arguments.of(1, "(:foo bar)", "foo", "bar", ExpressionType.E_EXPRESSION),
            Arguments.of(1, "(::foo)", "foo", null, ExpressionType.EXPRESSION_GROUP), // TODO do we want to require whitespace after ::?
            Arguments.of(1, "(:: foo bar)", "foo", "bar", ExpressionType.EXPRESSION_GROUP),
            Arguments.of(1, "(:: foo::bar)", "bar", null, ExpressionType.EXPRESSION_GROUP),
            Arguments.of(1, "(::)", null, null, ExpressionType.EXPRESSION_GROUP),
            Arguments.of(1, "(.foo)", ".", "foo", ExpressionType.NONE),
            Arguments.of(1, "(.. foo)", "..", "foo", ExpressionType.NONE),
            Arguments.of(1, "(.+ foo)", ".+", "foo", ExpressionType.NONE),
            Arguments.of(1, "(..+ foo)", "..+", "foo", ExpressionType.NONE),
            Arguments.of(1, "(.+ foo)", ".+", "foo", ExpressionType.NONE),
            Arguments.of(1, "(..+ foo)", "..+", "foo", ExpressionType.NONE),
            Arguments.of(0, "{ foo: bar }", "bar", null, ExpressionType.NONE),
            Arguments.of(1, "{ foo: bar }", "bar", null, ExpressionType.NONE),
            Arguments.of(1, "(foo::bar)", "bar", null, ExpressionType.NONE),
        };
    }

    private static IonReaderTextRawX newTextReader(String input) {
        return new IonReaderTextUserX(
            new SimpleCatalog(),
            LocalSymbolTable.DEFAULT_LST_FACTORY,
            UnifiedInputStreamX.makeStream(input)
        );
    }

    @ParameterizedTest(name = "v={0}:{1}")
    @MethodSource("validSyntax")
    public void validExpressionSyntax(int minorVersion, String input, String firstSymbol, String secondSymbol, ExpressionType expressionType) throws Exception {
        try (IonReaderTextRawX reader = newTextReader(input)) {
            reader.setMinorVersion(minorVersion);
            reader.nextRaw();
            expressionType.verifyExpressionType(reader);
            reader.stepIn();
            if (firstSymbol == null) {
                assertNull(reader.nextRaw());
            } else {
                assertEquals(IonType.SYMBOL, reader.nextRaw());
                assertEquals(firstSymbol, reader.stringValue());
            }
            if (secondSymbol == null) {
                assertNull(reader.nextRaw());
            } else {
                assertEquals(IonType.SYMBOL, reader.nextRaw());
                assertEquals(secondSymbol, reader.stringValue());
                assertNull(reader.nextRaw());
            }
            reader.stepOut();
            assertNull(reader.nextRaw());
        }
    }

    static Arguments[] invalidSyntax() {
        return new Arguments[] {
            // Colon is not a valid operator in Ion 1.0.
            Arguments.of(0, "(:foo)", null),
            Arguments.of(0, "(::foo)", null),
            // Colon is not a valid operator in Ion 1.1 except at the beginning of an s-expression.
            Arguments.of(1, "(:foo :)", "foo"),
            // The following fails on the first next() because the second double-colon does not have a value to follow.
            Arguments.of(1, "(::foo ::)", null),
            // The following fails on the first next() because the double-colon does not have a value to follow.
            Arguments.of(1, "(foo ::)", null),
            Arguments.of(1, "(foo :)", "foo"),
            Arguments.of(1, "{:foo}", null),
            Arguments.of(1, "{::foo}", null),
            Arguments.of(1, "[:foo]", null),
            Arguments.of(1, "[::foo]", null),
        };
    }

    @ParameterizedTest(name = "v={0}:{1}")
    @MethodSource("invalidSyntax")
    public void invalidExpressionSyntax(int minorVersion, String input, String firstSymbol) throws Exception {
        try (IonReaderTextRawX reader = newTextReader(input)) {
            reader.setMinorVersion(minorVersion);
            reader.nextRaw();
            reader.stepIn();
            if (firstSymbol != null) {
                assertEquals(IonType.SYMBOL, reader.nextRaw());
                assertEquals(firstSymbol, reader.stringValue());
            }
            assertThrows(IonReaderTextRawX.IonReaderTextParsingException.class, reader::nextRaw);
        }
    }
}
