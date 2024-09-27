// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
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

        void verifyExpressionType(IonReader reader) {
            IonReaderTextRawX rawReader = (IonReaderTextRawX) reader;
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
            Arguments.of("$ion_1_1 (:foo)", "foo", null, ExpressionType.E_EXPRESSION),
            Arguments.of("$ion_1_1 (:foo bar)", "foo", "bar", ExpressionType.E_EXPRESSION),
            Arguments.of("$ion_1_1 (::foo)", "foo", null, ExpressionType.EXPRESSION_GROUP), // TODO do we want to require whitespace after ::?
            Arguments.of("$ion_1_1 (:: foo bar)", "foo", "bar", ExpressionType.EXPRESSION_GROUP),
            Arguments.of("$ion_1_1 (:: foo::bar)", "bar", null, ExpressionType.EXPRESSION_GROUP),
            Arguments.of("$ion_1_1 (:)", null, null, ExpressionType.E_EXPRESSION),
            Arguments.of("$ion_1_1 (::)", null, null, ExpressionType.EXPRESSION_GROUP),
            Arguments.of("$ion_1_1 (.foo)", ".", "foo", ExpressionType.NONE),
            Arguments.of("$ion_1_1 (.. foo)", "..", "foo", ExpressionType.NONE),
            Arguments.of("$ion_1_1 (.+ foo)", ".+", "foo", ExpressionType.NONE),
            Arguments.of("$ion_1_1 (..+ foo)", "..+", "foo", ExpressionType.NONE),
            Arguments.of("$ion_1_1 (.+ foo)", ".+", "foo", ExpressionType.NONE),
            Arguments.of("$ion_1_1 (..+ foo)", "..+", "foo", ExpressionType.NONE),
            Arguments.of("$ion_1_0 { foo: bar }", "bar", null, ExpressionType.NONE),
            Arguments.of("$ion_1_1 { foo: bar }", "bar", null, ExpressionType.NONE),
            Arguments.of("$ion_1_1 (foo::bar)", "bar", null, ExpressionType.NONE),
        };
    }

    private static IonReader newTextReader(String input) {
        return new IonReaderTextUserX(
            new SimpleCatalog(),
            LocalSymbolTable.DEFAULT_LST_FACTORY,
            UnifiedInputStreamX.makeStream(input)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validSyntax")
    public void validExpressionSyntax(String input, String firstSymbol, String secondSymbol, ExpressionType expressionType) throws Exception {
        try (IonReader reader = newTextReader(input)) {
            reader.next();
            reader.stepIn();
            if (firstSymbol == null) {
                assertNull(reader.next());
            } else {
                assertEquals(IonType.SYMBOL, reader.next());
                assertEquals(firstSymbol, reader.stringValue());
            }
            expressionType.verifyExpressionType(reader);
            if (secondSymbol == null) {
                assertNull(reader.next());
            } else {
                assertEquals(IonType.SYMBOL, reader.next());
                assertEquals(secondSymbol, reader.stringValue());
                assertNull(reader.next());
            }
            reader.stepOut();
            assertNull(reader.next());
        }
    }

    static Arguments[] invalidSyntax() {
        return new Arguments[] {
            // Colon is not a valid operator in Ion 1.0.
            Arguments.of("$ion_1_0 (:foo)", null),
            Arguments.of("$ion_1_0 (::foo)", null),
            // Colon is not a valid operator in Ion 1.1 except at the beginning of an s-expression.
            Arguments.of("$ion_1_1 (:foo :)", "foo"),
            // The following fails on the first next() because the second double-colon does not have a value to follow.
            Arguments.of("$ion_1_1 (::foo ::)", null),
            // The following fails on the first next() because the double-colon does not have a value to follow.
            Arguments.of("$ion_1_1 (foo ::)", null),
            Arguments.of("$ion_1_1 (foo :)", "foo"),
            Arguments.of("$ion_1_1 {:foo}", null),
            Arguments.of("$ion_1_1 {::foo}", null),
            Arguments.of("$ion_1_1 [:foo]", null),
            Arguments.of("$ion_1_1 [::foo]", null),
        };
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidSyntax")
    public void invalidExpressionSyntax(String input, String firstSymbol) throws Exception {
        try (IonReader reader = newTextReader(input)) {
            reader.next();
            reader.stepIn();
            if (firstSymbol != null) {
                assertEquals(IonType.SYMBOL, reader.next());
                assertEquals(firstSymbol, reader.stringValue());
            }
            assertThrows(IonReaderTextRawX.IonReaderTextParsingException.class, reader::next);
        }
    }
}
