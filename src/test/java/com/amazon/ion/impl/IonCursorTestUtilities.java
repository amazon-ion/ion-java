// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonType;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static com.amazon.ion.IonCursor.Event.END_CONTAINER;
import static com.amazon.ion.IonCursor.Event.NEEDS_DATA;
import static com.amazon.ion.IonCursor.Event.NEEDS_INSTRUCTION;
import static com.amazon.ion.IonCursor.Event.START_CONTAINER;
import static com.amazon.ion.IonCursor.Event.START_SCALAR;
import static com.amazon.ion.IonCursor.Event.VALUE_READY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IonCursorTestUtilities {

    static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION = IonBufferConfiguration.Builder.standard().build();

    /**
     * Contains the logic to assert that the given Cursor meets a particular expectation. This is an abstract class
     * rather than a Consumer for two reasons, both related to debuggability: 1) the custom toString method makes it
     * much easier to identify the expectations that will be tested during debugging, and 2) certain IDE configurations
     * seem to have trouble stepping into the 'accept' method of java.util.function.Consumer.
     */
    static class Expectation<T extends IonCursorBinary> {

        private final String description;
        private final Consumer<T> test;

        Expectation(String description, Consumer<T> test) {
            this.description = description;
            this.test = test;
        }

        public void test(T cursor) {
            test.accept(cursor);
        }

        @Override
        public String toString() {
            return description;
        }
    }

    static final Expectation<? extends IonCursorBinary> SCALAR = new Expectation<>("scalar", cursor -> {
        assertEquals(START_SCALAR, cursor.nextValue());
    });
    static final Expectation<? extends IonCursorBinary> CONTAINER_START = new Expectation<>("container_start", cursor -> {
        assertEquals(START_CONTAINER, cursor.nextValue());
    });
    static final Expectation<? extends IonCursorBinary> STEP_IN = new Expectation<>("step_in", cursor -> {
        assertEquals(NEEDS_INSTRUCTION, cursor.stepIntoContainer());
    });
    static final Expectation<? extends IonCursorBinary> STEP_OUT = new Expectation<>("step_out", cursor -> {
        assertEquals(NEEDS_INSTRUCTION, cursor.stepOutOfContainer());
    });
    static final Expectation<? extends IonCursorBinary> CONTAINER_END = new Expectation<>("container_end", cursor -> {
        assertEquals(END_CONTAINER, cursor.nextValue());
    });
    static final Expectation<? extends IonCursorBinary> STREAM_END = new Expectation<>("stream_end", cursor -> {
        assertEquals(NEEDS_DATA, cursor.nextValue());
    });
    static final Expectation<? extends IonCursorBinary> NO_EXPECTATION = new Expectation<>("no_op", cursor -> {});

    /**
     * Feeds Expectations to a given Consumer, allowing for deferred collection and execution of expectations.
     */
    @FunctionalInterface
    interface ExpectationProvider<T extends IonCursorBinary> extends Consumer<Consumer<Expectation<T>>> {}

    /**
     * Collects the Expectations from all providers into a flat List.
     */
    @SafeVarargs
    static <T extends IonCursorBinary> List<Expectation<T>> collectExpectations(ExpectationProvider<T>... providers) {
        List<Expectation<T>> expectations = new ArrayList<>();
        for (Consumer<Consumer<Expectation<T>>> provider : providers) {
            provider.accept(expectations::add);
        }
        return expectations;
    }

    /**
     * Tests the given cursor against all expectations, in order. This provides the easiest debugging entrypoint.
     * Set a breakpoint on the invocation of this method in the test of interest, then step through the expectation
     * evaluations, stepping into the cursor when desired.
     */
    @SafeVarargs
    static <T extends IonCursorBinary> void assertSequence(T cursor, ExpectationProvider<T>... providers) {
        List<Expectation<T>> expectations = collectExpectations(providers);
        for (Expectation<T> expectation : expectations) {
            expectation.test(cursor);
        }
    }

    /**
     * Provides Expectations that verify that advancing the cursor positions it on a container value that matches the
     * given expectation, and that the container's child values match the given expectations, without filling the
     * container up-front.
     */
    @SafeVarargs
    @SuppressWarnings("unchecked")
    static <T extends IonCursorBinary> ExpectationProvider<T> container(Expectation<T> expectedOnContainer, ExpectationProvider<T>... expectations) {
        return consumer -> {
            consumer.accept((Expectation<T>) CONTAINER_START);
            if (expectedOnContainer != NO_EXPECTATION) {
                consumer.accept(expectedOnContainer);
            }
            consumer.accept((Expectation<T>) STEP_IN);
            for (Consumer<Consumer<Expectation<T>>> expectation : expectations) {
                expectation.accept(consumer);
            }
            consumer.accept((Expectation<T>) STEP_OUT);
        };
    }

    /**
     * Provides Expectations that verify that advancing the cursor positions it on a container value, and that the
     * container's child values match the given expectations, without filling the container up-front.
     */
    @SafeVarargs
    @SuppressWarnings("unchecked")
    static <T extends IonCursorBinary> ExpectationProvider<T> container(ExpectationProvider<T>... expectations) {
        return container((Expectation<T>) NO_EXPECTATION, expectations);
    }

    /**
     * Provides an Expectation that verifies that advancing the cursor positions it on a scalar value that matches the
     * given expectation, without filling that scalar.
     */
    @SuppressWarnings("unchecked")
    static <T extends IonCursorBinary> ExpectationProvider<T> scalar(Expectation<T> expectedOnScalar) {
        return consumer -> {
            consumer.accept((Expectation<T>) SCALAR);
            if (expectedOnScalar != NO_EXPECTATION) {
                consumer.accept(expectedOnScalar);
            }
        };
    }

    /**
     * Provides an Expectation that verifies that advancing the cursor positions it on a scalar value, without filling
     * that scalar.
     */
    @SuppressWarnings("unchecked")
    static <T extends IonCursorBinary> ExpectationProvider<T> scalar() {
        return scalar((Expectation<T>) NO_EXPECTATION);
    }

    /**
     * Provides an Expectation that verifies that the value on which the cursor is currently positioned has the given
     * type.
     */
    static <T extends IonReaderContinuableCoreBinary> ExpectationProvider<T> type(IonType expectedType) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("type(%s)", expectedType),
            cursor -> assertEquals(expectedType, cursor.getType()))
        );
    }

    /**
     * Provides Expectations that verify that advancing the cursor to the next value positions the cursor on a scalar
     * with type int and the given expected value.
     */
    static <T extends IonReaderContinuableCoreBinary> ExpectationProvider<T> fillIntValue(int expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("int(%d)", expectedValue),
            reader -> {
                assertEquals(VALUE_READY, reader.fillValue());
                assertEquals(IonType.INT, reader.getType());
                assertEquals(expectedValue, reader.intValue());
            }
        ));
    }

    /**
     * Provides Expectations that verify that advancing the cursor to the next value positions the cursor on a scalar
     * with type string and the given expected value.
     */
    static <T extends IonReaderContinuableCoreBinary> ExpectationProvider<T> fillStringValue(String expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("string(%s)", expectedValue),
            reader -> {
                assertEquals(VALUE_READY, reader.fillValue());
                assertEquals(IonType.STRING, reader.getType());
                assertEquals(expectedValue, reader.stringValue());
            }
        ));
    }

    /**
     * Provides Expectations that verify that advancing the cursor to the next value positions the cursor on a scalar
     * with type symbol and the given expected value.
     */
    static <T extends IonReaderContinuableApplicationBinary> ExpectationProvider<T> fillSymbolValue(String expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("symbol(%s)", expectedValue),
            reader -> {
                assertEquals(VALUE_READY, reader.fillValue());
                assertEquals(IonType.SYMBOL, reader.getType());
                assertEquals(expectedValue, reader.stringValue());
            }
        ));
    }

    /**
     * Provides Expectations that verify that advancing the cursor to the next value positions the cursor on a scalar
     * with type symbol and the given expected value.
     */
    static <T extends IonReaderContinuableCoreBinary> ExpectationProvider<T> fillSymbolValue(int expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("symbol($%s)", expectedValue),
            reader -> {
                assertEquals(VALUE_READY, reader.fillValue());
                assertEquals(IonType.SYMBOL, reader.getType());
                assertEquals(expectedValue, reader.symbolValueId());
            }
        ));
    }

    static <T extends IonReaderContinuableCoreBinary> ExpectationProvider<T> integerSize(IntegerSize expectedSize) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("integerSize(%s)", expectedSize),
            reader -> {
                assertEquals(expectedSize, reader.getIntegerSize());
            }
        ));
    }

    static <T extends IonReaderContinuableCoreBinary> ExpectationProvider<T> intValue(int expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("int(%d)", expectedValue),
            reader -> {
                assertEquals(IntegerSize.INT, reader.getIntegerSize());
                assertEquals(expectedValue, reader.intValue());
            }
        ));
    }

    static <T extends IonReaderContinuableCoreBinary> ExpectationProvider<T> longValue(long expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("long(%d)", expectedValue),
            reader -> {
                assertTrue(reader.getIntegerSize().ordinal() <= IntegerSize.LONG.ordinal());
                assertEquals(expectedValue, reader.longValue());
            }
        ));
    }

    static <T extends IonReaderContinuableCoreBinary> ExpectationProvider<T> bigIntegerValue(BigInteger expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("bigInteger(%s)", expectedValue),
            reader -> assertEquals(expectedValue, reader.bigIntegerValue())
        ));
    }

    static <T extends IonReaderContinuableCoreBinary> ExpectationProvider<T> doubleValue(double expectedValue) {
        return consumer -> consumer.accept(new Expectation<>(
            String.format("double(%f)", expectedValue),
            reader -> assertEquals(expectedValue, reader.doubleValue(), 1e-9)
        ));
    }

    /**
     * Provides an Expectation that verifies that advancing the cursor positions it on a container value, without
     * filling that container.
     */
    @SuppressWarnings("unchecked")
    static <T extends IonCursorBinary> ExpectationProvider<T> startContainer() {
        return consumer -> consumer.accept((Expectation<T>) CONTAINER_START);
    }

    /**
     * Provides an Expectation that verifies that advancing the cursor results in the end of the current container.
     */
    @SuppressWarnings("unchecked")
    static <T extends IonCursorBinary> ExpectationProvider<T> endContainer() {
        return consumer -> consumer.accept((Expectation<T>) CONTAINER_END);
    }

    /**
     * Provides Expectations that verify that advancing the cursor to the next value positions the cursor on a
     * container with the given type, that filling the value succeeds, and that the container's child values match the
     * given expectations.
     */
    @SafeVarargs
    @SuppressWarnings("unchecked")
    static ExpectationProvider<IonReaderContinuableCoreBinary> fillContainer(IonType expectedType, ExpectationProvider<IonReaderContinuableCoreBinary>... expectations) {
        return consumer -> {
            consumer.accept(new Expectation<>(
                String.format("fill(%s)", expectedType),
                cursor -> {
                    assertEquals(START_CONTAINER, cursor.nextValue());
                    assertEquals(VALUE_READY, cursor.fillValue());
                    assertEquals(expectedType, cursor.getType());
                }
            ));
            if (expectations.length > 0) {
                consumer.accept((Expectation<IonReaderContinuableCoreBinary>) STEP_IN);
                for (Consumer<Consumer<Expectation<IonReaderContinuableCoreBinary>>> expectation : expectations) {
                    expectation.accept(consumer);
                }
                consumer.accept((Expectation<IonReaderContinuableCoreBinary>) STEP_OUT);
            }
        };
    }

    /**
     * Provides an Expectation that verifies that advancing the cursor positions it at the current end of the stream.
     */
    @SuppressWarnings("unchecked")
    static <T extends IonCursorBinary> ExpectationProvider<T> endStream() {
        return consumer -> consumer.accept((Expectation<T>) STREAM_END);
    }
}
