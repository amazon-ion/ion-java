package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.google.code.tempusfugit.concurrency.ConcurrentRule;
import com.google.code.tempusfugit.concurrency.ConcurrentTestRunner;
import com.google.code.tempusfugit.concurrency.RepeatingRule;
import com.google.code.tempusfugit.concurrency.annotations.Concurrent;
import com.google.code.tempusfugit.concurrency.annotations.Repeating;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith(ConcurrentTestRunner.class)
public class IonRoundtripConcurrencyStressTest {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();
    private static final IonReaderBuilder STANDARD_READER_BUILDER = IonReaderBuilder.standard()
            .withIncrementalReadingEnabled(true).immutable();
    private static final IonBinaryWriterBuilder STANDARD_WRITER_BUILDER = IonBinaryWriterBuilder.standard().immutable();
    private static final Random RANDOM = new Random();

    @Rule public ConcurrentRule concurrently = new ConcurrentRule();
    @Rule public RepeatingRule repeatedly = new RepeatingRule();

    // Why not use parameterized tests? Because junitparams.JUnitParamsRunner doesn't play nicely with
    // com.google.code.tempusfugit.concurrency.ConcurrentTestRunner and I prefer the clarity that comes from @Concurrent
    // and @Repeating over that which I get from @junitparams.Parameters .
    // org.junit.runners.Parameterized includes some support for delegating to another runner factory in case you need
    // other functionality, but org.junit.runners.Parameterized.Parameters is awkward enough that I'd rather avoid it.

    // Some day we will move to JDK 8+ and JUnit 5, then perhaps we can use the JUnit 5 test parameterization.

    // The default number of repetitions for @Repeating is 100, and the default concurrency for @Concurrent is 5
    // These parameters seem sufficient to demonstrate the bug in https://github.com/amzn/ion-java/pull/436 every time
    // for the impacted types. I've experimented with lower concurrent and repeat values and found that with low enough
    // concurrency or repetitions (e.g. 2 on either) you don't necessarily fail all of these tests every time in the
    // presence of that bug. Rather than fidget around the numbers further I'll call the default values on these
    // annotations good enough, each test here costs <200ms and it's not worth trying to make them any lighter weight.
    @Repeating @Concurrent @Test
    public void round_trip_NULL() {
        assertSuccessfulRoundTrip(generate(IonType.NULL));
    }

    @Repeating @Concurrent @Test
    public void round_trip_BOOL() {
        assertSuccessfulRoundTrip(generate(IonType.BOOL));
    }

    @Repeating @Concurrent @Test
    public void round_trip_INT() {
        assertSuccessfulRoundTrip(generate(IonType.INT));
    }

    @Repeating @Concurrent @Test
    public void round_trip_FLOAT() {
        assertSuccessfulRoundTrip(generate(IonType.FLOAT));
    }

    @Repeating @Concurrent @Test
    public void round_trip_DECIMAL() {
        assertSuccessfulRoundTrip(generate(IonType.DECIMAL));
    }

    @Repeating @Concurrent @Test
    public void round_trip_TIMESTAMP() {
        assertSuccessfulRoundTrip(generate(IonType.TIMESTAMP));
    }

    @Repeating @Concurrent @Test
    public void round_trip_SYMBOL() {
        assertSuccessfulRoundTrip(generate(IonType.SYMBOL));
    }

    @Repeating @Concurrent @Test
    public void round_trip_STRING() {
        assertSuccessfulRoundTrip(generate(IonType.STRING));
    }

    @Repeating @Concurrent @Test
    public void round_trip_CLOB() {
        assertSuccessfulRoundTrip(generate(IonType.CLOB));
    }

    @Repeating @Concurrent @Test
    public void round_trip_BLOB() {
        assertSuccessfulRoundTrip(generate(IonType.BLOB));
    }

    @Repeating @Concurrent @Test
    public void round_trip_LIST() {
        assertSuccessfulRoundTrip(generate(IonType.LIST));
    }

    @Repeating @Concurrent @Test
    public void round_trip_SEXP() {
        assertSuccessfulRoundTrip(generate(IonType.SEXP));
    }

    @Repeating @Concurrent @Test
    public void round_trip_STRUCT() {
        assertSuccessfulRoundTrip(generate(IonType.STRUCT));
    }

    // ------------------------------------------------------------------------

    private void assertSuccessfulRoundTrip(IonValue value) {
        IonValue after = roundTrip(value);
        assertEquals("Found round-trip problem for type " + value.getType(), value, after);

        IonValue nullType = generateNull(value.getType());
        IonValue nullAfter = roundTrip(nullType);
        assertEquals("Found round-trip problem for null of type " + value.getType(), nullType, nullAfter);
    }

    private static IonValue generate(IonType type) {
        switch (type) {
            // scalars
            case NULL: return SYSTEM.newNull();
            case BOOL: return SYSTEM.newBool(RANDOM.nextBoolean());
            case INT: return SYSTEM.newInt(RANDOM.nextInt());
            case FLOAT: return  SYSTEM.newFloat(RANDOM.nextDouble());
            case DECIMAL: return  SYSTEM.newDecimal(Math.abs(RANDOM.nextLong()));
            // mod to keep long in bounds for newUtcTimestampFromMillis
            // picked time_t epoch overflow of 2147483647 seconds as bound just for fun
            case TIMESTAMP: return  SYSTEM.newUtcTimestampFromMillis(Math.abs(RANDOM.nextLong()) % 2147483647000L);
            case SYMBOL: return  SYSTEM.newSymbol(UUID.randomUUID().toString());
            case STRING: return  SYSTEM.newString(UUID.randomUUID().toString());
            case CLOB: return  SYSTEM.newClob(UUID.randomUUID().toString().getBytes());
            case BLOB: return  SYSTEM.newBlob(UUID.randomUUID().toString().getBytes());
            // containers
            case LIST: return SYSTEM.newList(generate(IonType.STRING));
            case SEXP: return SYSTEM.newSexp(generate(IonType.SYMBOL));
            case STRUCT: {
                IonStruct struct = SYSTEM.newEmptyStruct();
                struct.add("foo", generate(IonType.STRING));
                struct.add("bar", generate(IonType.SYMBOL));
                return struct;
            }
            default:
                throw new IllegalArgumentException("Don't do this, unsupported generation for type " + type);
        }
    }

    private static IonValue generateNull(IonType type) {
        return SYSTEM.newNull(type);
    }

    private static IonValue roundTrip(IonValue value) {
        try {
            return toIon(toBytes(value));
        } catch (Exception any) {
            throw new RuntimeException(any);
        }
    }

    private static byte[] toBytes(final IonValue ion) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (IonWriter out = STANDARD_WRITER_BUILDER.build(baos)) {
            ion.writeTo(out);
        }
        return baos.toByteArray();
    }

    private static IonValue toIon(final byte[] bytes) throws Exception {
        try (IonReader ionReader = STANDARD_READER_BUILDER.build(bytes)) {
            ionReader.next();
            return SYSTEM.newValue(ionReader);
        }
    }
}
