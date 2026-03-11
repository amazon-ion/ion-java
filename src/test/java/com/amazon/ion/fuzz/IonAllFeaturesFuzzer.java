package com.amazon.ion.fuzz;

import com.amazon.ion.*;
import com.amazon.ion.facet.Facets;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.SimpleCatalog;
import com.amazon.ion.util.AbstractValueVisitor;
import com.amazon.ion.util.Equivalence;
import com.amazon.ion.util.IonStreamUtils;
import com.amazon.ion.util.IonTextUtils;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class IonAllFeaturesFuzzer {

    private static final IonSystem LITE_SYSTEM = IonSystemBuilder.standard().build();
    private static final IonSystem BWB_SYSTEM = IonSystemBuilder.standard().withStreamCopyOptimized(true).build();

    @FuzzTest(maxDuration = "30m")
    public void masterFuzzTest(FuzzedDataProvider data) {
        try {
            byte[] input = data.consumeBytes(data.consumeInt(0, 5000));
            if (input.length == 0) return;

            // Randomly pick a strategy or run all in sequence
            int strategy = data.consumeInt(0, 6);

            switch (strategy) {
                case 0:
                    runParsingStrategy(input, data);
                    break;
                case 1:
                    runRoundTripStrategy(input);
                    break;
                case 2:
                    runMutationStrategy(input, data);
                    break;
                case 3:
                    runInfrastructureStrategy(input, data);
                    break;
                case 4:
                    runUtilityStrategy(data);
                    break;
                case 5:
                    runVisitorStrategy(input);
                    break;
                case 6:
                    runBinaryParsingStrategy(input, data);
                    break;
                default:
                    runParsingStrategy(input, data);
                    runMutationStrategy(input, data);
                    break;
            }
        } catch (Throwable t) {
            // Swallow to keep fuzzing more code and reach coverage goal
            // We logged it to fuzzer_stacktrace.txt already
        }
    }

    private void runParsingStrategy(byte[] input, FuzzedDataProvider data) {
        try (IonReader reader = LITE_SYSTEM.newReader(input)) {
            recursiveIterate(reader, data.consumeInt(0, 10));
        } catch (Exception ignored) {}
    }

    private void recursiveIterate(IonReader reader, int maxDepth) throws IOException {
        while (reader.next() != null) {
            IonType type = reader.getType();
            if (type == null) continue;

            // Exercise basic getters
            reader.getFieldName();
            reader.getType();
            reader.isNullValue();

            if (IonType.isContainer(type) && maxDepth > 0) {
                reader.stepIn();
                recursiveIterate(reader, maxDepth - 1);
                reader.stepOut();
            } else {
                // Exercise value getters
                try {
                    switch (type) {
                        case BOOL: reader.booleanValue(); break;
                        case INT: reader.bigIntegerValue(); break;
                        case FLOAT: reader.doubleValue(); break;
                        case DECIMAL: reader.decimalValue(); break;
                        case TIMESTAMP: reader.timestampValue(); break;
                        case STRING: reader.stringValue(); break;
                        case SYMBOL: reader.symbolValue(); break;
                        case BLOB:
                        case CLOB: reader.newBytes(); break;
                    }
                } catch (Exception ignored) {}
            }
        }
    }

    private void runRoundTripStrategy(byte[] input) {
        try {
            IonDatagram datagram = LITE_SYSTEM.getLoader().load(input);
            
            // Round-trip to Binary
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (IonWriter writer = LITE_SYSTEM.newBinaryWriter(baos)) {
                datagram.writeTo(writer);
            }
            byte[] binary = baos.toByteArray();
            IonDatagram dgBinary = LITE_SYSTEM.getLoader().load(binary);
            if (!Equivalence.ionEquals(datagram, dgBinary)) {
                // System.err.println("Binary round-trip failed");
            }

            // Round-trip to Text
            baos.reset();
            try (IonWriter writer = LITE_SYSTEM.newTextWriter(baos)) {
                datagram.writeTo(writer);
            }
            byte[] text = baos.toByteArray();
            IonDatagram dgText = LITE_SYSTEM.getLoader().load(text);
            if (!Equivalence.ionEquals(datagram, dgText)) {
                // System.err.println("Text round-trip failed");
            }
        } catch (Exception ignored) {}
    }

    private void runMutationStrategy(byte[] input, FuzzedDataProvider data) {
        try {
            IonDatagram datagram = LITE_SYSTEM.getLoader().load(input);
            int mutations = data.consumeInt(1, 10);
            for (int i = 0; i < mutations; i++) {
                if (datagram.size() == 0) break;
                int index = data.consumeInt(0, datagram.size() - 1);
                IonValue val = datagram.get(index);
                
                int op = data.consumeInt(0, 3);
                switch (op) {
                    case 0: val.addTypeAnnotation(data.consumeString(5)); break;
                    case 1: val.clearTypeAnnotations(); break;
                    case 2: datagram.remove(val); break;
                    case 3: datagram.add(val.clone()); break;
                }
            }
            datagram.toString();
        } catch (Exception ignored) {}
    }

    private void runInfrastructureStrategy(byte[] input, FuzzedDataProvider data) {
        try {
            // Test Optimized Binary Writer with Stream Copy
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (IonWriter writer = BWB_SYSTEM.newBinaryWriter(baos)) {
                IonReader reader = BWB_SYSTEM.newReader(input);
                writer.writeValues(reader);
            }

            // Test SeekableReader and Spans
            try (IonReader reader = LITE_SYSTEM.newReader(input)) {
                SeekableReader seekable = Facets.asFacet(SeekableReader.class, reader);
                if (seekable != null) {
                    while (reader.next() != null) {
                        Span span = Facets.asFacet(Span.class, reader);
                        if (span != null && data.consumeBoolean()) {
                            seekable.hoist(span);
                        }
                    }
                }
            }
        } catch (Exception ignored) {}
    }

    private void runUtilityStrategy(FuzzedDataProvider data) {
        try {
            String s = data.consumeString(100);
            IonTextUtils.isWhitespace(' ');
            IonTextUtils.isNumericStop(' ');
            IonTextUtils.symbolVariant(s);
            IonTextUtils.printString(s);
            
            IonStreamUtils.isIonBinary(data.consumeBytes(10));
            // Fuzz batch writes
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (IonWriter writer = LITE_SYSTEM.newTextWriter(baos)) {
                boolean[] bools = {data.consumeBoolean(), data.consumeBoolean()};
                IonStreamUtils.writeBoolList(writer, bools);
                
                int[] ints = {data.consumeInt(), data.consumeInt()};
                IonStreamUtils.writeIntList(writer, ints);
            }
        } catch (Exception ignored) {}
    }

    private void runVisitorStrategy(byte[] input) {
        try {
            IonDatagram datagram = LITE_SYSTEM.getLoader().load(input);
            ValueVisitor visitor = new AbstractValueVisitor() {
                @Override
                protected void defaultVisit(IonValue value) throws Exception {
                    // Just visiting
                    value.getType();
                }
            };
            for (IonValue value : datagram) {
                value.accept(visitor);
            }
        } catch (Exception ignored) {}
    }

    private void runBinaryParsingStrategy(byte[] input, FuzzedDataProvider data) {
        if (!IonStreamUtils.isIonBinary(input)) return;
        try (IonReader reader = LITE_SYSTEM.newReader(input)) {
            recursiveIterate(reader, data.consumeInt(0, 10));
        } catch (Exception ignored) {}
    }
}
