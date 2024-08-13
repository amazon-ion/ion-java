// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;


import com.amazon.ion.system.IonSystemBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class IonValueGetTypeBenchmark {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    private final IonBool bool = SYSTEM.newBool(false);
    private final IonInt integer = SYSTEM.newInt(123);
    private final IonNull ionNull = SYSTEM.newNull();
    private final IonFloat ionFloat = SYSTEM.newFloat(42);
    private final IonDecimal ionDecimal = SYSTEM.newDecimal(1.23);
    private final IonString string = SYSTEM.newString("abc");
    private final IonSymbol symbol = SYSTEM.newSymbol("def");
    private final IonBlob blob = SYSTEM.newBlob(new byte[]{});
    private final IonClob clob = SYSTEM.newClob(new byte[]{});
    private final IonStruct struct = SYSTEM.newEmptyStruct();
    private final IonList list = SYSTEM.newEmptyList();
    private final IonSexp sexp = SYSTEM.newEmptySexp();
    private final IonDatagram dg = SYSTEM.newDatagram();

    @Benchmark
    public int ionValueGetType() {
        return integer.getType().ordinal() +
            bool.getType().ordinal() +
            ionNull.getType().ordinal() +
            ionFloat.getType().ordinal() +
            ionDecimal.getType().ordinal() +
            string.getType().ordinal() +
            symbol.getType().ordinal() +
            blob.getType().ordinal() +
            clob.getType().ordinal() +
            struct.getType().ordinal() +
            list.getType().ordinal() +
            sexp.getType().ordinal() +
            dg.getType().ordinal();
    }

    private final IonDatagram container;
    private Blackhole bh;

    public IonValueGetTypeBenchmark() {
        try {
            container = SYSTEM.getLoader().load(Paths.get("ion-tests/iontestdata/good/message2.ion").toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Setup
    public void setup(Blackhole bh) {
        this.bh = bh;
    }

    @Benchmark
    public void getTypeContainer() {
        getTypeRecursive(container);
    }

    public void getTypeRecursive(IonContainer container) {
        bh.consume(container.getType());
        for (IonValue child : container) {
            IonType type = child.getType();
            bh.consume(type);
            switch (type) {
                case STRUCT:
                case LIST:
                case SEXP:
                    getTypeRecursive((IonContainer) child);
                    break;
                default:
                    break;
            }
        }
    }
}
