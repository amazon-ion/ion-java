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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * Measures the cost of {@link IonValue#hashCode()} on containers under monomorphic and megamorphic conditions.
 * <p>
 * The {@code poison} parameter exercises the JIT pathology described in
 * <a href="https://bugs.openjdk.org/browse/JDK-8368292">JDK-8368292</a>: when a virtual call site (here,
 * {@code scalarHashCode()}) has been invoked on many distinct receiver types, the JIT cannot devirtualize or
 * inline it — even for a caller whose receivers are locally monomorphic. The {@code poison=true} variant
 * pre-pollutes the call site with 40,000+ invocations across 6 IonValueLite subtypes before measuring the
 * monomorphic container, simulating real-world conditions where diverse Ion types have been hashed in the
 * same JVM.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1, jvmArgs = {"-Xms3g", "-Xmx3g", "-XX:+UseParallelGC"})
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class IonContainerHashCodeBenchmark {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    @Param({"true", "false"})
    private boolean poison;

    private IonList monomorphicContainer;
    private IonList megamorphicContainer;

    @Setup
    public void setup() {
        // Build a container with 6 distinct IonValueLite subtypes to poison hashSignature()
        megamorphicContainer = SYSTEM.newEmptyList();
        for (int i = 0; i < 20; i++) {
            megamorphicContainer.add(SYSTEM.newInt(i));
            megamorphicContainer.add(SYSTEM.newString("s" + i));
            megamorphicContainer.add(SYSTEM.newFloat(i * 1.1));
            megamorphicContainer.add(SYSTEM.newBool(i % 2 == 0));
            megamorphicContainer.add(SYSTEM.newSymbol("sym" + i));
            megamorphicContainer.add(SYSTEM.newDecimal(BigDecimal.valueOf(i)));
        }

        if (poison) {
            // Poison the scalarHashCode() call site with 40k+ invocations across diverse types
            for (int i = 0; i < 40_000; i++) {
                megamorphicContainer.hashCode();
            }
        }

        // The benchmark target: a container holding only IonStrings (locally monomorphic, same size)
        monomorphicContainer = SYSTEM.newEmptyList();
        for (int i = 0; i < 120; i++) {
            monomorphicContainer.add(SYSTEM.newString("value" + i));
        }
    }

    @Benchmark
    public int containerHashCode_monomorphic() {
        return monomorphicContainer.hashCode();
    }

    @Benchmark
    public int containerHashCode_megamorphic() {
        return megamorphicContainer.hashCode();
    }
}
