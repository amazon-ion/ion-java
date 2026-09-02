// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.SimpleCatalog;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures the cost of copying an {@link IonSystemBuilder}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class IonSystemBuilderCopyBenchmark {

    private final IonCatalog catalog = new SimpleCatalog();

    @Benchmark
    public IonSystemBuilder copy() {
        return IonSystemBuilder.standard().copy();
    }

    @Benchmark
    public IonSystemBuilder withCatalog() {
        return IonSystemBuilder.standard().withCatalog(catalog);
    }

    @Benchmark
    public IonSystem buildWithCatalog() {
        return IonSystemBuilder.standard().withCatalog(catalog).build();
    }
}
