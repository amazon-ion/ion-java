// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import com.amazon.ion.system.IonSystemBuilder;
import java.util.concurrent.TimeUnit;
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

/**
 * Clones values after different annotation histories, including retained capacity after removals.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class IonValueCloneBenchmark {

    @Param({"UNINITIALIZED", "EMPTY_ARRAY", "CLEARED", "REMOVED", "ANNOTATED", "ANNOTATED_SINGLE", "SPARSE"})
    public String annotations;

    private IonValue value;

    @Setup
    public void setup() {
        value = IonSystemBuilder.standard().build().newInt(1);
        switch (annotations) {
            case "EMPTY_ARRAY":
                value.setTypeAnnotationSymbols();
                break;
            case "CLEARED":
                value.addTypeAnnotation("first");
                value.addTypeAnnotation("second");
                value.addTypeAnnotation("third");
                value.clearTypeAnnotations();
                break;
            case "REMOVED":
                value.addTypeAnnotation("first");
                value.removeTypeAnnotation("first");
                break;
            case "ANNOTATED":
                value.addTypeAnnotation("first");
                value.addTypeAnnotation("second");
                value.addTypeAnnotation("third");
                break;
            case "ANNOTATED_SINGLE":
                value.addTypeAnnotation("first");
                break;
            case "SPARSE":
                for (int i = 0; i < 1024; i++) {
                    value.addTypeAnnotation("annotation" + i);
                }
                for (int i = 0; i < 1023; i++) {
                    value.removeTypeAnnotation("annotation" + i);
                }
                break;
            case "UNINITIALIZED":
                break;
            default:
                throw new IllegalArgumentException(annotations);
        }
    }

    @Benchmark
    public IonValue cloneValue() {
        return value.clone();
    }
}
