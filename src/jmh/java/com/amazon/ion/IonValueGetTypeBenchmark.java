package com.amazon.ion;


import com.amazon.ion.system.IonSystemBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class IonValueGetTypeBenchmark {

    private static final IonSystem system = IonSystemBuilder.standard().build();

    private final IonBool bool = system.newBool(false);
    private final IonInt integer = system.newInt(123);
    private final IonNull ionNull = system.newNull();
    private final IonFloat ionFloat = system.newFloat(42);
    private final IonDecimal ionDecimal = system.newDecimal(1.23);
    private final IonString string = system.newString("abc");
    private final IonSymbol symbol = system.newSymbol("def");
    private final IonBlob blob = system.newBlob(new byte[]{});
    private final IonClob clob = system.newClob(new byte[]{});
    private final IonStruct struct = system.newEmptyStruct();
    private final IonList list = system.newEmptyList();
    private final IonSexp sexp = system.newEmptySexp();
    private final IonDatagram dg = system.newDatagram();

    @Benchmark
    public int testAddSuffixViaInstanceVariablePlus() {
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

    private final IonDatagram realWorld;
    private Blackhole bh;

    public IonValueGetTypeBenchmark() {
        try {
            realWorld = system.getLoader().load(Paths.get("/Users/greggt/Documents/StructuredLogging/kinesis/service_log_legacy.ion").toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Setup
    public void setup(Blackhole bh) {
        this.bh = bh;
    }

    @Benchmark
    public void getTypeReadWorld() {
        getTypeRecursive(realWorld);
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
