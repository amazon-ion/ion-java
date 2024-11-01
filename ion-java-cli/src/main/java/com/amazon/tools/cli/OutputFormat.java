package com.amazon.tools.cli;

import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.system.IonWriterBuilder;

import java.io.OutputStream;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Represents the different Ion output formats supported by the command line com.amazon.tools in this package.
 */
public enum OutputFormat {
    /** Nicely spaced, 'prettified' text Ion */
    PRETTY(IonTextWriterBuilder::pretty),
    /** Minimally spaced text Ion */
    TEXT(IonTextWriterBuilder::standard),
    /** Compact, read-optimized binary Ion */
    BINARY(IonBinaryWriterBuilder::standard),
    /** Event Stream */
    EVENTS(IonTextWriterBuilder::pretty),
    /** No output, /dev/null */
    NONE(IonTextWriterBuilder::standard, o -> new NoOpOutputStream());

    OutputFormat(Supplier<IonWriterBuilder> supplier) {
        this(supplier, o -> o);
    }

    OutputFormat(Supplier<IonWriterBuilder> supplier, Function<OutputStream, OutputStream> function) {
        this.writerBuilderSupplier = supplier;
        this.outputTransformer = function;
    }

    IonWriter createIonWriter(OutputStream outputStream) {
        return writerBuilderSupplier.get().build(outputTransformer.apply(outputStream));
    }

    IonWriter createIonWriterWithImports(OutputStream outputStream, SymbolTable[] symbolTable) {
        IonWriterBuilder builder = writerBuilderSupplier.get();
        OutputStream out = outputTransformer.apply(outputStream);
        if (builder instanceof IonTextWriterBuilder) {
            return ((IonTextWriterBuilder)builder).withImports(symbolTable).build(out);
        } else if (builder instanceof IonBinaryWriterBuilder) {
            return ((IonBinaryWriterBuilder)builder).withImports(symbolTable).build(out);
        } else {
            throw new IllegalStateException("This is impossible, none of the enums can do this");
        }
    }

    private final Supplier<IonWriterBuilder> writerBuilderSupplier;
    private final Function<OutputStream, OutputStream> outputTransformer;

}
