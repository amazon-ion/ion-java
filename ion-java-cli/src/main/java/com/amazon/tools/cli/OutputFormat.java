package com.amazon.tools.cli;

import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;

import java.io.OutputStream;

/**
 * Represents the different Ion output formats supported by the command line com.amazon.tools in this package.
 */
public enum OutputFormat {
    /** Nicely spaced, 'prettified' text Ion */ PRETTY,
    /** Minimally spaced text Ion */            TEXT,
    /** Compact, read-optimized binary Ion */   BINARY,
    /** Event Stream */                         EVENTS,
    /** No output, /dev/null */                 NONE;

    IonWriter createIonWriter(OutputStream outputStream) {
        return createIonWriter(this, outputStream);
    }

    IonWriter createIonWriterWithImports(OutputStream outputStream, SymbolTable[] symbolTable) {
        return createIonWriter(this, outputStream, symbolTable);
    }

    private static IonWriter createIonWriter(OutputFormat format, OutputStream outputStream) {
        switch (format) {
            case TEXT: return IonTextWriterBuilder.standard().build(outputStream);
            case PRETTY: return IonTextWriterBuilder.pretty().build(outputStream);
            case EVENTS: return IonTextWriterBuilder.pretty().build(outputStream);
            case BINARY: return IonBinaryWriterBuilder.standard().build(outputStream);
            case NONE: return IonTextWriterBuilder.standard().build(new NoOpOutputStream());
            default: throw new IllegalStateException("Unsupported output format: " + format);
        }
    }

    private static IonWriter createIonWriter(OutputFormat format, OutputStream out, SymbolTable... symbols) {
        switch (format) {
            case TEXT: return IonTextWriterBuilder.standard().withImports(symbols).build(out);
            case PRETTY: return IonTextWriterBuilder.pretty().withImports(symbols).build(out);
            case EVENTS: return IonTextWriterBuilder.standard().withImports(symbols).build(out);
            case BINARY: return IonBinaryWriterBuilder.standard().withImports(symbols).build(out);
            case NONE: return IonTextWriterBuilder.standard().withImports(symbols).build(new NoOpOutputStream());
            default: throw new IllegalStateException("Unsupported output format: " + format);
        }
    }
}
