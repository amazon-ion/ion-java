package tools.cli;

import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.SharedSymbolTableTest;
import com.amazon.ion.impl._Private_IonValue;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import tools.events.ImportDescriptor;

import java.io.OutputStream;

/**
 * Represents the different Ion output formats supported by the command line tools in this package.
 */
public enum OutputFormat {
    /**
     * Nicely spaced, 'prettified' text Ion.
     */
    PRETTY {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.pretty().build(outputStream);
        }

        @Override
        public IonWriter createIonWriterWithImports(OutputStream outputStream, SymbolTable[] imports) {
            return IonTextWriterBuilder.pretty().withImports(imports).build(outputStream);
        }
    },
    /**
     * Minimally spaced text Ion.
     */
    TEXT {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.standard().build(outputStream);
        }

        @Override
        public IonWriter createIonWriterWithImports(OutputStream outputStream, SymbolTable[] imports) {
            return IonTextWriterBuilder.standard().withImports(imports).build(outputStream);
        }
    },
    /**
     * Compact, read-optimized binary Ion.
     */
    BINARY {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonBinaryWriterBuilder.standard().build(outputStream);
        }

        @Override
        public IonWriter createIonWriterWithImports(OutputStream outputStream, SymbolTable[] imports) {
            return IonBinaryWriterBuilder.standard().withImports(imports).build(outputStream);
        }
    },
    /**
     * Event Stream
     */
    EVENTS {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.pretty().build(outputStream);
        }

        @Override
        public IonWriter createIonWriterWithImports(OutputStream outputStream, SymbolTable[] imports) {
            return IonTextWriterBuilder.pretty().withImports(imports).build(outputStream);
        }
    },
    /**
     * None
     */
    NONE {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            NoOpOutputStream out = new NoOpOutputStream();
            return IonTextWriterBuilder.pretty().build(out);
        }

        @Override
        public IonWriter createIonWriterWithImports(OutputStream outputStream, SymbolTable[] imports) {
            return IonTextWriterBuilder.pretty().withImports(imports).build(outputStream);
        }
    };

    abstract IonWriter createIonWriter(OutputStream outputStream);
    abstract IonWriter createIonWriterWithImports(OutputStream outputStream, SymbolTable[] symbolTable);
}
