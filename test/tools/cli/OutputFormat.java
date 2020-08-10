package tools.cli;

import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;

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
    },
    /**
     * Minimally spaced text Ion.
     */
    TEXT {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.standard().build(outputStream);
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
    },
    /**
     * Event Stream
     */
    EVENTS {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.pretty().build(outputStream);
        }
    },
    /**
     * None
     */
    NONE {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.pretty().build(outputStream);
        }
    };

    abstract IonWriter createIonWriter(OutputStream outputStream);
}
