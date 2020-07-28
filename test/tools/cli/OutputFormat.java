package tools.cli;

import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represents the different Ion output formats supported by the command line tools in this package.
 */
public enum OutputFormat implements IonWriterCreator {
    /**
     * Nicely spaced, 'prettified' text Ion.
     */
    PRETTY("pretty") {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.pretty().build(outputStream);
        }
    },
    /**
     * Minimally spaced text Ion.
     */
    TEXT("text") {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.standard().build(outputStream);
        }
    },
    /**
     * Compact, read-optimized binary Ion.
     */
    BINARY("binary") {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonBinaryWriterBuilder.standard().build(outputStream);
        }
    },
    EVENT("events"){
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.pretty().build(outputStream);
        }
    };

    private static final Map<String, OutputFormat> FORMATS_BY_NAME;
    static {
        FORMATS_BY_NAME = new HashMap<>();
        for (OutputFormat outputFormat : OutputFormat.values()) {
            FORMATS_BY_NAME.put(outputFormat.getName(), outputFormat);
        }
    }

    /**
     * Finds the OutputFormat associated with the provided name.
     * @param name  The name of the OutputFormat being requested.
     * @return  OutputFormat or empty if the provided name is not associated with an OutputFormat.
     */
    public static Optional<OutputFormat> forName(String name) {
        return Optional.ofNullable(FORMATS_BY_NAME.get(name));
    }

    private String name;

    OutputFormat(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
