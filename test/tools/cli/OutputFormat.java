package tools.cli;

import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import org.kohsuke.args4j.CmdLineException;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represents the different Ion output formats supported by the command line tools in this package.
 */
public enum OutputFormat {
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
    EVENTS("events") {
        @Override
        public IonWriter createIonWriter(OutputStream outputStream) {
            return IonTextWriterBuilder.pretty().build(outputStream);
        }
    };

    abstract IonWriter createIonWriter(OutputStream outputStream);

    private String name;

    OutputFormat(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static OutputFormat nameToOutputFormat(String name) throws CmdLineException {
        for (OutputFormat o : OutputFormat.values()) {
            if(o.getName().equals(name)){
                return o;
            }
        }
        throw new CmdLineException("Invalid option for --output-format -f: " + name);
    }
}
