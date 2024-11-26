package com.amazon.tools.cli;


import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonReaderBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;

@Command(
        name = SimpleIonCli.NAME,
        version = SimpleIonCli.VERSION,
        subcommands = {HelpCommand.class},
        mixinStandardHelpOptions = true
)
class SimpleIonCli {

    public static final String NAME = "jion";
    public static final String VERSION = "2024-10-31";
    //TODO: Replace with InputStream.nullInputStream in JDK 11+
    public static final InputStream EMPTY = new ByteArrayInputStream(new byte[0]);

    public static void main(String[] args) {
        CommandLine commandLine = new CommandLine(new SimpleIonCli())
                .setCaseInsensitiveEnumValuesAllowed(true)
                .setUsageHelpAutoWidth(true);
        System.exit(commandLine.execute(args));
    }

    @Option(names={"-f", "--format", "--output-format"}, defaultValue = "pretty",
            description = "Output format, from the set (text | pretty | binary | none).",
            paramLabel = "<format>",
    scope = CommandLine.ScopeType.INHERIT)
    OutputFormat outputFormat;

    @Option(names={"-o", "--output"}, paramLabel = "FILE", description = "Output file",
    scope = CommandLine.ScopeType.INHERIT)
    File outputFile;

    @Command(name = "cat", aliases = {"process"},
            description = "concatenate FILE(s) in the requested Ion output format",
            mixinStandardHelpOptions = true)
    int cat( @Parameters(paramLabel = "FILE") File... files) {

        if (outputFormat == OutputFormat.EVENTS) {
            System.err.println("'events' output format is not supported");
            return CommandLine.ExitCode.USAGE;
        }

        //TODO: Handle stream cutoff- java.io.IOException: Broken pipe
        //TODO: This is not resilient to problems with a single file. Should it be?
        try (InputStream in = getInputStream(files);
             IonReader reader = IonReaderBuilder.standard().build(in);
             OutputStream out = getOutputStream(outputFile);
             IonWriter writer = outputFormat.createIonWriter(out)) {
            // getInputStream will look for stdin if we don't supply
            writer.writeValues(reader);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        // process files
        return CommandLine.ExitCode.OK;
    }

    private static InputStream getInputStream(File... files) {
        if (files == null || files.length == 0) return new FileInputStream(FileDescriptor.in);

        // As convenient as this formulation is I'm not sure of the ordering guarantees here
        // Revisit if that is ever problematic
        return Arrays.stream(files)
                .map(SimpleIonCli::getInputStream)
                .reduce(EMPTY, SequenceInputStream::new);
    }

    private static InputStream getInputStream(File inputFile) {
        try {
            return new FileInputStream(inputFile);
        } catch (FileNotFoundException e) {
            throw cloak(e);
        }
    }

    // Removing some boilerplate from checked-exception consuming paths, without RuntimeException wrapping
    // JLS Section 18.4 covers type inference for generic methods,
    // including the rule that `throws T` is inferred as RuntimeException if possible.
    // See e.g. https://www.rainerhahnekamp.com/en/ignoring-exceptions-in-java/
    private static <T extends Throwable> T cloak(Throwable t) throws T {
        @SuppressWarnings("unchecked")
        T result = (T) t;
        return result;
    }

    private static FileOutputStream getOutputStream(File outputFile) throws IOException {
        // non-line-buffered stdout, or the requested file output
        return outputFile == null ? new FileOutputStream(FileDescriptor.out) : new FileOutputStream(outputFile);
    }
}
