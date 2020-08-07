package tools.cli;

import com.amazon.ion.IonWriter;
import com.amazon.ion.IonReader;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.IonException;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import tools.errorReport.ErrorDescription;
import tools.errorReport.ErrorType;
import tools.events.Event;
import tools.events.EventType;
import tools.events.ImportDescriptor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 *  Read the input file(s) (optionally, specifying ReadInstructions or a filter) and re-write in the format
 *  specified by --output
 *
 *  For information about the supported output formats, see {@link OutputFormat}.
 */
public final class IonProcess {
    private static final int CONSOLE_WIDTH = 120; // Only used for formatting the USAGE message
    private static final int USAGE_ERROR_EXIT_CODE = 1;
    private static final int IO_ERROR_EXIT_CODE = 2;
    private static final int BUFFER_SIZE = 128 * 1024;
    private static final String SYSTEM_OUT_DEFAULT_VALUE = "out";
    private static final String SYSTEM_ERR_DEFAULT_VALUE = "err";
    private static final String EMBEDDED_STREAM_ANNOTATION = "embedded_documents";
    private static String CURRENT_FILE = null;
    private static int EVENT_INDEX = 1;

    public static void main(String[] args) {

        String[] b = {"-f","events","out.10n"};
        args = b;

        IonProcess.ProcessArgs parsedArgs = new IonProcess.ProcessArgs();
        CmdLineParser parser = new CmdLineParser(parsedArgs);
        parser.getProperties().withUsageWidth(CONSOLE_WIDTH);
        OutputFormat outputFormat = null;
        String outputFileName = null;
        String errorReportName = null;

        try {
            parser.parseArgument(args);
            outputFormat = parsedArgs.getOutputFormat();
            outputFileName = parsedArgs.getOutputFile();
            errorReportName = parsedArgs.getErrorReport();
        } catch (CmdLineException | IllegalArgumentException e) {
            printHelpTextAndExit(e.getMessage(), parser);
        }

        try (
                //Initialize output stream (default value: STDOUT)
                OutputStream outputStream = initOutputStream(outputFileName, SYSTEM_OUT_DEFAULT_VALUE);
                //Initialize error report (default value: STDERR)
                OutputStream errorReportOutputStream = initOutputStream(errorReportName, SYSTEM_ERR_DEFAULT_VALUE);

                IonWriter ionWriterForOutput = outputFormat.createIonWriter(outputStream);
                IonWriter ionWriterForErrorReport = outputFormat.createIonWriter(errorReportOutputStream);
        ) {
            processFiles(ionWriterForOutput, ionWriterForErrorReport, parsedArgs);
        } catch (IOException e) {
            System.err.println("Failed to close OutputStream: " + e.getMessage());
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
        }
    }

    private static void processFiles(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     IonProcess.ProcessArgs args) throws IOException {
        if (args.getOutputFormat() == OutputFormat.EVENTS) {
            ionWriterForOutput.writeSymbol("$ion_event_stream");
        }

        for (String path : args.getInputFiles()) {
            try {
                CURRENT_FILE = path;
                InputStream inputStream = new BufferedInputStream(new FileInputStream(path));
                IonReader ionReader = IonReaderBuilder.standard().build(inputStream);


                if (args.getOutputFormat() == OutputFormat.EVENTS) {
                    processEvents(ionWriterForOutput, ionWriterForErrorReport, ionReader);
                } else {
                    if(isEventStream(ionReader))
                    process(ionWriterForOutput, ionWriterForErrorReport, ionReader);
                }

                ionWriterForOutput.finish();
                ionWriterForErrorReport.finish();
            } catch (IonException e) {
                if (args.getOutputFormat() == OutputFormat.EVENTS) {
                    new ErrorDescription(ErrorType.READ, e.getMessage(), CURRENT_FILE, EVENT_INDEX)
                            .writeOutput(ionWriterForErrorReport);
                } else {
                    new ErrorDescription(ErrorType.READ, e.getMessage(), CURRENT_FILE)
                            .writeOutput(ionWriterForErrorReport);
                }
                System.exit(IO_ERROR_EXIT_CODE);
            }

        }
    }

    private static void process(IonWriter ionWriterForOutput,
                                IonWriter ionWriterForErrorReport,
                                IonReader ionReader) throws IOException {
        while (ionReader.next() != null) {
            if (isEmbeddedStream(ionReader)) {
                ionReader.stepIn();
                ionWriterForOutput.addTypeAnnotation(EMBEDDED_STREAM_ANNOTATION);
                ionWriterForOutput.stepIn(IonType.SEXP);

                while (ionReader.next() != null) {
                    //create a temporary ionReader and ionWriter
                    String stream = ionReader.stringValue();
                    IonReader tempIonReader = IonReaderBuilder.standard().build(stream);
                    ByteArrayOutputStream text = new ByteArrayOutputStream();
                    IonWriter tempIonWriter = IonTextWriterBuilder.standard().build(text);

                    //read each string
                    while (tempIonReader.next() != null) {
                        tempIonWriter.writeValue(tempIonReader);
                    }

                    //close ionReader and ionWriter
                    ionWriterForOutput.writeString(text.toString());
                    tempIonWriter.finish();
                    tempIonReader.close();
                }
                ionWriterForOutput.stepOut();
                ionReader.stepOut();
            } else {
                ionWriterForOutput.writeValue(ionReader);
            }
        }
    }

    private static void processEvents(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     IonReader ionReader) throws IOException {
        //curTable is used for checking if the symbol table changes during the processEvent function.
        SymbolTable curTable = ionReader.getSymbolTable();

        processEvent(ionWriterForOutput, ionWriterForErrorReport, ionReader, curTable);
        EVENT_INDEX = new Event(EventType.STREAM_END, null, null, null,
                null, null, null, 0)
                .writeOutput(ionWriterForOutput, ionWriterForErrorReport, CURRENT_FILE, EVENT_INDEX);
    }

    private static void processEvent(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     IonReader ionReader,
                                     SymbolTable curTable) throws IOException {
        while (ionReader.next() != null) {
            if (!isSameSymbolTable(ionReader.getSymbolTable(), curTable)) {
                curTable = ionReader.getSymbolTable();
                ImportDescriptor imports[] = symbolTableToImports(curTable.getImportedTables());
                EVENT_INDEX = new Event(EventType.SYMBOL_TABLE, null, null, null,
                        null, null, imports, 0)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, CURRENT_FILE, EVENT_INDEX);
            }

            if (isEmbeddedStream(ionReader)) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                EVENT_INDEX = ionStreamToEvent(EventType.CONTAINER_START, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, CURRENT_FILE ,EVENT_INDEX);
                ionReader.stepIn();

                //for each string in embedded stream
                while (ionReader.next() != null) {
                    String stream = ionReader.stringValue();
                    IonReader tempIonReader = IonReaderBuilder.standard().build(stream);

                    while (tempIonReader.next() != null) {
                        System.out.println("\nprinting: tempIonReader.getType: ");
                        System.out.println(tempIonReader.getType());
                        EVENT_INDEX = ionStreamToEvent(EventType.SCALAR, tempIonReader)
                                .writeOutput(ionWriterForOutput, ionWriterForErrorReport, CURRENT_FILE, EVENT_INDEX);
                    }
                    tempIonReader.close();

                    EVENT_INDEX = new Event(EventType.STREAM_END, null, null, null,
                            null, null, null, 0)
                            .writeOutput(ionWriterForOutput, ionWriterForErrorReport, CURRENT_FILE, EVENT_INDEX);
                }

                //write a Container_End event and step out
                EVENT_INDEX = new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, null, curDepth)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, CURRENT_FILE, EVENT_INDEX);
                ionReader.stepOut();
            } else if (IonType.isContainer(ionReader.getType())) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                EVENT_INDEX = ionStreamToEvent(EventType.CONTAINER_START, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, CURRENT_FILE, EVENT_INDEX);
                ionReader.stepIn();

                //recursive call
                processEvent(ionWriterForOutput, ionWriterForErrorReport, ionReader, curTable);

                //write a Container_End event and step out
                EVENT_INDEX = new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, null, curDepth)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, CURRENT_FILE, EVENT_INDEX);
                ionReader.stepOut();
            } else {
                EVENT_INDEX = ionStreamToEvent(EventType.SCALAR, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, CURRENT_FILE, EVENT_INDEX);
            }
        }
    }

    /**
     *  This function creates a new file for output stream with the fileName, If file name is empty or undefined,
     *  it will create a default output which is System.out or System.err.
     */
    private static BufferedOutputStream initOutputStream(String fileName, String defaultValue) throws IOException {
        BufferedOutputStream outputStream = null;
        if (fileName != null && fileName.length() != 0) {
            File myFile = new File(fileName);
            FileOutputStream out = new FileOutputStream(myFile);
            outputStream = new BufferedOutputStream(out, BUFFER_SIZE);
        } else {
            switch (defaultValue) {
                case SYSTEM_OUT_DEFAULT_VALUE:
                    outputStream = new BufferedOutputStream(System.out, BUFFER_SIZE);
                    break;
                case SYSTEM_ERR_DEFAULT_VALUE:
                    outputStream = new BufferedOutputStream(System.err, BUFFER_SIZE);
                    break;
                default:
                    break;
            }
        }
        return outputStream;
    }

    /**
     * this function is only used for CONTAINER_START and SCALAR eventType since other types don't need initial data.
     */
    private static Event ionStreamToEvent(EventType eventType,
                                          IonReader ionReader) throws IOException, IllegalStateException {
        if (ionReader.getType() == null) throw new IllegalStateException("Can't convert ionReader null type to Event");

        IonType ionType = ionReader.getType();
        SymbolToken fieldName = ionReader.getFieldNameSymbol();
        SymbolToken[] annotations = ionReader.getTypeAnnotationSymbols();

        String valueText = null;
        byte[] valueBinary = null;
        if (eventType == EventType.SCALAR) {
            //write Text
            ByteArrayOutputStream textOut = new ByteArrayOutputStream();
            IonWriter textWriter = IonTextWriterBuilder.standard().build(textOut);
            textWriter.writeValue(ionReader);
            textWriter.finish();
            valueText = textOut.toString();
            //write binary
            ByteArrayOutputStream binaryOut = new ByteArrayOutputStream();
            IonWriter binaryWriter = IonBinaryWriterBuilder.standard().build(binaryOut);
            binaryWriter.writeValue(ionReader);
            binaryWriter.finish();
            valueBinary = binaryOut.toByteArray();
        }

        ImportDescriptor[] imports = null;
        int depth = ionReader.getDepth();
        return new Event(eventType, ionType, fieldName, annotations, valueText, valueBinary, imports, depth);
    }

    /**
     * this function converts a Event struct to ion field
     */
    private static void EventToIonStream(IonReader ionReader){}

    private static boolean isSameSymbolTable(SymbolTable x, SymbolTable y) {
        if (x.isSystemTable() && y.isSystemTable()) {
            return (x.getVersion() == y.getVersion());
        } else if (x.isSharedTable() && y.isSharedTable()) {
            return (x.getName() == y.getName() & (x.getVersion() == y.getVersion()));
        } else if (x.isLocalTable() && y.isLocalTable()) {
            SymbolTable[] xTable = x.getImportedTables();
            SymbolTable[] yTable = y.getImportedTables();
            //compare imports
            if (xTable.length == yTable.length) {
                for (int i = 0; i < xTable.length; i++) {
                    if (!isSameSymbolTable(xTable[i], yTable[i]))  return false;
                }
            } else {
                return false;
            }
            //compare symbols
            Iterator<String> xIterator = x.iterateDeclaredSymbolNames();
            Iterator<String> yIterator = y.iterateDeclaredSymbolNames();
            while (xIterator.hasNext() && yIterator.hasNext()) {
                if (!xIterator.next().equals(yIterator.next())) {
                    return false;
                }
            }
            if (xIterator.hasNext() || yIterator.hasNext()) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    private static ImportDescriptor[] symbolTableToImports(SymbolTable[] tables) {
        int size = tables.length;
        ImportDescriptor imports[] = new ImportDescriptor[size];
        for (int i = 0; i < size; i++) {
            ImportDescriptor table = new ImportDescriptor(tables[i]);
            imports[i] = table;
        }
        return imports;
    }

    private static boolean isEmbeddedStream(IonReader ionReader) {
        return (ionReader.getType() == IonType.SEXP || ionReader.getType() == IonType.LIST)
                && ionReader.getDepth() == 0
                && ionReader.getTypeAnnotations() != null
                && ionReader.getTypeAnnotations().length > 0
                && ionReader.getTypeAnnotations()[0].equals(EMBEDDED_STREAM_ANNOTATION);
    }

    private static void printHelpTextAndExit(String msg, CmdLineParser parser) {
        System.err.println(msg + "\n");
        System.err.println("\"Process\" reads the input file(s) and re-write in the format specified by --output.\n");
        System.err.println("Usage:\n");
        System.err.println("ion process [--output <file>] [--error-report <file>] [--output-format \n"
                + "(text | pretty | binary | events | none)] [--catalog <file>]... [--imports <file>]... \n"
                + "[--perf-report <file>] [--filter <filter> | --traverse <file>]  [-] [<input_file>]...\n");
        System.err.println("Options:\n");
        parser.printUsage(System.err);
        System.exit(USAGE_ERROR_EXIT_CODE);
    }

    private static boolean isEventStream(IonReader ionReader) {
        return false;
    }


    static class ProcessArgs {
        private static final String DEFAULT_FORMAT_VALUE = OutputFormat.PRETTY.toString();

        @Option(name = "--output",
                aliases = {"-o"},
                metaVar = "FILE",
                usage = "Output file")
        private String outputFile;

        @Option(name = "--error-report",
                aliases = {"-e"},
                metaVar = "FILE",
                usage = "Error report file")
        private String errorReport;

        @Option(name = "--output-format",
                aliases = {"-f"},
                metaVar = "TYPE",
                usage = "Output format, from the set (text | pretty | binary |\n"
                        + "events | none). 'events' is only available with the\n"
                        + "'process' command, and outputs a serialized EventStream\n"
                        + "representing the input Ion stream(s).")
        private String outputFormatName = DEFAULT_FORMAT_VALUE;

        @Argument(metaVar = "FILES", required = true)
        private List<String> inputFiles;

        public String getOutputFormatName() {
            return outputFormatName;
        }

        public OutputFormat getOutputFormat() throws IllegalArgumentException {
            return OutputFormat.valueOf(outputFormatName.toUpperCase());
        }

        public List<String> getInputFiles() {
            return inputFiles;
        }
        public String getOutputFile() { return outputFile; }
        public String getErrorReport() { return errorReport; }
    }
}
