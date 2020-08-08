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

    public static void main(String[] args) {

        String[] b = {"-f","events","embedded","-o","123"};
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
                                     ProcessArgs args) throws IOException {
        if (args.getOutputFormat() == OutputFormat.EVENTS) {
            ionWriterForOutput.writeSymbol("$ion_event_stream");
        }

        CurrentInfo currentInfo = new CurrentInfo(null, 1);

        for (String path : args.getInputFiles()) {
            try (
                    InputStream inputStream = new BufferedInputStream(new FileInputStream(path));
                    IonReader ionReader = IonReaderBuilder.standard().build(inputStream);
                    ) {

                currentInfo.setFileName(path);

                if (args.getOutputFormat() == OutputFormat.EVENTS) {
                    processEvents(ionWriterForOutput, ionWriterForErrorReport, ionReader, currentInfo, args);
                } else {
                    process(ionWriterForOutput, ionWriterForErrorReport, ionReader, args);
                }

                ionWriterForOutput.finish();
                ionWriterForErrorReport.finish();
            } catch (IonException e) {
                if (args.getOutputFormat() == OutputFormat.EVENTS) {
                    new ErrorDescription(ErrorType.READ, e.getMessage(), currentInfo.getFileName(), currentInfo.getEventIndex())
                            .writeOutput(ionWriterForErrorReport);
                } else {
                    new ErrorDescription(ErrorType.READ, e.getMessage(), currentInfo.getFileName())
                            .writeOutput(ionWriterForErrorReport);
                }
                System.exit(IO_ERROR_EXIT_CODE);
            }

        }
    }

    private static void process(IonWriter ionWriterForOutput,
                                IonWriter ionWriterForErrorReport,
                                IonReader ionReader,
                                ProcessArgs args) throws IOException {
        while (ionReader.next() != null) {
            if (isEmbeddedStream(ionReader)) {
                ionReader.stepIn();
                ionWriterForOutput.addTypeAnnotation(EMBEDDED_STREAM_ANNOTATION);
                ionWriterForOutput.stepIn(IonType.SEXP);

                while (ionReader.next() != null) {
                    String stream = ionReader.stringValue();
                    try (
                            IonReader tempIonReader = IonReaderBuilder.standard().build(stream);
                            ByteArrayOutputStream text = new ByteArrayOutputStream();
                            IonWriter tempIonWriter =  args.getOutputFormat().createIonWriter(text);
                            ) {
                        while (tempIonReader.next() != null) {
                            tempIonWriter.writeValue(tempIonReader);
                        }
                        ionWriterForOutput.writeString(text.toString("utf-8"));
                        tempIonWriter.finish();
                    } catch (IOException e) {
                        System.err.println(e.getMessage());
                    }

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
                                      IonReader ionReader,
                                      CurrentInfo currentInfo,
                                      ProcessArgs args) throws IOException {
        //curTable is used for checking if the symbol table changes during the processEvent function.
        SymbolTable curTable = ionReader.getSymbolTable();

        processEvent(ionWriterForOutput, ionWriterForErrorReport, ionReader, curTable, currentInfo);
        new Event(EventType.STREAM_END, null, null, null,
                null, null, null, 0)
                .writeOutput(ionWriterForOutput, ionWriterForErrorReport, currentInfo);
    }

    private static void processEvent(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     IonReader ionReader,
                                     SymbolTable curTable,
                                     CurrentInfo currentInfo) throws IOException {
        while (ionReader.next() != null) {
            if (!isSameSymbolTable(ionReader.getSymbolTable(), curTable)) {
                curTable = ionReader.getSymbolTable();
                ImportDescriptor imports[] = symbolTableToImports(curTable.getImportedTables());
                new Event(EventType.SYMBOL_TABLE, null, null, null,
                        null, null, imports, 0)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, currentInfo);
            }

            if (isEmbeddedStream(ionReader)) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                ionStreamToEvent(EventType.CONTAINER_START, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, currentInfo);
                ionReader.stepIn();
                while (ionReader.next() != null) {
                    String stream = ionReader.stringValue();
                    try (IonReader tempIonReader = IonReaderBuilder.standard().build(stream)) {
                        do {
                            processEvent(ionWriterForOutput, ionWriterForErrorReport, tempIonReader,
                                    curTable, currentInfo);
                        } while (tempIonReader.next() != null);
                    } catch (IOException e) {
                        System.err.println(e.getMessage());
                    }
                    new Event(EventType.STREAM_END, null, null, null,
                            null, null, null, 0)
                            .writeOutput(ionWriterForOutput, ionWriterForErrorReport, currentInfo);
                }
                //write a Container_End event and step out
                new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, null, curDepth)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, currentInfo);
                ionReader.stepOut();
            } else if (IonType.isContainer(ionReader.getType())) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                ionStreamToEvent(EventType.CONTAINER_START, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, currentInfo);
                ionReader.stepIn();

                //recursive call
                processEvent(ionWriterForOutput, ionWriterForErrorReport, ionReader, curTable, currentInfo);

                //write a Container_End event and step out
                new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, null, curDepth)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, currentInfo);
                ionReader.stepOut();
            } else {
                ionStreamToEvent(EventType.SCALAR, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, currentInfo);
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
            }
        }
        return outputStream;
    }

    private static void processEventEmbedded() {

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
            valueText = textOut.toString("utf-8");
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

    private static boolean isSameSymbolTable(SymbolTable x, SymbolTable y) {
        if (x == null && y == null) return true;
        else if (x != null && y == null) return false;
        else if (x == null && y != null) return false;
        else if (x.isSystemTable() && y.isSystemTable()) {
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
            }
            return true;
        } else {
            return false;
        }
    }

    private static ImportDescriptor[] symbolTableToImports(SymbolTable[] tables) {
        if(tables == null || tables.length == 0) return null;
        int size = tables.length;

        ImportDescriptor imports[] = new ImportDescriptor[size];
        for (int i = 0; i < size; i++) {
            ImportDescriptor table = new ImportDescriptor(tables[i]);
            imports[i] = table;
        }
        return imports;
    }

    private static boolean isEmbeddedStream(IonReader ionReader) {
        String[] annotations = ionReader.getTypeAnnotations();


        return (ionReader.getType() == IonType.SEXP || ionReader.getType() == IonType.LIST)
                && ionReader.getDepth() == 0
                && annotations.length > 0
                && annotations[0].equals(EMBEDDED_STREAM_ANNOTATION);
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

    public static class CurrentInfo {
        private String fileName;
        private int eventIndex;

        public CurrentInfo(String file, int index) {
            this.fileName = file;
            this.eventIndex = index;
        }

        public String getFileName() {
            return this.fileName;
        }

        public int getEventIndex() {
            return this.eventIndex;
        }

        public void setFileName(String file) {
            this.fileName = file;
        }

        public void setEventIndex(int index) {
            this.eventIndex = index;
        }
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
