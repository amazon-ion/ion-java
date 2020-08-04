package tools.cli;

import com.amazon.ion.*;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import tools.events.Event;
import tools.events.EventType;
import tools.events.ImportDescriptor;


import java.io.*;
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
    private static final int BUFFER_SIZE = 128 * 1024;
    private static final String SYSTEM_OUT_DEFAULT_VALUE = "out";
    private static final String SYSTEM_ERR_DEFAULT_VALUE = "err";

    public static void main(final String[] args) {

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
        } catch (CmdLineException e) {
            System.err.println(e.getMessage() + "\n");
            System.err.println("\"Process\" reads the input file(s) and re-write in the format specified by --output.\n");
            System.err.println("Usage:\n");
            System.err.println("ion process [--output <file>] [--error-report <file>] [--output-format \n"
                    + "(text | pretty | binary | events | none)] [--catalog <file>]... [--imports <file>]... \n"
                    + "[--perf-report <file>] [--filter <filter> | --traverse <file>]  [-] [<input_file>]...\n");
            System.err.println("Options:\n");
            parser.printUsage(System.err);
            System.exit(USAGE_ERROR_EXIT_CODE);
        }

        //Initialize output stream (default value: STDOUT)
        OutputStream  outputStream = initOutputStream(outputFileName, SYSTEM_OUT_DEFAULT_VALUE);
        //Initialize error report (default value: STDERR)
        OutputStream errorReportOutputStream = initOutputStream(errorReportName, SYSTEM_ERR_DEFAULT_VALUE);

        try (IonWriter ionWriterForOutput = outputFormat.createIonWriter(outputStream);
             IonWriter ionWriterForErrorReport = outputFormat.createIonWriter(errorReportOutputStream);
        ) {
            if (parsedArgs.getOutputFormatName() == "events") {
                processFilesInEvents(ionWriterForOutput, ionWriterForErrorReport, parsedArgs);
            } else {
                processFiles(ionWriterForOutput, ionWriterForErrorReport, parsedArgs);
            }
        } catch (IOException e) {
            System.err.println("Failed to close OutputStream: " + e.getMessage());
        }
    }

    private static void processFiles(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     IonProcess.ProcessArgs args) {
        for (String path : args.getInputFiles()) {
            try (
                    InputStream inputStream = new BufferedInputStream(new FileInputStream(path));
                    IonReader ionReader = IonReaderBuilder.standard().build(inputStream);
            ) {
                process(ionWriterForOutput, ionWriterForErrorReport, ionReader);

                ionWriterForOutput.finish();
                ionWriterForErrorReport.finish();
            } catch (IOException e) {
                System.err.println("Could not process '" + path + "': " + e.getMessage());
            }
        }
    }

    private static void process(IonWriter ionWriterForOutput,
                                IonWriter ionWriterForErrorReport,
                                IonReader ionReader) throws IOException {
        ionWriterForOutput.writeValues(ionReader);
    }

    private static void processFilesInEvents(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     IonProcess.ProcessArgs args) throws IOException {
        ionWriterForOutput.writeSymbol("$ion_event_stream");

        for (String path : args.getInputFiles()) {
            InputStream inputStream = new BufferedInputStream(new FileInputStream(path));
            IonReader ionReader = IonReaderBuilder.standard().build(inputStream);

            processEvents(ionWriterForOutput, ionWriterForErrorReport, ionReader);

            ionWriterForOutput.finish();
            ionWriterForErrorReport.finish();
        }
    }

    private static void processEvents(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     IonReader ionReader) throws IOException {
        //curTable is used for checking if the symbol table changes during the processEvent function.
        SymbolTable curTable = ionReader.getSymbolTable();

        processEvent(ionWriterForOutput, ionWriterForErrorReport, ionReader, curTable);
        new Event(EventType.STREAM_END, null, null, null,
                null, null, null, 0 )
                .writeOutput(ionWriterForOutput, ionWriterForErrorReport);
    }

    private static void processEvent(IonWriter ionWriterForOutput,
                                      IonWriter ionWriterForErrorReport,
                                      IonReader ionReader,
                                      SymbolTable curTable) throws IOException {
        while (ionReader.next() != null) {
            if (!isSameSymbolTable(ionReader.getSymbolTable(), curTable)) {
                curTable = ionReader.getSymbolTable();
                ImportDescriptor imports[] = symbolTableToImports(curTable.getImportedTables());
                new Event(EventType.SYMBOL_TABLE, null, null, null,
                        null, null, imports, 0 )
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport);
            }

            if (IonType.isContainer(ionReader.getType())) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                ionStreamToEvent(EventType.CONTAINER_START, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport);
                ionReader.stepIn();

                //recursive call
                processEvent(ionWriterForOutput, ionWriterForErrorReport, ionReader, curTable);

                //write a Container_End event and step out
                new Event(EventType.CONTAINER_END, curType, null, null,
                         null, null, null, curDepth )
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport);
                ionReader.stepOut();
            } else {
                ionStreamToEvent(EventType.SCALAR, ionReader).writeOutput(ionWriterForOutput, ionWriterForErrorReport);
            }
        }
    }

    /**
     *  This function creates a new file for output stream with the fileName, If file name is empty or undefined,
     *  it will create a default output which are System.out or System.err.
     */
    private static BufferedOutputStream initOutputStream(String fileName, String defaultValue) {
        BufferedOutputStream outputStream = null;
        try {
            if (fileName != null && fileName.length() != 0) {
                File myFile = new File(fileName);
                FileOutputStream out = new FileOutputStream(myFile);
                outputStream = new BufferedOutputStream(out, BUFFER_SIZE);
            } else {
                switch (defaultValue){
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
        } catch (IOException e ) {
            System.err.println("Initialize Output Stream Error:" + e.getMessage());
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

    private static boolean isSameSymbolTable(SymbolTable x, SymbolTable y) {
        if (x.isSystemTable() && y.isSystemTable()) {
            return (x.getVersion() == y.getVersion());
        } else if (x.isSharedTable() && y.isSharedTable()) {
            return (x.getName() == y.getName() & (x.getVersion()==y.getVersion()));
        } else if (x.isLocalTable() && y.isLocalTable()) {
            SymbolTable[] xTable = x.getImportedTables();
            SymbolTable[] yTable = y.getImportedTables();
            //compare imports
            if (xTable.length == yTable.length) {
                for (int i=0; i < xTable.length; i++) {
                    if (!isSameSymbolTable(xTable[i], yTable[i]))  return false;
                }
            } else {
                return false;
            }
            //compare symbols
            Iterator<String> xIterator = x.iterateDeclaredSymbolNames();
            Iterator<String> yIterator = x.iterateDeclaredSymbolNames();
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
        for (int i=0; i < size; i++) {
            ImportDescriptor table = new ImportDescriptor(tables[i]);
            imports[i] = table;
        }
        return imports;
    }

    static class ProcessArgs {
        private static final String DEFAULT_FORMAT_VALUE = OutputFormat.PRETTY.getName();

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

        public OutputFormat getOutputFormat() throws CmdLineException {
            return OutputFormat.nameToOutputFormat(outputFormatName);
        }

        public List<String> getInputFiles() {
            return inputFiles;
        }
        public String getOutputFile() { return outputFile; }
        public String getErrorReport() { return errorReport; }
    }
}
