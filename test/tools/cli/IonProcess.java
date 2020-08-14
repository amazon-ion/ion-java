package tools.cli;

import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.IonReader;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.IonException;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.util.Equivalence;
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
import java.util.ArrayList;
import java.util.List;

/**
 *  Read the input file(s) (optionally, specifying ReadInstructions or a filter) and re-write in the format
 *  specified by --output
 */
public final class IonProcess {
    private static final int CONSOLE_WIDTH = 120; // Only used for formatting the USAGE message
    private static final int USAGE_ERROR_EXIT_CODE = 1;
    private static final int IO_ERROR_EXIT_CODE = 2;
    private static final int BUFFER_SIZE = 128 * 1024;
    private static final String SYSTEM_OUT_DEFAULT_VALUE = "out";
    private static final String SYSTEM_ERR_DEFAULT_VALUE = "err";
    private static final String EMBEDDED_STREAM_ANNOTATION = "embedded_documents";
    private static final String EVENT_STREAM = "$ion_event_stream";

    public static void main(final String[] args) {
        ProcessArgs parsedArgs = new ProcessArgs();
        CmdLineParser parser = new CmdLineParser(parsedArgs);
        parser.getProperties().withUsageWidth(CONSOLE_WIDTH);
        OutputFormat outputFormat = null;

        try {
            parser.parseArgument(args);
            outputFormat = parsedArgs.getOutputFormat();
        } catch (CmdLineException | IllegalArgumentException e) {
            printHelpTextAndExit(e.getMessage(), parser);
        }

        //Use this variable for symbol_table event which changes ionWriter
        IonWriter ionWriterForOutput = null;
        try (
                //Initialize output stream, never return null. (default value: STDOUT)
                OutputStream outputStream = initOutputStream(parsedArgs, SYSTEM_OUT_DEFAULT_VALUE);
                //Initialize error report, never return null. (default value: STDERR)
                OutputStream errorReportOutputStream = initOutputStream(parsedArgs, SYSTEM_ERR_DEFAULT_VALUE);
                IonWriter initIonWriterForOutput = outputFormat.createIonWriter(outputStream);
                IonWriter ionWriterForErrorReport = outputFormat.createIonWriter(errorReportOutputStream);
        ) {
            ionWriterForOutput = initIonWriterForOutput;
            processFiles(ionWriterForOutput, ionWriterForErrorReport, parsedArgs, outputStream);
        } catch (IOException e) {
            System.err.println("Failed to close OutputStream: " + e.getMessage());
        }
    }

    private static void processFiles(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     ProcessArgs args,
                                     OutputStream outputStream) throws IOException {
        if (args.getOutputFormat() == OutputFormat.EVENTS) {
            ionWriterForOutput.writeSymbol(EVENT_STREAM);
        }
        //this object stores data for ErrorReport.
        ProcessContext processContext = new ProcessContext(null, 1, null, null);

        for (String path : args.getInputFiles()) {
            try (
                    InputStream inputStream = new BufferedInputStream(new FileInputStream(path));
                    IonReader ionReader = IonReaderBuilder.standard().build(inputStream);
            ) {
                processContext.setFileName(path);

                if (isEventStream(ionReader)) {
                    processContext.setEventIndex(processContext.getEventIndex() - 1);
                    processFromEventStream(ionWriterForOutput, ionWriterForErrorReport,
                            ionReader, processContext, args, outputStream);
                } else {
                    processFromIonStream(ionWriterForOutput, ionWriterForErrorReport, ionReader, processContext, args);
                }

                ionWriterForOutput.finish();
                ionWriterForErrorReport.finish();
            } catch (IonException e) {
                if (args.getOutputFormat() == OutputFormat.EVENTS || processContext.getState() == ErrorType.WRITE) {
                    new ErrorDescription(processContext.getState(), e.getMessage(), processContext.getFileName(),
                            processContext.getEventIndex()).writeOutput(ionWriterForErrorReport);
                } else {
                    new ErrorDescription(processContext.getState(), e.getMessage(), processContext.getFileName())
                            .writeOutput(ionWriterForErrorReport);
                }
                System.exit(IO_ERROR_EXIT_CODE);
            } catch (Exception e) {
                if (args.getOutputFormat() == OutputFormat.EVENTS) {
                    new ErrorDescription(ErrorType.STATE, e.getMessage(), processContext.getFileName(),
                            processContext.getEventIndex()).writeOutput(ionWriterForErrorReport);
                } else {
                    new ErrorDescription(ErrorType.STATE, e.getMessage(), processContext.getFileName())
                            .writeOutput(ionWriterForErrorReport);
                }
                System.exit(IO_ERROR_EXIT_CODE);
            }
        }
    }

    //
    //  functions for processing from IonStream
    //

    private static void processFromIonStream(IonWriter ionWriterForOutput,
                                             IonWriter ionWriterForErrorReport,
                                             IonReader ionReader,
                                             ProcessContext processContext,
                                             ProcessArgs args) throws IOException {
        processContext.setState(ErrorType.READ);
        if (args.getOutputFormat() == OutputFormat.EVENTS) {
            processToEventStreamFromIonStream(ionWriterForOutput,
                    ionWriterForErrorReport, ionReader, processContext);
        } else {
            processToNormalFromIonStream(ionWriterForOutput, ionWriterForErrorReport, ionReader, args);
        }
    }

    private static void processToNormalFromIonStream(IonWriter ionWriterForOutput,
                                                     IonWriter ionWriterForErrorReport,
                                                     IonReader ionReader,
                                                     ProcessArgs args) throws IOException {
        do {
            if (isEmbeddedStream(ionReader)) {
                ionReader.stepIn();
                ionWriterForOutput.addTypeAnnotation(EMBEDDED_STREAM_ANNOTATION);
                ionWriterForOutput.stepIn(IonType.SEXP);
                while (ionReader.next() != null) {
                    String stream = ionReader.stringValue();
                    StringBuilder out = new StringBuilder();
                    try (
                            IonReader tempIonReader = IonReaderBuilder.standard().build(stream);
                            IonWriter tempIonWriter = IonTextWriterBuilder.standard().build(out);
                    ) {
                        while (tempIonReader.next() != null) {
                            tempIonWriter.writeValue(tempIonReader);
                        }

                        ionWriterForOutput.writeString(out.toString());
                        tempIonWriter.finish();
                    }
                }
                ionWriterForOutput.stepOut();
                ionReader.stepOut();
            } else {
                ionWriterForOutput.writeValue(ionReader);
            }
        } while (ionReader.next() != null);
    }

    private static void processToEventStreamFromIonStream(IonWriter ionWriterForOutput,
                                                          IonWriter ionWriterForErrorReport,
                                                          IonReader ionReader,
                                                          ProcessContext processContext) throws IOException {
        //curTable is used for checking if the symbol table changes during the processEvent function.
        SymbolTable curTable = ionReader.getSymbolTable();

        processToEventFromIonStream(ionWriterForOutput, ionWriterForErrorReport, ionReader, curTable, processContext);
        new Event(EventType.STREAM_END, null, null, null,
                null, null, 0)
                .writeOutput(ionWriterForOutput, ionWriterForErrorReport, processContext);
    }

    private static void processToEventFromIonStream(IonWriter ionWriterForOutput,
                                                    IonWriter ionWriterForErrorReport,
                                                    IonReader ionReader,
                                                    SymbolTable curTable,
                                                    ProcessContext processContext) throws IOException {
        do {
            if (!isSameSymbolTable(ionReader.getSymbolTable(), curTable)) {
                curTable = ionReader.getSymbolTable();
                ImportDescriptor[] imports = symbolTableToImports(curTable.getImportedTables());
                new Event(EventType.SYMBOL_TABLE, null, null, null,
                        null, imports, 0)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, processContext);
            }

            if (isEmbeddedStream(ionReader)) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                ionStreamToEvent(EventType.CONTAINER_START, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, processContext);
                ionReader.stepIn();

                while (ionReader.next() != null) {
                    String stream = ionReader.stringValue();
                    try (IonReader tempIonReader = IonReaderBuilder.standard().build(stream)) {
                        while (tempIonReader.next() != null) {
                            processToEventFromIonStream(ionWriterForOutput, ionWriterForErrorReport, tempIonReader,
                                    curTable, processContext);
                        }
                    }
                    new Event(EventType.STREAM_END, null, null, null,
                            null, null, 0)
                            .writeOutput(ionWriterForOutput, ionWriterForErrorReport, processContext);
                }
                //write a Container_End event and step out
                new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, curDepth)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, processContext);
                ionReader.stepOut();
            } else if (IonType.isContainer(ionReader.getType())) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                ionStreamToEvent(EventType.CONTAINER_START, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, processContext);
                ionReader.stepIn();

                //recursive call
                ionReader.next();
                processToEventFromIonStream(ionWriterForOutput,
                        ionWriterForErrorReport, ionReader, curTable, processContext);

                //write a Container_End event and step out
                new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, curDepth)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, processContext);
                ionReader.stepOut();
            } else {
                ionStreamToEvent(EventType.SCALAR, ionReader)
                        .writeOutput(ionWriterForOutput, ionWriterForErrorReport, processContext);
            }
        } while (ionReader.next() != null);
    }

    //
    //  functions for processing from EventStream
    //

    private static void processFromEventStream(IonWriter ionWriterForOutput,
                                               IonWriter ionWriterForErrorReport,
                                               IonReader ionReader,
                                               ProcessContext processContext,
                                               ProcessArgs args,
                                               OutputStream outputStream) throws IOException {
        processContext.setState(ErrorType.WRITE);
        if (args.getOutputFormat() == OutputFormat.EVENTS) {
            processToEventStreamFromEventStream(ionWriterForOutput, ionWriterForErrorReport,
                    ionReader, processContext, args);
        } else {
            processToNormalFromEventStream(ionWriterForOutput, ionWriterForErrorReport,
                    ionReader, processContext, args, outputStream);
        }
    }

    private static void processToEventStreamFromEventStream(IonWriter ionWriterForOutput,
                                                            IonWriter ionWriterForErrorReport,
                                                            IonReader ionReader,
                                                            ProcessContext processContext,
                                                            ProcessArgs args) throws IOException {
        while (ionReader.next() != null) {
            Event event = eventStreamToEvent(ionReader);
            event.validate();
            processContext.setLastEventType(event.getEventType());

            event.writeOutput(ionWriterForOutput, ionWriterForErrorReport, processContext);
            //update eventIndex
            processContext.setEventIndex(processContext.getEventIndex() + 1);
        }
    }

    private static void processToNormalFromEventStream(IonWriter ionWriterForOutput,
                                                       IonWriter ionWriterForErrorReport,
                                                       IonReader ionReader,
                                                       ProcessContext processContext,
                                                       ProcessArgs args,
                                                       OutputStream outputStream) throws IOException {
        while (ionReader.next() != null) {
            //update eventIndex
            processContext.setEventIndex(processContext.getEventIndex() + 1);
            Event event = eventStreamToEvent(ionReader);
            //event.validate();
            processContext.setLastEventType(event.getEventType());
            if (event.getEventType() == EventType.CONTAINER_START) {

                if (isEmbeddedEvent(event)) {
                    embeddedEventToIon(ionReader, ionWriterForOutput, processContext, args, outputStream);
                } else {
                    IonType type = event.getIonType();
                    setAnnotationAndField(event, ionWriterForOutput);
                    ionWriterForOutput.stepIn(type);
                }
            } else if (event.getEventType().equals(EventType.CONTAINER_END)) {
                ionWriterForOutput.stepOut();
            } else if (event.getEventType().equals(EventType.SCALAR)) {
                writeIonByType(event, ionWriterForOutput);
            } else if (event.getEventType().equals(EventType.SYMBOL_TABLE)) {
                ionWriterForOutput.finish();
                ImportDescriptor[] imports = event.getImports();
                SymbolTable[] symbolTables = new SymbolTable[imports.length];
                for (int i = 0; i < imports.length; i++) {
                    SymbolTable symbolTable = IonSystemBuilder.standard().build().newSharedSymbolTable(
                            imports[i].getImportName(), imports[i].getVersion(), null);
                    symbolTables[i] = symbolTable;
                }
                ionWriterForOutput = args.getOutputFormat().createIonWriterWithImports(outputStream, symbolTables);
            } else if (event.getEventType().equals(EventType.STREAM_END)) {
                ionWriterForOutput.finish();
            }
        }
        if (processContext.getLastEventType() != EventType.STREAM_END) {
            throw new IonException("EventStream doesn't end with STREAM_END event");
        }
    }

    private static void setAnnotationAndField(Event event, IonWriter ionWriter) throws IOException {
        SymbolToken field = event.getFieldName();
        SymbolToken[] annotations = event.getAnnotations();
        if (field != null) {
            ionWriter.setFieldNameSymbol(field);
        }
        if (annotations != null && annotations.length != 0) {
            ionWriter.setTypeAnnotationSymbols(annotations);
        }
    }

    private static void embeddedEventToIon(IonReader ionReader,
                                           IonWriter ionWriterForOutput,
                                           ProcessContext processContext,
                                           ProcessArgs args,
                                           OutputStream outputStream) throws IOException {
        ionWriterForOutput.addTypeAnnotation(EMBEDDED_STREAM_ANNOTATION);
        ionWriterForOutput.stepIn(IonType.SEXP);
        int depth = 0;
        boolean finish = false;
        while (ionReader.next() != null) {
            StringBuilder out = new StringBuilder();
            try (
                    IonWriter tempWriter = IonTextWriterBuilder.standard().build(out);
            ) {
                do {
                    processContext.setEventIndex(processContext.getEventIndex() + 1);
                    Event event = eventStreamToEvent(ionReader);
                    event.validate();
                    processContext.setLastEventType(event.getEventType());

                    if (event.getEventType() == EventType.STREAM_END) {
                        break;
                    } else if (event.getEventType() == EventType.SCALAR) {
                        writeIonByType(event, tempWriter);
                    } else if (event.getEventType() == EventType.CONTAINER_START) {
                        depth++;
                        tempWriter.stepIn(event.getIonType());
                    } else if (event.getEventType() == EventType.CONTAINER_END) {
                        if (depth == 0) {
                            if (event.getIonType() == IonType.SEXP) {
                                finish = true;
                                break;
                            } else {
                                throw new IonException("invalid CONTAINER_END");
                            }
                        }
                        depth--;
                        tempWriter.stepOut();
                    } else if (event.getEventType() == EventType.SYMBOL_TABLE) {
                        ionWriterForOutput.finish();
                        ImportDescriptor[] imports = event.getImports();
                        SymbolTable[] symbolTables = new SymbolTable[imports.length];
                        for (int i = 0; i < imports.length; i++) {
                            SymbolTable symbolTable = IonSystemBuilder.standard().build().newSharedSymbolTable(
                                    imports[i].getImportName(), imports[i].getVersion(), null);
                            symbolTables[i] = symbolTable;
                        }
                        ionWriterForOutput = args.getOutputFormat()
                                .createIonWriterWithImports(outputStream, symbolTables);
                    }
                } while (ionReader.next() != null);

                if (out.length() > 0) {
                    tempWriter.finish();
                    ionWriterForOutput.writeString(out.toString());
                }
            }
            if (finish) break;
        }
        ionWriterForOutput.stepOut();
    }

    //
    //  helper functions
    //

    private static void writeIonByType(Event event, IonWriter ionWriter) throws IOException {
        setAnnotationAndField(event, ionWriter);
        event.getValue().writeTo(ionWriter);
    }

    private static Event eventStreamToEvent(IonReader ionReader) {
        Event event = new Event();
        ionReader.stepIn();
        String textValue = null;
        byte[] binaryValue = null;
        while (ionReader.next() != null) {
            switch (ionReader.getFieldName()) {
                case "event_type":
                    event.setEventType(EventType.valueOf(ionReader.stringValue().toUpperCase()));
                    break;
                case "ion_type":
                    event.setIonType(IonType.valueOf(ionReader.stringValue().toUpperCase()));
                    break;
                case "field_name":
                    ionReader.stepIn();
                    String fieldText = null;
                    int fieldSid = -1;
                    while (ionReader.next() != null) {
                        switch (ionReader.getFieldName()) {
                            case "text":
                                fieldText = ionReader.stringValue();
                                break;
                            case "sid":
                                fieldSid = ionReader.intValue();
                                break;
                        }
                    }
                    SymbolToken symbolToken = _Private_Utils.newSymbolToken(fieldText, fieldSid);
                    event.setFieldName(symbolToken);
                    ionReader.stepOut();
                    break;
                case "annotations":
                    ArrayList<SymbolToken> annotations = new ArrayList();
                    ionReader.stepIn();
                    while (ionReader.next() != null) {
                        ionReader.stepIn();
                        String text = "";
                        int sid = 0;
                        while (ionReader.next() != null) {
                            switch (ionReader.getFieldName()) {
                                case "text":
                                    text = ionReader.stringValue();
                                    break;
                                case "sid":
                                    sid = ionReader.intValue();
                                    break;
                            }
                        }
                        SymbolToken annotation = _Private_Utils.newSymbolToken(text, sid);
                        annotations.add(annotation);
                        ionReader.stepOut();
                    }
                    SymbolToken[] annotationsArray = annotations.toArray(new SymbolToken[annotations.size()]);
                    event.setAnnotations(annotationsArray);
                    ionReader.stepOut();
                    break;
                case "value_text":
                    textValue = ionReader.stringValue();
                    break;
                case "value_binary":
                    ArrayList<Integer> intArray = new ArrayList();
                    ionReader.stepIn();
                    while (ionReader.next() != null) {
                        intArray.add(ionReader.intValue());
                    }
                    byte[] binary = new byte[intArray.size()];
                    for (int i = 0; i < intArray.size(); i++) {
                        int val = intArray.get(i);
                        binary[i] = (byte) (val & 0xff);
                    }
                    binaryValue = binary;
                    ionReader.stepOut();
                    break;
                case "imports":
                    ImportDescriptor[] imports = ionStreamToImportDescriptors(ionReader);
                    event.setImports(imports);
                    break;
                case "depth":
                    event.setDepth(ionReader.intValue());
                    break;
            }

        }
        //compare text value and binary value
        if (textValue != null && binaryValue != null) {
            IonSystem ionSystem = IonSystemBuilder.standard().build();
            IonValue text = ionSystem.singleValue(textValue);
            IonValue binary = ionSystem.singleValue(binaryValue);
            if (!Equivalence.ionEquals(text, binary)) {
                throw new IonException("invalid Event: Text value and Binary value are different");
            } else {
                IonValue v = IonSystemBuilder.standard().build().singleValue(textValue);
                event.setValue(v);
            }
        }
        ionReader.stepOut();

        return event;
    }


    private static ImportDescriptor[] ionStreamToImportDescriptors(IonReader ionReader) {
        ArrayList<ImportDescriptor> imports = new ArrayList();
        ionReader.stepIn();
        while (ionReader.next() != null) {
            ionReader.stepIn();
            ImportDescriptor table = new ImportDescriptor();
            while (ionReader.next() != null) {
                switch (ionReader.getFieldName()) {
                    case "name":
                        table.setImportName(ionReader.stringValue());
                        break;
                    case "max_id":
                        table.setMaxId(ionReader.intValue());
                        break;
                    case "version":
                        table.setVersion(ionReader.intValue());
                        break;
                }
            }
            ionReader.stepOut();
            imports.add(table);
        }
        ImportDescriptor[] importsArray = imports.toArray(new ImportDescriptor[imports.size()]);

        ionReader.stepOut();
        return importsArray;
    }

    /**
     *  This function creates a new file for output stream with the fileName, If file name is empty or undefined,
     *  it will create a default output which is System.out or System.err.
     *
     *  this function never return null
     */
    private static BufferedOutputStream initOutputStream(ProcessArgs args, String defaultValue) throws IOException {
        BufferedOutputStream outputStream = null;
        switch (defaultValue) {
            case SYSTEM_OUT_DEFAULT_VALUE:
                if (args.getOutputFormat() == OutputFormat.NONE) {
                    try (NoOpOutputStream trivialOut = new NoOpOutputStream()) {
                        outputStream = new BufferedOutputStream(trivialOut, BUFFER_SIZE);
                    }
                } else {
                    String outputFile = args.getOutputFile();
                    if (outputFile != null && outputFile.length() != 0) {
                        File myFile = new File(outputFile);
                        FileOutputStream out = new FileOutputStream(myFile);
                        outputStream = new BufferedOutputStream(out, BUFFER_SIZE);
                    } else {
                        outputStream = new BufferedOutputStream(System.out, BUFFER_SIZE);
                    }
                }
                break;
            case SYSTEM_ERR_DEFAULT_VALUE:
                String errorReport = args.getErrorReport();
                if (errorReport != null && errorReport.length() != 0) {
                    File myFile = new File(errorReport);
                    FileOutputStream out = new FileOutputStream(myFile);
                    outputStream = new BufferedOutputStream(out, BUFFER_SIZE);
                } else {
                    outputStream = new BufferedOutputStream(System.err, BUFFER_SIZE);
                }
                break;
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

        IonValue value = null;
        if (eventType == EventType.SCALAR) {
            try (
                    ByteArrayOutputStream textOut = new ByteArrayOutputStream();
                    IonWriter tempWriter = IonTextWriterBuilder.standard().build(textOut);
            ) {
                //write Text
                tempWriter.writeValue(ionReader);
                tempWriter.finish();
                String valueText = textOut.toString("utf-8");
                String[] s = valueText.split("::");
                value = IonSystemBuilder.standard().build().singleValue(s[s.length -1]);
            }
        }

        ImportDescriptor[] imports = null;
        int depth = ionReader.getDepth();
        return new Event(eventType, ionType, fieldName, annotations, value, imports, depth);
    }

    private static boolean isSameSymbolTable(SymbolTable newTable, SymbolTable curTable) {
        if (newTable == null && curTable == null) return true;
        else if (newTable != null && curTable == null) return false;
        else if (newTable == null) return false;
        if (newTable.isSystemTable() && newTable.getName() == "$ion") {
            return true;
        } else if (newTable.isLocalTable() && newTable.getImportedTables().length == 0) {
            return true;
        } else if (newTable.isSystemTable() && curTable.isSystemTable()) {
            return newTable.getVersion() == curTable.getVersion();
        } else if (newTable.isSharedTable() && curTable.isSharedTable()) {
            return (newTable.getName().equals(curTable.getName())) & (newTable.getVersion() == curTable.getVersion());
        } else if (newTable.isLocalTable() && curTable.isLocalTable()) {
            SymbolTable[] xTable = newTable.getImportedTables();
            SymbolTable[] yTable = curTable.getImportedTables();
            //compare imports
            if (xTable.length == yTable.length) {
                for (int i = 0; i < xTable.length; i++) {
                    if (!isSameSymbolTable(xTable[i], yTable[i]))  return false;
                }
            } else {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    private static ImportDescriptor[] symbolTableToImports(SymbolTable[] tables) {
        if (tables == null || tables.length == 0) return null;
        int size = tables.length;

        ImportDescriptor[] imports = new ImportDescriptor[size];
        for (int i = 0; i < size; i++) {
            ImportDescriptor table = new ImportDescriptor(tables[i]);
            imports[i] = table;
        }
        return imports;
    }

    private static boolean isEmbeddedStream(IonReader ionReader) {
        IonType ionType = ionReader.getType();
        String[] annotations = ionReader.getTypeAnnotations();
        return (ionType == IonType.SEXP || ionType == IonType.LIST)
                && ionReader.getDepth() == 0
                && annotations.length > 0
                && annotations[0].equals(EMBEDDED_STREAM_ANNOTATION);
    }

    private static boolean isEventStream(IonReader ionReader) {
        return ionReader.next() != null
                && ionReader.getType() == IonType.SYMBOL
                && EVENT_STREAM.equals(ionReader.symbolValue().getText());
    }

    private static boolean isEmbeddedEvent(Event event) {
        SymbolToken[] annotations = event.getAnnotations();
        return event.getEventType() == EventType.CONTAINER_START
                && (event.getIonType() == IonType.SEXP || event.getIonType() == IonType.LIST)
                && annotations != null
                && annotations.length != 0
                && annotations[0].getText().equals(EMBEDDED_STREAM_ANNOTATION)
                && event.getDepth() == 0;
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

    //
    //  classes
    //

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
