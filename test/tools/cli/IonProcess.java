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

import static com.amazon.ion.SystemSymbols.ION;

/**
 *  Read the input file(s) (optionally, specifying ReadInstructions or a filter) and re-write in the format
 *  specified by --output
 */
public final class IonProcess {
    private static final int CONSOLE_WIDTH = 120; // Only used for formatting the USAGE message
    private static final int USAGE_ERROR_EXIT_CODE = 1;
    private static final int IO_ERROR_EXIT_CODE = 2;
    private static final int BUFFER_SIZE = 128 * 1024;
    private static final IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();
    private static final IonTextWriterBuilder ION_TEXT_WRITER_BUILDER = IonTextWriterBuilder.standard();
    private static final String SYSTEM_OUT_DEFAULT_VALUE = "out";
    private static final String SYSTEM_ERR_DEFAULT_VALUE = "err";
    private static final String EMBEDDED_STREAM_ANNOTATION = "embedded_documents";
    private static final String EVENT_STREAM = "$ion_event_stream";

    public static void main(String[] args) {
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

        ProcessContext processContext = new ProcessContext(null, -1,
                null, null, null);
        try (
                //Initialize output stream, never return null. (default value: STDOUT)
                OutputStream outputStream = initOutputStream(parsedArgs, SYSTEM_OUT_DEFAULT_VALUE, processContext);
                //Initialize error report, never return null. (default value: STDERR)
                OutputStream errorReportOutputStream =
                        initOutputStream(parsedArgs, SYSTEM_ERR_DEFAULT_VALUE, processContext);
                IonWriter ionWriterForErrorReport = outputFormat.createIonWriter(errorReportOutputStream);
        ) {
            processContext.setIonWriter(outputFormat.createIonWriter(outputStream));
            processFiles(ionWriterForErrorReport, parsedArgs, processContext);
        } catch (IOException e) {
            System.err.println("Failed to close OutputStream: " + e.getMessage());
        } finally {
            IonWriter ionWriter = processContext.getIonWriter();
            if (ionWriter != null) {
                try {
                    ionWriter.close();
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    System.exit(IO_ERROR_EXIT_CODE);
                }
            }
        }
    }

    private static void processFiles(IonWriter ionWriterForErrorReport,
                                     ProcessArgs args,
                                     ProcessContext processContext) throws IOException {
        if (args.getOutputFormat() == OutputFormat.EVENTS) {
            processContext.getIonWriter().writeSymbol(EVENT_STREAM);
        }

        for (String path : args.getInputFiles()) {
            try (
                    InputStream inputStream = new BufferedInputStream(new FileInputStream(path));
                    IonReader ionReader = IonReaderBuilder.standard().build(inputStream);
            ) {
                processContext.setFileName(path);
                if (isEventStream(ionReader)) {
                    processContext.setEventIndex(0);
                    processFromEventStream(ionWriterForErrorReport,
                            ionReader, processContext, args);
                } else {
                    processFromIonStream(processContext.getIonWriter(), ionWriterForErrorReport,
                            ionReader, processContext, args);
                }

                processContext.getIonWriter().finish();
                ionWriterForErrorReport.finish();
            } catch (IonException e) {
                new ErrorDescription(processContext.getState(), e.getMessage(), processContext.getFileName(),
                        processContext.getEventIndex()).writeOutput(ionWriterForErrorReport);
                System.exit(IO_ERROR_EXIT_CODE);
            } catch (Exception e) {
                new ErrorDescription(ErrorType.STATE, e.getMessage(), processContext.getFileName(),
                        processContext.getEventIndex()).writeOutput(ionWriterForErrorReport);
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
            processContext.setEventIndex(1);
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
                            IonWriter tempIonWriter = ION_TEXT_WRITER_BUILDER.build(out);
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
                .writeOutput(ionWriterForErrorReport, processContext);
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
                        .writeOutput(ionWriterForErrorReport, processContext);
            }

            if (isEmbeddedStream(ionReader)) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                ionStreamToEvent(EventType.CONTAINER_START, ionReader)
                        .writeOutput(ionWriterForErrorReport, processContext);
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
                            .writeOutput(ionWriterForErrorReport, processContext);
                }
                //write a Container_End event and step out
                new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, curDepth)
                        .writeOutput(ionWriterForErrorReport, processContext);
                ionReader.stepOut();
            } else if (IonType.isContainer(ionReader.getType())) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                ionStreamToEvent(EventType.CONTAINER_START, ionReader)
                        .writeOutput(ionWriterForErrorReport, processContext);
                ionReader.stepIn();

                //recursive call
                ionReader.next();
                processToEventFromIonStream(ionWriterForOutput,
                        ionWriterForErrorReport, ionReader, curTable, processContext);

                //write a Container_End event and step out
                new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, curDepth)
                        .writeOutput(ionWriterForErrorReport, processContext);
                ionReader.stepOut();
            } else {
                ionStreamToEvent(EventType.SCALAR, ionReader)
                        .writeOutput(ionWriterForErrorReport, processContext);
            }
        } while (ionReader.next() != null);
    }

    //
    //  functions for processing from EventStream
    //

    private static void processFromEventStream(IonWriter ionWriterForErrorReport,
                                               IonReader ionReader,
                                               ProcessContext processContext,
                                               ProcessArgs args) throws IOException {
        processContext.setState(ErrorType.WRITE);
        if (args.getOutputFormat() == OutputFormat.EVENTS) {
            processToEventStreamFromEventStream(ionWriterForErrorReport,
                    ionReader, processContext, args);
        } else {
            processToNormalFromEventStream(ionWriterForErrorReport,
                    ionReader, processContext, args);
        }
    }

    private static void processToEventStreamFromEventStream(IonWriter ionWriterForErrorReport,
                                                            IonReader ionReader,
                                                            ProcessContext processContext,
                                                            ProcessArgs args) throws IOException {
        while (ionReader.next() != null) {
            Event event = eventStreamToEvent(ionReader);
            processContext.setLastEventType(event.getEventType());

            event.writeOutput(ionWriterForErrorReport, processContext);
            //update eventIndex
            processContext.setEventIndex(processContext.getEventIndex() + 1);
        }
    }

    private static void processToNormalFromEventStream(IonWriter ionWriterForErrorReport,
                                                       IonReader ionReader,
                                                       ProcessContext processContext,
                                                       ProcessArgs args) throws IOException {
        while (ionReader.next() != null) {
            //update eventIndex
            processContext.setEventIndex(processContext.getEventIndex() + 1);
            Event event = eventStreamToEvent(ionReader);

            processContext.setLastEventType(event.getEventType());

            if (event.getEventType() == EventType.CONTAINER_START) {
                if (isEmbeddedEvent(event)) {
                    embeddedEventToIon(ionReader, processContext, args, event.getIonType());
                } else {
                    IonType type = event.getIonType();
                    setAnnotationAndField(event, processContext.getIonWriter());
                    processContext.getIonWriter().stepIn(type);
                }
            } else if (event.getEventType().equals(EventType.CONTAINER_END)) {

                processContext.getIonWriter().stepOut();
            } else if (event.getEventType().equals(EventType.SCALAR)) {
                writeIonByType(event, processContext.getIonWriter());
            } else if (event.getEventType().equals(EventType.SYMBOL_TABLE)) {
                handleSymbolTableEvent(processContext, event, args, false);
            } else if (event.getEventType().equals(EventType.STREAM_END)) {
                processContext.getIonWriter().finish();
            }
        }
        if (processContext.getLastEventType() != EventType.STREAM_END) {
            throw new IonException("EventStream doesn't end with STREAM_END event");
        }
    }

    private static void embeddedEventToIon(IonReader ionReader,
                                           ProcessContext processContext,
                                           ProcessArgs args,
                                           IonType ionType) throws IOException {
        processContext.getIonWriter().addTypeAnnotation(EMBEDDED_STREAM_ANNOTATION);
        processContext.getIonWriter().stepIn(ionType);
        int depth = 0;
        boolean finish = false;
        while (ionReader.next() != null) {
            StringBuilder out = new StringBuilder();
            ProcessContext embeddedContext = new ProcessContext(null,0,null, null,
                    ION_TEXT_WRITER_BUILDER.withImports(_Private_Utils.systemSymtab(1)).build(out));
            embeddedContext.setEmbeddedOut(out);
            try {
                do {
                    processContext.setEventIndex(processContext.getEventIndex() + 1);
                    Event event = eventStreamToEvent(ionReader);
                    processContext.setLastEventType(event.getEventType());

                    if (event.getEventType() == EventType.STREAM_END) {
                        break;
                    } else if (event.getEventType() == EventType.SCALAR) {
                        writeIonByType(event, embeddedContext.getIonWriter());
                    } else if (event.getEventType() == EventType.CONTAINER_START) {
                        depth++;
                        embeddedContext.getIonWriter().stepIn(event.getIonType());
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
                        embeddedContext.getIonWriter().stepOut();
                    } else if (event.getEventType() == EventType.SYMBOL_TABLE) {
                        handleSymbolTableEvent(embeddedContext, event, args, true);
                    }
                } while (ionReader.next() != null);

                if (!finish && out.length() > 0) {
                    embeddedContext.getIonWriter().finish();
                    processContext.getIonWriter().writeString(out.toString());
                }
            } finally {
                IonWriter ionWriter = embeddedContext.getIonWriter();
                if (ionWriter != null) {
                    try {
                        ionWriter.close();
                    } catch (IOException e) {
                        System.err.println(e.getMessage());
                        System.exit(IO_ERROR_EXIT_CODE);
                    }
                }
            }
            if (finish) break;
        }
        processContext.getIonWriter().stepOut();
    }

    private static void handleSymbolTableEvent(ProcessContext processContext,
                                               Event event,
                                               ProcessArgs args,
                                               boolean isEmbedded) throws IOException {
        processContext.getIonWriter().close();
        ImportDescriptor[] imports = event.getImports();
        SymbolTable[] symbolTables = new SymbolTable[imports.length];
        for (int i = 0; i < imports.length; i++) {
            SymbolTable symbolTable = ION_SYSTEM.newSharedSymbolTable(
                    imports[i].getImportName(), imports[i].getVersion(), null);
            symbolTables[i] = symbolTable;
        }
        if (!isEmbedded) {
            processContext.setIonWriter(args.getOutputFormat().createIonWriterWithImports(
                    new FileOutputStream(processContext.getFile(), true), symbolTables));
        } else {
            processContext.getEmbeddedOut().append(" ");
            processContext.setIonWriter(ION_TEXT_WRITER_BUILDER.withImports(symbolTables)
                    .build(processContext.getEmbeddedOut()));
        }
    }

    //
    //  helper functions
    //

    private static void writeIonByType(Event event, IonWriter ionWriter) {
        setAnnotationAndField(event, ionWriter);
        IonValue value = event.getValue();
        value.setTypeAnnotationSymbols(event.getAnnotations());
        value.writeTo(ionWriter);
    }

    private static void setAnnotationAndField(Event event, IonWriter ionWriter) {
        SymbolToken field = event.getFieldName();
        SymbolToken[] annotations = event.getAnnotations();
        if (field != null) {
            if (!ionWriter.isInStruct()) throw new IonException("invalid field_name inside STRUCT");
            ionWriter.setFieldNameSymbol(field);
        } else if (ionWriter.isInStruct()) {
            String s = null;
            SymbolToken symbolToken = _Private_Utils.newSymbolToken(s, 0);
            ionWriter.setFieldNameSymbol(symbolToken);
        }
        if (annotations != null && annotations.length != 0) {
            ionWriter.setTypeAnnotationSymbols(annotations);
        }
    }

    private static Event eventStreamToEvent(IonReader ionReader) { ;
        String textValue = null;
        byte[] binaryValue = null;
        IonValue eventValue = null;
        EventType eventType = null;
        IonType ionType = null;
        SymbolToken fieldName = null;
        SymbolToken[] annotations = null;
        ImportDescriptor[] imports = null;
        int depth = -1;

        ionReader.stepIn();
        while (ionReader.next() != null) {
            switch (ionReader.getFieldName()) {
                case "event_type":
                    if (eventType != null) throw new IonException("invalid Event: repeat event_type");
                    eventType = EventType.valueOf(ionReader.stringValue().toUpperCase());
                    break;
                case "ion_type":
                    if (ionType != null) throw new IonException("invalid Event: repeat ion_type");
                    ionType = IonType.valueOf(ionReader.stringValue().toUpperCase());
                    break;
                case "field_name":
                    if (fieldName != null) throw new IonException("invalid Event: repeat field_name");
                    ionReader.stepIn();
                    String fieldText = null;
                    int fieldSid = 0;
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
                    fieldName = _Private_Utils.newSymbolToken(fieldText, fieldSid);
                    ionReader.stepOut();
                    break;
                case "annotations":
                    if (annotations != null) throw new IonException("invalid Event: repeat annotations");
                    ArrayList<SymbolToken> annotationsList = new ArrayList<>();
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
                        annotationsList.add(annotation);
                        ionReader.stepOut();
                    }
                    annotations = annotationsList.toArray(new SymbolToken[0]);
                    ionReader.stepOut();
                    break;
                case "value_text":
                    if (textValue != null) throw new IonException("invalid Event: repeat value_text");
                    textValue = ionReader.stringValue();
                    break;
                case "value_binary":
                    if (binaryValue != null) throw new IonException("invalid Event: repeat binary_value");
                    ArrayList<Integer> intArray = new ArrayList<>();
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
                    if (imports != null) throw new IonException("invalid Event: repeat imports");
                    imports = ionStreamToImportDescriptors(ionReader);
                    break;
                case "depth":
                    if (depth != -1) throw new IonException("invalid Event: repeat depth");
                    depth = ionReader.intValue();
                    break;
            }
        }
        ionReader.stepOut();
        //validate event
        validateEvent(textValue, binaryValue, eventType, fieldName, ionType, imports, depth);
        if (textValue != null) eventValue = ION_SYSTEM.singleValue(textValue);

        return new Event(eventType, ionType, fieldName, annotations, eventValue, imports, depth);
    }

    private static void validateEvent(String textValue, byte[] binaryValue, EventType eventType, SymbolToken fieldName,
                                      IonType ionType, ImportDescriptor[] imports, int depth) {
        if (eventType == null) throw new IonException("event_type can't be null");

        switch (eventType) {
            case CONTAINER_START:
                if (ionType == null || depth == -1) {
                    throw new IonException("Invalid CONTAINER_START: missing field(s)");
                } else if (!IonType.isContainer(ionType)) {
                    throw new IonException("Invalid CONTAINER_START: not a container");
                } else if (textValue != null || binaryValue != null) {
                    throw new IonException("Invalid CONTAINER_START: value_binary and value_text are only applicable"
                            + " for SCALAR events");
                } else if (imports != null) {
                    throw new IonException("Invalid CONTAINER_START: imports must only be present with SYMBOL_TABLE "
                            + "events");
                }
                break;
            case SCALAR:
                if (ionType == null || textValue == null || binaryValue == null || depth == -1) {
                    throw new IonException("Invalid SCALAR: missing field(s)");
                } else if (IonType.isContainer(ionType)) {
                    throw new IonException("Invalid SCALAR: ion_type error");
                } else if (imports != null) {
                    throw new IonException("Invalid SCALAR: imports must only be present with SYMBOL_TABLE "
                            + "events");
                }
                //compare text value and binary value
                IonValue text = ION_SYSTEM.singleValue(textValue);
                IonValue binary = ION_SYSTEM.singleValue(binaryValue);
                if (!Equivalence.ionEquals(text, binary)) {
                    throw new IonException("invalid Event: Text value and Binary value are different");
                }
                break;
            case SYMBOL_TABLE:
                if (depth == -1) {
                    throw new IonException("Invalid SYMBOL_TABLE: missing depth");
                } else if (imports == null ) {
                    throw new IonException("Invalid SYMBOL_TABLE: missing imports");
                } else if (textValue != null && binaryValue != null) {
                    throw new IonException("Invalid SYMBOL_TABLE: text_value and binary_value "
                            + "are only applicable for SCALAR events");
                } else if (fieldName != null && ionType != null) {
                    throw new IonException("Invalid SYMBOL_TABLE: unnecessary fields");
                }
                break;
            case CONTAINER_END:
                if (depth == -1 || ionType == null) {
                    throw new IonException("Invalid CONTAINER_END: missing depth");
                } else if (textValue != null && binaryValue != null) {
                    throw new IonException("Invalid CONTAINER_END: text_value and binary_value "
                            + "are only applicable for SCALAR events");
                } else if (fieldName != null && imports != null) {
                    throw new IonException("Invalid CONTAINER_END: unnecessary fields");
                }
                break;
            case STREAM_END:
                if (depth == -1) {
                    throw new IonException("Invalid STREAM_END: missing depth");
                } else if (textValue != null && binaryValue != null) {
                    throw new IonException("Invalid STREAM_END: text_value and binary_value "
                            + "are only applicable for SCALAR events");
                } else if (fieldName != null && ionType != null && imports != null) {
                    throw new IonException("Invalid STREAM_END: unnecessary fields");
                }
                break;
            default:
                throw new IonException("Invalid event_type");
        }
    }

    private static ImportDescriptor[] ionStreamToImportDescriptors(IonReader ionReader) {
        ArrayList<ImportDescriptor> imports = new ArrayList<>();

        ionReader.stepIn();
        while (ionReader.next() != null) {
            String importName = null;
            int maxId = 0;
            int version = 0;

            ionReader.stepIn();
            while (ionReader.next() != null) {
                switch (ionReader.getFieldName()) {
                    case "name":
                        importName = ionReader.stringValue();
                        break;
                    case "max_id":
                        maxId = ionReader.intValue();
                        break;
                    case "version":
                        version = ionReader.intValue();
                        break;
                }
            }
            ionReader.stepOut();

            ImportDescriptor table = new ImportDescriptor(importName, maxId, version);
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
    private static BufferedOutputStream initOutputStream(ProcessArgs args,
                                                         String defaultValue,
                                                         ProcessContext processContext) throws IOException {
        BufferedOutputStream outputStream = null;
        switch (defaultValue) {
            case SYSTEM_OUT_DEFAULT_VALUE:
                String outputFile = args.getOutputFile();
                if (outputFile != null && outputFile.length() != 0) {
                    File myFile = new File(outputFile);
                    processContext.setFile(myFile);
                    FileOutputStream out = new FileOutputStream(myFile);
                    outputStream = new BufferedOutputStream(out, BUFFER_SIZE);
                } else {
                    outputStream = new BufferedOutputStream(System.out, BUFFER_SIZE); }
                break;
            case SYSTEM_ERR_DEFAULT_VALUE:
                String errorReport = args.getErrorReport();
                if (errorReport != null && errorReport.length() != 0) {
                    File myFile = new File(errorReport);
                    FileOutputStream out = new FileOutputStream(myFile, true);
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
        StringBuilder textOut = new StringBuilder();
        if (eventType == EventType.SCALAR) {
            try (
                    IonWriter tempWriter = ION_TEXT_WRITER_BUILDER.build(textOut);
            ) {
                //write Text
                tempWriter.writeValue(ionReader);
                tempWriter.finish();
                String valueText = textOut.toString();
                String[] s = valueText.split("::");
                value = ION_SYSTEM.singleValue(s[s.length -1]);
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

        if (newTable.isLocalTable() && newTable.getImportedTables().length == 0) {
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

    private static boolean isEmbeddedEvent(Event event) {
        SymbolToken[] annotations = event.getAnnotations();
        return event.getEventType() == EventType.CONTAINER_START
                && (event.getIonType() == IonType.SEXP || event.getIonType() == IonType.LIST)
                && annotations != null
                && annotations.length != 0
                && annotations[0].getText().equals(EMBEDDED_STREAM_ANNOTATION)
                && event.getDepth() == 0;
    }

    public static boolean isEventStream(IonReader ionReader) {
        return ionReader.next() != null
                && ionReader.getType() == IonType.SYMBOL
                && EVENT_STREAM.equals(ionReader.symbolValue().getText());
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
