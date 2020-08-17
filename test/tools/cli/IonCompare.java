package tools.cli;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.util.Equivalence;
import com.sun.tools.javac.util.Pair;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import tools.comparisonReport.ComparisonContext;
import tools.comparisonReport.ComparisonResult;
import tools.comparisonReport.ComparisonResultType;
import tools.errorReport.ErrorDescription;
import tools.errorReport.ErrorType;
import tools.events.Event;
import tools.events.EventType;
import tools.events.ImportDescriptor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IonCompare {
    private static final int CONSOLE_WIDTH = 120; // Only used for formatting the USAGE message
    private static final int USAGE_ERROR_EXIT_CODE = 1;
    private static final int IO_ERROR_EXIT_CODE = 2;
    private static final int BUFFER_SIZE = 128 * 1024;
    private static final String SYSTEM_OUT_DEFAULT_VALUE = "out";
    private static final String SYSTEM_ERR_DEFAULT_VALUE = "err";
    private static final IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();
    private static final IonTextWriterBuilder ION_TEXT_WRITER_BUILDER = IonTextWriterBuilder.standard();
    private static final String EMBEDDED_STREAM_ANNOTATION = "embedded_documents";

    public static void main(final String[] args) {
        CompareArgs parsedArgs = new CompareArgs();
        CmdLineParser parser = new CmdLineParser(parsedArgs);
        parser.getProperties().withUsageWidth(CONSOLE_WIDTH);
        ComparisonType comparisonType = parsedArgs.getComparisonType();

        try {
            parser.parseArgument(args);
            comparisonType = parsedArgs.getComparisonType();
        } catch (CmdLineException | IllegalArgumentException e) {
            System.err.println(e.getMessage() + "\n");
            System.err.println("\"Compare\" Compare all inputs (which may contain Ion streams and/or EventStreams) \n"
                    + "against all other inputs using the Ion data model's definition of equality. Write a \n"
                    + "ComparisonReport to the output.\n");
            System.err.println("Usage:\n");
            System.err.println("ion compare [--output <file>] [--error-report <file>] [--output-format (text | \n"
                    + "pretty | binary | none)]  [--catalog <file>]... [--comparison-type (basic | equivs | \n"
                    + "non-equivs | equiv-timeline)] [-] [<input_file>]...\n");
            parser.printUsage(System.err);
            System.exit(USAGE_ERROR_EXIT_CODE);
        }

        try (//Initialize output stream, never return null. (default value: STDOUT)
             OutputStream outputStream = initOutputStream(parsedArgs, SYSTEM_OUT_DEFAULT_VALUE);
             //Initialize error report, never return null. (default value: STDERR)
             OutputStream errorReportOutputStream = initOutputStream(parsedArgs, SYSTEM_ERR_DEFAULT_VALUE);
             IonWriter ionWriterForOutput = IonTextWriterBuilder.pretty().build(outputStream);
             IonWriter ionWriterForErrorReport = IonTextWriterBuilder.pretty().build(errorReportOutputStream);
        ) {
            CompareFiles(ionWriterForOutput, ionWriterForErrorReport, parsedArgs, comparisonType);
        } catch (IOException e) {
            System.err.println("Failed to close OutputStream: " + e.getMessage());
        }
    }

    private static void CompareFiles(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     CompareArgs args,
                                     ComparisonType comparisonType) throws IOException {
        CompareContext compareContext = new CompareContext(null, null);
        List<String> files = args.getInputFiles();

        for (String compareToPath : files) {
            for (String path : files) {
                compareContext.reset(path, compareToPath);
                try (InputStream inputFirst = new BufferedInputStream(new FileInputStream(path));
                     IonReader ionReaderFirst = IonReaderBuilder.standard().build(inputFirst);
                     InputStream inputSecond = new BufferedInputStream(new FileInputStream(compareToPath));
                     IonReader ionReaderSecond = IonReaderBuilder.standard().build(inputSecond);
                ) {
                    if (!path.equals(compareToPath)) {
                        boolean isFirstEventStream = IonJavaTestUtil.isEventStream(ionReaderFirst);
                        boolean isSecondEventStream = IonJavaTestUtil.isEventStream(ionReaderSecond);
                        ArrayList<Event> eventsFirst = getEventStream(ionReaderFirst, isFirstEventStream);
                        ArrayList<Event> eventsSecond = getEventStream(ionReaderSecond, isSecondEventStream);
                        compareContext.setEventStreamFirst(eventsFirst);
                        compareContext.setEventStreamSecond(eventsSecond);

                        if (comparisonType == ComparisonType.BASIC) {
                            if (!compare(compareContext, true,
                                    0, eventsFirst.size() - 1, 0, eventsSecond.size() - 1)) {
                                new ComparisonResult(ComparisonResultType.NOT_EQUAL,
                                        new ComparisonContext(compareContext.getFile(),
                                                eventsFirst.get(compareContext.getFileEventIndex()),
                                                compareContext.getFileEventIndex() + 1),
                                        new ComparisonContext(compareContext.getCompareToFile(),
                                                eventsSecond.get(compareContext.getCompareToFileEventIndex()),
                                                compareContext.getCompareToFileEventIndex() + 1),
                                        compareContext.getMessage()).writeOutput(ionWriterForOutput);
                            }
                        } else if (comparisonType == ComparisonType.EQUIVS) {
                            //TODO will be in next commit
                        } else if (comparisonType == ComparisonType.NON_EQUIVS) {
                            //TODO will be in next commit
                        } else if (comparisonType == ComparisonType.EQUIVSS_TIMELINE) {
                            //TODO will be in next commit
                        }
                    }
                } catch (IonException e) {
                    new ErrorDescription(ErrorType.STATE, e.getMessage(), path+';'+compareToPath,
                            -1).writeOutput(ionWriterForErrorReport);
                    System.exit(IO_ERROR_EXIT_CODE);
                }
            }
        }
    }

    private static boolean compare(CompareContext compareContext,
                                   boolean writeReport,
                                   int startI, int endI, int startJ, int endJ) throws IOException {
        ArrayList<Event> eventsFirst = compareContext.getEventStreamFirst();
        ArrayList<Event> eventsSecond = compareContext.getEventStreamSecond();
        int i = startI;
        int j = startJ;
        while (i <= endI && j <= endJ && i < eventsFirst.size() && j < eventsSecond.size()) {
            Event eventFirst = eventsFirst.get(i);
            Event eventSecond = eventsSecond.get(j);
            SymbolToken fieldNameFirst = eventFirst.getFieldName();
            SymbolToken fieldNameSecond = eventSecond.getFieldName();
            SymbolToken[] annotationFirst = eventFirst.getAnnotations();
            SymbolToken[] annotationSecond = eventSecond.getAnnotations();
            EventType eventTypeFirst = eventFirst.getEventType();
            EventType eventTypeSecond = eventSecond.getEventType();

            if (eventTypeFirst != eventTypeSecond) {
                if (writeReport) {
                    setReportInfo(i, j,"Doesn't match event_type", compareContext);
                }
                return false;
            } else if (eventFirst.getDepth() != eventSecond.getDepth()) {
                if (writeReport) {
                    setReportInfo(i, j,"Doesn't match depth", compareContext);
                }
                return false;
            } else if (eventFirst.getIonType() != eventSecond.getIonType()) {
                if (writeReport) {
                    setReportInfo(i, j,"Doesn't match ion_type", compareContext);
                }
                return false;
            } else if (!isSameSymbolToken(fieldNameFirst, fieldNameSecond)) {
                if (writeReport) {
                    setReportInfo(i, j,"Doesn't match field_name", compareContext);
                }
                return false;
            } else if (!isSameSymbolTokenArray(annotationFirst, annotationSecond)) {
                if (writeReport) {
                    setReportInfo(i, j,"Doesn't match annotation", compareContext);
                }
                return false;
            }
            // following three branches for:
            // 1. checking embedded stream equivalence
            // 2. checking struct equivalence
            // 3. checking SCALAR value equivalence
            if (isEmbeddedEvent(eventFirst)) {
                i++;
                j++;
                while (!((eventsFirst.get(i).getEventType() == EventType.CONTAINER_END
                        && eventsFirst.get(i).getIonType() == IonType.SEXP)
                        || (eventsSecond.get(j).getEventType() == EventType.CONTAINER_END
                        && eventsSecond.get(j).getIonType() == IonType.SEXP))) {

                    int xI = i;
                    while (eventsFirst.get(i).getEventType() != EventType.STREAM_END) {
                        i++;
                    }
                    int xJ = j;
                    while (eventsSecond.get(j).getEventType() != EventType.STREAM_END) {
                        j++;
                    }

                    if (!compare(compareContext, true, xI, i, xJ, j)) {
                        return false;
                    }
                    i++;
                    j++;
                    if (eventsFirst.get(i).getEventType() != eventsSecond.get(j).getEventType()) {
                        setReportInfo(i, j,"embedded stream has different size", compareContext);
                        return false;
                    }
                }
            } else if (eventTypeFirst == EventType.CONTAINER_START
                    && eventFirst.getIonType() == IonType.STRUCT) {
                int iStart = i + 1;
                int jStart = j + 1;
                i = parseContainer(i, eventsFirst.size(), compareContext, true).snd;
                j = parseContainer(j, eventsSecond.size(), compareContext, false).snd;

                if (!isSameStruct(compareContext, iStart, i, jStart, j)) {
                    if (writeReport) {
                        setReportInfo(i, -1,"Doesn't match struct", compareContext);
                    }
                    return false;
                }
            } else if (eventTypeFirst == EventType.SCALAR
                    && !Equivalence.ionEquals(eventFirst.getValue(), eventSecond.getValue())) {
                if (writeReport) {
                    setReportInfo(i, j, eventFirst.getValue() + " vs. "
                            + eventSecond.getValue(), compareContext);
                }
                return false;
            }
            i++;
            j++;
        }
        return true;
    }

    private static boolean isSameStruct(CompareContext compareContext,
                                        int startI, int endI,
                                        int startJ, int endJ) throws IOException {
        HashMap<String, ArrayList<Pair<Integer,Integer>>> hashMap = new HashMap<>();
        for (int i = startI; i < endI; i++) {
            Pair<Integer, Integer> eachValue = new Pair<>(i, i);
            Event event = compareContext.getEventStreamFirst().get(i);
            String fieldName = event.getFieldName().getText();

            if (IonType.isContainer(event.getIonType())) {
                eachValue = parseContainer(i , endI, compareContext, true);
                i = eachValue.snd;
            }

            if (hashMap.get(fieldName) != null) {
                hashMap.get(fieldName).add(eachValue);
            } else {
                ArrayList<Pair<Integer, Integer>> mapList = new ArrayList<>();
                mapList.add(eachValue);
                hashMap.put(fieldName, mapList);
            }
        }

        for (int j = startJ; j < endJ; j++) {
            Pair<Integer, Integer> testEvents = new Pair<>(j,j);
            Event event = compareContext.getEventStreamSecond().get(j);
            String fieldName = event.getFieldName().getText();

            if (IonType.isContainer(event.getIonType())) {
                testEvents = parseContainer(j , endI, compareContext, false);
                j = testEvents.snd;
            }

            ArrayList<Pair<Integer, Integer>> sameFieldEvents = hashMap.get(fieldName);
            if (sameFieldEvents == null || sameFieldEvents.size() == 0) return false;
            boolean find = false;

            for (int k = 0; k < sameFieldEvents.size(); k++) {
                Pair<Integer, Integer> aEventStream = sameFieldEvents.get(k);

                if (compare(compareContext,false,
                        aEventStream.fst, aEventStream.snd, testEvents.fst, testEvents.snd)) {
                    hashMap.get(fieldName).remove(k);
                    if (hashMap.get(fieldName).size() == 0) {
                        hashMap.remove(fieldName);
                    }
                    find = true;
                    break;
                }
            }
            if (!find) {
                setReportInfo(-1, j,null, compareContext);
                return false;
            }
        }
        return hashMap.size() == 0;
    }

    private static Pair<Integer, Integer> parseContainer(int count, int end, CompareContext compareContext, boolean first) {
        int depth = 1;
        int start = count;

        while (count++ < end) {
            Event aEvent;
            if (first) aEvent = compareContext.getEventStreamFirst().get(count);
            else aEvent = compareContext.getEventStreamSecond().get(count);

            if (aEvent.getEventType() == EventType.CONTAINER_START) {
                depth++;
            } else if (aEvent.getEventType() == EventType.CONTAINER_END) {
                if (--depth == 0) break;
            }
        }
        return new Pair<>(start, count);
    }


    private static boolean isSameSymbolToken(SymbolToken symbolTokenX, SymbolToken symbolTokenY) {
        if (symbolTokenX == null && symbolTokenY == null) return true;
        else if (symbolTokenX != null && symbolTokenY != null) {
            return symbolTokenX.getText().equals(symbolTokenY.getText())
                    && symbolTokenX.getSid() == symbolTokenY.getSid();
        } else return false;
    }

    private static boolean isSameSymbolTokenArray(SymbolToken[] symbolTokenArrayX, SymbolToken[] symbolTokenArrayY) {
        if (symbolTokenArrayX == null && symbolTokenArrayY == null) return true;
        else if (symbolTokenArrayX != null && symbolTokenArrayY != null) {
            if (symbolTokenArrayX.length == symbolTokenArrayY.length) {
                for (int i = 0; i < symbolTokenArrayX.length; i++) {
                    if (!isSameSymbolToken(symbolTokenArrayX[i], symbolTokenArrayY[i])) return false;
                }
            } else  return false;
            return true;
        } else {
            return false;
        }
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
        IonType ionType = event.getIonType();
        SymbolToken[] annotations = event.getAnnotations();
        return (ionType == IonType.SEXP || ionType == IonType.LIST)
                && event.getDepth() == 0
                && annotations != null
                && annotations.length > 0
                && annotations[0].getText().equals(EMBEDDED_STREAM_ANNOTATION);
    }

    private static ArrayList<Event> getEventStream(IonReader ionReader, boolean isEventStream) throws IOException {
        ArrayList<Event> events = new ArrayList<>();

        if (isEventStream) {
            int depth = 0;
            while (ionReader.next() != null) {
                Event event = eventStreamToEvent(ionReader);
                EventType type = event.getEventType();
                if (event.getEventType() == EventType.SYMBOL_TABLE) {
                    continue;
                }
                events.add(event);

                if (type == EventType.CONTAINER_START) depth++;
                else if (type == EventType.CONTAINER_END) depth--;
            }
            if (depth != 0) throw new IonException("Invalid number of CONTAINER_START/CONTAINER_END events.");
        } else {
            events = ionStreamToEventStream(ionReader);
            events.add(new Event(EventType.STREAM_END, null, null, null,
                    null, null, 0));
        }
        return events;
    }

    private static ArrayList<Event> ionStreamToEventStream(IonReader ionReader) throws IOException {
        ArrayList<Event> events = new ArrayList<>();
        if (ionReader.getType() == null ) return events;

        do {
            if (isEmbeddedStream(ionReader)) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                events.add(ionStreamToEvent(ionReader));
                ionReader.stepIn();

                while (ionReader.next() != null) {
                    String stream = ionReader.stringValue();
                    try (IonReader tempIonReader = IonReaderBuilder.standard().build(stream)) {
                        while (tempIonReader.next() != null) {
                            ArrayList<Event> append = ionStreamToEventStream(tempIonReader);
                            events.addAll(append);
                        }
                    }
                    events.add (new Event(EventType.STREAM_END, null, null, null,
                            null, null, 0));
                }
                //write a Container_End event and step out
                events.add(new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, curDepth));
                ionReader.stepOut();
            } else if (IonType.isContainer(ionReader.getType())) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                events.add(ionStreamToEvent(ionReader));
                ionReader.stepIn();

                //recursive call
                ionReader.next();
                ArrayList<Event> append = ionStreamToEventStream(ionReader);
                events.addAll(append);
                //write a Container_End event and step out
                events.add(new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, curDepth));
                ionReader.stepOut();
            } else {
                events.add(ionStreamToEvent(ionReader));
            }
        } while (ionReader.next() != null);

        return events;
    }

    private static void setReportInfo(int i, int j, String message, CompareContext compareContext) {
        if (i != -1) compareContext.setFileEventIndex(i);
        if (j != -1) compareContext.setCompareToFileEventIndex(j);
        if (message != null) compareContext.setMessage(message);
    }

    /**
     * this function is only used for CONTAINER_START and SCALAR eventType since other types don't need initial data.
     */
    private static Event ionStreamToEvent(IonReader ionReader) throws IOException, IllegalStateException {
        if (ionReader.getType() == null) throw new IllegalStateException("Can't convert ionReader null type to Event");

        IonType ionType = ionReader.getType();
        SymbolToken fieldName = ionReader.getFieldNameSymbol();
        SymbolToken[] annotations = ionReader.getTypeAnnotationSymbols();
        int depth = ionReader.getDepth();
        ImportDescriptor[] imports = null;

        EventType eventType;
        IonValue value = null;

        if (IonType.isContainer(ionType)) {
            eventType = EventType.CONTAINER_START;
        } else {
            eventType = EventType.SCALAR;
            StringBuilder textOut = new StringBuilder();
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

        return new Event(eventType, ionType, fieldName, annotations, value, imports, depth);
    }

    private static Event eventStreamToEvent(IonReader ionReader) {
        if (ionReader.getType() != IonType.STRUCT) throw new IonException("cant convert null");
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
        if (eventType == null) throw new IllegalStateException("only can convert Struct to Event");

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
            case CONTAINER_END:
                if (depth == -1 || ionType == null) {
                    throw new IonException("Invalid CONTAINER_END: missing depth");
                } else if (textValue != null && binaryValue != null) {
                    throw new IonException("Invalid CONTAINER_END: text_value and binary_value "
                            + "are only applicable for SCALAR events");
                } else if (fieldName != null && imports != null) {
                    throw new IonException("Invalid CONTAINER_END: unnecessary fields");
                }
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
    private static BufferedOutputStream initOutputStream(CompareArgs args, String defaultValue) throws IOException {
        BufferedOutputStream outputStream = null;
        switch (defaultValue) {
            case SYSTEM_OUT_DEFAULT_VALUE:
                String outputFile = args.getOutputFile();
                if (outputFile != null && outputFile.length() != 0) {
                    File myFile = new File(outputFile);
                    FileOutputStream out = new FileOutputStream(myFile);
                    outputStream = new BufferedOutputStream(out, BUFFER_SIZE);
                } else {
                    outputStream = new BufferedOutputStream(System.out, BUFFER_SIZE); }
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

    static class CompareArgs {
        private static final String DEFAULT_FORMAT_VALUE = OutputFormat.PRETTY.toString();
        private static final String DEFAULT_COMPARISON_TYPE = ComparisonType.BASIC.toString();

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

        @Option(name = "--comparison-type",
                aliases = {"-y"},
                metaVar = "TYPE",
                usage = "Comparison semantics to be used with the compare command, from the set (basic | equivs |\n"
                        + "non-equivs | equiv-timeline). Any embedded streams in the inputs are compared for\n"
                        + "EventStream equality. 'basic' performs a standard data-model comparison between the\n"
                        + "corresponding events (or embedded streams) in the inputs. 'equivs' verifies that each \n"
                        + "value (or embedded stream) in a top-level sequence is equivalent to every other value \n"
                        + "(or embedded stream) in that sequence. 'non-equivs' does the same, but verifies that the \n"
                        + "values (or embedded streams) are not equivalent. 'equiv-timeline' is the same as 'equivs',\n"
                        + " except that when top-level sequences contain timestamp values, they are considered \n"
                        + "equivalent if they represent the same instant regardless of whether they are considered \n"
                        + "equivalent by the Ion data model.")
        private String comparisonType = DEFAULT_COMPARISON_TYPE;


        @Argument(metaVar = "FILES", required = true)
        private List<String> inputFiles;

        public ComparisonType getComparisonType() throws IllegalArgumentException {
            return ComparisonType.valueOf(comparisonType.toUpperCase());
        }

        public List<String> getInputFiles() {
            return inputFiles;
        }
        public String getOutputFile() { return outputFile; }
        public String getErrorReport() { return errorReport; }
    }
}
