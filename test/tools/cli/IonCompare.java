package tools.cli;

import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
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
import java.util.List;

public class IonCompare {
    private static final int CONSOLE_WIDTH = 120; // Only used for formatting the USAGE message
    private static final int USAGE_ERROR_EXIT_CODE = 1;
    private static final int BUFFER_SIZE = 128 * 1024;
    private static final String SYSTEM_OUT_DEFAULT_VALUE = "out";
    private static final String SYSTEM_ERR_DEFAULT_VALUE = "err";
    private static final IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();
    private static final String EMBEDDED_STREAM_ANNOTATION = "embedded_documents";
    private static final String EVENT_STREAM = "$ion_event_stream";

    public static void main(final String[] args) {
        CompareArgs parsedArgs = new CompareArgs();
        CmdLineParser parser = new CmdLineParser(parsedArgs);
        parser.getProperties().withUsageWidth(CONSOLE_WIDTH);
        ComparisonType comparisonType = parsedArgs.getComparisonType();
        OutputFormat outputFormat = null;

        try {
            parser.parseArgument(args);
            comparisonType = parsedArgs.getComparisonType();
            outputFormat = parsedArgs.getOutputFormat();
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
             IonWriter ionWriterForOutput = outputFormat.createIonWriter(outputStream);
             IonWriter ionWriterForErrorReport = outputFormat.createIonWriter(errorReportOutputStream);
        ) {
            compareFiles(ionWriterForOutput, ionWriterForErrorReport, parsedArgs, comparisonType);
        } catch (IOException e) {
            System.err.println("Failed to close OutputStream: " + e.getMessage());
        }
    }

    private static void compareFiles(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     CompareArgs args,
                                     ComparisonType comparisonType) throws IOException {
        List<String> files = args.getInputFiles();
        CompareContext compareContext = new CompareContext(null, null);
        compareContext.setType(args.getComparisonType());
        for (String path : files) {
            for (String compareToPath : files) {
                compareContext.reset(path, compareToPath);
                try (InputStream inputFirst = new BufferedInputStream(new FileInputStream(path));
                     IonReader ionReaderFirst = IonReaderBuilder.standard().build(inputFirst);
                     InputStream inputSecond = new BufferedInputStream(new FileInputStream(compareToPath));
                     IonReader ionReaderSecond = IonReaderBuilder.standard().build(inputSecond);
                ) {
                    if (comparisonType == ComparisonType.BASIC) {
                        if (path.equals(compareToPath)) continue;
                    }

                    boolean isFirstEventStream = isEventStream(ionReaderFirst);
                    boolean isSecondEventStream = isEventStream(ionReaderSecond);
                    ArrayList<Event> eventsFirst = getEventStream(ionReaderFirst, isFirstEventStream);
                    ArrayList<Event> eventsSecond = getEventStream(ionReaderSecond, isSecondEventStream);
                    compareContext.setEventStreamFirst(eventsFirst);
                    compareContext.setEventStreamSecond(eventsSecond);

                    if (comparisonType == ComparisonType.EQUIVS
                            || comparisonType == ComparisonType.EQUIVS_TIMELINE
                            || comparisonType == ComparisonType.NON_EQUIVS) {
                        if (compareEquivs(compareContext) ^
                                (comparisonType == ComparisonType.EQUIVS
                                        || comparisonType == ComparisonType.EQUIVS_TIMELINE)) {
                            ComparisonResultType type = comparisonType == ComparisonType.NON_EQUIVS ?
                                    ComparisonResultType.EQUAL : ComparisonResultType.NOT_EQUAL;
                            writeReport(compareContext, ionWriterForOutput, type);
                        }
                    } else if (comparisonType == ComparisonType.BASIC) {
                        if (!compare(compareContext, 0, eventsFirst.size() - 1,
                                0, eventsSecond.size() - 1)) {
                            writeReport(compareContext, ionWriterForOutput, ComparisonResultType.NOT_EQUAL);
                        }
                    }
                } catch (IonException | NullPointerException e) {
                    new ErrorDescription(ErrorType.STATE, e.getMessage(), path+';'+compareToPath,
                            -1).writeOutput(ionWriterForErrorReport);
                }
            }
        }
    }

    public static boolean isEventStream(IonReader ionReader) {
        return ionReader.next() != null
                && ionReader.getType() == IonType.SYMBOL
                && EVENT_STREAM.equals(ionReader.symbolValue().getText());
    }


    private static boolean compare(CompareContext compareContext,
                                   int startI, int endI, int startJ, int endJ) throws IOException {
        ArrayList<Event> eventsFirst = (ArrayList<Event>) compareContext.getEventStreamFirst();
        ArrayList<Event> eventsSecond = (ArrayList<Event>) compareContext.getEventStreamSecond();
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
                setReportInfo(i, j,"Did't match event_type", compareContext);
                return false;
            } else if (eventFirst.getDepth() != eventSecond.getDepth()) {
                setReportInfo(i, j,"Did't match depth", compareContext);
                return false;
            } else if (eventFirst.getIonType() != eventSecond.getIonType()) {
                setReportInfo(i, j,"Did't match ion_type", compareContext);
                return false;
            } else if (!isSameSymbolToken(fieldNameFirst, fieldNameSecond)) {
                setReportInfo(i, j,"Did't match field_name", compareContext);
                return false;
            } else if (!isSameSymbolTokenArray(annotationFirst, annotationSecond)) {
                setReportInfo(i, j,"Did't match annotation", compareContext);
                return false;
            }

            if (eventTypeFirst == EventType.CONTAINER_START
                    && eventFirst.getIonType() == IonType.STRUCT) {
                int iStart = i;
                int jStart = j;
                ContainerContext containerContextFirst = new ContainerContext(i);
                IonStruct structFirst = parseStruct(containerContextFirst, compareContext, endI, true);
                i = containerContextFirst.getIndex();
                ContainerContext containerContextSecond = new ContainerContext(j);
                IonStruct structSecond = parseStruct(containerContextSecond, compareContext, endJ, false);
                j = containerContextSecond.getIndex();
                if (!Equivalence.ionEquals(structFirst, structSecond)) {
                    setReportInfo(iStart, jStart,
                            "Did not find matching field for " + structFirst.toString(),
                            compareContext);
                    return false;
                }
            } else if (eventTypeFirst == EventType.SCALAR) {
                boolean compareResult;
                if (compareContext.getType() == ComparisonType.EQUIVS_TIMELINE
                        && eventFirst.getIonType() == IonType.TIMESTAMP) {
                    IonTimestamp ionTimestampFirst = (IonTimestamp) eventFirst.getValue();
                    IonTimestamp ionTimestampSecond = (IonTimestamp) eventSecond.getValue();
                    compareResult = ionTimestampFirst.timestampValue()
                            .compareTo(ionTimestampSecond.timestampValue()) == 0;
                } else {
                    compareResult = Equivalence.ionEquals(eventFirst.getValue(), eventSecond.getValue());
                }
                if (!compareResult) {
                    setReportInfo(i, j, eventFirst.getValue() + " vs. "
                            + eventSecond.getValue(), compareContext);
                    return false;
                }
            }
            i++;
            j++;
        }
        return true;
    }

    private static IonStruct parseStruct(ContainerContext containerContext,
                                         CompareContext compareContext,
                                         int end,
                                         boolean isFirst) {
        Event initEvent = isFirst ?
                compareContext.getEventStreamFirst().get(containerContext.getIndex()) :
                compareContext.getEventStreamSecond().get(containerContext.getIndex());
        if (initEvent.getEventType() != EventType.CONTAINER_START || initEvent.getIonType() != IonType.STRUCT) {
            return null;
        }

        IonStruct ionStruct = ION_SYSTEM.newEmptyStruct();
        while (containerContext.increaseIndex() < end) {
            Event event = isFirst ?
                    compareContext.getEventStreamFirst().get(containerContext.getIndex()) :
                    compareContext.getEventStreamSecond().get(containerContext.getIndex());
            EventType eventType = event.getEventType();
            if (eventType == EventType.CONTAINER_START) {
                switch (event.getIonType()) {
                    case LIST:
                        ionStruct.add(event.getFieldName(),
                                parseList(containerContext, compareContext, end, isFirst));
                        break;
                    case STRUCT:
                        ionStruct.add(event.getFieldName(),
                                parseStruct(containerContext, compareContext, end, isFirst));
                        break;
                    case SEXP:
                        ionStruct.add(event.getFieldName(),
                                parseSexp(containerContext, compareContext, end, isFirst));
                        break;
                }
            } else if (eventType == EventType.CONTAINER_END) {
                break;
            } else if (eventType == EventType.STREAM_END) {
                throw new IonException("Invalid struct: eventStream ends without CONTAINER_END");
            } else {
                IonValue value = event.getValue();
                value.removeFromContainer();
                value.setTypeAnnotationSymbols(event.getAnnotations());
                ionStruct.add(event.getFieldName(), event.getValue());
            }
        }
        return ionStruct;
    }

    private static IonList parseList(ContainerContext containerContext,
                                               CompareContext compareContext,
                                               int end,
                                               boolean isFirst) {
        IonList ionList = ION_SYSTEM.newEmptyList();
        while (containerContext.increaseIndex() < end) {
            Event event = isFirst ?
                    compareContext.getEventStreamFirst().get(containerContext.getIndex()) :
                    compareContext.getEventStreamSecond().get(containerContext.getIndex());
            EventType eventType = event.getEventType();
            if (eventType == EventType.CONTAINER_START) {
                switch (event.getIonType()) {
                    case LIST:
                        ionList.add(parseList(containerContext, compareContext, end, isFirst));
                        break;
                    case STRUCT:
                        ionList.add(parseStruct(containerContext, compareContext, end, isFirst));
                        break;
                    case SEXP:
                        ionList.add(parseSexp(containerContext, compareContext, end, isFirst));
                }
            } else if (eventType == EventType.CONTAINER_END) {
                break;
            } else if (eventType == EventType.STREAM_END) {
                throw new IonException("Invalid list: eventStream ends without CONTAINER_END");
            } else {
                IonValue value = event.getValue();
                value.removeFromContainer();
                value.setTypeAnnotationSymbols(event.getAnnotations());
                ionList.add(event.getValue());
            }
        }
        return ionList;
    }

    private static IonSexp parseSexp(ContainerContext containerContext,
                                          CompareContext compareContext,
                                          int end,
                                          boolean isFirst) {
        IonSexp ionSexp = ION_SYSTEM.newEmptySexp();
        while (containerContext.increaseIndex() < end) {
            Event event = isFirst ?
                    compareContext.getEventStreamFirst().get(containerContext.getIndex()) :
                    compareContext.getEventStreamSecond().get(containerContext.getIndex());
            EventType eventType = event.getEventType();
            if (eventType == EventType.CONTAINER_START) {
                switch (event.getIonType()) {
                    case LIST:
                        ionSexp.add(parseList(containerContext, compareContext, end, isFirst));
                        break;
                    case STRUCT:
                        ionSexp.add(parseStruct(containerContext, compareContext, end, isFirst));
                        break;
                    case SEXP:
                        ionSexp.add(parseSexp(containerContext, compareContext, end, isFirst));
                        break;
                }
            } else if (eventType == EventType.CONTAINER_END) {
                break;
            } else if (eventType == EventType.STREAM_END) {
                throw new IonException("Invalid sexp: eventStream ends without CONTAINER_END");
            } else {
                IonValue value = event.getValue();
                value.removeFromContainer();
                value.setTypeAnnotationSymbols(event.getAnnotations());
                ionSexp.add(event.getValue());
            }
        }
        return ionSexp;
    }

    private static void writeReport(CompareContext compareContext,
                                    IonWriter ionWriter,
                                    ComparisonResultType type) throws IOException {
        new ComparisonResult(type,
                new ComparisonContext(compareContext.getFile(),
                        compareContext.getEventStreamFirst().get(compareContext.getFileEventIndex()),
                        compareContext.getFileEventIndex() + 1),
                new ComparisonContext(compareContext.getCompareToFile(),
                        compareContext.getEventStreamSecond().get(compareContext.getCompareToFileEventIndex()),
                        compareContext.getCompareToFileEventIndex() + 1),
                compareContext.getMessage()).writeOutput(ionWriter);
    }

    private static boolean compareEquivs(CompareContext compareContext) throws IOException {
        int i = 0;
        int j = 0;
        ArrayList<Event> eventStreamFirst = (ArrayList<Event>) compareContext.getEventStreamFirst();
        ArrayList<Event> eventStreamSecond = (ArrayList<Event>) compareContext.getEventStreamSecond();
        ComparisonType type = compareContext.getType();

        while (i < eventStreamFirst.size() && j < eventStreamSecond.size()) {
            Event eventFirst = eventStreamFirst.get(i);
            Event eventSecond = eventStreamSecond.get(j);

            if (eventFirst.getEventType() == EventType.STREAM_END
                    && eventSecond.getEventType() == EventType.STREAM_END) {
                break;
            } else if (eventFirst.getEventType() == EventType.STREAM_END
                    || eventSecond.getEventType() == EventType.STREAM_END) {
                setReportInfo(i, j,
                        "The input streams had a different number of comparison sets.", compareContext);
                return type == ComparisonType.NON_EQUIVS;
            } else if (!(eventFirst.getIonType() == IonType.LIST || eventFirst.getIonType() == IonType.SEXP)
                    || !(eventSecond.getIonType() == IonType.LIST || eventSecond.getIonType() == IonType.SEXP)) {
                throw new IonException("Comparison sets must be lists or s-expressions.");
            } else if (isEmbeddedEvent(eventFirst) ^ isEmbeddedEvent(eventSecond)) {
                throw new IonException("Embedded streams set expected.");
            }

            ArrayList<Pair<Integer, Integer>> pairsFirst;
            ArrayList<Pair<Integer, Integer>> pairsSecond;

            if (isEmbeddedEvent(eventFirst) && isEmbeddedEvent(eventSecond)) {
                pairsFirst = parseEmbeddedStream(eventStreamFirst, i);
                pairsSecond = parseEmbeddedStream(eventStreamSecond, j);
            } else {
                pairsFirst = parseContainer(eventStreamFirst, i);
                pairsSecond = parseContainer(eventStreamSecond, j);
            }
            i = pairsFirst.size() == 0 ? i + 1 : pairsFirst.get(pairsFirst.size() - 1).snd + 1;
            j = pairsSecond.size() == 0 ? j + 1 : pairsSecond.get(pairsSecond.size() - 1).snd + 1;

            for (int m = 0; m < pairsFirst.size(); m++) {
                for (int n = 0; n < pairsSecond.size(); n++) {
                    if (m == n) continue;
                    Pair<Integer, Integer> pairFirst = pairsFirst.get(m);
                    Pair<Integer, Integer> pairSecond = pairsSecond.get(n);
                    if (compare(compareContext, pairFirst.fst, pairFirst.snd, pairSecond.fst, pairSecond.snd) ^
                            (type == ComparisonType.EQUIVS || type == ComparisonType.EQUIVS_TIMELINE)) {
                        if (type == ComparisonType.NON_EQUIVS) {
                            setReportInfo(pairFirst.fst, pairSecond.fst,
                                    "Equivalent values in a non-equivs set.", compareContext);
                        }
                        return type == ComparisonType.NON_EQUIVS;
                    }
                }
            }
            i++;
            j++;
        }
        return (type == ComparisonType.EQUIVS || type == ComparisonType.EQUIVS_TIMELINE);
    }

    /**
     *  This function will parse a embedded stream into pairs<int,int> that stores index range for
     *  each string it includes.
     *
     *  Input value 'start' must be index of a CONTAINER_START event.
     *
     *  return List is never null. It will return a list with size zero if the embedded stream is empty.
     */
    private static ArrayList<Pair<Integer, Integer>> parseEmbeddedStream(ArrayList<Event> events, int start) {
        ArrayList<Pair<Integer, Integer>> parsedEvents = new ArrayList<>(0);
        int count = start + 1;
        int depth = 1;
        while (++start < events.size()) {
            EventType eventType = events.get(start).getEventType();

            if (eventType == EventType.STREAM_END) {
                parsedEvents.add(new Pair<>(count, start));
                count = start + 1;
            } else if (eventType == EventType.CONTAINER_END) {
                if (--depth == 0) break;
            } else if (eventType == EventType.CONTAINER_START) {
                depth++;
            }
        }
        return parsedEvents;
    }

    /**
     *  This function will parse a container's values into pairs<int,int> that stores its index range.
     *  1. For SCALAR event with index i, the pair will be (i, i)
     *  2. For CONTAINER event, the pair will be (start, end) where start is the index of the CONTAINER_START event
     *  and end is the index of the CONTAINER_END event.
     *
     *  Input value 'start' must be index of a CONTAINER_START event.
     *
     *  return List is never null. It will return a list with size zero if the container is empty.
     */
    private static ArrayList<Pair<Integer, Integer>> parseContainer(ArrayList<Event> events, int start) {
        ArrayList<Pair<Integer, Integer>> parsedEvents = new ArrayList<>(0);
        while (++start < events.size()) {
            EventType eventType = events.get(start).getEventType();

            if (eventType == EventType.STREAM_END) {
                throw new IonException("Invalid container: end without CONTAINER_END event");
            } else if (eventType == EventType.CONTAINER_END) {
                break;
            } else if (eventType == EventType.SCALAR) {
                parsedEvents.add(new Pair<>(start, start));
            } else if (eventType == EventType.CONTAINER_START) {
                int startCount = start;
                int endCount = findNextContainer(events, start);
                start = endCount;
                parsedEvents.add(new Pair<>(startCount, endCount));
            }
        }
        return parsedEvents;
    }

    /**
     *  This function will move the count i from the CONTAINER_START event to its corresponding CONTAINER_END event
     *
     *  return int is the index of its corresponding CONTAINER_END event.
     */
    private static int findNextContainer(ArrayList<Event> eventStream, int i) {
        if (!IonType.isContainer(eventStream.get(i).getIonType())) return i;
        int depth = 1;
        while (i++ < eventStream.size()) {
            Event event = eventStream.get(i);
            if (event.getEventType() == EventType.CONTAINER_START) {
                depth++;
            } else if (event.getEventType() == EventType.CONTAINER_END) {
                if (--depth == 0) break;
            }
        }
        if (i == eventStream.size()) {
            throw new IonException("Invalid container, reach the end of the eventStream without CONTAINER_END event");
        }
        return i;
    }

    private static boolean isSameSymbolToken(SymbolToken symbolTokenX, SymbolToken symbolTokenY) {
        if (symbolTokenX == null && symbolTokenY == null) return true;
        else if (symbolTokenX != null && symbolTokenY != null) {
            if (symbolTokenX.getText() != null && symbolTokenY.getText() != null) {
                return symbolTokenX.getText().equals(symbolTokenY.getText());
            } else {
                return symbolTokenX.getSid() == symbolTokenY.getSid();
            }
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
            while (ionReader.next() != null) {
                Event event = eventStreamToEvent(ionReader);
                if (event.getEventType() == EventType.SYMBOL_TABLE) {
                    continue;
                }
                events.add(event);
            }
        } else {
            events = ionStreamToEventStream(ionReader);
            events.add(new Event(EventType.STREAM_END, null, null, null,
                    null, null, 0));
        }
        validateEventStream(events);
        return events;
    }

    private static void validateEventStream(ArrayList<Event> events) {
        if (events.get(events.size() - 1).getEventType() != EventType.STREAM_END) {
            throw new IonException("Invalid event stream: event stream end without STREAM_END event");
        }

        int depth = 0;
        int i = -1;
        while (++i < events.size()) {
            Event event = events.get(i);
            EventType eventType = event.getEventType();

            if (eventType == EventType.CONTAINER_START) {
                depth++;
            } else if (eventType == EventType.CONTAINER_END) {
                if (--depth < 0) {
                    throw new IonException("Invalid event stream: Invalid CONTAINER_END event");
                }
            }

            if (isEmbeddedEvent(event)) {
                while (i < events.size()) {
                    if (events.get(++i).getEventType() == EventType.CONTAINER_END) {
                        depth--;
                        break;
                    }
                    while (i < events.size()) {
                        Event aEvent = events.get(i);
                        EventType aEventType = aEvent.getEventType();
                        if (aEventType == EventType.CONTAINER_START) {
                            depth++;
                        } else if (aEventType == EventType.CONTAINER_END) {
                           if (--depth == 0) {
                               throw new IonException("Invalid EventStream: end without STREAM_END event");
                           }
                        } else if (aEventType == EventType.STREAM_END) {
                            if (depth != 1) {
                                throw new IonException("Invalid EventStream: " +
                                        "CONTAINER_START and CONTAINER_END not match");
                            }
                            break;
                        }
                        i++;
                    }
                    if (i == events.size()) {
                        throw new IonException("Invalid EventStream: end without CONTAINER_END event");
                    }
                }
            }
        }
        if (depth != 0) {
            throw new IonException("Invalid event stream: CONTAINER_START and CONTAINER_END events doesn't match");
        }
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
                    if (ionReader.getType() != IonType.STRING) {
                        throw new IonException("Elements of embedded streams sets must be strings.");
                    }
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
    private static Event ionStreamToEvent(IonReader ionReader) throws IllegalStateException {
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
            value = ION_SYSTEM.newValue(ionReader);
            value.clearTypeAnnotations();
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
        ImportDescriptor[] importsArray = imports.toArray(new ImportDescriptor[0]);

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

    static class ContainerContext {
        int index;

        ContainerContext(int index) {
            this.index = index;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public int increaseIndex() {
            this.index = this.index + 1;
            return this.index;
        }
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
            return ComparisonType.valueOf(comparisonType.replace('-','_').toUpperCase());
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
