package com.amazon.tools.cli;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.util.Equivalence;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import com.amazon.tools.comparisonReport.ComparisonContext;
import com.amazon.tools.comparisonReport.ComparisonResult;
import com.amazon.tools.comparisonReport.ComparisonResultType;
import com.amazon.tools.errorReport.ErrorDescription;
import com.amazon.tools.errorReport.ErrorType;
import com.amazon.tools.events.Event;
import com.amazon.tools.events.EventType;
import com.amazon.tools.events.ImportDescriptor;

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
import java.util.regex.Pattern;

public class IonJavaCli {
    private static final String VERSION = "1.0";
    private static final int CONSOLE_WIDTH = 120; // Only used for formatting the USAGE message
    private static final int USAGE_ERROR_EXIT_CODE = 1;
    private static final int IO_ERROR_EXIT_CODE = 2;
    private static final int BUFFER_SIZE = 128 * 1024;
    private static final String SYSTEM_OUT_DEFAULT_VALUE = "out";
    private static final String SYSTEM_ERR_DEFAULT_VALUE = "err";
    private static final IonTextWriterBuilder ION_TEXT_WRITER_BUILDER = IonTextWriterBuilder.standard();
    private static final IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();
    private static final String EMBEDDED_STREAM_ANNOTATION = "embedded_documents";
    private static final String EVENT_STREAM = "$ion_event_stream";
    private static final Pattern ION_VERSION_MARKER_REGEX = Pattern.compile("^\\$ion_[0-9]+_[0-9]+$");

    public static void main(final String[] args) {
        CommandArgs parsedArgs = new CommandArgs();
        CmdLineParser parser = new CmdLineParser(parsedArgs);
        parser.getProperties().withUsageWidth(CONSOLE_WIDTH);
        ComparisonType comparisonType = null;
        OutputFormat outputFormat = null;
        CommandType commandType = null;

        try {
            //parse
            parser.parseArgument(args);
            validateArgs(parsedArgs);
            //get value
            comparisonType = parsedArgs.getComparisonType();
            outputFormat = parsedArgs.getOutputFormat();
            commandType = parsedArgs.getCommand();
        } catch (CmdLineException | IllegalArgumentException e) {
            printHelpText(e.getMessage(), parser, parsedArgs);
            System.exit(USAGE_ERROR_EXIT_CODE);
        }

        if (commandType == CommandType.VERSION) {
            System.err.println(VERSION);
            System.exit(0);
        }

        ProcessContext processContext = commandType == CommandType.PROCESS ? new ProcessContext(null, -1,
                null, ErrorType.READ, null) : null;
        try (
                //Initialize output stream, never return null. (default value: STDOUT)
                OutputStream outputStream = initOutputStream(parsedArgs, SYSTEM_OUT_DEFAULT_VALUE, processContext);
                //Initialize error report, never return null. (default value: STDERR)
                OutputStream errorReportOutputStream =
                        initOutputStream(parsedArgs, SYSTEM_ERR_DEFAULT_VALUE, processContext);
                IonWriter ionWriterForErrorReport = outputFormat.createIonWriter(errorReportOutputStream);
                IonWriter ionWriterForOutput = outputFormat.createIonWriter(outputStream);
        ) {
            if (commandType == CommandType.COMPARE) {
                compareFiles(ionWriterForOutput, ionWriterForErrorReport, parsedArgs, comparisonType);
            } else if (commandType == CommandType.PROCESS) {
                throw new CmdLineException("COMPARE doesn't support option format \"-f events\"");
//                processContext.setIonWriter(ionWriterForOutput);
//                processFiles(ionWriterForErrorReport, parsedArgs, processContext);
            }
        } catch (IOException e) {
            System.err.println("Failed to close OutputStream: " + e.getMessage());
        } finally {
            if (processContext != null) {
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
    }

    private static void validateArgs(CommandArgs parsedArgs) throws CmdLineException {
        parsedArgs.setCommand();
        CommandType type = parsedArgs.getCommand();
        switch (type) {
            case PROCESS:
                break;
            case COMPARE:
                if (parsedArgs.getOutputFormat() == OutputFormat.EVENTS) {
                    throw new CmdLineException("COMPARE doesn't support option format \"-f events\"");
                }
                break;
        }
    }

    private static void printHelpText(String msg, CmdLineParser parser, CommandArgs parsedArgs) {
        CommandType commandType = parsedArgs.getCommand();

        if (commandType == null) {
            System.err.println(msg + "\n");
            System.err.println("Usage : \n" +
                    " [-h|--help] [-v|--version]\n" +
                    "  -h, --help                print the help message and exit\n" +
                    "  -v, --version             print the command's version number and exit");
        } else if (commandType == CommandType.PROCESS) {
            System.err.println(msg + "\n");
            System.err.println("\"Process\" reads the input file(s) and re-write in the format specified by " +
                    "--output.\n");
            System.err.println("Usage:\n");
            System.err.println("ion process [--output <file>] [--error-report <file>] [--output-format \n"
                    + "(text | pretty | binary | events | none)] [--catalog <file>]... [--imports <file>]... \n"
                    + "[--perf-report <file>] [--filter <filter> | --traverse <file>]  [-] [<input_file>]...\n");
            System.err.println("Options:\n");
            parser.printUsage(System.err);
        } else if (commandType == CommandType.COMPARE) {
            System.err.println(msg + "\n");
            System.err.println("\"Compare\" Compare all inputs (which may contain Ion streams and/or EventStreams) \n"
                    + "against all other inputs using the Ion data model's definition of equality. Write a \n"
                    + "ComparisonReport to the output.\n");
            System.err.println("Usage:\n");
            System.err.println("ion compare [--output <file>] [--error-report <file>] [--output-format (text | \n"
                    + "pretty | binary | none)]  [--catalog <file>]... [--comparison-type (basic | equivs | \n"
                    + "non-equivs | equiv-timeline)] [-] [<input_file>]...\n");
            parser.printUsage(System.err);
        }
    }

    //
    //
    // functions for processing
    //
    //

    private static void processFiles(IonWriter ionWriterForErrorReport,
                                     CommandArgs args,
                                     ProcessContext processContext) throws IOException {
        boolean finish = false;
        for (String path : args.getInputFiles()) {
            try (
                    InputStream inputStream = new BufferedInputStream(new FileInputStream(path));
                    IonReader ionReader = IonReaderBuilder.standard().build(inputStream);
            ) {
                processContext.setFileName(path);
                ReadContext readContext = new ReadContext(new ArrayList<>());
                try {
                    getEventStream(ionReader, CommandType.PROCESS, readContext);
                } catch (IonException | NullPointerException e) {
                    new ErrorDescription(readContext.getState(), e.getMessage(), processContext.getFileName(),
                            processContext.getEventIndex()).writeOutput(ionWriterForErrorReport);
                    finish = true;
                } catch (Exception e) {
                    new ErrorDescription(ErrorType.STATE, e.getMessage(), processContext.getFileName(),
                            processContext.getEventIndex()).writeOutput(ionWriterForErrorReport);
                    finish = true;
                }

                processContext.setEventStream(readContext.getEventStream());
                processContext.setEventIndex(0);

                if (args.getOutputFormat() == OutputFormat.EVENTS) {
                    processContext.getIonWriter().writeSymbol(EVENT_STREAM);
                    processToEventStream(ionWriterForErrorReport, processContext);
                } else {
                    processToIonStream(processContext, args);
                }
                if (finish) System.exit(IO_ERROR_EXIT_CODE);
                processContext.getIonWriter().finish();
                ionWriterForErrorReport.finish();
            } catch (IonException | NullPointerException e) {
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

    private static void processToEventStream(IonWriter ionWriterForErrorReport,
                                             ProcessContext processContext) throws IOException {
        for (Event event : processContext.getEventStream()) {
            event.writeOutput(ionWriterForErrorReport, processContext);
        }
    }

    private static void processToIonStream(ProcessContext processContext,
                                           CommandArgs args) throws IOException {
        List<Event> events = processContext.getEventStream();
        int count = 0;
        while (count != events.size()) {
            //update eventIndex
            Event event = events.get(count);
            processContext.setEventIndex(processContext.getEventIndex() + 1);
            processContext.setLastEventType(event.getEventType());
            if (event.getEventType() == EventType.CONTAINER_START) {
                if (isEmbeddedEvent(event)) {
                    count = embeddedEventToIon(processContext, args, count, event.getIonType());
                } else {
                    IonType type = event.getIonType();
                    setFieldName(event, processContext.getIonWriter());
                    setAnnotations(event, processContext.getIonWriter());
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
            count++;
        }
    }

    private static int embeddedEventToIon(ProcessContext processContext,
                                          CommandArgs args,
                                          int count,
                                          IonType ionType) throws IOException {
        processContext.getIonWriter().addTypeAnnotation(EMBEDDED_STREAM_ANNOTATION);
        processContext.getIonWriter().stepIn(ionType);
        List<Event> events =  processContext.getEventStream();
        int depth = 1;
        boolean finish = false;
        while (++count < events.size()) {
            StringBuilder out = new StringBuilder();
            ProcessContext embeddedContext = new ProcessContext(null,0,null, null,
                    ION_TEXT_WRITER_BUILDER.withImports(_Private_Utils.systemSymtab(1)).build(out));
            embeddedContext.setEmbeddedOut(out);
            try {
                do {
                    Event event = events.get(count);
                    processContext.setEventIndex(processContext.getEventIndex() + 1);
                    processContext.setLastEventType(event.getEventType());
                    if (event.getEventType() == EventType.STREAM_END) {
                        break;
                    } else if (event.getEventType() == EventType.SCALAR) {
                        writeIonByType(event, embeddedContext.getIonWriter());
                    } else if (event.getEventType() == EventType.CONTAINER_START) {
                        depth++;
                        setFieldName(event, embeddedContext.getIonWriter());
                        setAnnotations(event, embeddedContext.getIonWriter());
                        embeddedContext.getIonWriter().stepIn(event.getIonType());
                    } else if (event.getEventType() == EventType.CONTAINER_END) {
                        depth--;
                        if (depth == 0) {
                            if (event.getIonType() == IonType.SEXP || event.getIonType() == IonType.LIST) {
                                finish = true;
                                break;
                            } else {
                                throw new IonException("invalid CONTAINER_END");
                            }
                        }
                        embeddedContext.getIonWriter().stepOut();
                    } else if (event.getEventType() == EventType.SYMBOL_TABLE) {
                        handleSymbolTableEvent(embeddedContext, event, args, true);
                    }
                } while (++count < events.size());

                if (!finish) {
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
            if (finish) { break; }
        }
        processContext.getIonWriter().stepOut();
        return count;
    }

    //
    //
    //  functions for comparing
    //
    //

    private static void compareFiles(IonWriter ionWriterForOutput,
                                     IonWriter ionWriterForErrorReport,
                                     CommandArgs args,
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
                        if (path.equals(compareToPath)) { continue; }
                    }
                    ReadContext readContextFirst = new ReadContext(new ArrayList<>());
                    ReadContext readContextSecond = new ReadContext(new ArrayList<>());
                    getEventStream(ionReaderFirst, CommandType.COMPARE, readContextFirst);
                    getEventStream(ionReaderSecond, CommandType.COMPARE, readContextSecond);

                    compareContext.setEventStreamFirst(readContextFirst.getEventStream());
                    compareContext.setEventStreamSecond(readContextSecond.getEventStream());

                    if (comparisonType != ComparisonType.BASIC) {
                        if (compareEquivs(compareContext) ^
                                (comparisonType == ComparisonType.EQUIVS
                                        || comparisonType == ComparisonType.EQUIVS_TIMELINE)) {
                            ComparisonResultType type = comparisonType == ComparisonType.NON_EQUIVS ?
                                    ComparisonResultType.EQUAL : ComparisonResultType.NOT_EQUAL;
                            writeReport(compareContext, ionWriterForOutput, type);
                        }
                    } else {
                        if (!compare(compareContext, 0, readContextFirst.getEventStream().size() - 1,
                                0, readContextSecond.getEventStream().size() - 1)) {
                            writeReport(compareContext, ionWriterForOutput, ComparisonResultType.NOT_EQUAL);
                        }
                    }
                } catch (Exception e) {
                    new ErrorDescription(ErrorType.STATE, e.getMessage(), path + ';' + compareToPath,
                            -1).writeOutput(ionWriterForErrorReport);
                    System.exit(IO_ERROR_EXIT_CODE);
                }
            }
        }
    }

    private static boolean compare(CompareContext compareContext,
                                   int startI, int endI, int startJ, int endJ) throws IOException {
        List<Event> eventsFirst = compareContext.getEventStreamFirst();
        List<Event> eventsSecond = compareContext.getEventStreamSecond();
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
                setReportInfo(i, j,"Didn't match event_type", compareContext);
                return false;
            } else if (eventFirst.getDepth() != eventSecond.getDepth()) {
                setReportInfo(i, j,"Didn't match depth", compareContext);
                return false;
            } else if (eventFirst.getIonType() != eventSecond.getIonType()) {
                setReportInfo(i, j,"Didn't match ion_type", compareContext);
                return false;
            } else if (!isSameSymbolToken(fieldNameFirst, fieldNameSecond)) {
                setReportInfo(i, j,"Didn't match field_name", compareContext);
                return false;
            } else if (!isSameSymbolTokenArray(annotationFirst, annotationSecond)) {
                setReportInfo(i, j,"Didn't match annotation", compareContext);
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
        if (i <= endI || j <= endJ) {
            setReportInfo(i , j, "two event streams have different size", compareContext);
            return false;
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
                                parseSequence(containerContext, compareContext, end, isFirst, ION_SYSTEM.newEmptyList()));
                        break;
                    case STRUCT:
                        ionStruct.add(event.getFieldName(),
                                parseStruct(containerContext, compareContext, end, isFirst));
                        break;
                    case SEXP:
                        ionStruct.add(event.getFieldName(),
                                parseSequence(containerContext, compareContext, end, isFirst, ION_SYSTEM.newEmptySexp()));
                        break;
                }
            } else if (eventType == EventType.CONTAINER_END) {
                break;
            } else if (eventType == EventType.STREAM_END) {
                throw new IonException("Invalid struct: eventStream ends without CONTAINER_END");
            } else {
                IonValue cloneValue = event.getValue().clone();
                cloneValue.setTypeAnnotationSymbols(event.getAnnotations());
                ionStruct.add(event.getFieldName(), cloneValue);
            }
        }
        return ionStruct;
    }

    private static IonSequence parseSequence(ContainerContext containerContext,
                                             CompareContext compareContext,
                                             int end,
                                             boolean isFirst,
                                             IonSequence ionSequence) {
        while (containerContext.increaseIndex() < end) {
            Event event = isFirst ?
                    compareContext.getEventStreamFirst().get(containerContext.getIndex()) :
                    compareContext.getEventStreamSecond().get(containerContext.getIndex());
            EventType eventType = event.getEventType();
            if (eventType == EventType.CONTAINER_START) {
                switch (event.getIonType()) {
                    case LIST:
                        ionSequence.add(parseSequence(containerContext, compareContext, end, isFirst, ION_SYSTEM.newEmptyList()));
                        break;
                    case SEXP:
                        ionSequence.add(parseSequence(containerContext, compareContext, end, isFirst, ION_SYSTEM.newEmptySexp()));
                        break;
                    case STRUCT:
                        ionSequence.add(parseStruct(containerContext, compareContext, end, isFirst));
                        break;
                }
            } else if (eventType == EventType.CONTAINER_END) {
                break;
            } else if (eventType == EventType.STREAM_END) {
                throw new IonException("Invalid ionSequence: eventStream ends without CONTAINER_END");
            } else {
                IonValue cloneValue = event.getValue().clone();
                cloneValue.setTypeAnnotationSymbols(event.getAnnotations());
                ionSequence.add(cloneValue);
            }
        }
        return ionSequence;
    }

    private static boolean compareEquivs(CompareContext compareContext) throws IOException {
        int i = 0;
        int j = 0;
        List<Event> eventStreamFirst = compareContext.getEventStreamFirst();
        List<Event> eventStreamSecond = compareContext.getEventStreamSecond();
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

            List<Pair> pairsFirst;
            List<Pair> pairsSecond;

            if (isEmbeddedEvent(eventFirst) && isEmbeddedEvent(eventSecond)) {
                pairsFirst = locateEmbeddedStreamBoundaries(eventStreamFirst, i);
                pairsSecond = locateEmbeddedStreamBoundaries(eventStreamSecond, j);
            } else {
                pairsFirst = locateContainerBoundaries(eventStreamFirst, i);
                pairsSecond = locateContainerBoundaries(eventStreamSecond, j);
            }
            i = pairsFirst.size() == 0 ? i + 1 : pairsFirst.get(pairsFirst.size() - 1).right + 1;
            j = pairsSecond.size() == 0 ? j + 1 : pairsSecond.get(pairsSecond.size() - 1).right + 1;

            for (int m = 0; m < pairsFirst.size(); m++) {
                for (int n = 0; n < pairsSecond.size(); n++) {
                    if (compareContext.getType() == ComparisonType.NON_EQUIVS) {
                        if (m == n) continue;
                    }
                    Pair pairFirst = pairsFirst.get(m);
                    Pair pairSecond = pairsSecond.get(n);
                    if (compare(compareContext, pairFirst.left, pairFirst.right, pairSecond.left, pairSecond.right) ^
                            (type == ComparisonType.EQUIVS || type == ComparisonType.EQUIVS_TIMELINE)) {
                        if (type == ComparisonType.NON_EQUIVS) {
                            setReportInfo(pairFirst.left, pairSecond.left,
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
     *  This function parses an embedded stream into pairs<int,int> that stores index range for
     *  each string the embedded stream includes.
     *
     *  Input value 'start' must be index of a CONTAINER_START event.
     *
     *  return List is never null. It will return a list with size zero if the embedded stream is empty.
     */
    private static List<Pair> locateEmbeddedStreamBoundaries(List<Event> events, int start) {
        List<Pair> parsedEvents = new ArrayList<>(0);
        int left = start + 1;
        int right = start;
        int depth = 1;
        while (++right < events.size()) {
            EventType eventType = events.get(right).getEventType();

            if (eventType == EventType.STREAM_END) {
                parsedEvents.add(new Pair(left, right));
                left = right + 1;
            } else if (eventType == EventType.CONTAINER_END) {
                if (--depth == 0) break;
            } else if (eventType == EventType.CONTAINER_START) {
                depth++;
            }
        }
        return parsedEvents;
    }

    /**
     *  This function parses a container's values into pairs<int,int> that stores its index range.
     *  1. For SCALAR event with index i, the pair will be (i, i).
     *  2. For CONTAINER event, the pair will be (start, end) where start is the index of the CONTAINER_START event
     *  and end is the index of the CONTAINER_END event.
     *
     *  Input value 'start' must be index of a CONTAINER_START event.
     *
     *  return List is never null. It will return a list with size zero if the container is empty.
     */
    private static List<Pair> locateContainerBoundaries(List<Event> events, int start) {
        List<Pair> parsedEvents = new ArrayList<>(0);
        while (++start < events.size()) {
            EventType eventType = events.get(start).getEventType();

            if (eventType == EventType.STREAM_END) {
                throw new IonException("Invalid container: end without CONTAINER_END event");
            } else if (eventType == EventType.CONTAINER_END) {
                break;
            } else if (eventType == EventType.SCALAR) {
                parsedEvents.add(new Pair(start, start));
            } else if (eventType == EventType.CONTAINER_START) {
                int startCount = start;
                int endCount = findNextContainer(events, start);
                start = endCount;
                parsedEvents.add(new Pair(startCount, endCount));
            }
        }
        return parsedEvents;
    }

    /**
     *  This function will move the count i from the CONTAINER_START event to its corresponding CONTAINER_END event.
     *
     *  return the index of its corresponding CONTAINER_END event.
     *
     *  throw IonException if CONTAINER_END event doesn't exist.
     */
    private static int findNextContainer(List<Event> eventStream, int i) {
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

    //
    //
    // helper functions
    //
    //

    private static void handleSymbolTableEvent(ProcessContext processContext,
                                               Event event,
                                               CommandArgs args,
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
            OutputStream out = args.getOutputFile() == null ?
                    new NoCloseOutputStream(System.out) : new FileOutputStream(processContext.getFile(), true);
            processContext.setIonWriter(args.getOutputFormat().createIonWriterWithImports(out, symbolTables));
        } else {
            processContext.getEmbeddedOut().append(" ");
            processContext.setIonWriter(ION_TEXT_WRITER_BUILDER.withImports(symbolTables)
                    .build(processContext.getEmbeddedOut()));
        }
    }

    private static void writeIonByType(Event event, IonWriter ionWriter) {
        setFieldName(event, ionWriter);
        IonValue value = event.getValue();
        value.setTypeAnnotationSymbols(event.getAnnotations());
        value.writeTo(ionWriter);
    }

    private static void setFieldName(Event event, IonWriter ionWriter) {
        SymbolToken field = event.getFieldName();
        if (field != null) {
            if (!ionWriter.isInStruct()) throw new IonException("invalid field_name inside STRUCT");
            ionWriter.setFieldNameSymbol(field);
        } else if (ionWriter.isInStruct()) {
            SymbolToken symbolToken = _Private_Utils.newSymbolToken((String) null, 0);
            ionWriter.setFieldNameSymbol(symbolToken);
        }
    }

    private static void setAnnotations(Event event, IonWriter ionWriter) {
        SymbolToken[] annotations = event.getAnnotations();
        if (annotations != null && annotations.length != 0) {
            ionWriter.setTypeAnnotationSymbols(annotations);
        }
    }

    private static boolean isSameSymbolToken(SymbolToken symbolTokenX, SymbolToken symbolTokenY) {
        if (symbolTokenX == null && symbolTokenY == null) {
            return true;
        } else if (symbolTokenX != null && symbolTokenY != null) {
            if (symbolTokenX.getText() != null && symbolTokenY.getText() != null) {
                return symbolTokenX.getText().equals(symbolTokenY.getText());
            } else {
                return symbolTokenX.getSid() == symbolTokenY.getSid();
            }
        } else return false;
    }

    private static boolean isSameSymbolTokenArray(SymbolToken[] symbolTokenArrayX, SymbolToken[] symbolTokenArrayY) {
        if ((symbolTokenArrayX == null || symbolTokenArrayX.length == 0)
                && (symbolTokenArrayY == null || symbolTokenArrayY.length == 0)) {
            return true;
        } else if (symbolTokenArrayX != null && symbolTokenArrayY != null) {
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

    public static boolean isEventStream(IonReader ionReader) {
        return ionReader.next() != null
                && !ionReader.isNullValue()
                && ionReader.getType() == IonType.SYMBOL
                && EVENT_STREAM.equals(ionReader.symbolValue().getText());
    }

    private static void setReportInfo(int i, int j, String message, CompareContext compareContext) {
        if (i != -1) compareContext.setFileEventIndex(i);
        if (j != -1) compareContext.setCompareToFileEventIndex(j);
        if (message != null) compareContext.setMessage(message);
    }

    private static void writeReport(CompareContext compareContext,
                                    IonWriter ionWriter,
                                    ComparisonResultType type) throws IOException {
        new ComparisonResult(type,
                new ComparisonContext(compareContext.getFile(),
                        (compareContext.getFileEventIndex() >= compareContext.getEventStreamFirst().size()
                                || compareContext.getFileEventIndex() < 0) ? null :
                                compareContext.getEventStreamFirst().get(compareContext.getFileEventIndex()),
                        compareContext.getFileEventIndex()),
                new ComparisonContext(compareContext.getCompareToFile(),
                        (compareContext.getCompareToFileEventIndex() >= compareContext.getEventStreamSecond().size()
                                || compareContext.getCompareToFileEventIndex() < 0) ? null :
                                compareContext.getEventStreamSecond().get(compareContext.getCompareToFileEventIndex()),
                        compareContext.getCompareToFileEventIndex()),
                compareContext.getMessage()).writeOutput(ionWriter);
    }

    private static void getEventStream(IonReader ionReader,
                                       CommandType commandType,
                                       ReadContext readContext) throws IOException {
        SymbolTable curTable = ionReader.getSymbolTable();
        boolean isEventStream = isEventStream(ionReader);

        if (isEventStream) {
            readContext.setState(ErrorType.WRITE);
            while (ionReader.next() != null) {
                Event event = eventStreamToEvent(ionReader);
                if (event.getEventType() == EventType.SYMBOL_TABLE && commandType == CommandType.COMPARE) {
                    continue;
                }
                readContext.getEventStream().add(event);
            }
        } else {
            readContext.setState(ErrorType.READ);
            ionStreamToEventStream(ionReader, commandType, curTable, readContext);
            readContext.getEventStream().add(new Event(EventType.STREAM_END, null, null, null,
                    null, null, 0));
        }

        if (commandType == CommandType.PROCESS) {
            validateEventStream(readContext.getEventStream());
        }
        return;
    }

    private static void validateEventStream(List<Event> events) {
        if (events.size() == 0) return;

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

    private static void ionStreamToEventStream(IonReader ionReader,
                                               CommandType commandType,
                                               SymbolTable curTable,
                                               ReadContext readContext) throws IOException {
        if (ionReader.getType() == null) return;
        do {
            if (ionReader.isNullValue()) {
                IonValue value = ION_SYSTEM.newValue(ionReader);
                value.clearTypeAnnotations();
                readContext.getEventStream().add(new Event(EventType.SCALAR, ionReader.getType(), ionReader.getFieldNameSymbol(),
                        ionReader.getTypeAnnotationSymbols(), value, null, ionReader.getDepth()));
                continue;
            }

            if (!isSameSymbolTable(ionReader.getSymbolTable(), curTable)) {
                curTable = ionReader.getSymbolTable();
                ImportDescriptor[] imports = symbolTableToImports(curTable.getImportedTables());

                if (commandType != CommandType.COMPARE) {
                    readContext.getEventStream().add(new Event(EventType.SYMBOL_TABLE, null, null, null,
                            null, imports, 0));
                }
            }
            if (isEmbeddedStream(ionReader)) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                readContext.getEventStream().add(ionStreamToEvent(ionReader));
                ionReader.stepIn();

                while (ionReader.next() != null) {
                    if (ionReader.getType() != IonType.STRING) {
                        throw new IonException("Elements of embedded streams sets must be strings.");
                    }
                    String stream = ionReader.stringValue();
                    try (IonReader tempIonReader = IonReaderBuilder.standard().build(stream)) {
                        SymbolTable symbolTable = tempIonReader.getSymbolTable();
                        while (tempIonReader.next() != null) {
                            ionStreamToEventStream(tempIonReader, commandType, symbolTable, readContext);
                        }
                    }
                    readContext.getEventStream().add (new Event(EventType.STREAM_END, null, null, null,
                            null, null, 0));
                }
                //write a Container_End event and step out
                readContext.getEventStream().add(new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, curDepth));
                ionReader.stepOut();
            } else if (IonType.isContainer(ionReader.getType())) {
                //get current Ion type and depth
                IonType curType = ionReader.getType();
                int curDepth = ionReader.getDepth();

                //write a Container_Start event and step in
                readContext.getEventStream().add(ionStreamToEvent(ionReader));
                ionReader.stepIn();

                //recursive call
                ionReader.next();
                ionStreamToEventStream(ionReader, commandType, curTable, readContext);
                //write a Container_End event and step out
                readContext.getEventStream().add(new Event(EventType.CONTAINER_END, curType, null, null,
                        null, null, curDepth));
                ionReader.stepOut();
            } else {
                readContext.getEventStream().add(ionStreamToEvent(ionReader));
            }
        } while (ionReader.next() != null);;
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

    private static boolean isSameSymbolTable(SymbolTable newTable, SymbolTable curTable) {
        if (newTable == null && curTable == null) { return true; }
        else if (newTable != null && curTable == null) { return false; }
        else if (newTable == null) { return false; }

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

    private static boolean isIonVersionMarker(String text) {
        return text != null && ION_VERSION_MARKER_REGEX.matcher(text).matches();
    }

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
            if (isIonVersionMarker(value.toString())) {
                value.setTypeAnnotationSymbols(_Private_Utils.newSymbolToken("$ion_user_value", 0));
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
                        String text = null;
                        int sid = 0;
                        while (ionReader.next() != null) {
                            switch (ionReader.getFieldName()) {
                                case "text":
                                    text = ionReader.isNullValue() ? null : ionReader.stringValue();
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
                    annotations = annotationsList.toArray(SymbolToken.EMPTY_ARRAY);
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
    private static BufferedOutputStream initOutputStream(CommandArgs args,
                                                         String defaultValue,
                                                         ProcessContext processContext) throws IOException {
        BufferedOutputStream outputStream = null;
        switch (defaultValue) {
            case SYSTEM_OUT_DEFAULT_VALUE:
                String outputFile = args.getOutputFile();
                if (outputFile != null && outputFile.length() != 0) {
                    File myFile = new File(outputFile);
                    if (args.getCommand() == CommandType.PROCESS && processContext != null) {
                        processContext.setFile(myFile);
                    }
                    FileOutputStream out = new FileOutputStream(myFile);
                    outputStream = new BufferedOutputStream(out, BUFFER_SIZE);
                } else {
                    outputStream = new BufferedOutputStream(new NoCloseOutputStream(System.out), BUFFER_SIZE);
                }
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

    static class Pair {
        final int left;
        final int right;

        Pair(int left, int right) {
            this.left = left;
            this.right = right;
        }
    }

    static class ReadContext {
        final private List<Event> eventStream;
        private ErrorType state;

        public ReadContext(List<Event> eventStream) {
            this.eventStream = eventStream;
            this.state = ErrorType.READ;
        }

        public List<Event> getEventStream() {
            return eventStream;
        }

        public ErrorType getState() {
            return state;
        }

        public void setState(ErrorType state) {
            this.state = state;
        }
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

    static class CommandArgs {
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

        @Argument(required = true)
        private List<String> inputs;

        private CommandType commandType = null;

        public String getOutputFormatName() {
            return outputFormatName;
        }

        public ComparisonType getComparisonType() throws IllegalArgumentException {
            return ComparisonType.valueOf(comparisonType.replace('-','_').toUpperCase());
        }

        public CommandType getCommand() {
            return this.commandType;
        }

        public void setCommand() {
            this.commandType = CommandType.valueOf(inputs.get(0).toUpperCase());
        }

        public OutputFormat getOutputFormat() throws IllegalArgumentException {
            return OutputFormat.valueOf(outputFormatName.toUpperCase());
        }

        public List<String> getInputFiles() {
            int length = this.inputs.size();
            return this.inputs.subList(1, length);
        }
        public String getOutputFile() { return outputFile; }
        public String getErrorReport() { return errorReport; }
    }
}
