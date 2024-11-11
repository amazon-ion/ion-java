// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.BufferedOutputStreamFastAppendable;
import com.amazon.ion.impl.IonRawTextWriter_1_1;
import com.amazon.ion.impl.IonRawWriter_1_1;
import com.amazon.ion.impl._Private_IonTextAppender;
import com.amazon.ion.impl._Private_IonTextWriterBuilder_1_1;
import com.amazon.ion.impl.bin.BlockAllocatorProviders;
import com.amazon.ion.impl.bin.IonRawBinaryWriter_1_1;
import com.amazon.ion.impl.bin.WriteBuffer;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Re-writes a stream of Ion data to the Ion 1.1 equivalent, leveraging Ion 1.1 macros.
 */
public class Macroize {

    private static final IonSystem SYSTEM = IonSystemBuilder.standard().build();

    public static void main(String[] args) throws IOException {
        // TODO replace argument handling with a library like pico CLI
        String specFile = null;
        boolean outputBinary = false;
        int i;
        for (i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--spec":
                    specFile = args[++i];
                    break;
                case "--format":
                    switch(args[++i]) {
                        case "binary":
                            outputBinary = true;
                            break;
                        case "text":
                            outputBinary = false;
                            break;
                        default:
                            throw new IllegalArgumentException("Unrecognized format: " + args[i]);
                    }
                    break;
                case "--help":
                case "-h":
                    System.out.println("IonJava Macroize Tool v0.1");
                    System.out.println("Usage:\n--spec <spec_file> [--format <text|binary>] <input_file>");
                    System.exit(0);
                    break;
                default:
                    if (i == args.length - 1) {
                        // This is the final argument; it must be the input file name.
                        break;
                    }
                    throw new IllegalArgumentException("Unrecognized option: " + args[i]);
            }
        }
        if (specFile == null) {
            throw new IllegalArgumentException("Expected a spec file to be provided via the --spec option.");
        }

        String inputFileWithSuffix = args[args.length - 1];
        Path inputPath = checkPath(inputFileWithSuffix);
        String outputFileSuffix = outputBinary ? ".10n" : ".ion";
        String inputName = inputPath.toFile().getName();
        int dotIndex = inputName.lastIndexOf('.');
        String inputNameWithoutSuffix = (dotIndex < 0) ? inputName : inputName.substring(0, dotIndex);
        Path specPath = checkPath(specFile);

        Path invocationsPath = Files.createTempFile(inputNameWithoutSuffix + "-invocations", ".ion");
        invocationsPath.toFile().deleteOnExit();
        Path headlessPath = Files.createTempFile(inputNameWithoutSuffix + "-headless-1-1", outputFileSuffix);
        headlessPath.toFile().deleteOnExit();
        Path parentDirectory = inputPath.toAbsolutePath().getParent();
        if (parentDirectory == null) {
            throw new IllegalArgumentException("Invalid input path: " + inputPath);
        }
        Path convertedPath = parentDirectory.resolve(inputNameWithoutSuffix + "-1-1" + outputFileSuffix);

        macroize(
            () -> IonReaderBuilder.standard().build(Files.newInputStream(inputPath)),
            () -> IonTextWriterBuilder.standard().build(Files.newOutputStream(invocationsPath)),
            () -> IonReaderBuilder.standard().build(Files.newInputStream(invocationsPath)),
            () -> Files.newOutputStream(headlessPath),
            () -> Files.newOutputStream(convertedPath),
            () -> appendCopy(headlessPath, convertedPath),
            () -> IonReaderBuilder.standard().build(Files.newInputStream(specPath)),
            outputBinary,
            System.out
        );
        System.out.println("Ion 1.1 file written to: " + convertedPath.toAbsolutePath());
    }

    /**
     * Re-writes a stream of Ion data to the Ion 1.1 equivalent, leveraging Ion 1.1 macros.
     * @param inputReaderSupplier supplies an IonReader over the input data.
     * @param invocationsWriterSupplier supplies an IonWriter to write a description of where macro invocations should be substituted into the stream.
     * @param invocationsReaderSupplier supplies an IonReader over the macro invocation description stream.
     * @param headlessOutputSupplier supplies an OutputStream to which the body of the converted stream will be written (i.e., without a preceding encoding context).
     * @param fullOutputSupplier supplies an OutputStream to which the entire converted stream (including encoding context) will be written.
     * @param assembleFullOutput the procedure for appending the headless stream to the end of the stream containing the encoding context, creating the full output.
     * @param specReaderSupplier supplies an IonReader over the spec file that informs the conversion.
     * @param outputBinary true if the stream will be converted to binary Ion 1.1; false if it will be converted to text Ion 1.1.
     * @param log an appendable log of any messages produced during the conversion, such as statistics and status.
     * @throws IOException if thrown during the conversion.
     */
    static void macroize(
        ThrowingSupplier<IonReader> inputReaderSupplier,
        ThrowingSupplier<IonWriter> invocationsWriterSupplier,
        ThrowingSupplier<IonReader> invocationsReaderSupplier,
        ThrowingSupplier<OutputStream> headlessOutputSupplier,
        ThrowingSupplier<OutputStream> fullOutputSupplier,
        ThrowingProcedure assembleFullOutput,
        ThrowingSupplier<IonReader> specReaderSupplier,
        boolean outputBinary,
        Appendable log
    ) throws IOException {
        // Read the input data into memory.
        IonDatagram source;
        try (IonReader reader = inputReaderSupplier.get()) {
            source = SYSTEM.getLoader().load(reader);
        }

        // Prepare the context and the spec to be used during the conversion.
        ManualEncodingContext context = new ManualEncodingContext();
        MacroizeSpec spec = new MacroizeSpec();
        try (IonReader reader = specReaderSupplier.get()) {
            spec.readSpec(reader, context);
        }

        // Using the spec, produce a marked up text Ion 1.0 representation of the input that
        // indicates which structs should be replaced with macro invocations.
        try (IonWriter writer = invocationsWriterSupplier.get()) {
            writeMacroMatchesUsingMarkedUpIon10(writer, source, spec, log);
        }

        // Go through the marked up invocations and re-write to Ion 1.1, intercepting the special marked up
        // Ion 1.0 values and replacing them with proper Ion 1.1 e-expressions.
        log.append("\n\nConverting to 1.1\n");
        IonRawWriter_1_1 writer = newRawWriter_1_1(headlessOutputSupplier.get(), outputBinary);
        try (IonReader reader = invocationsReaderSupplier.get()) {
            while (reader.next() != null) {
                replaceMatchesWithInvocations(reader, writer, context, outputBinary, spec.textPatterns);
            }
        } finally {
            writer.close();
        }

        // Write the symbol and macro tables
        IonRawWriter_1_1 symbolTableWriter = newRawWriter_1_1(fullOutputSupplier.get(), outputBinary);
        try {
            symbolTableWriter.writeIVM();
            context.writeTo(symbolTableWriter);
        } finally {
            symbolTableWriter.close();
        }
        // Now, copy the headless Ion 1.1 data to the end.
        assembleFullOutput.execute();
        log.append("\nDone.\n");
    }

    /**
     * Substitute value literals that match any of the specified macros with invocation instructions, represented using
     * annotated Ion 1.0 s-expressions of the form `$ion_invocation::(name_of_macro arguments...)`. This intermediate
     * form is used to make it possible to mutate the existing IonValue structure, which does not support modeling
     * macro invocations. If this is supported in the future, this can likely be simplified.
     * @param writer the writer.
     * @param source the source data.
     * @param spec the spec containing the macros to match.
     * @param log an appendable log.
     * @throws IOException if thrown during writing.
     */
    private static void writeMacroMatchesUsingMarkedUpIon10(
        IonWriter writer,
        IonDatagram source,
        MacroizeSpec spec,
        Appendable log
    ) throws IOException {
        Map<String, SuggestedSignature> suggestedSignatures = spec.matchMacros(source, log);
        for (int topLevelValueIndex = 0; topLevelValueIndex < source.size(); topLevelValueIndex++) {
            IonValue topLevelValue = source.get(topLevelValueIndex);
            if (!IonType.isContainer(topLevelValue.getType())) {
                topLevelValue.writeTo(writer);
                continue;
            }
            // key: depth, value: invocations at that depth
            Map<Integer, List<InvocationSubstitute>> invocationSubstitutes = new HashMap<>();
            findMatch(topLevelValue, source, topLevelValueIndex, spec.customMatchers, suggestedSignatures, invocationSubstitutes, 0);
            matchMacrosRecursive((IonContainer) topLevelValue, spec.customMatchers, suggestedSignatures, invocationSubstitutes, 1);
            // Iterate over all invocation matches, sorted by depth from shallowest to deepest.
            for (
                Map.Entry<Integer, List<InvocationSubstitute>> substitutesByDepth
                    : invocationSubstitutes.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList())
            ) {
                int depth = substitutesByDepth.getKey();
                for (InvocationSubstitute substitute : substitutesByDepth.getValue()) {
                    substitute.substitute(invocationSubstitutes.get(depth + 1));
                }
                // 'topLevelValue' has been replaced with an invocation; update it with the replacement before writing.
                if (depth == 0) {
                    topLevelValue = source.get(topLevelValueIndex);
                }
            }
            topLevelValue.writeTo(writer);
        }
    }

    /**
     * Attempts to match the given value with any of the given macro matchers.
     * @param value the value to attempt to match.
     * @param parent the value's parent container (which may be an IonDatagram if 'value' is at the top level).
     * @param containerIndex the index of 'value' within 'parent'.
     * @param customMacroMatchers the macro matchers to evaluate.
     * @param suggestedSignatures the macro signatures available.
     * @param substituteInvocations receives the invocation substitutes identified for this value, organized by depth.
     * @param depth the depth at which the given container resides.
     * @return true if a match was found.
     */
    private static boolean findMatch(
        IonValue value,
        IonContainer parent,
        int containerIndex,
        List<MacroizeMacroMatcher> customMacroMatchers,
        Map<String, SuggestedSignature> suggestedSignatures,
        Map<Integer, List<InvocationSubstitute>> substituteInvocations,
        int depth
    ) {
        // TODO efficiency is not a main concern for the first release of this tool, but if it does become
        //  important, then it should be considered how the following might be optimized. Currently every value
        //  every depth must be compared against all macro matchers.
        for (MacroizeMacroMatcher customMacroMatcher : customMacroMatchers) {
            if (customMacroMatcher.match(value)) {
                String name = customMacroMatcher.name();
                InvocationSubstitute substitute = new InvocationSubstitute(SYSTEM, parent, containerIndex, value.getFieldName(), name, suggestedSignatures.get(name));
                substituteInvocations.computeIfAbsent(depth, k -> new ArrayList<>()).add(substitute);
                return true;
            }
        }
        return false;
    }

    /**
     * Recursively visits the given container, evaluating it against the possible macro matches.
     * @param container a container.
     * @param customMacroMatchers the macro matchers to evaluate.
     * @param suggestedSignatures the macro signatures available.
     * @param substituteInvocations receives the invocation substitutes identified for this value, organized by depth.
     * @param depth the depth at which the given container resides.
     * @return the name of the macro that this container matched, or null if there was no match.
     */
    private static String matchMacrosRecursive(
        IonContainer container,
        List<MacroizeMacroMatcher> customMacroMatchers,
        Map<String, SuggestedSignature> suggestedSignatures,
        Map<Integer, List<InvocationSubstitute>> substituteInvocations,
        int depth
    ) {
        Iterator<IonValue> children = container.iterator();
        int containerIndex = 0;
        Set<String> childFields = new LinkedHashSet<>();
        while (children.hasNext()) {
            IonValue child = children.next();
            if (findMatch(child, container, containerIndex, customMacroMatchers, suggestedSignatures, substituteInvocations, depth)) {
                // A custom matcher was matched; don't descend further.
                containerIndex++;
                continue;
            }
            if (container.getType() == IonType.STRUCT) {
                childFields.add(child.getFieldName());
            }
            switch (child.getType()) {
                case STRUCT:
                case LIST:
                case SEXP:
                    String shapeName = matchMacrosRecursive((IonContainer) child, customMacroMatchers, suggestedSignatures, substituteInvocations, depth + 1);
                    if (shapeName != null) {
                        InvocationSubstitute substitute = new InvocationSubstitute(SYSTEM, container, containerIndex, child.getFieldName(), shapeName, suggestedSignatures.get(shapeName));
                        substituteInvocations.computeIfAbsent(depth, k -> new ArrayList<>()).add(substitute);
                    }
                    break;
                default:
                    break;
            }
            containerIndex++;
        }
        String shapeName = getNameOfShape(container);
        if (shapeName == null) {
            return null;
        }
        SuggestedSignature suggestedSignature = suggestedSignatures.get(shapeName);
        if (suggestedSignature != null && suggestedSignature.isCompatible(childFields)) {
            if (container.getType() == IonType.STRUCT) {
                return shapeName;
            }
        }
        return null;
    }

    /**
     * Iterates through a stream that may contain macro invocation markup (e.g.
     * `$ion_invocation::(name_of_macro arguments...)`), replacing these special marked up s-expressions with
     * actual Ion 1.1 e-expressions.
     * TODO the structure of this method is copied from `AbstractIonWriter.writeValueRecursive`, though several changes
     *  were made to fit this purpose. Ideally the code could be shared somehow.
     * @param reader the reader over the marked-up Ion 1.0 stream.
     * @param writer an Ion 1.1 raw writer.
     * @param context the encoding context, containing the symbols and macros that will be used in the Ion 1.1 stream.
     * @param isBinary true if the output encoding is binary; false if it is text.
     * @param textPatterns the text patterns to match and replace when writing.
     */
    private static void replaceMatchesWithInvocations(
        IonReader reader,
        IonRawWriter_1_1 writer,
        ManualEncodingContext context,
        boolean isBinary,
        List<TextPattern> textPatterns
    ) {
        // The IonReader does not need to be at the top level (getDepth()==0) when the function is called.
        // We take note of its initial depth so we can avoid advancing the IonReader beyond the starting value.
        int startingDepth = writer.depth();

        // The IonReader will be at `startingDepth` when the function is first called and then again when we
        // have finished traversing all of its children. This boolean tracks which of those two states we are
        // in when `getDepth() == startingDepth`.
        boolean alreadyProcessedTheStartingValue = false;

        // The IonType of the IonReader's current value.
        IonType type;

        while (true) {
            // Each time we reach the top of the loop we are in one of three states:
            // 1. We have not yet begun processing the starting value.
            // 2. We are currently traversing the starting value's children.
            // 3. We have finished processing the starting value.
            if (writer.depth() == startingDepth) {
                // The IonReader is at the starting depth. We're either beginning our traversal or finishing it.
                if (alreadyProcessedTheStartingValue) {
                    // We're finishing our traversal.
                    break;
                }
                // We're beginning our traversal. Don't advance the cursor; instead, use the current
                // value's IonType.
                type = reader.getType();
                // We've begun processing the starting value.
                alreadyProcessedTheStartingValue = true;
            } else {
                // We're traversing the starting value's children (that is: values at greater depths). We need to
                // advance the cursor by calling next().
                type = reader.next();
            }

            if (type == null) {
                // There are no more values at this level. If we're at the starting level, we're done.
                if (writer.depth() == startingDepth) {
                    break;
                }
                // Otherwise, step out once and then try to move forward again.
                reader.stepOut();
                writer.stepOut();
                continue;
            }

            final SymbolToken fieldName = reader.getFieldNameSymbol();
            if (fieldName != null && !writer._private_hasFieldName() && writer.isInStruct()) {
                // TODO apply text patterns to field names
                writer.writeFieldName(context.internSymbol(fieldName.getText()));
            }
            if (fieldName == null && writer.isInStruct()) {
                throw new IonException("Missing field name");
            }
            final SymbolToken[] annotations = reader.getTypeAnnotationSymbols();
            boolean isEexp = false;
            boolean isEmptyExpressionGroup = false;
            if (annotations.length == 1 && annotations[0].getText().equals(InvocationSubstitute.INVOCATION_ANNOTATION)) {
                isEexp = true;
            } else if (annotations.length == 1 && annotations[0].getText().equals(InvocationSubstitute.EMPTY_GROUP_ANNOTATION)) {
                isEmptyExpressionGroup = true;
            } else {
                for (SymbolToken annotation : annotations) {
                    // TODO apply text patterns to annotations
                    writer.writeAnnotations(context.internSymbol(annotation.getText()));
                }
            }
            if (reader.isNullValue()) {
                writer.writeNull(type);
                continue;
            }

            switch (type) {
                case BOOL:
                    final boolean booleanValue = reader.booleanValue();
                    writer.writeBool(booleanValue);
                    break;
                case INT:
                    switch (reader.getIntegerSize()) {
                        case INT:
                            final int intValue = reader.intValue();
                            writer.writeInt(intValue);
                            break;
                        case LONG:
                            final long longValue = reader.longValue();
                            writer.writeInt(longValue);
                            break;
                        case BIG_INTEGER:
                            final BigInteger bigIntegerValue = reader.bigIntegerValue();
                            writer.writeInt(bigIntegerValue);
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                    break;
                case FLOAT:
                    final double doubleValue = reader.doubleValue();
                    writer.writeFloat(doubleValue);
                    break;
                case DECIMAL:
                    BigDecimal decimalValue = reader.decimalValue();
                    if (decimalValue.precision() > 16) {
                        decimalValue = decimalValue.round(MathContext.DECIMAL64);
                    }
                    writer.writeDecimal(decimalValue);
                    break;
                case TIMESTAMP:
                    final Timestamp timestampValue = reader.timestampValue();
                    writer.writeTimestamp(timestampValue);
                    break;
                case SYMBOL:
                    final SymbolToken symbolToken = reader.symbolValue();
                    writer.writeSymbol(context.internSymbol(symbolToken.getText()));
                    break;
                case STRING:
                    final String stringValue = reader.stringValue();
                    boolean isMatched = false;
                    for (TextPattern stringPattern : textPatterns) {
                        if (stringPattern.matches(stringValue)) {
                            stringPattern.invoke(stringValue, context, writer, isBinary);
                            isMatched = true;
                            break;
                        }
                    }
                    if (!isMatched) {
                        writer.writeString(stringValue);
                    }
                    break;
                case CLOB:
                    final byte[] clobValue = reader.newBytes();
                    writer.writeClob(clobValue);
                    break;
                case BLOB:
                    final byte[] blobValue = reader.newBytes();
                    writer.writeBlob(blobValue);
                    break;
                case SEXP:
                    reader.stepIn();
                    if (isEmptyExpressionGroup) {
                        writer.stepInExpressionGroup(false);
                    } else if (isEexp) {
                        reader.next();
                        String macroName = reader.stringValue();
                        if (isBinary) {
                            writer.stepInEExp(context.getMacroId(macroName), false, context.getMacro(macroName));
                        } else {
                            writer.stepInEExp(macroName);
                        }
                    } else {
                        writer.stepInSExp(false);
                    }
                    break;
                case LIST:
                    reader.stepIn();
                    writer.stepInList(false);
                    break;
                case STRUCT:
                    reader.stepIn();
                    writer.stepInStruct(false);
                    break;
                default:
                    throw new IllegalStateException("Unexpected type: " + type);
            }
        }
    }

    /**
     * Checks that the file with the given name exists and can be read.
     * @param name the file name.
     * @return a Path to the file.
     */
    private static Path checkPath(String name) {
        File file = new File(name);
        if (!file.canRead()) {
            throw new IllegalArgumentException("Cannot read file: " + name);
        }
        return file.toPath();
    }

    /**
     * Appends a copy of the contents of 'from' to the end of the contents of 'to'.
     * @param from the path to copy from.
     * @param to the path to append to.
     * @throws IOException if thrown during the copy.
     */
    private static void appendCopy(Path from, Path to) throws IOException {
        try (OutputStream output = new FileOutputStream(to.toFile(), true)) {
            Files.copy(from, output);
        }
    }

    private static IonRawWriter_1_1 newRawWriter_1_1(OutputStream out, boolean isBinary) {
        return isBinary ? newRawBinaryWriter_1_1(out) : newRawTextWriter_1_1(out);
    }

    private static IonRawWriter_1_1 newRawBinaryWriter_1_1(OutputStream out) {
        return new IonRawBinaryWriter_1_1(
            out,
            new WriteBuffer(BlockAllocatorProviders.basicProvider().vendAllocator(32768), () -> {}),
            0
        );
    }

    private static IonRawWriter_1_1 newRawTextWriter_1_1(OutputStream out) {
        _Private_IonTextWriterBuilder_1_1 builder = _Private_IonTextWriterBuilder_1_1.standard()
            .withNewLineType(IonTextWriterBuilder.NewLineType.LF)
            .withPrettyPrinting();
        BufferedOutputStreamFastAppendable appendable = new BufferedOutputStreamFastAppendable(
            out,
            BlockAllocatorProviders.basicProvider().vendAllocator(4096),
            1.0
        );
        return new IonRawTextWriter_1_1(
            builder,
            _Private_IonTextAppender.forFastAppendable(appendable, StandardCharsets.UTF_8)
        );
    }

    /**
     * Sanitizes the given string so that it may be used as a macro name.
     * @param original the original string.
     * @return the sanitized name.
     */
    private static String sanitizeName(String original) {
        String sanitized = original.replaceAll("[.:\\-/]", "_");
        if (!Character.isAlphabetic(sanitized.charAt(0))) {
            return "z" + sanitized; // This is arbitrary.
        }
        return sanitized;
    }

    /**
     * Gets a name describing the given container. This will either be its field name, if in a struct, or the field
     * name of its parent sequence, if applicable. Otherwise, this method will return null.
     * @param container the value for which to get a shape name.
     * @return the name, or null if no name can be determined.
     */
    private static String getNameOfShape(IonContainer container) {
        String shapeName = container.getFieldName();
        if (shapeName == null) {
            // Homogeneous sequences of structs are common. In this case use the field name of the sequence, if any.
            IonContainer parentContainer = container.getContainer();
            if (parentContainer != null && (parentContainer.getType() == IonType.LIST || parentContainer.getType() == IonType.SEXP)) {
                shapeName = parentContainer.getFieldName();
            }
        }
        if (shapeName == null) {
            return null;
        }
        return sanitizeName(shapeName);
    }
}
