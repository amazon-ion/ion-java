// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.MacroAwareIonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl.bin.PresenceBitmap;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.LazyMacroEvaluator;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroEvaluator;
import com.amazon.ion.impl.macro.ReaderAdapter;
import com.amazon.ion.impl.macro.ReaderAdapterContinuable;
import com.amazon.ion.impl.macro.ReaderAdapterIonReader;
import com.amazon.ion.impl.macro.SystemMacro;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * An {@link LazyEExpressionArgsReader} reads an E-Expression from a {@link ReaderAdapter}, constructs
 * a list of {@link Expression}s representing the E-Expression and its arguments, and prepares a {@link MacroEvaluator}
 * to evaluate these expressions.
 * <p>
 * There are two sources of expressions. The template macro definitions, and the macro arguments.
 * The {@link MacroEvaluator} merges those.
 * <p>
 * The {@link Expression} model does not (yet) support lazily reading values, so for now, all macro arguments must
 * be read eagerly.
 */
// TODO this isn't necessarily only related to e-expression args... this could be used generally to create a lazy tape
//  for any Ion value. This might be the most efficient way to "fill" Ion 1.1 delimited containers, as it reduces
//  the amount of redundant processing required in that case. The structural information learned during the fill
//  does not need to be rediscovered during the parse phase (if any).
abstract class LazyEExpressionArgsReader {

    private final IonReaderContinuableCoreBinary reader;

    // The active expression tape into which expressions are read. Either `expressionTapeOrdered` or 'expressionTapeScratch'.
    protected ExpressionTape expressionTape;

    // Reusable tape for recording value boundaries for lazy parsing.
    protected final ExpressionTape expressionTapeOrdered;

    // Reusable scratch tape for storing out-of-order expression arguments.
    private final ExpressionTape expressionTapeScratch;

    private ExpressionTape verbatimExpressionTape = null;

    // Pool for presence bitmap instances.
    protected final PresenceBitmap.Companion.PooledFactory presenceBitmapPool = new PresenceBitmap.Companion.PooledFactory();

    // Whether to produce a tape that preserves the invocation verbatim.
    private boolean verbatimTranscode = false;

    private Marker[] markerPool = new Marker[256];
    private int markerPoolIndex = -1;

    /**
     * Constructor.
     * @param reader the {@link ReaderAdapter} from which to read {@link Expression}s.
     * @see ReaderAdapterIonReader
     * @see ReaderAdapterContinuable
     */
    LazyEExpressionArgsReader(IonReaderContinuableCoreBinary reader) {
        this.reader = reader;
        expressionTapeOrdered = new ExpressionTape(reader, 256);
        expressionTapeScratch = new ExpressionTape(reader, 64);
        expressionTape = expressionTapeOrdered;
        this.reader.setLeftShiftHandler(shiftAmount -> {
            expressionTapeOrdered.shiftIndicesLeft(shiftAmount);
            expressionTapeScratch.shiftIndicesLeft(shiftAmount);
        });
    }

    private Marker getMarker(int index) {
        markerPoolIndex = Math.max(markerPoolIndex, index);
        while (index >= markerPool.length) {
            markerPool = Arrays.copyOf(markerPool, markerPool.length * 2);
        }
        Marker marker = markerPool[index];
        if (marker == null) {
            marker = new Marker(-1, -1);
            markerPool[index] = marker;
        }
        return marker;
    }

    /**
     * @return true if the value upon which the reader is positioned represents a macro invocation; otherwise, false.
     */
    protected abstract boolean isMacroInvocation();

    /**
     * @return true if the container value on which the reader is positioned represents an expression group; otherwise,
     *  false.
     */
    protected abstract boolean isContainerAnExpressionGroup();

    /**
     * Eagerly collects the annotations on the current value.
     * @return the annotations, or an empty list if there are none.
     */
    protected abstract List<SymbolToken> getAnnotations();

    /**
     * Navigates the reader to the next raw value, without interpreting any system values.
     * @return true if there is a next value; false if the end of the container was reached.
     */
    protected abstract boolean nextRaw();

    /**
     * Steps into a container on which the reader has been positioned by calling {@link #nextRaw()}.
     */
    protected abstract void stepInRaw();

    /**
     * Steps out of a container on which the reader had been positioned by calling {@link #nextRaw()}.
     */
    protected abstract void stepOutRaw();

    /**
     * Steps into an e-expression.
     */
    protected abstract void stepIntoEExpression();

    /**
     * Steps out of an e-expression.
     */
    protected abstract void stepOutOfEExpression();

    /**
     * Reads a single parameter to a macro invocation.
     * @param parameter information about the parameter from the macro signature.
     * @param parameterPresence the presence bits dedicated to this parameter (unused in text).
     */
    protected abstract void readParameter(Macro.Parameter parameter, long parameterPresence, String fieldName);

    /**
     * Reads the macro's address and attempts to resolve that address to a Macro from the macro table.
     * @return the loaded macro.
     */
    protected abstract Macro loadMacro();

    /**
     * Reads the argument encoding bitmap into a PresenceBitmap. This is only applicable to binary.
     * @param signature the macro signature.
     * @return a PresenceBitmap created from the argument encoding bitmap, or null.
     */
    protected abstract PresenceBitmap loadPresenceBitmapIfNecessary(List<Macro.Parameter> signature);

    /**
     * Reads a scalar value from the stream into an expression.
     */
    private void readScalarValueAsExpression(String fieldName) {
        expressionTape.add(reader.valueTid, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, (int) reader.valueMarker.startIndex, (int) reader.valueMarker.endIndex, fieldName);
    }

    /**
     * Reads a container value from the stream into a list of expressions that will eventually be passed to
     * the MacroEvaluator responsible for evaluating the e-expression to which this container belongs.
     * @param type the type of container.
     */
    private void readContainerValueAsExpression(IonType type, String fieldName) {
        boolean isExpressionGroup = isContainerAnExpressionGroup();
        if (isExpressionGroup) {
            expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_ORDINAL, -1, -1, fieldName);
        } else {
            expressionTape.add(reader.valueTid, ExpressionType.DATA_MODEL_CONTAINER_ORDINAL, -1, -1, fieldName);
        }
        // TODO if the container is prefixed, don't recursively step through it
        stepInRaw();
        while (nextRaw()) {
            String childFieldName = null;
            if (type == IonType.STRUCT) {
                // TODO avoid having to create SymbolToken every time
                childFieldName = reader.getFieldName();
            }
            readValueAsExpression(false, childFieldName); // TODO avoid recursion
        }
        if (isExpressionGroup) {
            expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_END_ORDINAL, -1, -1, null);
        } else {
            expressionTape.add(null, ExpressionType.DATA_MODEL_CONTAINER_END_ORDINAL, -1, -1, null);
        }
        stepOutRaw();
    }

    /**
     * Reads the rest of the stream into a single expression group.
     */
    private void readStreamAsExpressionGroup(String fieldName) {
        expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_ORDINAL, -1, -1, fieldName);
        do {
            readValueAsExpression(false, null); // TODO avoid recursion // TODO or, should the field name be distributed to all?
        } while (nextRaw());
        expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_END_ORDINAL, -1, -1, null);
    }

    /**
     * Reads a value from the stream into expression(s) that will eventually be passed to the MacroEvaluator
     * responsible for evaluating the e-expression to which this value belongs.
     * @param isImplicitRest true if this is the final parameter in the signature, it is variadic, and the format
     *                       supports implicit rest parameters (text only); otherwise, false.
     */
    protected void readValueAsExpression(boolean isImplicitRest, String fieldName) {
        if (isImplicitRest && !isContainerAnExpressionGroup()) {
            readStreamAsExpressionGroup(fieldName);
            return;
        } else if (isMacroInvocation()) {
            collectEExpressionArgs(false, fieldName); // TODO avoid recursion
            return;
        }
        IonType type = reader.getEncodingType();
        if (reader.hasAnnotations()) {
            List<SymbolToken> annotations = getAnnotations(); // TODO make this lazy too
            expressionTape.add(annotations, ExpressionType.ANNOTATION_ORDINAL, -1, -1, null);
        }
        if (IonType.isContainer(type) && !reader.isNullValue()) {
            readContainerValueAsExpression(type, fieldName);
        } else {
            readScalarValueAsExpression(fieldName);
        }
    }

    private int collectAndFlattenComplexEExpression(ExpressionTape.Core macroBodyTape, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap, int markerPoolStartIndex) {
        // TODO drop expression groups? No longer needed after the variables are resolved. Further simplifies the evaluator. Might miss validation? Also, would need to distribute field names over the elements of the group.
        int macroBodyTapeIndex = 0;
        int numberOfVariables = macroBodyTape.getNumberOfVariables();
        int numberOfDuplicatedVariables = 0;
        int numberOfOutOfOrderVariables = 0;
        for (int i = 0; i < numberOfVariables; i++) {
            // Now, materialize and insert the argument from the invocation.
            // Copy everything up to the next variable.
            macroBodyTapeIndex = macroBodyTape.copyToVariable(macroBodyTapeIndex, i, expressionTape);
            int targetVariableOrdinal = macroBodyTape.getVariableOrdinal(i);
            int invocationOrdinal = i - numberOfDuplicatedVariables + numberOfOutOfOrderVariables;
            while (true) {
                if (invocationOrdinal == targetVariableOrdinal) {
                    // This is the common case: the parameter that is about to be read matches the ordinal of the next
                    // variable in the signature. This means it can be copied into the tape without using scratch space.
                    String childFieldName = macroBodyTape.fieldNameForVariable(i);
                    int startIndexInTape = expressionTape.size();
                    readParameter(
                        signature.get(invocationOrdinal),
                        presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(invocationOrdinal),
                        childFieldName
                    );
                    Marker marker = getMarker(markerPoolStartIndex + invocationOrdinal);
                    marker.typeId = null;
                    marker.startIndex = startIndexInTape;
                    marker.endIndex = expressionTape.size();
                    break;
                } else if (invocationOrdinal < targetVariableOrdinal) {
                    // The variable for this ordinal cannot have been read yet.
                    int scratchStartIndex = expressionTapeScratch.size();
                    expressionTape = expressionTapeScratch;
                    int matchingVariableOrdinal = macroBodyTape.getVariableOrdinal(invocationOrdinal);
                    readParameter(
                        signature.get(invocationOrdinal),
                        presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(invocationOrdinal),
                        macroBodyTape.fieldNameForVariable(matchingVariableOrdinal)
                    );
                    expressionTape = expressionTapeOrdered;
                    Marker marker = getMarker(markerPoolStartIndex + invocationOrdinal);
                    marker.startIndex = scratchStartIndex;
                    marker.endIndex = expressionTapeScratch.size();
                    // This is a sentinel to denote that the expression is in the scratch tape.
                    marker.typeId = IonTypeID.ALWAYS_INVALID_TYPE_ID;
                    numberOfOutOfOrderVariables++;
                } else {
                    // This is a variable that has already been encountered.
                    Marker marker = markerPool[markerPoolStartIndex + targetVariableOrdinal];
                    if (marker == null) {
                        throw new IllegalStateException("Every variable ordinal must be recorded as it is encountered.");
                    }
                    numberOfDuplicatedVariables++;
                    if (marker.startIndex == marker.endIndex) {
                        // This variable was None, so it is elided from the expression tape.
                        break;
                    }
                    ExpressionTape source = marker.typeId == IonTypeID.ALWAYS_INVALID_TYPE_ID ? expressionTapeScratch : expressionTape;
                    // The argument for this variable has already been read. Copy it from the tape.
                    expressionTape.copyFromRange(source.core(), (int) marker.startIndex, (int) marker.endIndex);
                    break;
                }
                invocationOrdinal++;
            }
        }
        markerPoolIndex = markerPoolStartIndex;
        return macroBodyTapeIndex;
    }

    private int collectAndFlattenSimpleEExpression(ExpressionTape.Core macroBodyTape, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap) {
        // TODO drop expression groups? No longer needed after the variables are resolved. Further simplifies the evaluator. Might miss validation? Also, would need to distribute field names over the elements of the group.
        int macroBodyTapeIndex = 0;
        int numberOfVariables = macroBodyTape.getNumberOfVariables();
        for (int i = 0; i < numberOfVariables; i++) {
            // Copy everything up to the next variable.
            macroBodyTapeIndex = macroBodyTape.copyToVariable(macroBodyTapeIndex, i, expressionTape);
            String childFieldName = macroBodyTape.fieldNameForVariable(i);
            // This is the common case: the parameter that is about to be read matches the ordinal of the next
            // variable in the signature. This means it can be copied into the tape without using scratch space.
            readParameter(
                signature.get(i),
                presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(i),
                childFieldName
            );
        }
        return macroBodyTapeIndex;
    }

    private ExpressionTape collectAndFlattenUserEExpressionArgs(boolean isTopLevel, Macro macro, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap, String fieldName) {
        ExpressionTape.Core macroBodyTape = macro.getBodyTape();
        // TODO avoid eagerly stepping through prefixed expressions
        int numberOfParameters = signature.size();
        int macroBodyTapeIndex = 0;
        int startIndex = expressionTape.size();
        if (numberOfParameters == 0) {
            if (isTopLevel) {
                // This avoids copying and reuses the macro body tape core as-is.
                ExpressionTape constantTape = new ExpressionTape(macroBodyTape); // TODO pool?
                stepOutOfEExpression();
                return constantTape;
            }
        } else if (macro.isSimple()) {
            macroBodyTapeIndex = collectAndFlattenSimpleEExpression(macroBodyTape, signature, presenceBitmap);
        } else {
            macroBodyTapeIndex = collectAndFlattenComplexEExpression(macroBodyTape, signature, presenceBitmap, markerPoolIndex + 1);
        }
        // Copy everything after the last parameter.
        expressionTape.copyFromRange(macroBodyTape, macroBodyTapeIndex, macroBodyTape.size());
        expressionTape.setFieldNameAt(startIndex, fieldName);
        stepOutOfEExpression();
        return expressionTape;
    }

    private void collectVerbatimEExpressionArgs(Macro macro, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap, String fieldName) {
        // TODO avoid eagerly stepping through prefixed expressions
        expressionTape.add(macro, ExpressionType.E_EXPRESSION_ORDINAL, -1, -1, fieldName);
        int numberOfParameters = signature.size();
        for (int i = 0; i < numberOfParameters; i++) {
            readParameter(
                signature.get(i),
                presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(i),
                null
            );
        }
        stepOutOfEExpression();
        expressionTape.add(null, ExpressionType.E_EXPRESSION_END_ORDINAL, -1, -1, null);
    }

    /**
     * Collects the expressions that compose the current macro invocation.
     */
    private ExpressionTape collectEExpressionArgs(boolean isTopLevel, String fieldName) {
        // TODO if it's a system macro don't eagerly expand
        Macro macro = loadMacro();
        stepIntoEExpression();
        List<Macro.Parameter> signature = macro.getSignature();
        PresenceBitmap presenceBitmap = loadPresenceBitmapIfNecessary(signature);
        if (verbatimTranscode) {
            collectVerbatimEExpressionArgs(macro, signature, presenceBitmap, fieldName);
            return expressionTape;
        } else if ((macro instanceof SystemMacro && macro.getBodyTape().size() == 0)) {
            if (macro == SystemMacro.None) {
                // The following is a very cheap optimization that does not cover all possible cases where eliding
                // None is possible. When there is a field name, we know that the None occurs in a struct and the field
                // can simply be suppressed. The only time it cannot be suppressed is within a non-flattened system
                // macro invocation. This is good enough for now.
                if (fieldName == null) { // TODO can always skip at the top level too. This is useful for Meta
                    expressionTape.add(null, ExpressionType.NONE_ORDINAL, -1, -1, null);
                }
                stepOutOfEExpression();
            } else {
                collectVerbatimEExpressionArgs(macro, signature, presenceBitmap, fieldName);
            }
            return expressionTape;
        }
        return collectAndFlattenUserEExpressionArgs(isTopLevel, macro, signature, presenceBitmap, fieldName);
    }

    private void takeDefaultArgument(ExpressionTape.Core tape, int argumentsStart, int eExpressionIndex) {
        // Tombstone the invocation and the first argument;
        // take the second. Note: argumentsStart - 1 points to the Default invocation.
        ExpressionTape.Element firstTombstoneInSequence = tape.elementAt(argumentsStart - 1);
        String fieldName = firstTombstoneInSequence.fieldName;
        firstTombstoneInSequence.type = ExpressionType.TOMBSTONE_ORDINAL;
        firstTombstoneInSequence.containerEnd = tape.findEndOfTombstoneSequence(tape.getExpressionStartIndex(eExpressionIndex, 1));
        // Move the field name from the invocation to the value that remains.
        tape.elementAt(firstTombstoneInSequence.containerEnd).fieldName = fieldName;
        // Tombstone the invocation end marker.
        int invocationEndIndex = tape.getEExpressionEndIndex(eExpressionIndex);
        ExpressionTape.Element invocationEnd = tape.elementAt(invocationEndIndex);
        invocationEnd.type = ExpressionType.TOMBSTONE_ORDINAL;
        invocationEnd.containerEnd = tape.findEndOfTombstoneSequence(invocationEndIndex + 1);
    }

    private void simplifyDefaultInvocation(ExpressionTape.Core tape, int argumentsStart, int eExpressionIndex) {
        int actualArgumentStart = tape.findEndOfTombstoneSequence(argumentsStart);
        ExpressionTape.Element firstArgument = tape.elementAt(actualArgumentStart);
        if (ExpressionType.isDataModelValue(firstArgument.type)) {
            // The first argument is not None. Tombstone the invocation and the second argument;
            // take the first. Note: argumentsStart - 1 points to the Default invocation.
            ExpressionTape.Element firstTombstoneInSequence = tape.elementAt(argumentsStart - 1);
            String fieldName = firstTombstoneInSequence.fieldName;
            firstTombstoneInSequence.type = ExpressionType.TOMBSTONE_ORDINAL;
            firstTombstoneInSequence.containerEnd = actualArgumentStart;
            // Move the field name from the invocation to the value that remains.
            firstArgument.fieldName = fieldName;
            int invocationEndIndex = tape.getEExpressionEndIndex(eExpressionIndex);
            ExpressionTape.Element invocationEnd = tape.elementAt(tape.findEndOfTombstoneSequence(tape.getExpressionStartIndex(eExpressionIndex, 1)));
            invocationEnd.type = ExpressionType.TOMBSTONE_ORDINAL;
            invocationEnd.containerEnd = tape.findEndOfTombstoneSequence(invocationEndIndex + 1);
        } else if (firstArgument.type == ExpressionType.EXPRESSION_GROUP_ORDINAL) {
            ExpressionTape.Element firstElementInGroup = tape.elementAt(tape.findEndOfTombstoneSequence(argumentsStart + 1));
            if (firstElementInGroup.type == ExpressionType.EXPRESSION_GROUP_END_ORDINAL) {
                // The first argument is an empty group.
                takeDefaultArgument(tape, argumentsStart, eExpressionIndex);
            }
            // Otherwise, this is a non-empty group, but it may still resolve to None if it contains a system macro that
            // cannot be simplified.
        } else if (firstArgument.type == ExpressionType.NONE_ORDINAL) {
            // The first argument is None.
            takeDefaultArgument(tape, argumentsStart, eExpressionIndex);
        }
        // Otherwise, the first argument must be an e-expression that could not be simplified, so it can't be
        // determined if it's None. Do not modify this Default invocation.
    }

    private void simplifyResolvableMacroInvocations(ExpressionTape.Core tape) {
        for (int eExpressionIndex = tape.getNumberOfEExpressions() - 1; eExpressionIndex >= 0; eExpressionIndex--) {
            // Note: iterating back to front guarantees that any child invocations are resolved before the parent.
            // This prevents us from having to do anything recursive. It also allows us to connect longer tombstone
            // sequences because all tombstones that follow are known.
            int eExpressionStart = tape.findEndOfTombstoneSequence(tape.getEExpressionStartIndex(eExpressionIndex));
            ExpressionTape.Element element = tape.elementAt(eExpressionStart);
            Macro macro = (Macro) element.context;
            // Note optimizations for additional system macros could be added if necessary. The ones required for the
            // other branching macros (IfNone, etc.) are similar to the one for Default.
            if (macro == SystemMacro.Default) {
                simplifyDefaultInvocation(tape, eExpressionStart + 1, eExpressionIndex);
            }
            // Otherwise, this system macro is not handled specially here; do nothing to it.
        }
    }

    /**
     * Materializes the expressions that compose the macro invocation on which the reader is positioned and feeds
     * them to the macro evaluator.
     */
    public void beginEvaluatingMacroInvocation(LazyMacroEvaluator macroEvaluator) {
        reader.pinBytesInCurrentValue();
        String fieldName = null;
        if (reader.isInStruct()) {
            // TODO avoid having to create SymbolToken every time
            fieldName = reader.getFieldName();
        }
        ExpressionTape tape = collectEExpressionArgs(true, fieldName);
        simplifyResolvableMacroInvocations(tape.core());
        macroEvaluator.initExpansion(fieldName, tape);
    }

    public void transcodeAndBeginEvaluatingMacroInvocation(LazyMacroEvaluator macroEvaluator, MacroAwareIonWriter transcoder) throws IOException {
        // Note: macro-aware transcoding is not considered performance-critical. Adopting this two-pass technique
        //  allows us to use a single, simplified evaluator that relies on flattened invocations that are not
        //  present in the verbatim tape. We could simply generate a non-flattened tape via an option to the above
        //  method, but then we'd need another more general evaluator implementation, which adds significant complexity
        //  to the code base.
        this.verbatimTranscode = true;
        if (verbatimExpressionTape == null) {
            verbatimExpressionTape = new ExpressionTape(reader, 64);
        }
        reader.pinBytesInCurrentValue();
        String fieldName = null;
        if (reader.isInStruct()) {
            fieldName = reader.getFieldName();
        }
        expressionTape = verbatimExpressionTape;
        int start = (int) reader.valueMarker.startIndex;
        int end = (int) reader.valueMarker.endIndex;
        IonTypeID typeID = reader.valueTid;
        long macroAddress = reader.getMacroInvocationId();
        boolean isSystemMacro = reader.isSystemInvocation();
        Macro macro = loadMacro();
        stepIntoEExpression();
        List<Macro.Parameter> signature = macro.getSignature();
        PresenceBitmap presenceBitmap = loadPresenceBitmapIfNecessary(signature);
        collectVerbatimEExpressionArgs(macro, signature, presenceBitmap, fieldName);
        reader.sliceAfterMacroInvocationHeader(start, end, typeID, macroAddress, isSystemMacro); // Rewind, prepare to read again.
        verbatimTranscode = false;
        expressionTape = expressionTapeOrdered;
        macroEvaluator.initExpansion(fieldName, collectEExpressionArgs(true, fieldName));
        verbatimExpressionTape.transcodeArgumentsTo(transcoder);
        verbatimExpressionTape.clear();
        markerPoolIndex = -1;
    }

    /**
     * Finishes evaluating the current macro invocation, resetting any associated state.
     */
    public void finishEvaluatingMacroInvocation() {
        presenceBitmapPool.clear();
        reader.unpinBytes();
        reader.returnToCheckpoint();
        expressionTapeOrdered.clear();
        expressionTapeScratch.clear();
        expressionTape = expressionTapeOrdered;
        verbatimTranscode = false;
        markerPoolIndex = -1;
    }
}
