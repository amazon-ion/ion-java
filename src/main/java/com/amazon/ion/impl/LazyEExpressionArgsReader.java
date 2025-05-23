// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonCursor;
import com.amazon.ion.IonException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * @param isTrailing true if this parameter is the last one in the signature; otherwise, false (unused in binary).
     */
    protected abstract void readParameter(Macro.Parameter parameter, long parameterPresence, boolean isTrailing);

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
    private void readScalarValueAsExpression() {
        expressionTape.add(reader.valueTid, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, (int) reader.valueMarker.startIndex, (int) reader.valueMarker.endIndex);
    }

    /**
     * Reads a container value from the stream into a list of expressions that will eventually be passed to
     * the MacroEvaluator responsible for evaluating the e-expression to which this container belongs.
     * @param type the type of container.
     */
    private void readContainerValueAsExpression(
        IonType type
    ) {
        boolean isExpressionGroup = isContainerAnExpressionGroup();
        if (isExpressionGroup) {
            expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_ORDINAL, -1, -1);
        } else {
            expressionTape.add(reader.valueTid, ExpressionType.DATA_MODEL_CONTAINER_ORDINAL, -1, -1);
        }
        // TODO if the container is prefixed, don't recursively step through it
        stepInRaw();
        while (nextRaw()) {
            if (type == IonType.STRUCT) {
                // TODO avoid having to create SymbolToken every time
                expressionTape.add(reader.getFieldNameSymbol(), ExpressionType.FIELD_NAME_ORDINAL, -1, -1);
            }
            readValueAsExpression(false); // TODO avoid recursion
        }
        if (isExpressionGroup) {
            expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_END_ORDINAL, -1, -1);
        } else {
            expressionTape.add(null, ExpressionType.DATA_MODEL_CONTAINER_END_ORDINAL, -1, -1);
        }
        stepOutRaw();
    }

    /**
     * Reads the rest of the stream into a single expression group.
     */
    private void readStreamAsExpressionGroup() {
        expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_ORDINAL, -1, -1);
        do {
            readValueAsExpression(false); // TODO avoid recursion
        } while (nextRaw());
        expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_END_ORDINAL, -1, -1);
    }

    /**
     * Reads a value from the stream into expression(s) that will eventually be passed to the MacroEvaluator
     * responsible for evaluating the e-expression to which this value belongs.
     * @param isImplicitRest true if this is the final parameter in the signature, it is variadic, and the format
     *                       supports implicit rest parameters (text only); otherwise, false.
     */
    protected void readValueAsExpression(boolean isImplicitRest) {
        if (isImplicitRest && !isContainerAnExpressionGroup()) {
            readStreamAsExpressionGroup();
            return;
        } else if (isMacroInvocation()) {
            collectEExpressionArgs(false); // TODO avoid recursion
            return;
        }
        IonType type = reader.getEncodingType();
        if (reader.hasAnnotations()) {
            List<SymbolToken> annotations = getAnnotations(); // TODO make this lazy too
            expressionTape.add(annotations, ExpressionType.ANNOTATION_ORDINAL, -1, -1);
        }
        if (IonType.isContainer(type) && !reader.isNullValue()) {
            readContainerValueAsExpression(type);
        } else {
            readScalarValueAsExpression();
        }
    }

    private int collectAndFlattenComplexEExpression(int numberOfParameters, ExpressionTape.Core macroBodyTape, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap, int markerPoolStartIndex) {
        // TODO drop expression groups? No longer needed after the variables are resolved. Further simplifies the evaluator. Might miss validation? Also, would need to distribute field names over the elements of the group.
        int macroBodyTapeIndex = 0;
        int numberOfVariables = macroBodyTape.getNumberOfVariables();
        int numberOfDuplicatedVariables = 0;
        int numberOfOutOfOrderVariables = 0;
        for (int i = 0; i < numberOfVariables; i++) {
            // Now, materialize and insert the argument from the invocation.
            // Copy everything up to the next variable.
            macroBodyTapeIndex = macroBodyTape.copyToVariable(macroBodyTapeIndex, i, expressionTape);
            int variableOrdinal = macroBodyTape.getVariableOrdinal(i);
            int invocationOrdinal = i - numberOfDuplicatedVariables + numberOfOutOfOrderVariables;
            while (true) {
                if (invocationOrdinal == variableOrdinal) {
                    // This is the common case: the parameter that is about to be read matches the ordinal of the next
                    // variable in the signature. This means it can be copied into the tape without using scratch space.
                    int startIndexInTape = expressionTape.size();
                    readParameter(
                        signature.get(invocationOrdinal),
                        presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(invocationOrdinal),
                        invocationOrdinal == (numberOfParameters - 1)
                    );
                    Marker marker = getMarker(markerPoolStartIndex + invocationOrdinal);
                    marker.typeId = null;
                    marker.startIndex = startIndexInTape;
                    marker.endIndex = expressionTape.size();
                    break;
                } else if (invocationOrdinal < variableOrdinal) {
                    // The variable for this ordinal cannot have been read yet.
                    int scratchStartIndex = expressionTapeScratch.size();
                    expressionTape = expressionTapeScratch;
                    readParameter(
                        signature.get(invocationOrdinal),
                        presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(invocationOrdinal),
                        invocationOrdinal == (numberOfParameters - 1)
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
                    Marker marker = markerPool[markerPoolStartIndex + variableOrdinal];
                    if (marker == null) {
                        throw new IllegalStateException("Every variable ordinal must be recorded as it is encountered.");
                    }
                    numberOfDuplicatedVariables++;
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

    private int collectAndFlattenSimpleEExpression(int numberOfParameters, ExpressionTape.Core macroBodyTape, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap) {
        // TODO drop expression groups? No longer needed after the variables are resolved. Further simplifies the evaluator. Might miss validation? Also, would need to distribute field names over the elements of the group.
        int macroBodyTapeIndex = 0;
        int numberOfVariables = macroBodyTape.getNumberOfVariables();
        for (int i = 0; i < numberOfVariables; i++) {
            // Copy everything up to the next variable.
            macroBodyTapeIndex = macroBodyTape.copyToVariable(macroBodyTapeIndex, i, expressionTape);
            // This is the common case: the parameter that is about to be read matches the ordinal of the next
            // variable in the signature. This means it can be copied into the tape without using scratch space.
            readParameter(
                signature.get(i),
                presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(i),
                i == (numberOfParameters - 1)
            );
        }
        return macroBodyTapeIndex;
    }

    private ExpressionTape collectAndFlattenUserEExpressionArgs(boolean isTopLevel, Macro macro, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap) {
        ExpressionTape.Core macroBodyTape = macro.getBodyTape();
        // TODO avoid eagerly stepping through prefixed expressions
        int numberOfParameters = signature.size();
        int macroBodyTapeIndex = 0;
        if (numberOfParameters == 0) {
            if (isTopLevel) {
                // This avoids copying and reuses the macro body tape core as-is.
                ExpressionTape constantTape = new ExpressionTape(macroBodyTape); // TODO pool?
                stepOutOfEExpression();
                return constantTape;
            }
        } else if (macro.isSimple()) {
            macroBodyTapeIndex = collectAndFlattenSimpleEExpression(numberOfParameters, macroBodyTape, signature, presenceBitmap);
        } else {
            macroBodyTapeIndex = collectAndFlattenComplexEExpression(numberOfParameters, macroBodyTape, signature, presenceBitmap, markerPoolIndex + 1);
        }
        // Copy everything after the last parameter.
        expressionTape.copyFromRange(macroBodyTape, macroBodyTapeIndex, macroBodyTape.size());
        stepOutOfEExpression();
        return expressionTape;
    }

    private void collectVerbatimEExpressionArgs(Macro macro, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap) {
        // TODO avoid eagerly stepping through prefixed expressions
        expressionTape.add(macro, ExpressionType.E_EXPRESSION_ORDINAL, -1, -1);
        int numberOfParameters = signature.size();
        for (int i = 0; i < numberOfParameters; i++) {
            readParameter(
                signature.get(i),
                presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(i),
                i == (numberOfParameters - 1)
            );
        }
        stepOutOfEExpression();
        expressionTape.add(null, ExpressionType.E_EXPRESSION_END_ORDINAL, -1, -1);
    }

    /**
     * Collects the expressions that compose the current macro invocation.
     */
    private ExpressionTape collectEExpressionArgs(boolean isTopLevel) {
        // TODO if it's a system macro don't eagerly expand
        Macro macro = loadMacro();
        stepIntoEExpression();
        List<Macro.Parameter> signature = macro.getSignature();
        PresenceBitmap presenceBitmap = loadPresenceBitmapIfNecessary(signature);
        if (verbatimTranscode || (macro instanceof SystemMacro && macro.getBodyTape().size() == 0)) {
            collectVerbatimEExpressionArgs(macro, signature, presenceBitmap);
            return expressionTape;
        }
        return collectAndFlattenUserEExpressionArgs(isTopLevel, macro, signature, presenceBitmap);
    }

    /**
     * Materializes the expressions that compose the macro invocation on which the reader is positioned and feeds
     * them to the macro evaluator.
     */
    public void beginEvaluatingMacroInvocation(LazyMacroEvaluator macroEvaluator) {
        reader.pinBytesInCurrentValue();
        SymbolToken fieldName = null;
        if (reader.isInStruct()) {
            // TODO avoid having to create SymbolToken every time
            fieldName = reader.getFieldNameSymbol();
        }
        macroEvaluator.initExpansion(fieldName, collectEExpressionArgs(true));
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
        SymbolToken fieldName = null;
        if (reader.isInStruct()) {
            fieldName = reader.getFieldNameSymbol();
            verbatimExpressionTape.add(fieldName, ExpressionType.FIELD_NAME_ORDINAL, -1, -1);
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
        collectVerbatimEExpressionArgs(macro, signature, presenceBitmap);
        reader.sliceAfterMacroInvocationHeader(start, end, typeID, macroAddress, isSystemMacro); // Rewind, prepare to read again.
        verbatimTranscode = false;
        expressionTape = expressionTapeOrdered;
        macroEvaluator.initExpansion(fieldName, collectEExpressionArgs(true));
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
