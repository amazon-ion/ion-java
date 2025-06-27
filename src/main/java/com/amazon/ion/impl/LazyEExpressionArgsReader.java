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
    protected ExpressionTape expressionTapeOrdered;

    // Reusable scratch tape for storing out-of-order expression arguments.
    private final ExpressionTape expressionTapeScratch;

    //private final ExpressionTape expressionTapeUncached;

    private ExpressionTape verbatimExpressionTape = null;

    // Pool for presence bitmap instances.
    protected final PresenceBitmap.Companion.PooledFactory presenceBitmapPool = new PresenceBitmap.Companion.PooledFactory();

    // Whether to produce a tape that preserves the invocation verbatim.
    private boolean verbatimTranscode = false;

    private Marker[] markerPool = new Marker[256];
    private int markerPoolIndex = -1;

    private int eExpressionStart = -1;
    private int eExpressionEnd = -1;

    private int numberOfEExpressionsBeforeInjection = -1;
    private int numberOfEExpressionsAfterInjection = -1;

    // TODO might want to integrate this invocation cache into the encoding context so only a single lookup is needed
    // TODO figure out when/how to purge the cache. When the encoding context is reset. Maybe also limit size.
    private final Map<MacroCacheKey, ExpressionTape> macroInvocationTapeCache = new HashMap<>(16);
    private final MacroCacheKey testKey = new MacroCacheKey();

    private boolean markVariables = false;

    private static class MacroCacheKey {
        Macro macro;
        PresenceBitmap presenceBits;

        MacroCacheKey copy() {
            MacroCacheKey copy = new MacroCacheKey();
            copy.macro = this.macro;
            copy.presenceBits = this.presenceBits;
            return copy;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof MacroCacheKey)) return false;
            MacroCacheKey otherKey = (MacroCacheKey) other;
            return otherKey.macro == this.macro
                && otherKey.presenceBits.getByteSize() == this.presenceBits.getByteSize()
                && otherKey.presenceBits.getFirst64PresenceBits() == this.presenceBits.getFirst64PresenceBits();
        }

        @Override
        public int hashCode() {
            return 27 + (int) (System.identityHashCode(macro) + (17 * presenceBits.getByteSize()) + (13 * presenceBits.getFirst64PresenceBits()));
        }
    }

    /**
     * Constructor.
     * @param reader the {@link ReaderAdapter} from which to read {@link Expression}s.
     * @see ReaderAdapterIonReader
     * @see ReaderAdapterContinuable
     */
    LazyEExpressionArgsReader(IonReaderContinuableCoreBinary reader) {
        this.reader = reader;
        //expressionTapeUncached = new ExpressionTape(reader, 256);
        expressionTapeOrdered = new ExpressionTape(reader, 256);
        expressionTapeScratch = new ExpressionTape(reader, 64);
        expressionTape = expressionTapeOrdered;
        this.reader.setLeftShiftHandler(shiftAmount -> {
            expressionTapeOrdered.shiftIndicesLeft(shiftAmount);
            expressionTapeScratch.shiftIndicesLeft(shiftAmount);
            if (eExpressionStart > 0) {
                eExpressionStart -= shiftAmount;
                if (eExpressionEnd > 0) {
                    eExpressionEnd -= shiftAmount;
                }
            }
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
            Macro macro = loadMacro();
            stepIntoEExpression();
            PresenceBitmap presenceBitmap = loadPresenceBitmapIfNecessary(macro.getSignature());
            collectEExpressionArgs(macro, presenceBitmap, false, fieldName); // TODO avoid recursion
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

    private int collectAndFlattenComplexEExpression(boolean isTopLevel, ExpressionTape.Core macroBodyTape, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap, int markerPoolStartIndex) {
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
                    if (markVariables && isTopLevel) { // TODO try removing. Experiment with adding did nothing (same for other occurrences)
                        expressionTape.markVariableStart(targetVariableOrdinal); // TODO could consider moving this calculation to after the tape is known to be reused
                    }
                    int startIndexInTape = expressionTape.currentIndex(); // TODO if this is injected, this needs to be i
                    readParameter(
                        signature.get(invocationOrdinal),
                        presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(invocationOrdinal),
                        childFieldName
                    );
                    // TODO reserve some tombstone space after the parameter as a % of the parameter size to accommodate a
                    //  future larger parameter for this spot that can be overwritten in-place.
                    Marker marker = getMarker(markerPoolStartIndex + invocationOrdinal);
                    marker.typeId = null;
                    marker.startIndex = startIndexInTape;
                    marker.endIndex = expressionTape.currentIndex(); // TODO if this is injected, this needs to be i
                    break;
                } else if (invocationOrdinal < targetVariableOrdinal) {
                    // The variable for this ordinal cannot have been read yet.
                    int scratchStartIndex = expressionTapeScratch.size();
                    expressionTape = expressionTapeScratch;
                    readParameter(
                        signature.get(invocationOrdinal),
                        presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(invocationOrdinal),
                        // Note: currently there is not a good way of knowing what the field name for this argument
                        // will be. If this can be cheaply calculated then it could be provided here to enable eliding
                        // of None when the field name is not null, a small optimization. If this is done, the field
                        // name does not need to be set in the next branch.
                        null
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
                    if (markVariables && isTopLevel) {
                        expressionTape.markVariableStart(targetVariableOrdinal); // TODO could consider moving this calculation to after the tape is known to be reused
                    }
                    // The argument for this variable has already been read. Copy it from the tape.
                    int startIndex = expressionTape.currentIndex(); //expressionTape.size();
                    expressionTape.copyFromRange(source.core(), (int) marker.startIndex, (int) marker.endIndex);
                    String childFieldName = macroBodyTape.fieldNameForVariable(i);
                    expressionTape.setFieldNameAt(startIndex, childFieldName);
                    // TODO reserve some tombstone space after the parameter as a % of the parameter size to accommodate a
                    //  future larger parameter for this spot that can be overwritten in-place.
                    break;
                }
                invocationOrdinal++;
            }
        }
        markerPoolIndex = markerPoolStartIndex;
        return macroBodyTapeIndex;
    }

    private int collectAndFlattenSimpleEExpression(boolean isTopLevel, ExpressionTape.Core macroBodyTape, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap) {
        // TODO drop expression groups? No longer needed after the variables are resolved. Further simplifies the evaluator. Might miss validation? Also, would need to distribute field names over the elements of the group.
        int macroBodyTapeIndex = 0;
        int numberOfVariables = macroBodyTape.getNumberOfVariables();
        for (int i = 0; i < numberOfVariables; i++) {
            // Copy everything up to the next variable.
            macroBodyTapeIndex = macroBodyTape.copyToVariable(macroBodyTapeIndex, i, expressionTape);
            String childFieldName = macroBodyTape.fieldNameForVariable(i);
            if (markVariables && isTopLevel) {
                expressionTape.markVariableStart(expressionTape.core().getNumberOfVariables()); // TODO could consider moving this calculation to after the tape is known to be reused
            }
            readParameter(
                signature.get(i),
                presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(i),
                childFieldName
            );
            // TODO reserve some tombstone space after the parameter as a % of the parameter size to accommodate a
            //  future larger parameter for this spot that can be overwritten in-place.
        }
        return macroBodyTapeIndex;
    }

    private ExpressionTape collectAndFlattenUserEExpressionArgs(boolean isTopLevel, Macro macro, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap, String fieldName) {
        ExpressionTape.Core macroBodyTape = macro.getBodyTape();
        // TODO avoid eagerly stepping through prefixed expressions
        int numberOfParameters = signature.size();
        int macroBodyTapeIndex = 0;
        int startIndex = expressionTape.currentIndex(); //expressionTape.size();
        if (numberOfParameters == 0) {
            if (isTopLevel) {
                // This avoids copying and reuses the macro body tape core as-is.
                ExpressionTape constantTape = new ExpressionTape(null, macroBodyTape); // TODO pool?
                stepOutOfEExpression();
                return constantTape;
            }
        } else if (macro.isSimple()) {
            macroBodyTapeIndex = collectAndFlattenSimpleEExpression(isTopLevel, macroBodyTape, signature, presenceBitmap);
        } else {
            macroBodyTapeIndex = collectAndFlattenComplexEExpression(isTopLevel, macroBodyTape, signature, presenceBitmap, markerPoolIndex + 1);
        }
        // Copy everything after the last parameter.
        expressionTape.copyFromRange(macroBodyTape, macroBodyTapeIndex, macroBodyTape.size());
        if (expressionTape.currentIndex() > startIndex) { // TODO this might not be correct or necessary
            expressionTape.setFieldNameAt(startIndex, fieldName);
        }
        stepOutOfEExpression();
        return expressionTape;
    }

    private void collectVerbatimEExpressionArgs(Macro macro, List<Macro.Parameter> signature, PresenceBitmap presenceBitmap, boolean isTopLevel, String fieldName) {
        // TODO avoid eagerly stepping through prefixed expressions
        expressionTape.add(macro, ExpressionType.E_EXPRESSION_ORDINAL, -1, -1, fieldName);
        int numberOfParameters = signature.size();
        for (int i = 0; i < numberOfParameters; i++) {
            if (markVariables && isTopLevel) {
                expressionTape.markVariableStart(expressionTape.core().getNumberOfVariables()); // TODO could consider moving this calculation to after the tape is known to be reused
            }
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
    private ExpressionTape collectEExpressionArgs(Macro macro, PresenceBitmap presenceBitmap, boolean isTopLevel, String fieldName) {
        // TODO if it's a system macro don't eagerly expand
        //Macro macro = loadMacro();
        //stepIntoEExpression();
        List<Macro.Parameter> signature = macro.getSignature();
        //PresenceBitmap presenceBitmap = loadPresenceBitmapIfNecessary(signature);
        if (verbatimTranscode) {
            collectVerbatimEExpressionArgs(macro, signature, presenceBitmap, isTopLevel, fieldName);
            return expressionTape;
        } else if ((macro instanceof SystemMacro && macro.getBodyTape().size() == 0)) {
            if (macro == SystemMacro.None) {
                // The following is a very cheap optimization that does not cover all possible cases where eliding
                // None is possible. When there is a field name, we know that the None occurs in a struct and the field
                // can simply be suppressed. The only time it cannot be suppressed is within a non-flattened system
                // macro invocation. This is good enough for now.
                // TODO tried removing to enable reuse of tape given same presence bits.
                //if (fieldName == null) { // TODO can always skip at the top level too. This is useful for Meta
                    expressionTape.add(null, ExpressionType.NONE_ORDINAL, -1, -1, fieldName);
                //}
                stepOutOfEExpression();
            } else {
                collectVerbatimEExpressionArgs(macro, signature, presenceBitmap, isTopLevel, fieldName);
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

    private boolean simplifyDefaultInvocation(ExpressionTape.Core tape, int argumentsStart, int eExpressionIndex) {
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
            return true;
        } else if (firstArgument.type == ExpressionType.EXPRESSION_GROUP_ORDINAL) {
            ExpressionTape.Element firstElementInGroup = tape.elementAt(tape.findEndOfTombstoneSequence(argumentsStart + 1));
            if (firstElementInGroup.type == ExpressionType.EXPRESSION_GROUP_END_ORDINAL) {
                // The first argument is an empty group.
                takeDefaultArgument(tape, argumentsStart, eExpressionIndex);
                return true;
            }
            // Otherwise, this is a non-empty group, but it may still resolve to None if it contains a system macro that
            // cannot be simplified.
        } else if (firstArgument.type == ExpressionType.NONE_ORDINAL) {
            // The first argument is None.
            takeDefaultArgument(tape, argumentsStart, eExpressionIndex);
            return true;
        }
        // Otherwise, the first argument must be an e-expression that could not be simplified, so it can't be
        // determined if it's None. Do not modify this Default invocation.
        return false;
    }

    private void simplifyResolvableMacroInvocations(ExpressionTape.Core tape) {
        int currentInvocationIndex = -1;
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
                if (simplifyDefaultInvocation(tape, eExpressionStart + 1, eExpressionIndex)) {
                    continue;
                }
            }
            // Otherwise, this system macro is not handled specially here; do nothing to it.
            // TODO this didn't work; might not be happening at the right point in the processing of the tape. Check
            //tape.recalculateEExpressionEnd(++currentInvocationIndex, eExpressionStart); // TODO isEExpressionStart right, or should include any beginning tombstone sequence?
        }
        //tape.setNumberOfEExpressions(currentInvocationIndex + 1);
    }

    private void undoSimplificationOfDefaultInvocation(ExpressionTape.Core tape, int startIndex, int endIndex, boolean existingIsNone, ExpressionTape.Element precedingElement) {
        ExpressionTape.Element firstArgument = tape.elementAt(startIndex);
        if (firstArgument.type == ExpressionType.NONE_ORDINAL) {
            if (existingIsNone) {
                // The first argument to Default is still None, so the branch does not change.
                return;
            }
            // The first argument is now None, but was not before. Take the second argument by tombstoning
            // the first one.
            firstArgument.type = ExpressionType.TOMBSTONE_ORDINAL;
            firstArgument.containerEnd = endIndex;
            // Un-tombstone the second argument if necessary.
            ExpressionTape.Element secondArgument = tape.elementAt(endIndex);
            if (secondArgument.type == ExpressionType.TOMBSTONE_ORDINAL) {
                // When this element was tombstoned, its type was replaced. Recalculate it based on the remaining
                // context.
                // TODO this could be avoided if tombstone status was stored separately from type.
                // TODO move this into a method, probably in ExpressionTape.
                int secondArgumentEnd;
                if (secondArgument.value != null || secondArgument.start >= 0) {
                    secondArgument.type = ExpressionType.DATA_MODEL_SCALAR_ORDINAL;
                    secondArgumentEnd = endIndex + 1;
                    secondArgument.containerEnd = -1;
                } else if (secondArgument.containerEnd > 0) {
                    secondArgument.type = ExpressionType.DATA_MODEL_CONTAINER_ORDINAL;
                    // TODO this assumes that the tombstone for the e-expression end won't have been chained with
                    //  a tombstone sequence that follows, which may not be correct. Needs a test.
                    secondArgumentEnd = secondArgument.containerEnd;
                    secondArgument.containerEnd = secondArgumentEnd - 1;
                } else if (secondArgument.context instanceof Macro) {
                    secondArgument.type = ExpressionType.E_EXPRESSION_ORDINAL;
                    secondArgumentEnd = tape.findEndOfExpression(endIndex);
                    secondArgument.containerEnd = -1;
                } else {
                    secondArgument.type = ExpressionType.EXPRESSION_GROUP_ORDINAL;
                    secondArgumentEnd = tape.findEndOfExpression(endIndex);
                    secondArgument.containerEnd = -1;
                }
                // The e-expression end marker may have been tombstoned by index as part of the sequence. Make sure it
                // is explicitly tombstoned now, with its containerEnd set properly.
                ExpressionTape.Element eExpressionEndElement = tape.elementAt(secondArgumentEnd);
                eExpressionEndElement.type = ExpressionType.TOMBSTONE_ORDINAL;
                eExpressionEndElement.containerEnd = secondArgumentEnd + 1; // TODO technically this could be chained with any tombstone sequence that follows
            }
        } else {
            if (!existingIsNone) {
                // The first argument to Default is still non-None, so the branch does not change.
                return;
            }
            // The first argument is no longer None.
            if (firstArgument.type == ExpressionType.E_EXPRESSION_ORDINAL) {
                // It cannot be cheaply determined whether the first argument is None when it is an invocation. Leave
                // it to the evaluator by un-tombstoning the Default invocation.
                // Note: it has been previously verified that the context is still SystemMacro.Default. The following
                // revives the invocation so that it can be expanded by the evaluator.
                precedingElement.type = ExpressionType.E_EXPRESSION_ORDINAL;
                precedingElement.containerEnd = -1;
                // Un-tombstone the e-expression end marker, which was previously tombstoned.
                tape.elementAt(tape.findNextTombstone(endIndex)).type = ExpressionType.E_EXPRESSION_END_ORDINAL;
            } else {
                // Take the first argument.
                // Shorten the tombstone so that it no longer includes the first argument.
                precedingElement.containerEnd = startIndex;
                // Extend the tombstone sequence from the second argument through the one that is known to start at
                // the position that previously held the e-expression end marker.
                tape.extendTombstoneFrom(endIndex);
            }
        }
    }

    private void undoSimplificationOfResolvableMacroInvocationsIfNecessary(ExpressionTape.Core tape, int startIndex, int endIndex, boolean existingIsNone) {
        if (startIndex < 1) {
            // The expression cannot be part of an e-expression.
            return;
        }
        ExpressionTape.Element precedingElement = tape.elementAt(startIndex - 1);
        if (precedingElement.type == ExpressionType.TOMBSTONE_ORDINAL) {
            if (precedingElement.context == SystemMacro.Default) {
                undoSimplificationOfDefaultInvocation(tape, startIndex, endIndex, existingIsNone, precedingElement);
            }
        }
    }

    private boolean injectArgumentsIntoTapeTemplate(ExpressionTape tape, PresenceBitmap presenceBitmap, List<Macro.Parameter> signature) {
        // TODO will this work for previously flattened nested invocations? E.g. default where the argument has a different None-ness on the second invocation?
        // Iterate through the variables in the tape, overwriting segments if an argument won't fit.
        ExpressionTape.Core core = tape.core();
        //for (int invocationArgumentOrdinal = 0; invocationArgumentOrdinal < core.getNumberOfVariables(); invocationArgumentOrdinal++) {
        for (int variableOrdinal = 0; variableOrdinal < signature.size(); variableOrdinal++) {
            //int variableOrdinal = core.getVariableOrdinal(invocationArgumentOrdinal);
            int[] variableStarts = core.getVariableStartIndices(variableOrdinal);
            int firstUsageStart = -1;
            int firstUsageEnd = -1;
            for (int variableUsageCount = 0; variableUsageCount <= core.getNumberOfDuplicateUsages(variableOrdinal); variableUsageCount++) {
                int variableStart = variableStarts[variableUsageCount];
                // TODO the following is fairly expensive. Can cache variable expression ends if necessary.

                // TODO note: seeking to the end of the tombstone sequence allows us to reuse space that was
                //  used by a previous invocation, but not the most recent invocation. But what if the next argument
                //  begins with a tombstone? This is ambiguous. Need to differentiate between the two cases.
                int limit = core.findEndOfTombstoneSequence(core.findEndOfExpression(variableStart));
                tape.setInsertionLimit(limit);
                // Position core at variableStart before copying.
                tape.prepareToOverwriteAt(variableStart);
                boolean existingIsNone = core.elementAt(variableStart).type == ExpressionType.NONE_ORDINAL;
                if (variableUsageCount == 0) {
                    // This is the first usage. Read directly into the location.
                    firstUsageStart = variableStart;
                    readParameter(signature.get(variableOrdinal), presenceBitmap.get(variableOrdinal), tape.fieldName());
                    firstUsageEnd = tape.currentIndex();
                } else {
                    // This is a duplicate usage. Copy from the first location to the next location.
                    //int availableSize = limit - variableStart;
                    //int copySize = firstUsageEnd - firstUsageStart;
                    String fieldName = tape.fieldName();
                    tape.copyFromRange(core, firstUsageStart, firstUsageEnd);
                    // The copy will overwrite the field name with the one at the copied location, but the field name
                    // at the destination is likely different.
                    tape.setFieldNameAt(variableStart, fieldName);
                }
                tape.clearInsertionLimit();
                if (tape.checkAndClearOverflow()) {
                    // There was not enough space for this argument in the existing invocation tape.
                    stepOutOfEExpression();
                    return false;
                }
                // The limit is recalculated since shift may have happened.
                int actualEnd = tape.currentIndex();
                // Note: if shift was required, any excess was marked with tombstones.
                //limit = core.findEndOfTombstoneSequence(actualEnd);
                if (actualEnd < limit) {
                    // Less space was used than was available
                    core.setTombstoneAt(actualEnd, limit);
                }
                undoSimplificationOfResolvableMacroInvocationsIfNecessary(core, variableStart, actualEnd, existingIsNone);
            }
        }
        stepOutOfEExpression();
        // TODO see if this can work and if it has performance benefits
        //simplifyResolvableMacroInvocations(core);
        return true;
    }

    private ExpressionTape createAndCacheNewInvocationTape(PresenceBitmap presenceBitmap, Macro macro, String fieldName) {
        markVariables = true;
        ExpressionTape tape = collectEExpressionArgs(macro, presenceBitmap, true, fieldName);
        markVariables = false;
        simplifyResolvableMacroInvocations(tape.core());
        macroInvocationTapeCache.put(testKey.copy(), tape);
        // The following will be false in the case of a constant macro, for which a new tape will have already been
        // created.
        /*
        if (tape == expressionTape) {
            expressionTape = tape.blankCopy();
            expressionTapeOrdered = expressionTape;
        }

         */
        return tape;
    }

    /*
    private ExpressionTape attemptToReuseCachedInvocationTape(ExpressionTape tape, PresenceBitmap presenceBitmap, Macro macro, String fieldName) {
        // Note: only attempting to resolve from the cache at the top level avoids having to handle the case where
        // a macro is provided an invocation of itself as an argument, and reduces the amount of cache-checking,
        // which is fairly expensive.
        ExpressionTape savedExpressionTape = expressionTape;
        expressionTape = tape;
        expressionTapeOrdered = tape;
        int start = (int) reader.valueMarker.startIndex;
        int end = (int) reader.valueMarker.endIndex;
        IonTypeID typeID = reader.valueTid;
        long macroAddress = reader.getMacroInvocationId();
        boolean isSystemMacro = reader.isSystemInvocation();
        // TODO optimization: record how far the following operation made it if it wasn't successful using
        //  checkpointing; only re-read from the checkpoint
        boolean success = injectArgumentsIntoTapeTemplate(tape, presenceBitmap, macro.getSignature());
        expressionTape = savedExpressionTape;
        expressionTapeOrdered = savedExpressionTape;
        if (!success) {
            reader.sliceAfterMacroInvocationHeader(start, end, typeID, macroAddress, isSystemMacro); // Rewind, prepare to read again.
            stepIntoEExpression();
            loadPresenceBitmapIfNecessary(macro.getSignature());
            tape = createAndCacheNewInvocationTape(presenceBitmap, macro, fieldName);
        }
        tape.rewindTo(0); // TODO necessary?
        return tape;
    }

     */

    // TODO remove all these before merge. They are for tracking hit rate during development.qq
    public static int totalNumberOfInvocations = 0;
    public static int numberOfSuccessfulCachedInvocations = 0;
    public static int numberOfUnsuccessfulCachedInvocations = 0;
    public static int numberOfCacheMisses = 0;

    /**
     * Materializes the expressions that compose the macro invocation on which the reader is positioned and feeds
     * them to the macro evaluator.
     */
    public void beginEvaluatingMacroInvocation(LazyMacroEvaluator macroEvaluator) {
        reader.pinBytesInCurrentValue();
        String fieldName = null;
        if (reader.isInStruct()) {
            fieldName = reader.getFieldName();
        }
        eExpressionStart = (int) reader.valueMarker.startIndex;
        eExpressionEnd = (int) reader.valueMarker.endIndex;
        IonTypeID typeID = reader.valueTid;
        long macroAddress = reader.getMacroInvocationId();
        boolean isSystemMacro = reader.isSystemInvocation();
        Macro macro = loadMacro();
        stepIntoEExpression();
        PresenceBitmap presenceBitmap = loadPresenceBitmapIfNecessary(macro.getSignature());
        testKey.macro = macro;
        testKey.presenceBits = presenceBitmap; // TODO presence bitmap might not be the right key, unless changes in noneness can be handled.
        ExpressionTape tape = macroInvocationTapeCache.get(testKey);
        totalNumberOfInvocations++;
        if (tape == null) {
            expressionTape = new ExpressionTape(reader, 256);
            expressionTapeOrdered = expressionTape;
            tape = createAndCacheNewInvocationTape(presenceBitmap, macro, fieldName);
            numberOfCacheMisses++;
        } else {
            // Note: only attempting to resolve from the cache at the top level avoids having to handle the case where
            // a macro is provided an invocation of itself as an argument, and reduces the amount of cache-checking,
            // which is fairly expensive.
            expressionTape = tape;
            expressionTapeOrdered = tape;
            int numberOfEExpressions = tape.core().getNumberOfEExpressions();
            // Restore the number of invocations in the cached tape before the injection. More may be added if
            // injected arguments are e-expressions. Dropping these prevents the cached tape's e-expression index
            // caches from growing infinitely.
            if (injectArgumentsIntoTapeTemplate(tape, presenceBitmap, macro.getSignature())) {
                // TODO the following line results in 1/10 the allocation rate compared to the same line of code in
                //  finishEvaluating...(), but causes groupTestMulti to fail. How can the memory reduction be achieved
                //  correctly?
                //tape.core().truncateEExpressionCaches(numberOfEExpressions, tape.core().getNumberOfEExpressions());
                numberOfEExpressionsBeforeInjection = numberOfEExpressions;
                numberOfEExpressionsAfterInjection = tape.core().getNumberOfEExpressions();
                numberOfSuccessfulCachedInvocations++;
            } else {
                // TODO the effectiveness of the cache is limited if any required growth causes !success and a reparse.
                //  Could also thrash if it's a different argument each time that causes the growth. If they switch
                //  off, then the newly cached invocation tape isn't sufficient for the next invocation. Consider
                //  not caching the redone tape? Or don't cache until seeing X invocations, and keep space for the
                //  maximum size of each argument seen during the learning phase, then never re-cache on bail out. Or,
                //  evict and re-learn every X uses.
                reader.sliceAfterMacroInvocationHeader(eExpressionStart, eExpressionEnd, typeID, macroAddress, isSystemMacro); // Rewind, prepare to read again.
                stepIntoEExpression();
                reader.fillArgumentEncodingBitmap(presenceBitmap.getByteSize()); // TODO this could be even simpler -- just seek forward to valueMarker.endIndex
                tape.prepareToOverwrite();
                markVariables = true;
                tape = collectEExpressionArgs(macro, presenceBitmap, true, fieldName);
                markVariables = false;
                // TODO: the following has no observed performance benefit, but it is required for correctness. This
                //  indicates a defect in the evaluator.
                simplifyResolvableMacroInvocations(tape.core());
                numberOfUnsuccessfulCachedInvocations++;
            }
            // TODO re-simplify resolvable macro invocations. Need to modify simplifyResolvableMacroInvocations(tape.core());
            //  so that it can handle the case where it was previously simplified (and there is no longer a macro at one of the locations)
            //  experiment with and without this optimization.
            tape.rewindTo(0); // TODO necessary?
        }
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
        eExpressionStart = (int) reader.valueMarker.startIndex;
        eExpressionEnd = (int) reader.valueMarker.endIndex;
        IonTypeID typeID = reader.valueTid;
        long macroAddress = reader.getMacroInvocationId();
        boolean isSystemMacro = reader.isSystemInvocation();
        Macro macro = loadMacro();
        stepIntoEExpression();
        List<Macro.Parameter> signature = macro.getSignature();
        PresenceBitmap presenceBitmap = loadPresenceBitmapIfNecessary(signature);
        collectVerbatimEExpressionArgs(macro, signature, presenceBitmap, true, fieldName);
        reader.sliceAfterMacroInvocationHeader(eExpressionStart, eExpressionEnd, typeID, macroAddress, isSystemMacro); // Rewind, prepare to read again.
        stepIntoEExpression();
        reader.fillArgumentEncodingBitmap(presenceBitmap.getByteSize());
        verbatimTranscode = false;
        expressionTape = expressionTapeOrdered;
        macroEvaluator.initExpansion(fieldName, collectEExpressionArgs(macro, presenceBitmap, true, fieldName));
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
        //expressionTapeOrdered.clear();
        expressionTapeScratch.clear();
        //expressionTape = expressionTapeOrdered;
        verbatimTranscode = false;
        markerPoolIndex = -1;
        eExpressionStart = -1;
        eExpressionEnd = -1;
        if (numberOfEExpressionsBeforeInjection >= 0) {
            expressionTape.core().truncateEExpressionCaches(numberOfEExpressionsAfterInjection, numberOfEExpressionsBeforeInjection);
            numberOfEExpressionsBeforeInjection = -1;
            numberOfEExpressionsAfterInjection = -1;
        }
    }
}
