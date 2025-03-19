// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl.bin.PresenceBitmap;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.LazyMacroEvaluator;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroEvaluator;
import com.amazon.ion.impl.macro.ReaderAdapter;
import com.amazon.ion.impl.macro.ReaderAdapterContinuable;
import com.amazon.ion.impl.macro.ReaderAdapterIonReader;

import java.util.ArrayList;
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

    // Reusable tape for recording value boundaries for lazy parsing.
    protected final ExpressionTape expressionTape;

    // Pool for presence bitmap instances.
    protected final PresenceBitmap.Companion.PooledFactory presenceBitmapPool = new PresenceBitmap.Companion.PooledFactory();

    /**
     * Constructor.
     * @param reader the {@link ReaderAdapter} from which to read {@link Expression}s.
     * @see ReaderAdapterIonReader
     * @see ReaderAdapterContinuable
     */
    LazyEExpressionArgsReader(IonReaderContinuableCoreBinary reader) {
        this.reader = reader;
        expressionTape = new ExpressionTape(reader, 256);
        this.reader.setLeftShiftHandler(expressionTape::shiftIndicesLeft);
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
        expressionTape.add(reader.valueTid, ExpressionType.DATA_MODEL_SCALAR, (int) reader.valueMarker.startIndex, (int) reader.valueMarker.endIndex);
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
            expressionTape.add(null, ExpressionType.EXPRESSION_GROUP, -1, -1);
        } else {
            expressionTape.add(reader.valueTid, ExpressionType.DATA_MODEL_CONTAINER, -1, -1);
        }
        // TODO if the container is prefixed, don't recursively step through it
        stepInRaw();
        while (nextRaw()) {
            if (type == IonType.STRUCT) {
                // TODO avoid having to create SymbolToken every time
                expressionTape.add(reader.getFieldNameSymbol(), ExpressionType.FIELD_NAME, -1, -1);
            }
            readValueAsExpression(false); // TODO avoid recursion
        }
        if (isExpressionGroup) {
            expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_END, -1, -1);
        } else {
            expressionTape.add(null, ExpressionType.DATA_MODEL_CONTAINER_END, -1, -1);
        }
        stepOutRaw();
    }

    /**
     * Reads the rest of the stream into a single expression group.
     */
    private void readStreamAsExpressionGroup() {
        expressionTape.add(null, ExpressionType.EXPRESSION_GROUP, -1, -1);
        do {
            readValueAsExpression(false); // TODO avoid recursion
        } while (nextRaw());
        expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_END, -1, -1);
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
            collectEExpressionArgs(); // TODO avoid recursion
            return;
        }
        IonType type = reader.getEncodingType();
        if (reader.hasAnnotations()) {
            List<SymbolToken> annotations = getAnnotations(); // TODO make this lazy too
            expressionTape.add(annotations, ExpressionType.ANNOTATION, -1, -1);
        }
        if (IonType.isContainer(type) && !reader.isNullValue()) {
            readContainerValueAsExpression(type);
        } else {
            readScalarValueAsExpression();
        }
    }

    /**
     * Collects the expressions that compose the current macro invocation.
     */
    private void collectEExpressionArgs() {
        Macro macro = loadMacro();
        stepIntoEExpression();
        List<Macro.Parameter> signature = macro.getSignature();
        PresenceBitmap presenceBitmap = loadPresenceBitmapIfNecessary(signature);
        // TODO avoid eagerly stepping through prefixed expressions
        expressionTape.add(macro, ExpressionType.E_EXPRESSION, -1, -1);
        int numberOfParameters = signature.size();
        for (int i = 0; i < numberOfParameters; i++) {
            readParameter(
                signature.get(i),
                presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(i),
                i == (numberOfParameters - 1)
            );
        }
        stepOutOfEExpression();
        expressionTape.add(null, ExpressionType.E_EXPRESSION_END, -1, -1);
    }

    // TODO step 1: modify this args reader to produce some sort of "tape" of non-materialized expressions (offsets/types)
    //  Right before calling initExpansion, materialize the tape into a list of expressions. This proves it can be done
    // TODO step 2: modify MacroEvaluator to operate on the "tape" instead of the materialized expressions. Remove
    //  the materialization

    /**
     * Materializes the expressions that compose the macro invocation on which the reader is positioned and feeds
     * them to the macro evaluator.
     */
    public void beginEvaluatingMacroInvocation(LazyMacroEvaluator macroEvaluator) {
        reader.pinBytesInCurrentValue();
        if (reader.isInStruct()) {
            // TODO avoid having to create SymbolToken every time
            expressionTape.add(reader.getFieldNameSymbol(), ExpressionType.FIELD_NAME, -1, -1);
        }
        collectEExpressionArgs();
        macroEvaluator.initExpansion(expressionTape);
    }

    /**
     * Finishes evaluating the current macro invocation, resetting any associated state.
     */
    public void finishEvaluatingMacroInvocation() {
        presenceBitmapPool.clear();
        expressionTape.clear();
        reader.unpinBytes();
        reader.returnToCheckpoint();
        expressionTape.clear();
    }
}
